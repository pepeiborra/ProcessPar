{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StaticPointers #-}
-- | Run computations out of process.
--   While this is usually unnecessary while writing Haskell programs, it can be useful when working with the FFI.
--   It relies on the 'distributed-closure' package to serialize closures to slave processes.
--   Note that most of the API calls are blocking, so you will want to pair this library with something like 'async'.
--   Example:
--
-- > purefib :: Int -> IO Int
-- > purefib = return . fib
-- >
-- > main = parMain $ withPar 4 $ \par -> do
-- >   args <- map read <$> getArgs
-- >   res <- forConcurrently args $ \n ->
-- >     try @SomeException $ runPar par (static Dict) (static purefib `cap` cpure (static Dict) n)
-- >   print res
module ProcessPar
  ( Par
  , withPar
  , viewParStats
  , runPar
  , parMain
  -- * Reexports from 'Control.Distributed.Closure'
  , Closure
  , Dict(..)
  , closure
  , cpure
  , cap
  ) where

import Control.Concurrent.STM
import Control.Distributed.Closure
import Control.Exception
import Control.Monad
import Control.Monad.Trans.Class
import Control.Monad.Trans.Cont
import Data.Bifunctor
import Data.Binary
import Data.Int
import Data.List
import qualified Data.ByteString.Char8 as B
import qualified Data.ByteString.Lazy.Char8 as L
import Data.Functor
import Data.Functor.Compose
import Data.IORef
import Data.Typeable
import Foreign.C.Types
import GHC.IO.FD (FD(..))
import Numeric.Natural
import System.Environment
import System.IO as IO
import System.Posix.Types
import System.Process.Internals (createPipeFd, fdToHandle)
import System.Process.Typed
import Debug.Trace

-- | A handle over a pool of local slave processes
data Par = Par
  { -- | Runs a computation in a slave process. Blocks until the result is available.
    runPar :: forall a . Closure (Dict (Serializable a)) -> Closure (IO a) -> IO a,
    -- | Returns a snapshot of the pool status
    viewParStats :: IO ParStats
  }

data ParStats = ParStats
  { scheduledJobs :: Natural -- ^ Number of jobs in the pool
  , poolSize :: Natural -- ^ Initial pool size
  , busy     :: Natural -- ^ Number of busy slaves
  , idle     :: Natural -- ^ number of idle slaves
  }
  deriving Show

-- | Starts a fixed number of slave processes and calls the continuation with a 'Par' handle.
--   The handle is only valid in the scope of the continuation
--   and the processes will be deallocated immediately after.
withPar :: Natural -> (Par -> IO res) -> IO res
withPar poolSize k = flip runContT return $ do
  exe <- lift getExecutablePath
  let slaveProc n = setStdout createPipe $ setStdin createPipe $ proc exe [slaveToken, show n]
  pp <- forM [1 .. fromIntegral poolSize] $ ContT . withProcess . slaveProc
  lift $ do
    runners <- forM pp $ \p -> do
      putDebugLn "[master] Spawning slave"
      let sendH = getStdin p
      hSetBuffering sendH NoBuffering
      hSetBuffering (getStdout p) NoBuffering
      outp <- L.hGetContents (getStdout p)
      return $ runSlave sendH outp
    runnersT  <- newTVarIO runners
    busyRef   <- newIORef 0
    queuedRef <- newIORef 0
    putDebugLn "[master] All slaves spawned"
    let runPar dict input = do
          atomicModifyIORef' queuedRef $ \n -> (n+1,())
          runner <- atomically $ do
            pp <- readTVar runnersT
            case pp of
              [] -> retry
              p : pp -> writeTVar runnersT pp $> p
          atomicModifyIORef' queuedRef $ \n -> (n-1,())
          atomicModifyIORef' busyRef $ \n -> (n+1,())
          (runner', res) <- runRResolver runner dict input `finally` atomicModifyIORef' busyRef (\n -> (n-1,()))
          atomically $ modifyTVar runnersT (runner' :)
          either fail return res
        viewParStats = do
          scheduledJobs <- readIORef queuedRef
          idle <- genericLength <$> readTVarIO runnersT
          busy <- readIORef busyRef
          return ParStats{..}
    k Par {..}
  where
    runSlave :: Handle -> L.ByteString -> RResolver
    runSlave sendH lazyOutput = RResolver $ \dict input -> do
      putDebugLn "[master] Sending computation"
      case unclosure dict of
        Dict -> do
          let comp :: Closure (IO ()) = static reply `cap` dict `cap` input
          L.hPutStr sendH (encode comp)
          -- NOTE Error handling
          --  If the slave dies, lazyOutput will not block and instead finish early, which will
          --  cause decode to fail with the error "not enough bytes"
          case decodeOrFail lazyOutput of
            Right (rest, _, res) -> return (runSlave sendH rest, res >>= decode)
            Left (_, _, err) -> fail err

    reply :: Dict (Serializable a) -> IO a -> IO ()
    reply Dict action = do
      -- NOTE Error handling
      --  We encode twice to guard against lazy exceptions
      bytes <- try @SomeException $ do
        res <- try @SomeException action
        evaluate $ encode $ first show res
      L.putStr (encode $ first show bytes)

newtype RResolver = RResolver
  { runRResolver :: forall a . Closure (Dict (Serializable a)) -> Closure (IO a) -> IO (RResolver, Either String a)
  }

slaveToken = "PROCESS_PAR_SLAVE"

--------------------------------------------------------
-- parMain

-- | Wrapper for the 'main' function. Call it like:
--
-- > main = parMain $ do
-- >     ..
parMain :: IO () -> IO ()
parMain realMain = do
  args <- getArgs
  case args of
    [x, n] | x == slaveToken -> setupParServer (read n)
    _ -> realMain
  where
    setupParServer :: Int -> IO ()
    setupParServer n = do
      hSetBuffering stdin NoBuffering
      hSetBuffering stdout NoBuffering
      inp <- L.getContents
      let sayLoud msg = putDebugLn $ "[slave " ++ show n ++ "] " ++ msg
          loopServer inp =
            case decodeOrFail inp of
              Left (_,_,err) -> fail err
              Right (rest,_,task) -> do
                sayLoud "Received task"
                () <- unclosure task
                loopServer rest
      sayLoud "Starting"
      loopServer inp

---------------------------------
-- Debugging

debug :: Bool
debug = False

putDebugLn :: String -> IO ()
putDebugLn = if debug then IO.hPutStrLn stderr else const $ return ()
