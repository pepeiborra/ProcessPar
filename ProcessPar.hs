{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE RankNTypes          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StaticPointers      #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeOperators       #-}
-- | Run computations out of process.
--   While this is usually unnecessary while writing Haskell programs, it can be useful when working with the FFI.
--   It relies on the 'distributed-closure' package to serialize closures to slave processes.
--   Note that most of the API calls are blocking, so you will want to pair this library with something like 'async'.
--   Example:
--
-- > purefib :: Int -> IO Int
-- > purefib = return . fib
-- >
-- > main = withPar 4 $ \par -> do
-- >   args <- map read <$> getArgs
-- >   res <- forConcurrently args $ \n ->
-- >     try @SomeException $ runPar par (static Dict) (static purefib `cap` cpure (static Dict) n)
-- >   print res
module ProcessPar
  ( Par
  , withPar
  , viewParStats
  , runPar
  -- * Reexports from 'Control.Distributed.Closure'
  , Closure
  , Dict(..)
  , closure
  , cpure
  , cap
  )
where

import           Control.Concurrent.STM
import           Control.Distributed.Closure
import           Control.Exception
import           Control.Monad
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Cont
import           Data.Bifunctor
import           Data.Binary
import qualified Data.ByteString.Char8       as B
import qualified Data.ByteString.Lazy.Char8  as L
import           Data.Functor
import           Data.Functor.Compose
import           Data.Int
import           Data.IORef
import           Data.List
import           Data.Typeable
import           Debug.Trace
import           Foreign.C.Types
import           GHC.IO.FD                   (FD (..))
import           Numeric.Natural
import           System.Environment
import           System.IO                   as IO
import           System.Posix.Process
import           System.Posix.Types
import qualified System.Process              as P
import           System.Process.Typed

-- | A handle over a pool of local slave processes
data Par = Par
  { -- | Runs a computation in a slave process. Blocks until the result is available.
    runPar :: forall a . Closure (Dict (Serializable a)) -> Closure (IO a) -> IO a,
    -- | Returns a snapshot of the pool status
    viewParStats :: IO ParStats
  }

data ParStats = ParStats
  { scheduledJobs :: Natural -- ^ Number of jobs in the pool
  , poolSize      :: Natural -- ^ Initial pool size
  , busy          :: Natural -- ^ Number of busy slaves
  , idle          :: Natural -- ^ number of idle slaves
  }
  deriving Show

newtype RResolver = RResolver
  { runRResolver :: forall a . Closure (Dict (Serializable a)) -> Closure (IO a) -> IO (RResolver, Either String a)
  }

-- | Starts a fixed number of slave processes and calls the continuation with a 'Par' handle.
--   The handle is only valid in the scope of the continuation
--   and the processes will be deallocated immediately after.
withPar :: Natural -> (Par -> IO res) -> IO res
withPar poolSize k = do
  runners <- forM [1 .. poolSize] $ \n -> do
    (hInr , hInw ) <- P.createPipe
    (hOutr, hOutw) <- P.createPipe
    forkProcess $ do
      inp <- L.hGetContents hInr
      setupParServer n inp hOutw
    res <- L.hGetContents hOutr
    return $ runSlave hInw res
  par <- makePar runners
  k par

makePar :: [RResolver] -> IO Par
makePar runners = do
  runnersT  <- newTVarIO runners
  busyRef   <- newIORef 0
  queuedRef <- newIORef 0
  putDebugLn "[master] All slaves spawned"
  let
    runPar dict input = do
      atomicModifyIORef' queuedRef $ \n -> (n + 1, ())
      runner <- atomically $ do
        pp <- readTVar runnersT
        case pp of
          []     -> retry
          p : pp -> writeTVar runnersT pp $> p
      atomicModifyIORef' queuedRef $ \n -> (n - 1, ())
      atomicModifyIORef' busyRef $ \n -> (n + 1, ())
      (runner', res) <-
        runRResolver runner dict input
          `finally` atomicModifyIORef' busyRef (\n -> (n - 1, ()))
      atomically $ modifyTVar runnersT (runner' :)
      either fail return res
    viewParStats = do
      scheduledJobs <- readIORef queuedRef
      idle          <- genericLength <$> readTVarIO runnersT
      busy          <- readIORef busyRef
      return ParStats { .. }
  return Par { .. }
  where !poolSize = genericLength runners

runSlave :: Handle -> L.ByteString -> RResolver
runSlave sendH lazyOutput = RResolver $ \dict input -> do
  putDebugLn "[master] Sending computation"
  case unclosure dict of
    Dict -> do
      let comp = static reply `cap` dict `cap` input
      L.hPutStr sendH (encode comp)
      hFlush sendH
      -- NOTE Error handling
      --  If the slave dies, lazyOutput will not block and instead finish early, which will
      --  cause decode to fail with the error "not enough bytes"
      case decodeOrFail lazyOutput of
        Right (rest, _, res) -> return (runSlave sendH rest, res >>= decode)
        Left  (_   , _, err) -> fail err
 where
  reply :: Dict (Serializable a) -> IO a -> IO L.ByteString
  reply Dict action = do
    -- NOTE Error handling
    --  We encode twice to guard against lazy exceptions
    bytes <- try @SomeException $ do
      res <- try @SomeException action
      evaluate $ encode $ first show res
    return (encode $ first show bytes)

--------------------------------------------------------
-- parMain

setupParServer :: Show id => id -> L.ByteString -> Handle -> IO ()
setupParServer n inp hOut = do
  let sayLoud msg = putDebugLn $ "[slave " ++ show n ++ "] " ++ msg
      loopServer inp = case decodeOrFail inp of
        Left  (_   , _, err ) -> fail err
        Right (rest, _, task) -> do
          sayLoud "Received task"
          res <- unclosure task
          L.hPutStr hOut res
          hFlush hOut
          loopServer rest
  sayLoud "Starting"
  loopServer inp

---------------------------------
-- Debugging

debug :: Bool
debug = False

putDebugLn :: String -> IO ()
putDebugLn = if debug then IO.hPutStrLn stderr else const $ return ()
