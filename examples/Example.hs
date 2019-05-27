{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StaticPointers #-}

import Control.Concurrent.Async
import Control.Exception
import Control.Monad.IO.Class
import ProcessPar
import System.Environment
import System.IO

fib :: Int -> Int
fib 0 = 1
fib 1 = 1
fib x | x > 40 = error "Maximum is 40"
fib n = fib (n-1) + fib (n-2)

purefib :: Int -> IO Int
purefib = return . fib

main = parMain $ withPar 4 $ \par -> do
  args <- map read <$> getArgs
  res <- forConcurrently args $ \n ->
    try @SomeException $ runPar par (static Dict) (static purefib `cap` cpure (static Dict) n)
  print res
