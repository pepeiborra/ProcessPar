{-# LANGUAGE StaticPointers #-}
import qualified Data.ByteString.Lazy.Char8 as L
import ProcessPar
import System.IO

putErr = L.hPutStrLn stderr

main = withPar 1 $ \par -> do
  inp <- L.getContents
  let loop [] = pure ()
      loop (x:xx) = do
        L.putStrLn x
        runPar par (static Dict) $ static putErr `cap` cpure (static Dict) x
        loop xx
  loop $ L.lines inp
