
module Main where

import           Control.Exception        (try)
import           Control.Monad            (forM, guard)
import           Data.Either
import           HECS.Internal
import           Prelude                  hiding (catch)
import           System.Console.Haskeline hiding (handle)
import           System.Environment
import           System.Posix

main :: IO ()
main = do
  (n:f:fs) <- getArgs
  (failed, src) <- fmap partitionEithers . forM fs
                   $(\fn -> try $ openFd fn ReadOnly Nothing defaultFileFlags)
  if null (failed :: [IOError])
    then bracket
         (openFd f WriteOnly Nothing defaultFileFlags)
         (\trg -> closeFd trg >> mapM_ closeFd src)
         (\trg -> repair (length src) (read n) src trg)
    else mapM_ closeFd src
