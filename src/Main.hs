{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE PackageImports     #-}

import qualified Data.ByteString           as B
import           Data.ByteString.Char8     (pack)
import           Data.Digest.SHA256        (hash)
import           Data.LargeWord
import           Data.Maybe                (fromJust)
import qualified Data.Serialize            as S (decode, encode)
import           Data.Word
import           Filesystem.Path.CurrentOS ((</>))
import qualified Filesystem.Path.CurrentOS as FP
import           HECFS
import           System.Console.Haskeline
import           System.Environment        (getArgs)
import qualified Text.JSON.Generic         as J

data Config = Config{ nodeSet :: [FilePath], numPrimaryNodes :: Int } deriving (J.Typeable, J.Data, Show)

main :: IO ()
main = do
  (Config nodes k) <- readConf "conf.json"
  [op, file] <- getArgs
  key <- runInputT defaultSettings (getPassword (Just '*') "Password: ") >>= return . createKey . fromJust
  let parts = map (FP.encodeString . (</> (FP.decodeString file)) . FP.decodeString) nodes
  case op of
    "en" -> store key file k parts
    "de" -> retrieve key parts k (file ++ ".dec")
    _ -> error "invalid option"

createKey :: String -> Word64
createKey = either error id . S.decode . B.pack . hash . map (fromIntegral . fromEnum)

readConf :: FilePath -> IO Config
readConf fn = do
  str <- readFile fn
  either error return $
    J.resultToEither (J.decode str >>= J.fromJSON)
