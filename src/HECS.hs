{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Main where

import           Codec.Encryption.Padding (pkcs5, unPkcs5)
import qualified Codec.FEC                as F
import           Codec.Utils              (listFromOctets, listToOctets)
import           Control.Exception        (handle)
import           Control.Monad
import           Data.Array
import qualified Data.ByteString          as B
import qualified Data.ByteString.Internal as B
import           Foreign.C.Error
import           Foreign.ForeignPtr       (withForeignPtr)
import           Foreign.Ptr              (plusPtr)
import           Prelude                  hiding (catch)
import           System.Console.Haskeline hiding (handle)
import           System.Directory         (getDirectoryContents)
import           System.Fuse
import           System.IO
import           System.Posix
import qualified Text.JSON.Generic        as J

defaultChunkSize :: ByteCount
defaultChunkSize = 4

data Config = Config{ primaryNodes :: [FilePath], secondaryNodes :: [FilePath] } deriving (J.Typeable, J.Data, Show)

data HECS = HECS (Array Int Fd) (Array Int Fd)

type ChunkIndex = Int

asPrimary :: String -> Config -> [String]
asPrimary path cfg = map (++ path) (primaryNodes cfg)
-- asPrimary ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (primaryNodes cfg)

asSecondary :: String -> Config -> [String]
asSecondary path cfg = map (++ path) (secondaryNodes cfg)
-- asSecondary ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (secondaryNodes cfg)

readConf :: FilePath -> IO Config
readConf fn = do
  str <- readFile fn
  either error return $
    J.resultToEither (J.decode str >>= J.fromJSON)

type HT = HECS

main :: IO ()
main = do
  cfg <- readConf "conf.json"
  fuseMain (hecsFSOps cfg) hecsExceptionHandler

hecsExceptionHandler :: SomeException -> IO Errno
hecsExceptionHandler _  = getErrno
-- hecsExceptionHandler ioe
--     | isAlreadyExistsError ioe = return eALREADY
--     | isDoesNotExistError  ioe = return eNOENT
--     | isAlreadyInUseError  ioe = return eBUSY
--     | isFullError          ioe = return eAGAIN
--     | isEOFError           ioe = return eIO
--     | isIllegalOperation   ioe = return eNOTTY
--     | isPermissionError    ioe = return ePERM
--     | otherwise                = return eFAULT

hecsFSOps :: Config -> FuseOperations HT
hecsFSOps cfg =
    defaultFuseOps {
      fuseGetFileStat = hecsGetFileStat cfg
      , fuseGetFileSystemStats = hecsGetFileSystemStats

      , fuseCreateDirectory = hecsCreateDirectory cfg
      , fuseOpenDirectory = hecsOpenDirectory cfg
      , fuseReadDirectory = hecsReadDirectory cfg
      , fuseRemoveDirectory = hecsRemoveDirectory cfg

      , fuseRename = hecsRename cfg
      , fuseSetFileMode = hecsSetFileMode cfg
      , fuseSetFileTimes = hecsSetFileTimes cfg

      , fuseCreateDevice = hecsCreateDevice cfg
      , fuseOpen = hecsOpen cfg
      , fuseRead = hecsRead cfg
      , fuseWrite = hecsWrite cfg
      , fuseFlush = hecsFlush cfg
      , fuseRelease = hecsRelease cfg
      , fuseSynchronizeFile = hecsSynchronizeFile cfg

      , fuseSetOwnerAndGroup = hecsSetOwnerAndGroup cfg
      , fuseCreateLink = hecsCreateLink cfg
      , fuseRemoveLink = hecsRemoveLink cfg

      -- , fuseCreateSymbolicLink = hecsCreateSymbolicLink cfg
      -- , fuseReadSymbolicLink = hecsReadSymbolicLink cfg
      -- , fuseSetFileSize = hecsSetFileSize cfg
      }

fileStatusToEntryType :: FileStatus -> EntryType
fileStatusToEntryType status
    | isSymbolicLink    status = SymbolicLink
    | isNamedPipe       status = NamedPipe
    | isCharacterDevice status = CharacterSpecial
    | isDirectory       status = Directory
    | isBlockDevice     status = BlockSpecial
    | isRegularFile     status = RegularFile
    | isSocket          status = Socket
    | otherwise                = Unknown

fileStatusToFileStat :: FileStatus -> FileOffset -> FileStat
fileStatusToFileStat status size' =
    FileStat { statEntryType        = fileStatusToEntryType status
             , statFileMode         = fileMode status
             , statLinkCount        = linkCount status
             , statFileOwner        = fileOwner status
             , statFileGroup        = fileGroup status
             , statSpecialDeviceID  = specialDeviceID status
             , statFileSize         = fileSize status + size'
             -- fixme: 1024 is not always the size of a block
             , statBlocks           = fromIntegral ((fileSize status + size') `div` 1024)
             , statAccessTime       = accessTime status
             , statModificationTime = modificationTime status
             , statStatusChangeTime = statusChangeTime status
             }

hecsGetFileStat :: Config -> FilePath -> IO (Either Errno FileStat)
hecsGetFileStat cfg path =
  handle (\(_ :: SomeException) -> fmap Left getErrno) $ do
    let paths = asPrimary path cfg
    status <- getSymbolicLinkStatus . head $ paths
    size' <- fmap (sum . map fileSize) $ mapM getSymbolicLinkStatus (tail paths)
    return $ Right $ fileStatusToFileStat status size'

hecsGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
hecsGetFileSystemStats _ = return (Left eOK)

hecsCreateDirectory :: Config -> FilePath -> FileMode -> IO Errno
hecsCreateDirectory cfg path mode =
    do let paths = asPrimary path cfg ++ asSecondary path cfg
       print paths
       mapM_ (`createDirectory` mode) paths
       return eOK

hecsOpenDirectory :: Config -> FilePath -> IO Errno
hecsOpenDirectory cfg path =
    do openDirStream (head (asPrimary path cfg)) >>= closeDirStream
       return eOK

hecsReadDirectory :: Config -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
hecsReadDirectory cfg path0 =
  let path = head (asPrimary path0 cfg)
      pairType :: FilePath -> IO (FilePath, FileStat)
      pairType name = hecsGetFileStat cfg (path0 ++ "/" ++ name)
                      >>= either
                      (\errno -> throwIO $ errnoToIOError "hecsReadDirectory" errno Nothing (Just name))
                      (\stat -> return (name, stat))
  in handle (\(_ :: SomeException) -> fmap Left getErrno) $
    fmap Right (getDirectoryContents path >>= mapM pairType)

hecsRemoveDirectory :: Config -> FilePath -> IO Errno
hecsRemoveDirectory cfg path =
    do let paths = asPrimary path cfg ++ asSecondary path cfg
       mapM_ removeDirectory paths
       return eOK

hecsRename :: Config -> FilePath -> FilePath -> IO Errno
hecsRename cfg src dest =
    do let srcs = asPrimary src cfg ++ asSecondary src cfg
           dsts = asPrimary dest cfg ++ asSecondary dest cfg
       zipWithM_ rename srcs dsts
       return eOK

hecsSetFileMode :: Config -> FilePath -> FileMode -> IO Errno
hecsSetFileMode cfg path mode =
    do let paths = asPrimary path cfg ++ asSecondary path cfg
       mapM_ (`setFileMode` mode) paths
       return eOK

hecsSetFileTimes :: Config -> FilePath -> EpochTime -> EpochTime -> IO Errno
hecsSetFileTimes cfg path0 at mt =
    do let paths = asPrimary path0 cfg ++ asSecondary path0 cfg
       mapM_ (\path -> setFileTimes path at mt) paths
       return eOK

hecsCreateDevice :: Config -> FilePath -> EntryType -> FileMode -> DeviceID -> IO Errno
hecsCreateDevice cfg path0 entryType mode dev =
    do let combinedMode = entryTypeToFileMode entryType `unionFileModes` mode
           paths = asPrimary path0 cfg ++ asSecondary path0 cfg
       mapM_ (\path -> createDevice path combinedMode dev) paths
       return eOK

hecsOpen :: Config -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno HT)
hecsOpen cfg path0 mode flags =
    do ps <- mapM (\path -> openFd path mode Nothing flags) (asPrimary path0 cfg)
       ss <- mapM (\path -> openFd path mode Nothing flags) (asSecondary path0 cfg)
       return . Right $ HECS (listArray (0, length ps - 1) ps) (listArray (0, length ss - 1) ss)

quotRem' :: (Integral a, Integral a1, Num t, Num t1) => a -> a1 -> (t, t1)
quotRem' x y = let (q, r) = quotRem (fromIntegral x) (fromIntegral y) in (fromIntegral q, fromIntegral r)

toPhyAddrs :: Array Int Fd -> FileOffset -> ByteCount -> [(Fd, FileOffset, ByteCount)]
toPhyAddrs fds off0 len0 = go (quotRem' off0 defaultChunkSize) (fromIntegral len0)
  where
    go :: (ChunkIndex, FileOffset) -> ByteCount -> [(Fd, FileOffset, ByteCount)]
    go (chunkNo, off) len
      | len == 0 = []
      | len <= (defaultChunkSize - fromIntegral off) = [trans (chunkNo, off, len)]
      | otherwise = trans (chunkNo, off, defaultChunkSize - fromIntegral off) : go (chunkNo + 1, 0) (len - (defaultChunkSize - fromIntegral off))
    trans :: (ChunkIndex, FileOffset, ByteCount) -> (Fd, FileOffset, ByteCount)
    trans (chunkNo, off, bc) =
      let (q, nodeNo) = chunkNo `quotRem` length (elems fds)
      in (fds ! nodeNo, fromIntegral q * fromIntegral defaultChunkSize + off, bc)

toPhyChunks :: B.ByteString -> [(Fd, FileOffset, ByteCount)] -> [(Fd, FileOffset, B.ByteString)]
toPhyChunks buf ((fd, off, len):rest)
  | B.null buf = []
  | otherwise = let (chunk, buf') = B.splitAt (fromIntegral len) buf
                in (fd, off, chunk) : toPhyChunks buf' rest
toPhyChunks _ _ = []

hecsRead :: Config -> FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
hecsRead _ _ (HECS prims _) count off =
  handle (\(_ :: SomeException) -> fmap Left getErrno) $
  fmap (Right . B.concat) (mapM readChunk . toPhyAddrs prims off . fromIntegral $ count )
  where
    readChunk :: (Fd, FileOffset, ByteCount) -> IO B.ByteString
    readChunk (fd, goff, len) = do
      _ <- fdSeek fd AbsoluteSeek goff
      B.createAndTrim (fromIntegral len) (\ptr -> fmap fromIntegral (fdReadBuf fd ptr len))

hecsWrite :: Config -> FilePath -> HT -> B.ByteString -> FileOffset -> IO (Either Errno ByteCount)
hecsWrite _ _ (HECS prims _) src off =
  handle (\(_ :: SomeException) -> fmap Left getErrno) $
  fmap (Right . sum) (mapM writeChunk . toPhyChunks src . toPhyAddrs prims off . fromIntegral . B.length $ src)
  where
    writeChunk :: (Fd, FileOffset, B.ByteString) -> IO ByteCount
    writeChunk (fd, goff, B.PS fptr soff len) = do
      _ <- fdSeek fd AbsoluteSeek goff
      withForeignPtr fptr $ \ptr -> fdWriteBuf fd (ptr `plusPtr` soff) (fromIntegral len)

hecsFlush :: Config -> FilePath -> HT -> IO Errno
hecsFlush _ _ _ = return eOK

hecsRelease :: Config -> FilePath -> HT -> IO ()
hecsRelease _ _ (HECS prims secos) = do
  mapM_ closeFd . elems $ prims
  mapM_ closeFd . elems $ secos

hecsSynchronizeFile :: Config -> FilePath -> SyncType -> IO Errno
hecsSynchronizeFile _ _ _ = return eOK

hecsCreateLink :: Config -> FilePath -> FilePath -> IO Errno
hecsCreateLink cfg src dest =
    do let srcs = asPrimary src cfg ++ asSecondary src cfg
           dests = asPrimary dest cfg ++ asSecondary dest cfg
       zipWithM_ createLink srcs dests
       return eOK

hecsRemoveLink :: Config -> FilePath -> IO Errno
hecsRemoveLink cfg path =
    do let paths = asPrimary path cfg ++ asSecondary path cfg
       mapM_ removeLink paths
       return eOK

hecsSetOwnerAndGroup :: Config -> FilePath -> UserID -> GroupID -> IO Errno
hecsSetOwnerAndGroup cfg path0 uid gid =
    do let paths = asPrimary path0 cfg ++ asSecondary path0 cfg
       mapM_ (\path -> setOwnerAndGroup path uid gid) paths
       return eOK

-- hecsCreateSymbolicLink :: Config -> FilePath -> FilePath -> IO Errno
-- hecsCreateSymbolicLink cfg src dest =
--     do createSymbolicLink src dest
--        return eOK

-- hecsReadSymbolicLink :: Config -> FilePath -> IO (Either Errno FilePath)
-- hecsReadSymbolicLink cfg path =
--     do target <- readSymbolicLink . head $ asPrimary path cfg
--        return (Right target)

splitSize :: ByteCount -> ByteCount -> [ByteCount]
splitSize k sz = let (q, r) = sz `quotRem` (fromIntegral k * defaultChunkSize)
                     go n (x:xs) | n > defaultChunkSize = (x + defaultChunkSize) : go (n - defaultChunkSize) xs
                                 | otherwise = (x + n) : xs
                     go _ [] = []
                 in go r $ replicate (fromIntegral k) (q * defaultChunkSize)

-- hecsSetFileSize :: Config -> FilePath -> FileOffset -> IO Errno
-- hecsSetFileSize cfg path off =
--     do setFileSize path off
--        return eOK
