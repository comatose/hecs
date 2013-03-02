{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Main where

import           Codec.Encryption.Blowfish (decrypt, encrypt)
import           Codec.Encryption.Padding  (pkcs5, unPkcs5)
import qualified Codec.FEC                 as F
import           Codec.Utils               (listFromOctets, listToOctets)
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString           as B
import qualified Data.ByteString.Char8     as Bc
import qualified Data.ByteString.Internal  as B
import           Data.Word
import           Filesystem.Path.CurrentOS ((</>))
import qualified Filesystem.Path.CurrentOS as FP
import           Foreign.C.Error
import           Foreign.C.Types
import           Foreign.ForeignPtr        (withForeignPtr)
import           Foreign.Ptr               (plusPtr)
import           Prelude                   hiding (catch)
import           System.Directory          (getDirectoryContents)
import           System.Fuse
import           System.Fuse
import           System.IO
import           System.IO.Error
import           System.Posix
import           System.Posix.Directory
import           System.Posix.Files
import           System.Posix.IO
import           System.Posix.Types
import qualified Text.JSON.Generic         as J

defaultChunkSize :: Int
defaultChunkSize = 4

data Config = Config{ primaryNodes :: [FilePath], secondaryNodes :: [FilePath] } deriving (J.Typeable, J.Data, Show)

data HECFS = HECFS [Fd] [Fd]

asPrimary ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (primaryNodes cfg)

asSecondary ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (secondaryNodes cfg)

readConf :: FilePath -> IO Config
readConf fn = do
  str <- readFile fn
  either error return $
    J.resultToEither (J.decode str >>= J.fromJSON)

type HT = HECFS

main :: IO ()
main = do
  cfg <- readConf "conf.json"
  fuseMain (hecfsFSOps cfg) hecfsExceptionHandler

hecfsExceptionHandler :: SomeException -> IO Errno
hecfsExceptionHandler _  = getErrno
-- hecfsExceptionHandler ioe
--     | isAlreadyExistsError ioe = return eALREADY
--     | isDoesNotExistError  ioe = return eNOENT
--     | isAlreadyInUseError  ioe = return eBUSY
--     | isFullError          ioe = return eAGAIN
--     | isEOFError           ioe = return eIO
--     | isIllegalOperation   ioe = return eNOTTY
--     | isPermissionError    ioe = return ePERM
--     | otherwise                = return eFAULT

hecfsFSOps :: Config -> FuseOperations HT
hecfsFSOps cfg =
    defaultFuseOps {
      fuseGetFileStat = hecfsGetFileStat cfg
      , fuseGetFileSystemStats = hecfsGetFileSystemStats

      , fuseCreateDirectory = hecfsCreateDirectory cfg
      , fuseOpenDirectory = hecfsOpenDirectory cfg
      , fuseReadDirectory = hecfsReadDirectory cfg
      , fuseRemoveDirectory = hecfsRemoveDirectory cfg

      , fuseRename = hecfsRename cfg
      , fuseSetFileMode = hecfsSetFileMode cfg
      , fuseSetFileTimes = hecfsSetFileTimes cfg

      , fuseCreateDevice = hecfsCreateDevice cfg
      , fuseOpen = hecfsOpen cfg
      -- , fuseRead = hecfsRead cfg
      , fuseWrite = hecfsWrite cfg
      , fuseFlush = hecfsFlush cfg
      , fuseRelease = hecfsRelease cfg
      , fuseSynchronizeFile = hecfsSynchronizeFile cfg

      , fuseSetOwnerAndGroup = hecfsSetOwnerAndGroup cfg
      -- , fuseSetFileSize = hecfsSetFileSize cfg

      -- , fuseCreateSymbolicLink = hecfsCreateSymbolicLink cfg
      -- , fuseReadSymbolicLink = hecfsReadSymbolicLink cfg
      -- , fuseCreateLink = hecfsCreateLink cfg
      -- , fuseRemoveLink = hecfsRemoveLink cfg
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

fileStatusToFileStat :: FileStatus -> FileStat
fileStatusToFileStat status =
    FileStat { statEntryType        = fileStatusToEntryType status
             , statFileMode         = fileMode status
             , statLinkCount        = linkCount status
             , statFileOwner        = fileOwner status
             , statFileGroup        = fileGroup status
             , statSpecialDeviceID  = specialDeviceID status
             , statFileSize         = fileSize status
             -- fixme: 1024 is not always the size of a block
             , statBlocks           = fromIntegral (fileSize status `div` 1024)
             , statAccessTime       = accessTime status
             , statModificationTime = modificationTime status
             , statStatusChangeTime = statusChangeTime status
             }

hecfsGetFileStat :: Config -> FilePath -> IO (Either Errno FileStat)
hecfsGetFileStat cfg path =
  handle (\(_ :: SomeException) -> fmap Left getErrno) $ do
    status <- getSymbolicLinkStatus . head $ asPrimary path cfg
    return $ Right $ fileStatusToFileStat status

hecfsGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
hecfsGetFileSystemStats _ = return (Left eOK)

hecfsCreateDirectory :: Config -> FilePath -> FileMode -> IO Errno
hecfsCreateDirectory cfg path mode =
    do let paths = (asPrimary path cfg) ++ (asSecondary path cfg)
       print paths
       mapM_ (`createDirectory` mode) paths
       return eOK

hecfsOpenDirectory :: Config -> FilePath -> IO Errno
hecfsOpenDirectory cfg path =
    do openDirStream (head (asPrimary path cfg)) >>= closeDirStream
       return eOK

hecfsReadDirectory :: Config -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
hecfsReadDirectory cfg path0 =
  let path = head (asPrimary path0 cfg)
      pairType name = do status <- getSymbolicLinkStatus (path ++ "/" ++ name)
                         return (name, fileStatusToFileStat status)
  in do
    names <- getDirectoryContents path
    mapM pairType names >>= return . Right

hecfsRemoveDirectory :: Config -> FilePath -> IO Errno
hecfsRemoveDirectory cfg path =
    do let paths = (asPrimary path cfg) ++ (asSecondary path cfg)
       mapM_ removeDirectory paths
       return eOK

hecfsRename :: Config -> FilePath -> FilePath -> IO Errno
hecfsRename cfg src dest =
    do let srcs = (asPrimary src cfg) ++ (asSecondary src cfg)
           dsts = (asPrimary dest cfg) ++ (asSecondary dest cfg)
       zipWithM_ rename srcs dsts
       return eOK

hecfsSetFileMode :: Config -> FilePath -> FileMode -> IO Errno
hecfsSetFileMode cfg path mode =
    do let paths = (asPrimary path cfg) ++ (asSecondary path cfg)
       mapM_ (`setFileMode` mode) paths
       return eOK

hecfsSetFileTimes :: Config -> FilePath -> EpochTime -> EpochTime -> IO Errno
hecfsSetFileTimes cfg path accessTime modificationTime =
    do let paths = (asPrimary path cfg) ++ (asSecondary path cfg)
       mapM_ (\path -> setFileTimes path accessTime modificationTime) paths
       return eOK

hecfsCreateDevice :: Config -> FilePath -> EntryType -> FileMode -> DeviceID -> IO Errno
hecfsCreateDevice cfg path0 entryType mode dev =
    do let combinedMode = entryTypeToFileMode entryType `unionFileModes` mode
           paths = (asPrimary path0 cfg) ++ (asSecondary path0 cfg)
       mapM_ (\path -> createDevice path combinedMode dev) paths
       return eOK

hecfsOpen :: Config -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno HT)
hecfsOpen cfg path0 mode flags =
    do ps <- mapM (\path -> openFd path mode Nothing flags) (asPrimary path0 cfg)
       ss <- mapM (\path -> openFd path mode Nothing flags) (asSecondary path0 cfg)
       return . Right $ HECFS ps ss

toPhyAddrs :: [Fd] -> FileOffset -> Int -> [(Fd, FileOffset, ByteCount)]
toPhyAddrs fds (COff off0) len0 = go (quotRem (fromIntegral off0) defaultChunkSize) (fromIntegral len0)
  where go (chunkNo, off) len
          | len == 0 = []
          | len <= (defaultChunkSize - off) = [trans (chunkNo, off, len)]
          | otherwise = trans (chunkNo, off, defaultChunkSize - off) : go (chunkNo + 1, 0) (len - (defaultChunkSize - off))
        trans (chunkNo, off0, bc) =
          let (q, nodeNo) = chunkNo `quotRem` length fds
          in (fds !! nodeNo, fromIntegral $ q * defaultChunkSize + off0, fromIntegral bc)

toPhyChunks :: B.ByteString -> [(Fd, FileOffset, ByteCount)] -> [(Fd, FileOffset, B.ByteString)]
toPhyChunks buf ((fd, off, len):rest) =
  let (chunk, buf') = B.splitAt (fromIntegral len) buf
  in (fd, off, chunk) : toPhyChunks buf' rest

-- hecfsRead :: Config -> FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
-- hecfsRead _ _ (HECFS prims secos) count off =
--     do newOff <- fdSeek fd AbsoluteSeek off
--        if off /= newOff
--           then do return (Left eINVAL)
--           else do (content, bytesRead) <- fdRead fd count
--                   return (Right $ Bc.pack content)

hecfsWrite :: Config -> FilePath -> HT -> B.ByteString -> FileOffset -> IO (Either Errno ByteCount)
hecfsWrite _ _ (HECFS prims secos) src off =
  handle (\(_ :: SomeException) -> fmap Left getErrno) $
  (mapM write . toPhyChunks src . toPhyAddrs prims off . B.length $ src) >>= return . Right . sum

write :: (Fd, FileOffset, B.ByteString) -> IO ByteCount
write (fd, goff, (B.PS fptr soff len)) = do
  fdSeek fd AbsoluteSeek goff
  withForeignPtr fptr $
    \ptr -> fdWriteBuf fd (ptr `plusPtr` soff) (fromIntegral len)

hecfsFlush :: Config -> FilePath -> HT -> IO Errno
hecfsFlush cfg _ (HECFS prims secos) = return eOK

hecfsRelease :: Config -> FilePath -> HT -> IO ()
hecfsRelease cfg _ (HECFS prims secos) = mapM_ closeFd $ prims ++ secos

hecfsSynchronizeFile :: Config -> FilePath -> SyncType -> IO Errno
hecfsSynchronizeFile cfg _ _ = return eOK

-- hecfsRemoveLink :: Config -> FilePath -> IO Errno
-- hecfsRemoveLink cfg path =
--     do let paths = (asPrimary path cfg) ++ (asSecondary path cfg)
--        mapM_ removeLink paths
--        return eOK

-- hecfsCreateSymbolicLink :: Config -> FilePath -> FilePath -> IO Errno
-- hecfsCreateSymbolicLink cfg src dest =
--     do createSymbolicLink src dest
--        return eOK

-- hecfsReadSymbolicLink :: Config -> FilePath -> IO (Either Errno FilePath)
-- hecfsReadSymbolicLink cfg path =
--     do target <- readSymbolicLink . head $ asPrimary path cfg
--        return (Right target)

-- hecfsCreateLink :: Config -> FilePath -> FilePath -> IO Errno
-- hecfsCreateLink cfg src dest =
--     do createLink src dest
--        return eOK

hecfsSetOwnerAndGroup :: Config -> FilePath -> UserID -> GroupID -> IO Errno
hecfsSetOwnerAndGroup cfg path0 uid gid =
    do let paths = (asPrimary path0 cfg) ++ (asSecondary path0 cfg)
       mapM_ (\path -> setOwnerAndGroup path uid gid) paths
       return eOK

-- hecfsSetFileSize :: Config -> FilePath -> FileOffset -> IO Errno
-- hecfsSetFileSize cfg path off =
--     do setFileSize path off
--        return eOK
