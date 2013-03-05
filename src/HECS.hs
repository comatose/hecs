{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Main where

import           Codec.Encryption.Padding (pkcs5, unPkcs5)
import qualified Codec.FEC                as F
import           Codec.Utils              (listFromOctets, listToOctets)
-- import           Control.Exception        (handle)
import           Control.Monad
import qualified Data.ByteString          as B
import qualified Data.ByteString.Internal as B
import           Data.Monoid              ((<>))
import qualified Data.Vector              as V
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

defaultChunkSize :: (Integral a) => a
defaultChunkSize = 4

data Config = Config{ primaryNodes :: [FilePath], secondaryNodes :: [FilePath] } deriving (J.Typeable, J.Data, Show)

data HECS = HECS (V.Vector Fd) (V.Vector Fd)

type ChunkIndex = Int

type StripeIndex = Int

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
hecsExceptionHandler _  = getErrno >>= (\(Errno errno) -> return . Errno . negate $ errno)

hecsFSOps :: Config -> FuseOperations HT
hecsFSOps cfg =
    defaultFuseOps {
      fuseGetFileStat = hecsGetFileStat cfg

      , fuseCreateDirectory = hecsCreateDirectory cfg
      , fuseOpenDirectory = hecsOpenDirectory cfg
      , fuseReadDirectory = hecsReadDirectory cfg
      , fuseRemoveDirectory = hecsRemoveDirectory cfg

      , fuseRename = hecsRename cfg
      , fuseSetFileMode = hecsSetFileMode cfg
      , fuseSetFileTimes = hecsSetFileTimes cfg
      , fuseSetFileSize = hecsSetFileSize cfg

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
      -- , fuseGetFileSystemStats = hecsGetFileSystemStats
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
  -- handle (\(_ :: SomeException) -> fmap Left getErrno) $
  do let paths = asPrimary path cfg
     status <- getSymbolicLinkStatus . head $ paths
     size' <- fmap (sum . map fileSize) $ mapM getSymbolicLinkStatus (tail paths)
     return $ Right $ fileStatusToFileStat status size'

hecsCreateDirectory :: Config -> FilePath -> FileMode -> IO Errno
hecsCreateDirectory cfg path mode =
    do let paths = asPrimary path cfg ++ asSecondary path cfg
       mapM_ (`createDirectory` mode) paths
       return eOK

hecsOpenDirectory :: Config -> FilePath -> IO Errno
hecsOpenDirectory cfg path =
    do openDirStream (head (asPrimary path cfg)) >>= closeDirStream
       return eOK

hecsReadDirectory :: Config -> FilePath -> IO (Either Errno [(FilePath, FileStat)])
hecsReadDirectory cfg path0 =
  let pairType :: FilePath -> IO (FilePath, FileStat)
      pairType name = hecsGetFileStat cfg (path0 ++ "/" ++ name)
                      >>= either
                      (\errno -> throwIO $ errnoToIOError "hecsReadDirectory" errno Nothing (Just name))
                      (\stat -> return (name, stat))
  in
   -- handle (\(_ :: SomeException) -> fmap Left getErrno) $
   fmap Right $ (getDirectoryContents . head . asPrimary path0 $ cfg) >>= mapM pairType

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
  -- handle (\(_ :: SomeException) -> fmap Left getErrno) $
  do ps <- mapM (\path -> openFd path ReadWrite Nothing flags) (asPrimary path0 cfg)
     ss <- mapM (\path -> openFd path ReadWrite Nothing flags) (asSecondary path0 cfg)
     print (ps, ss)
     return . Right $ HECS (V.fromList ps) (V.fromList ss)

quotRem' :: (Integral a, Integral a1, Num t, Num t1) => a -> a1 -> (t, t1)
quotRem' x y = let (q, r) = quotRem (fromIntegral x) (fromIntegral y) in (fromIntegral q, fromIntegral r)

toPhyAddrs :: V.Vector Fd -> FileOffset -> ByteCount -> [(Fd, FileOffset, ByteCount)]
toPhyAddrs fds off0 len0 = go (quotRem' off0 defaultChunkSize) (fromIntegral len0)
  where
    go :: (ChunkIndex, FileOffset) -> ByteCount -> [(Fd, FileOffset, ByteCount)]
    go (chunkNo, off) len
      | len == 0 = []
      | len <= (defaultChunkSize - fromIntegral off) = [trans (chunkNo, off, len)]
      | otherwise = trans (chunkNo, off, defaultChunkSize - fromIntegral off) : go (chunkNo + 1, 0) (len - (defaultChunkSize - fromIntegral off))
    trans :: (ChunkIndex, FileOffset, ByteCount) -> (Fd, FileOffset, ByteCount)
    trans (chunkNo, off, bc) =
      let (q, nodeNo) = chunkNo `quotRem` V.length fds
      in (fds V.! nodeNo, fromIntegral q * defaultChunkSize + off, bc)

toPhyChunks :: B.ByteString -> [(Fd, FileOffset, ByteCount)] -> [(Fd, FileOffset, B.ByteString)]
toPhyChunks buf ((fd, off, len):rest)
  | B.null buf = []
  | otherwise = let (chunk, buf') = B.splitAt (fromIntegral len) buf
                in (fd, off, chunk) : toPhyChunks buf' rest
toPhyChunks _ _ = []

hecsRead :: Config -> FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
hecsRead _ _ (HECS prims _) count off =
  -- handle (\(_ :: SomeException) -> fmap Left getErrno) $
  fmap (Right . B.concat) (mapM readChunk . toPhyAddrs prims off . fromIntegral $ count )

readChunk :: (Fd, FileOffset, ByteCount) -> IO B.ByteString
readChunk (fd, goff, len) =
  do _ <- fdSeek fd AbsoluteSeek goff
     B.createAndTrim (fromIntegral len) (\ptr -> fmap fromIntegral (fdReadBuf fd ptr len))

hecsWrite :: Config -> FilePath -> HT -> B.ByteString -> FileOffset -> IO (Either Errno ByteCount)
hecsWrite _ _ hecs@(HECS prims _) src off =
  -- handle (\(_ :: SomeException) -> fmap Left getErrno) $
  do
    n0 <- fmap sum (mapM writeChunk . toPhyChunks src . toPhyAddrs prims off . fromIntegral . B.length $ src)
    mapM_ (`completeStripe` hecs) [start..(end n0)]
    return . Right $ n0
      where start = fromIntegral off `quot` (V.length prims * defaultChunkSize)
            end n = (fromIntegral off + fromIntegral n - 1) `quot` (V.length prims * defaultChunkSize)

writeChunk :: (Fd, FileOffset, B.ByteString) -> IO ByteCount
writeChunk (fd, goff, B.PS fptr soff len) = do
  print (fd, len)
  _ <- fdSeek fd AbsoluteSeek goff
  withForeignPtr fptr $ \ptr -> fdWriteBuf fd (ptr `plusPtr` soff) (fromIntegral len)

readStripe :: StripeIndex -> V.Vector Fd -> IO (V.Vector B.ByteString)
readStripe si =
  V.mapM (\fd -> readChunk (fd, fromIntegral si * defaultChunkSize, defaultChunkSize))

padZero :: [B.ByteString] -> [B.ByteString]
padZero = map (\bs -> bs <> B.replicate (defaultChunkSize - B.length bs) 0)

writeStripe :: StripeIndex -> V.Vector Fd -> V.Vector B.ByteString -> IO (V.Vector ByteCount)
writeStripe si =
  V.zipWithM (\fd buf -> writeChunk (fd, fromIntegral si * defaultChunkSize, buf))

completeStripe :: StripeIndex -> HECS -> IO ()
completeStripe si (HECS prims secos) = do
  _ <- readStripe si prims >>=
       return. F.encode (F.fec k n) . padZero . V.toList >>=
       writeStripe si secos . V.fromList
  return ()
    where k = V.length prims
          n = k + V.length secos

hecsFlush :: Config -> FilePath -> HT -> IO Errno
hecsFlush _ _ _ = return eOK

hecsRelease :: Config -> FilePath -> HT -> IO ()
hecsRelease _ _ (HECS prims secos) = do
  V.mapM_ closeFd prims
  V.mapM_ closeFd secos

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

splitSize :: Int -> Int -> FileOffset -> [FileOffset]
splitSize k k' sz =
  let (q, r) = sz `quotRem'` (k * defaultChunkSize)
      go n (x:xs) | n > defaultChunkSize = (x + defaultChunkSize) : go (n - defaultChunkSize) xs
                  | otherwise = (x + n) : xs
      go _ [] = []
  in go r (replicate k (q * defaultChunkSize)) <> replicate k' ((q + 1) * defaultChunkSize)

hecsSetFileSize :: Config -> FilePath -> FileOffset -> IO Errno
hecsSetFileSize cfg path0 off =
    do let prims = asPrimary path0 cfg
           secos = asSecondary path0 cfg
       zipWithM_ setFileSize (prims <> secos)
         (splitSize (length prims) (length secos) off)
       return eOK

-- hecsCreateSymbolicLink :: Config -> FilePath -> FilePath -> IO Errno
-- hecsCreateSymbolicLink cfg src dest =
--     do createSymbolicLink src dest
--        return eOK

-- hecsReadSymbolicLink :: Config -> FilePath -> IO (Either Errno FilePath)
-- hecsReadSymbolicLink cfg path =
--     do target <- readSymbolicLink . head $ asPrimary path cfg
--        return (Right target)

-- hecsGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
-- hecsGetFileSystemStats _ = return (Left eOK)
