{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeSynonymInstances      #-}

module Main where

-- import           Codec.Encryption.Padding (pkcs5, unPkcs5)
-- import           Codec.Utils              (listFromOctets, listToOctets)
import           Control.Concurrent.MVar (MVar, newMVar, withMVar)
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString         as B
import           Data.Either
import           Data.Monoid             ((<>))
import qualified Data.Vector             as V
import           Foreign.C.Error
import           HECS.Internal
import           Prelude                 hiding (catch)
-- import           System.Console.Haskeline hiding (handle)
import           System.Directory        (getDirectoryContents)
import           System.Fuse
import           System.IO
import           System.Posix

data HECS = HECS (V.Vector Fd) (V.Vector Fd) (MVar ())

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

hecsGetFileStat :: Config -> FilePath -> IO (Either Errno FileStat)
hecsGetFileStat cfg path =
  do status <- mapM getSymbolicLinkStatus $ entireFiles path cfg
     size <- calcFileSize cfg status
     return $ Right $ fileStatusToFileStat (head status) size

hecsCreateDirectory :: Config -> FilePath -> FileMode -> IO Errno
hecsCreateDirectory cfg path mode =
  do mapM_ (`createDirectory` mode) $ entireFiles path cfg
     return eOK

hecsOpenDirectory :: Config -> FilePath -> IO Errno
hecsOpenDirectory cfg path =
    do openDirStream (head $ primeFiles path cfg) >>= closeDirStream
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
   fmap Right $ getDirectoryContents (head $ primeFiles path0 cfg) >>= mapM pairType

hecsRemoveDirectory :: Config -> FilePath -> IO Errno
hecsRemoveDirectory cfg path =
    do mapM_ removeDirectory $ entireFiles path cfg
       return eOK

hecsRename :: Config -> FilePath -> FilePath -> IO Errno
hecsRename cfg src dest =
    do zipWithM_ rename (entireFiles src cfg) (entireFiles dest cfg)
       return eOK

hecsSetFileMode :: Config -> FilePath -> FileMode -> IO Errno
hecsSetFileMode cfg path mode =
    do mapM_ (`setFileMode` mode) $ entireFiles path cfg
       return eOK

hecsSetFileTimes :: Config -> FilePath -> EpochTime -> EpochTime -> IO Errno
hecsSetFileTimes cfg path at mt =
    do mapM_ (\p -> setFileTimes p at mt) $ entireFiles path cfg
       return eOK

hecsCreateDevice :: Config -> FilePath -> EntryType -> FileMode -> DeviceID -> IO Errno
hecsCreateDevice cfg path entryType mode dev
  = case entryType of
    Directory -> makeDevices >> return eOK
    RegularFile -> makeDevices >> initMetadata >> return eOK
    _ -> return eNOSYS
  where
    combinedMode = entryTypeToFileMode entryType `unionFileModes` mode
    paths = entireFiles path cfg
    makeDevices = mapM_ (\p -> createDevice p combinedMode dev) paths
    initMetadata =
      zipWithM_ (\f i -> withFile f WriteMode $ handleToFd >=> writeMetadata (0, i)) paths [0..]

hecsOpen :: Config -> FilePath -> OpenMode -> OpenFileFlags -> IO (Either Errno HT)
hecsOpen cfg path ReadOnly flags =
  do primes <- mapM (\p -> openFd p ReadOnly Nothing flags) (primeFiles path cfg)
     lock <- newMVar ()
     return . Right $ HECS (V.fromList primes) V.empty lock

hecsOpen cfg path mode flags =
  do primes <- mapM (\p -> openFd p ReadWrite Nothing flags) (primeFiles path cfg)
     spares <- mapM (\p -> openFd p mode Nothing flags) (spareFiles path cfg)
     lock <- newMVar ()
     return . Right $ HECS (V.fromList primes) (V.fromList spares) lock

hecsRead :: Config -> FilePath -> HT -> ByteCount -> FileOffset -> IO (Either Errno B.ByteString)
hecsRead _ _ (HECS primes _ lock) count offset =
  withMVar lock . const $ do
    bs <- mapM fdReadBS chunks
    return . Right . B.concat $ bs
      where chunks :: [(Fd, FileOffset, ByteCount)]
            chunks = toPhyAddrs primes offset (fromIntegral count)

hecsWrite :: Config -> FilePath -> HT -> B.ByteString -> FileOffset -> IO (Either Errno ByteCount)
hecsWrite _ _ (HECS primes spares lock) src offset =
  withMVar lock . const $ do
    len <- fmap sum $ mapM fdWriteBS (toPhyChunks primes offset src)
    mapM_ (\si -> completeStripe si primes spares) [start..(end len)]
    let newSize = offset + fromIntegral len
    size <- readFileSize (V.head primes)
    when (newSize > size) $ V.forM_ (primes <> spares) (writeFileSize newSize)
    return . Right $ len
      where stripeSize = V.length primes * defaultChunkSize
            start = fromIntegral offset `quot` stripeSize
            end n = (fromIntegral offset + fromIntegral n - 1) `quot` stripeSize

hecsFlush :: Config -> FilePath -> HT -> IO Errno
hecsFlush _ _ _ = return eOK

hecsRelease :: Config -> FilePath -> HT -> IO ()
hecsRelease _ _ (HECS primes spares _) = V.mapM_ closeFd (primes <> spares)

hecsSynchronizeFile :: Config -> FilePath -> SyncType -> IO Errno
hecsSynchronizeFile _ _ _ = return eOK

hecsCreateLink :: Config -> FilePath -> FilePath -> IO Errno
hecsCreateLink cfg src dest =
    do zipWithM_ createLink (entireFiles src cfg) (entireFiles dest cfg)
       return eOK

hecsRemoveLink :: Config -> FilePath -> IO Errno
hecsRemoveLink cfg path =
    do mapM_ removeLink $ entireFiles path cfg
       return eOK

hecsSetOwnerAndGroup :: Config -> FilePath -> UserID -> GroupID -> IO Errno
hecsSetOwnerAndGroup cfg path uid gid =
    do mapM_ (\p -> setOwnerAndGroup p uid gid) $ entireFiles path cfg
       return eOK

hecsSetFileSize :: Config -> FilePath -> FileOffset -> IO Errno
hecsSetFileSize cfg path newSize =
    do let primes = primeFiles path cfg
           spares = spareFiles path cfg
       zipWithM_ setFileSize (primes <> spares)
         (splitSize (length primes) (length spares) newSize)
       forM_ (primes <> spares) $ \f ->
         bracket (openFd f WriteOnly Nothing defaultFileFlags) closeFd
         (writeFileSize newSize)
       return eOK

-- hecsCreateSymbolicLink :: Config -> FilePath -> FilePath -> IO Errno
-- hecsCreateSymbolicLink cfg src dest =
--     do createSymbolicLink src dest
--        return eOK

-- hecsReadSymbolicLink :: Config -> FilePath -> IO (Either Errno FilePath)
-- hecsReadSymbolicLink cfg path =
--     do target <- readSymbolicLink . head $ primeFiles path cfg
--        return (Right target)

-- hecsGetFileSystemStats :: String -> IO (Either Errno FileSystemStats)
-- hecsGetFileSystemStats _ = return (Left eOK)
