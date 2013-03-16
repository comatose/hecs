{-# OPTIONS_GHC -fno-warn-orphans #-}
{-# OPTIONS_GHC -fno-warn-type-defaults #-}
{-# OPTIONS_GHC -fno-warn-unused-do-bind #-}
{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeSynonymInstances      #-}

module HECS.Internal where

-- import           Codec.Encryption.Padding (pkcs5, unPkcs5)
import qualified Codec.FEC                as F
-- import           Codec.Utils              (listFromOctets, listToOctets)
import           Control.Monad
import qualified Data.ByteString          as B
import qualified Data.ByteString.Internal as B
import           Data.Either
import           Data.Function            (on)
import           Data.Monoid              ((<>))
import qualified Data.Vector              as V
import           Data.Word
import           Foreign                  (with)
import           Foreign.ForeignPtr       (withForeignPtr)
import           Foreign.Ptr              (castPtr, plusPtr)
import           Foreign.Storable
import           Prelude                  hiding (catch)
import           System.Fuse
import           System.IO
import qualified System.IO.Streams        as S
import           System.Posix
import qualified Text.JSON.Generic        as J

-- | 4 kB
defaultChunkSize :: (Integral a) => a
defaultChunkSize = 4 * 1024

data Config = Config {
  primaryNodes     :: [FilePath] -- ^ where primary files are stored, should be absolute.
  , secondaryNodes :: [FilePath] -- ^ where redundant files are stored, should be absolute.
  } deriving (J.Typeable, J.Data, Show)

type ChunkIndex = Int

type StripeIndex = Int

type Metadata = (FileOffset, Word8)  -- ^ (file size, node id)

instance Storable Metadata where
  sizeOf _ = sizeOf (undefined :: FileOffset) + sizeOf (undefined :: Word8)
  alignment _ = alignment (undefined :: FileOffset)
  peek p = do
    off <- peek (castPtr p)
    nid <- peek (p `plusPtr` sizeOf off)
    return (off, nid)
  poke p (off, nid) = do
    poke (castPtr p) off
    poke (p `plusPtr` sizeOf off) nid

-- | payload of metadata per file
metadataSize :: (Integral a) => a
metadataSize = fromIntegral . sizeOf $ (undefined :: Metadata)

-- | read the size fo the file from its metadata
readFileSize :: Fd -> IO FileOffset
readFileSize fd =
  do fdSeek fd AbsoluteSeek 0
     with 0 $ \ptr ->
       fdReadBuf fd (castPtr ptr) (fromIntegral $ sizeOf (undefined :: FileOffset)) >> peek ptr

-- | record the size of the file as metadata
writeFileSize :: FileOffset -> Fd -> IO ()
writeFileSize size fd =
  do fdSeek fd AbsoluteSeek 0
     void . with size $ \ptr ->
       poke ptr size >> fdWriteBuf fd (castPtr ptr) (fromIntegral $ sizeOf size)

-- | read the whole metadata
readMetadata :: Fd -> IO Metadata
readMetadata fd =
  do fdSeek fd AbsoluteSeek 0
     with ((0, 0) :: Metadata) $ \ptr ->
       fdReadBuf fd (castPtr ptr) metadataSize >> peek ptr

-- | overwrite metadata
writeMetadata :: Metadata -> Fd -> IO ()
writeMetadata md fd =
  do fdSeek fd AbsoluteSeek 0
     void . with md $ \ptr ->
       poke ptr md >> fdWriteBuf fd (castPtr ptr) metadataSize

-- | get the absolute paths of primary files
primeFiles :: String  -- ^ hecs path
              -> Config
              -> [String]
primeFiles path cfg = map (++ path) (primaryNodes cfg)

-- | get the absolute paths of redundant files
spareFiles :: String -> Config -> [String]
spareFiles path cfg = map (++ path) (secondaryNodes cfg)

-- | primeFiles ++ spareFiles
entireFiles :: String -> Config -> [String]
entireFiles path cfg = primeFiles path cfg ++ spareFiles path cfg

readConf :: FilePath -> IO Config
readConf fn = do
  str <- readFile fn
  either error return $
    J.resultToEither (J.decode str >>= J.fromJSON)

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

fileStatusToFileStat :: FileStatus
                        -> FileOffset -- ^ size should be explicitly given. use 'calcFileSize'
                        -> FileStat
fileStatusToFileStat status size =
    FileStat { statEntryType        = fileStatusToEntryType status
             , statFileMode         = fileMode status
             , statLinkCount        = linkCount status
             , statFileOwner        = fileOwner status
             , statFileGroup        = fileGroup status
             , statSpecialDeviceID  = specialDeviceID status
             , statFileSize         = size
             -- fixme: 1024 is not always the size of a block
             , statBlocks           = fromIntegral size `div` 1024
             , statAccessTime       = accessTime status
             , statModificationTime = modificationTime status
             , statStatusChangeTime = statusChangeTime status
             }

calcFileSize :: [FileStatus]  -- ^ of only primary files
                -> IO FileOffset
calcFileSize stats =
  do let k = fromIntegral . length $ stats
         size' = sum . map fileSize $ stats
     return $ size' - (k * metadataSize)

quotRem' :: (Integral a, Integral a1, Num t, Num t1) => a -> a1 -> (t, t1)
quotRem' x y = let (q, r) = quotRem (fromIntegral x) (fromIntegral y) in (fromIntegral q, fromIntegral r)

-- | convert the logical location into the corresponding physical locations
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

-- | convert the logical location into the corresponding physical locations
toPhyChunks :: V.Vector Fd -> FileOffset -> B.ByteString -> [(Fd, FileOffset, B.ByteString)]
toPhyChunks fds off0 src = slice src $ toPhyAddrs fds off0 (fromIntegral . B.length $ src)
  where slice buf ((fd, off, len):rest)
          | B.null buf = []
          | otherwise = let (chunk, buf') = B.splitAt (fromIntegral len) buf
                        in (fd, off, chunk) : slice buf' rest
        slice _ _ = []

fdReadBS :: (Fd, FileOffset, ByteCount) -> IO B.ByteString
fdReadBS (fd, goff, len) = fdSeek fd AbsoluteSeek (goff + metadataSize) >> fdReadBS_ fd len

fdReadBS_ :: Fd -> ByteCount -> IO B.ByteString
fdReadBS_ fd len = B.createAndTrim (fromIntegral len)
                   (\ptr -> fmap fromIntegral $ fdReadBuf fd ptr len)

fdWriteBS :: (Fd, FileOffset, B.ByteString) -> IO ByteCount
fdWriteBS (fd, goff, bs) = fdSeek fd AbsoluteSeek (goff + metadataSize) >> fdWriteBS_ fd bs

fdWriteBS_ :: Fd -> B.ByteString -> IO ByteCount
fdWriteBS_ fd (B.PS fptr soff len) = withForeignPtr fptr $ \ptr ->
  fdWriteBuf fd (ptr `plusPtr` soff) (fromIntegral len)

readStripe :: StripeIndex -> V.Vector Fd -> IO (V.Vector B.ByteString)
readStripe si = V.mapM (\fd -> fdReadBS (fd, fromIntegral si * defaultChunkSize, defaultChunkSize))

writeStripe :: StripeIndex -> V.Vector Fd -> V.Vector B.ByteString -> IO (V.Vector ByteCount)
writeStripe si = V.zipWithM (\fd buf -> fdWriteBS (fd, fromIntegral si * defaultChunkSize, buf))

-- | append '\0's to make the size equal to 'defaultChunkSize'
padZero :: [B.ByteString] -> [B.ByteString]
padZero = map (\bs -> bs <> B.replicate (defaultChunkSize - B.length bs) 0)

-- | read a stripe from the primary files, calculate redundant parts of the stripe, and store them in the spare files.
completeStripe :: StripeIndex
                  -> V.Vector Fd -- ^ primary files
                  -> V.Vector Fd -- ^ spare files
                  -> IO ()
completeStripe si primes spares = void $
  let k = V.length primes
      n = k + V.length spares
  in readStripe si primes >>=
     return . F.encode (F.fec k n) . padZero . V.toList >>=
     writeStripe si spares . V.fromList

splitSize :: Int -> Int -> FileOffset -> [FileOffset]
splitSize k k' sz =
  let (q, r) = sz `quotRem'` (k * defaultChunkSize)
      go n (x:xs) | n > defaultChunkSize = (x + defaultChunkSize) : go (n - defaultChunkSize) xs
                  | otherwise = (x + n) : xs
      go _ [] = []
  in map (+metadataSize) $
       go r (replicate k (q * defaultChunkSize))
       <> replicate k' ((q + 1) * defaultChunkSize)

-- ^ build the original file from, at least, k partital files
recover :: Int -- ^ k
           -> Int -- ^ n
           -> [Fd] -- ^ partial files
           -> Fd -- ^ file to be recovered
           -> IO ()
recover k n src trg = do
  md <- mapM readMetadata src    -- also, shifting offset as a side-effect
  guard $ and $ zipWith ((==) `on` fst) md (tail md)
  let len = fst $ head md
  is <- S.makeInputStream . fmap (decode md) . forM src $ (`fdReadBS_` defaultChunkSize)
  os <- S.makeOutputStream $ void . maybe (return 0) (fdWriteBS_ trg)
  S.takeBytes (fromIntegral len) is >>= S.connectTo os
  where
    decode :: [Metadata] -> [B.ByteString] -> Maybe B.ByteString
    decode md bs = if any B.null bs
                   then Nothing
                   else Just . B.concat . F.decode (F.fec k n)
                        $ zip (map (fromIntegral . snd) md) (padZero bs)
