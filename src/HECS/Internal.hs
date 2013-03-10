{-# LANGUAGE DeriveDataTypeable        #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-# LANGUAGE TypeSynonymInstances      #-}

module HECS.Internal where

import           Codec.Encryption.Padding (pkcs5, unPkcs5)
import qualified Codec.FEC                as F
import           Codec.Utils              (listFromOctets, listToOctets)
import           Control.Monad
import qualified Data.ByteString          as B
import qualified Data.ByteString.Internal as B
import           Data.Either
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
import           System.Posix
import qualified Text.JSON.Generic        as J

defaultChunkSize :: (Integral a) => a
defaultChunkSize = 4 * 1024

data Config = Config { primaryNodes :: [FilePath], secondaryNodes :: [FilePath] } deriving (J.Typeable, J.Data, Show)

type ChunkIndex = Int

type StripeIndex = Int

type Metadata = (FileOffset, Word8)  -- (file size, id)

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

metadataSize :: (Integral a) => a
metadataSize = fromIntegral . sizeOf $ (undefined :: Metadata)

readFileSize :: Fd -> IO FileOffset
readFileSize fd =
  do _ <- fdSeek fd AbsoluteSeek 0
     with 0 $ \ptr -> fdReadBuf fd (castPtr ptr) (fromIntegral $ sizeOf (undefined :: FileOffset)) >> peek ptr

writeFileSize :: FileOffset -> Fd -> IO ()
writeFileSize size fd =
  do _ <- fdSeek fd AbsoluteSeek 0
     _ <- with size $ \ptr -> poke ptr size >> fdWriteBuf fd (castPtr ptr) (fromIntegral $ sizeOf size)
     return ()

readMetadata :: Fd -> IO Metadata
readMetadata fd =
  do _ <- fdSeek fd AbsoluteSeek 0
     let md = (0, 0) :: Metadata
     with md $ \ptr -> fdReadBuf fd (castPtr ptr) metadataSize >> peek ptr

writeMetadata :: Metadata -> Fd -> IO ()
writeMetadata md fd =
  do _ <- fdSeek fd AbsoluteSeek 0
     _ <- with md $ \ptr -> poke ptr md >> fdWriteBuf fd (castPtr ptr) metadataSize
     return ()

primeFiles :: String -> Config -> [String]
primeFiles path cfg = map (++ path) (primaryNodes cfg)
-- primeFiles ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (primaryNodes cfg)

spareFiles :: String -> Config -> [String]
spareFiles path cfg = map (++ path) (secondaryNodes cfg)
-- spareFiles ('/':path) cfg = map (FP.encodeString . (</> (FP.decodeString path)) . FP.decodeString) (secondaryNodes cfg)

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

fileStatusToFileStat :: FileStatus -> FileOffset -> FileStat
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

calcFileSize :: Config -> FilePath -> IO FileOffset
calcFileSize cfg path =
  do let paths = primeFiles path cfg
     size' <- fmap (sum . map fileSize) $ mapM getSymbolicLinkStatus paths
     return $ size' - (fromIntegral (length paths) * metadataSize)

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

toPhyChunks :: V.Vector Fd -> FileOffset -> B.ByteString -> [(Fd, FileOffset, B.ByteString)]
toPhyChunks fds off0 src = slice src $ toPhyAddrs fds off0 (fromIntegral . B.length $ src)
  where slice buf ((fd, off, len):rest)
          | B.null buf = []
          | otherwise = let (chunk, buf') = B.splitAt (fromIntegral len) buf
                        in (fd, off, chunk) : slice buf' rest
        slice _ _ = []

fdReadBS :: (Fd, FileOffset, ByteCount) -> IO B.ByteString
fdReadBS (fd, goff, len) =
  do _ <- fdSeek fd AbsoluteSeek (goff + metadataSize)
     B.createAndTrim (fromIntegral len) (\ptr -> fmap fromIntegral (fdReadBuf fd ptr len))

fdWriteBS :: (Fd, FileOffset, B.ByteString) -> IO ByteCount
fdWriteBS (fd, goff, B.PS fptr soff len) = do
  _ <- fdSeek fd AbsoluteSeek (goff + metadataSize)
  withForeignPtr fptr $ \ptr -> fdWriteBuf fd (ptr `plusPtr` soff) (fromIntegral len)

readStripe :: StripeIndex -> V.Vector Fd -> IO (V.Vector B.ByteString)
readStripe si =
  V.mapM (\fd -> fdReadBS (fd, fromIntegral si * defaultChunkSize, defaultChunkSize))

writeStripe :: StripeIndex -> V.Vector Fd -> V.Vector B.ByteString -> IO (V.Vector ByteCount)
writeStripe si =
  V.zipWithM (\fd buf -> fdWriteBS (fd, fromIntegral si * defaultChunkSize, buf))

padZero :: [B.ByteString] -> [B.ByteString]
padZero = map (\bs -> bs <> B.replicate (defaultChunkSize - B.length bs) 0)

completeStripe :: StripeIndex -> V.Vector Fd -> V.Vector Fd -> IO ()
completeStripe si primes spares = do
  _ <- readStripe si primes >>=
       return . F.encode (F.fec k n) . padZero . V.toList >>=
       writeStripe si spares . V.fromList
  return ()
    where k = V.length primes
          n = k + V.length spares

splitSize :: Int -> Int -> FileOffset -> [FileOffset]
splitSize k k' sz =
  let (q, r) = sz `quotRem'` (k * defaultChunkSize)
      go n (x:xs) | n > defaultChunkSize = (x + defaultChunkSize) : go (n - defaultChunkSize) xs
                  | otherwise = (x + n) : xs
      go _ [] = []
  in map (+metadataSize) $
       go r (replicate k (q * defaultChunkSize))
       <> replicate k' ((q + 1) * defaultChunkSize)
