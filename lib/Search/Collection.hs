{-# LANGUAGE BangPatterns, MagicHash #-}
{-# LANGUAGE FlexibleInstances #-}

module Search.Collection
    (
      DocCollection, DocMeta, FileDocCollection, FragmentDocCollection
    , FileFragment(..)
    , loadDocCollection, loadTokenDoc, loadTokenDocs, loadDocMeta, loadTokenMap
    , tfCounter
    , collectionSize, enumerateDocIds, dictionarySize, documentLength
    , maxDocId
    ) where

import qualified System.IO  as IO

import System.IO.MMap

import Control.DeepSeq

import Control.Arrow ((&&&))
import Control.Monad
import Data.Function (on)

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.List as L

import Data.Bits

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put

import Data.Ord

import Data.Text.Encoding (encodeUtf8, decodeUtf8)
import qualified Data.Text as T

import Search.Common
import Search.Collection.Internal
import qualified Search.Stats as S
import qualified Search.Dictionary as D

-- needed for unboxing with magic hash
import GHC.Base
import GHC.Word

class Binary a => DocMeta a

instance DocMeta FilePath

type FileDocCollection = DocCollection FilePath

data FileFragment =
    FileFragment
    { fragmentFile :: FilePath
    , fragmentOffset :: Word32
    , fragmentLength :: Word32
    } deriving (Show, Eq)

instance Binary FileFragment where
  put f =
    do let fpbs = encodeUtf8 $ T.pack $ fragmentFile f
       putWord32be $ fromIntegral $ B.length fpbs
       putByteString fpbs
       putWord32be $ fragmentOffset f
       putWord32be $ fragmentLength f
  get = do len <- getWord32be
           fpbs <- getByteString $ fromIntegral len
           foffset <- getWord32be
           flength <- getWord32be
           return FileFragment
                  { fragmentFile = T.unpack $ decodeUtf8 fpbs
                  , fragmentOffset = foffset
                  , fragmentLength = flength
                  }

instance DocMeta FileFragment

type FragmentDocCollection = DocCollection FileFragment

data DocCollection a =
    DocCollection
    { baseDir :: !FilePath
    , containerNumbers :: !(VU.Vector Word8)
    , containerOffsets :: !(VU.Vector Word32)
    , documentLengths :: !(VU.Vector Word16)
    , tfCounter :: !S.TFCounter -- TODO: Replace this with stats
    }

_INDEX_ENTRY_SIZE_ :: Num a => a
_INDEX_ENTRY_SIZE_ = 1+4+2

shiftl_w16 :: Word16 -> Int -> Word16
shiftl_w16 (W16# w) (I# i) = W16# (w `uncheckedShiftL#`   i)

shiftl_w32 :: Word32 -> Int -> Word32
shiftl_w32 (W32# w) (I# i) = W32# (w `uncheckedShiftL#`   i)

getContainer :: B.ByteString -> Int -> Word8
getContainer s entry = s `B.unsafeIndex` (_INDEX_ENTRY_SIZE_ * entry)
{-# INLINE getContainer #-}

getOffset :: B.ByteString -> Int -> Word32
getOffset s entry =
    let offset = _INDEX_ENTRY_SIZE_ * entry + 1
        w = (fromIntegral (s `B.unsafeIndex` (offset + 0)) `shiftl_w32` 24) .|.
            (fromIntegral (s `B.unsafeIndex` (offset + 1)) `shiftl_w32` 16) .|.
            (fromIntegral (s `B.unsafeIndex` (offset + 2)) `shiftl_w32`  8) .|.
            (fromIntegral (s `B.unsafeIndex` (offset + 3)))
    in w `seq` w
{-# INLINE getOffset #-}

getLength :: B.ByteString -> Int -> Word16
getLength s entry =
    let offset = _INDEX_ENTRY_SIZE_*entry + 5
        w = (fromIntegral (s `B.unsafeIndex` (offset + 0)) `shiftl_w16` 8) .|.
            (fromIntegral (s `B.unsafeIndex` (offset + 1)))
    in w `seq` w
{-# INLINE getLength #-}

_INDEX_CHUNK_SIZE_:: Num a => a
_INDEX_CHUNK_SIZE_ = 1000000

getEntry :: VU.Unbox a => (B.ByteString -> Int -> a) -> V.Vector B.ByteString -> Int -> a
getEntry f v entry =
    let chunk = entry `div` _INDEX_CHUNK_SIZE_
        offset = entry `mod` _INDEX_CHUNK_SIZE_
    in f (v V.! chunk) offset
{-# INLINE getEntry #-}

getChunks :: BL.ByteString -> V.Vector B.ByteString
getChunks = V.unfoldr f
    where f bs | BL.null bs = Nothing
               | otherwise = let (chunkL, rest) = BL.splitAt (_INDEX_CHUNK_SIZE_ * _INDEX_ENTRY_SIZE_) bs
                                 chunk = BL.toStrict chunkL
                             in Just (chunk `seq` chunk, rest)

loadDocCollection :: FilePath -> IO (DocCollection a)
loadDocCollection base = do
    -- Offsets
    IO.hPutStrLn IO.stderr "Reading index file..."
    indexbs <- BL.readFile $ indexFilePath base
    when (BL.length indexbs `mod` _INDEX_ENTRY_SIZE_ /= 0) $ error "invalid document index"
    let numEntries = fromIntegral $ BL.length indexbs `div` _INDEX_ENTRY_SIZE_
    let chunks = getChunks indexbs
    IO.hPutStrLn IO.stderr "Loading container indexes..."
    let !cv = VU.generate numEntries (getEntry getContainer chunks)
    IO.hPutStrLn IO.stderr "Loading document offsets..."
    let !ov = VU.generate numEntries (getEntry getOffset chunks)
    IO.hPutStrLn IO.stderr "Loading document lengths..."
    let !dlv = VU.generate numEntries (getEntry getLength chunks)
    -- Stats
    IO.hPutStrLn IO.stderr "Loading document frequencies..."
    let tfFile = statsFilePath base
    !tfs <- decodeFile tfFile
    IO.hPutStrLn IO.stderr "Finished loading document collection"
    return DocCollection
           { baseDir = base
           , containerNumbers = cv
           , containerOffsets = ov
           , documentLengths = dlv
           , tfCounter = tfs
           }

collectionSize :: DocCollection a -> Int
collectionSize collection = VU.length $ containerOffsets collection

maxDocId :: DocCollection a -> DocId
maxDocId collection = DocId $ fromIntegral $ collectionSize collection - 1

enumerateDocIds :: DocCollection a -> [DocId]
enumerateDocIds collection = map DocId [0..(unDocId $ maxDocId collection)]

loadTokenMap :: DocCollection a -> IO D.TokenMap
loadTokenMap coll = decodeFile dictFile
    where dictFile = dictionaryFilePath $ baseDir coll

loadTokenDoc :: DocCollection a -> DocId -> IO IndexedTokenDoc
loadTokenDoc coll did = liftM head $ loadTokenDocs coll [did]

documentLength :: DocCollection a -> DocId -> Int
documentLength coll did = (fromIntegral $ (documentLengths coll) VU.! (fromIntegral $ unDocId did)) `div` 4

myGroupBy :: (Ord b) => (a -> b) -> [a] -> [(b, [a])]
myGroupBy f = map (f . head &&& id)
                   . L.groupBy ((==) `on` f)
                   . L.sortBy (compare `on` f)

loadTokenDocs :: DocCollection a -> [DocId] -> IO [IndexedTokenDoc]
loadTokenDocs coll dids = do
    let grouped = myGroupBy ((containerNumbers coll VU.!) . fromIntegral . unDocId . snd) $ zip [(0 :: Int)..] dids
    liftM (map snd . L.sortBy (compare `on` fst) . concat) $ mapM loadDocs grouped
    where loadDocs (con, idpairs) = do
              let (origIndexes, ids) = unzip $ L.sortBy (comparing snd) idpairs
              let offsets = map (containerOffsets coll VU.!) $ map (fromIntegral . unDocId) ids
              let lens = map (documentLengths coll VU.!) $ map (fromIntegral . unDocId) ids
              let fileOffset = minimum offsets
              let (maxOffset,docLen) = L.maximumBy (compare `on` fst) $ zip offsets lens
              let readLen = fromIntegral $ maxOffset + (fromIntegral docLen) - fileOffset
              let containerfp = containerFilePath (baseDir coll) $ fromIntegral con
              fileContents <- mmapFileByteString containerfp $ Just (fromIntegral fileOffset, readLen)
              let subrange off len bs = B.copy $ B.take (fromIntegral len) $ B.drop (fromIntegral $ off - fileOffset) bs
              let extractDoc bs (did, off, len) =
                        IndexedTokenDoc did $ decode $ BL.fromChunks [subrange off len bs]
              let docs = map (extractDoc fileContents) $ zip3 ids offsets lens
              return $!! zip origIndexes docs

loadDocMeta :: (DocMeta a) => DocCollection a -> DocId -> IO a
loadDocMeta coll did = do
    let docIndex = fromIntegral $ unDocId did
    let metaOffsets = metaOffsetsFilePath $ baseDir coll
    offset <- IO.withBinaryFile metaOffsets IO.ReadMode $ \handle -> do
                  IO.hSeek handle IO.AbsoluteSeek $ docIndex*8
                  bytes <- BL.hGet handle 8
                  return $ fromIntegral $ runGet getWord64be bytes
    let metafp = metaFilePath $ baseDir coll
    IO.withBinaryFile metafp IO.ReadMode $ \handle -> do
        IO.hSeek handle IO.AbsoluteSeek offset
        bytes <- BL.hGetContents handle
        return $! decode bytes

dictionarySize :: DocCollection a -> Int
dictionarySize coll = S.tfSize $ tfCounter coll
