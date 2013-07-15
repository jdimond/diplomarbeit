{-# LANGUAGE ScopedTypeVariables #-}

module Search.Collection.Writer
    ( Stats
    , DocCollectionWriter
    , withCollectionWriter, saveDoc
    ) where

import System.IO (Handle, withBinaryFile, openBinaryFile, hClose, IOMode(..))

import Control.Concurrent.MVar

import Data.Binary
import Data.Binary.Put
import qualified Data.ByteString.Lazy as BL

import Search.Common
import Search.Collection
import Search.Collection.Internal
import qualified Search.Dictionary as D
import qualified Search.Stats as S

type Doc a = (a, TokenDoc)

type Stats = S.MTFCounter

data DocContainerState =
    DocContainerState
    { containerHandle :: Handle
    , currentOffset :: Word32
    , containerNumber :: Int
    }

data DocCollectionWriterState =
    DocCollectionWriterState
    { indexHandle :: Handle
    , metaHandle :: Handle
    , metaOffsetsHandle :: Handle
    , baseDir :: FilePath
    , currentMetaOffset :: Word64
    , containerWriter :: DocContainerState
    , collectionStats :: Stats
    }

newtype DocCollectionWriter = DocCollectionWriter (MVar DocCollectionWriterState)

updateStats :: Stats -> TokenDoc -> IO Stats
updateStats stats doc =
    do S.countTermFrequencies stats doc
       return stats

_MAX_CONTAINER_FILESIZE_ :: Word32
_MAX_CONTAINER_FILESIZE_ = 1024 * 1024 * 1024 * 3

_MAX_DOC_LENGTH_ :: Integral a => a
_MAX_DOC_LENGTH_ = 65528

newContainerWriter :: FilePath -> Int -> IO DocContainerState
newContainerWriter base num =
    do h <- openBinaryFile (containerFilePath base num) WriteMode
       return $ DocContainerState h 0 num

saveDoc :: (DocMeta a) => DocCollectionWriter -> Doc a -> IO ()
saveDoc (DocCollectionWriter stateMVar) (meta, doc) =
    modifyMVar_ stateMVar $ \state ->
    do let metabs = encode meta
       let bytes = encode doc
       let docLength = min (BL.length bytes) _MAX_DOC_LENGTH_
       let docbytes = BL.take docLength bytes
       cwriter <-
           let cw = containerWriter state
               o = currentOffset cw
           in if o > 0 && o + (fromIntegral docLength) > _MAX_CONTAINER_FILESIZE_
                 then do hClose (containerHandle cw)
                         newContainerWriter (baseDir state) $ containerNumber cw + 1
                 else return cw
       let index = runPut $ do putWord8 $ fromIntegral $ containerNumber cwriter
                               putWord32be $ currentOffset cwriter
                               putWord16be $ fromIntegral $ docLength
       let metaOffsetBs = runPut $ putWord64be $ currentMetaOffset state
       let nmo = currentMetaOffset state + fromIntegral (BL.length metabs)
       let no = currentOffset cwriter + (fromIntegral docLength)
       let nconw = cwriter { currentOffset = no }
       BL.hPutStr (indexHandle state) index
       BL.hPutStr (containerHandle cwriter) docbytes
       BL.hPutStr (metaHandle state) metabs
       BL.hPutStr (metaOffsetsHandle state) metaOffsetBs
       nstats <- updateStats (collectionStats state) doc
       let newstate = state
                    { currentMetaOffset = nmo
                    , collectionStats = nstats
                    , containerWriter = nconw}
       return newstate


withCollectionWriter :: FilePath -> (DocCollectionWriter -> IO D.WordMap) -> IO ()
withCollectionWriter base action =
    withBinaryFile (indexFilePath base) WriteMode $ \docIndexHandle ->
    withBinaryFile (metaFilePath base) WriteMode $ \docMetaHandle ->
    withBinaryFile (metaOffsetsFilePath base) WriteMode $ \docMetaOffsetsHandle ->
    do cwriter <- newContainerWriter base 0
       stats <- S.newMTFCounter
       let state = DocCollectionWriterState
                   { indexHandle = docIndexHandle
                   , metaHandle = docMetaHandle
                   , metaOffsetsHandle = docMetaOffsetsHandle
                   , baseDir = base
                   , currentMetaOffset = 0
                   , containerWriter = cwriter
                   , collectionStats = stats
                   }
       stateMVar <- newMVar state
       wm <- action (DocCollectionWriter stateMVar)
       state' <- readMVar stateMVar
       hClose $ containerHandle $ containerWriter state'
       encodeFile (dictionaryFilePath base) $ D.buildTokenMap wm
       frozenStats <- S.tfFreeze stats
       encodeFile (statsFilePath base) frozenStats
