{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
import GHC.Conc (numCapabilities)

import System.FilePath.Posix ((</>))

import System.IO

import System.Directory
import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Search.Common
import qualified Search.Collection as C
import qualified Search.SearchIndex as SI
import qualified Search.Cluster as Cl


import Data.Binary

import Control.Concurrent.ThreadPool

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

buildSearchIndex :: C.DocCollection a -> [DocId] -> IO SI.SearchIndex
buildSearchIndex coll dids = do
    builder <- SI.newSearchIndexBuilder coll
    putInfo "Loading docs..."
    docs <- C.loadTokenDocs coll dids
    putInfo "Indexing docs..."
    mapM_ (SI.indexTokenDoc builder) docs
    putInfo "Building index..."
    SI.buildSearchIndex builder

saveClusterSearchIndex :: FilePath -> C.DocCollection a -> Cl.Clustering DocId -> Cl.ClusterId -> IO ()
saveClusterSearchIndex baseDir coll clustering cid = do
    let dids = Cl.clusterToList clustering cid
    index <- buildSearchIndex coll dids
    let path = baseDir </> "searchindex_" ++ show (Cl.unClusterId cid)
    putInfo "Saving index..."
    encodeFile path index
    putInfo $ "Saved index for Cluster " ++ show cid

chunked :: Int -> [a] -> [[a]]
chunked _ [] = []
chunked size xs =
    let (chunk, rest) = splitAt size xs
    in chunk:chunked size rest

saveTempIndex :: FilePath -> C.DocCollection a -> [DocId] -> IO ()
saveTempIndex f coll dids = do
    fileExists <- doesFileExist f
    if not fileExists
       then do builder <- SI.newSearchIndexBuilder coll
               let processChunk chunk =
                       do putInfo $ "Processing chunk of size " ++ show (length chunk) ++ "..."
                          docs <- C.loadTokenDocs coll chunk
                          mapM_ (SI.indexTokenDoc builder) docs
               mapM_ processChunk $ chunked 100000 dids
               putInfo "Building temporary index..."
               index <- SI.buildSearchIndex builder
               putInfo "Saving..."
               encodeFile f index
               putInfo "Saved temporary index"
       else do putInfo "Index exists already, skipping..."
               return ()


saveSearchIndex :: FilePath -> C.DocCollection a -> Maybe Int -> Int -> IO ()
saveSearchIndex indexFile coll limit chunkSize = do
    let go i [] files = return files
        go i (ds:dss) files =
            do let f = tmpFileName i
               saveTempIndex f coll ds
               go (i+1) dss $ f:files
    putInfo "Generating temporary indexes..."
    files <- go 0 (chunked chunkSize dids) []
    putInfo "Joining index..."
    SI.joinIndexes files indexFile
    putInfo "Removing indexes..."
    mapM_ removeFile files
  where dids = case limit of
                 Just i -> take i $ C.enumerateDocIds coll
                 Nothing -> C.enumerateDocIds coll
        tmpFileName i = indexFile ++ ".tmp" ++ show i


data BIArgs = BIArgs
    { collectionDir :: FilePath
    , outputDir :: FilePath
    , clusteringFile :: Maybe FilePath
    , docLimit :: Maybe Int
    , tmpIndexSize :: Int
    } deriving (Eq, Show, A.Data, A.Typeable)

biArgs :: BIArgs
biArgs = BIArgs
    { collectionDir = A.def &= A.typ "COLLECTION_DIR" &= A.argPos 0
    , outputDir = A.def &= A.typ "OUTPUT_DIR/FILE" &= A.argPos 1
    , docLimit = A.def &= A.explicit &= A.name "doc-limit" &= A.help "maximum number of docs to cluster"
    , clusteringFile = Nothing &= A.explicit &= A.name "cluster-file" &= A.help "the clustering to use"
    , tmpIndexSize = 1000000 &= A.explicit &= A.name "temp-index-size" &= A.help "number of docs per temporary index"
    }

main :: IO ()
main = do
    args <- A.cmdArgs biArgs
    putInfo "Loading Document Collection..."
    !coll <- C.loadDocCollection (collectionDir args)
    case (clusteringFile args) of
      Just f -> do putInfo "Loading clustering..."
                   clustering <- decodeFile f
                   putInfo "Building Index..."
                   let save = saveClusterSearchIndex (outputDir args) coll clustering
                   --concurrentMapN_ numCapabilities save $ Cl.enumerateClusterIds clustering
                   mapM_ save $ Cl.enumerateClusterIds clustering
      Nothing -> do putInfo "Building Index..."
                    saveSearchIndex (outputDir args) coll (docLimit args) (tmpIndexSize args)
