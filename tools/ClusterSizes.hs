{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

import System.IO (hPutStrLn, stderr)

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Search.Common
import qualified Search.Dictionary as D
import qualified Search.Collection as C
import qualified Search.Cluster as Cl
import qualified Search.SearchIndex as SI
import qualified Search.Stats as S
import Search.Processing (extractWords)

import Control.Monad

import qualified Data.Vector.Unboxed as VU
import qualified Data.List as L
import qualified Data.ByteString as BS
import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import Data.Function
import Data.Binary
import Data.Maybe

data ClusterSizesArgs = ClusterSizesArgs
    { indexFile :: FilePath
    , collectionDir :: FilePath
    , clusterFile :: FilePath
    , queryLogFile :: Maybe FilePath
    , numTerms :: Int
    } deriving (Eq, Show, A.Data, A.Typeable)

clusterSizesArgs :: ClusterSizesArgs
clusterSizesArgs = ClusterSizesArgs
    { indexFile = A.def &= A.typ "SEARCH_INDEX" &= A.argPos 0
    , collectionDir = A.def &= A.typ "COLLECTION" &= A.argPos 1
    , clusterFile = A.def &= A.typ "CLUSTER_FILE" &= A.argPos 2
    , queryLogFile = Nothing &= A.explicit &= A.name "use-query-log" &= A.help "use query logs to determine term frequencies"
    , numTerms = 1000 &= A.explicit &= A.name "num-terms" &= A.help "number of terms to output (default 1000)"
    }

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

clusterSizes :: Cl.Clustering DocId -> VU.Vector DocId -> VU.Vector Word32
clusterSizes cl =
    let clmap = Cl.mapping cl
        clusterRange = 1 + (fromIntegral $ Cl.unClusterId $ maximum $ map snd $ Cl.clusteringToList cl)
        emptyCount = VU.replicate clusterRange 0
        dtoc did = (fromIntegral $ Cl.unClusterId $ fromJust $ clmap did, 1)
    in (VU.accumulate (+) emptyCount) . (VU.map dtoc)

runPostingLists :: FilePath -> VU.Vector Token -> (DocId -> Bool) -> (VU.Vector DocId -> IO ()) -> IO ()
runPostingLists fp tokens useDocId f =
    SI.withSearchIndexFile fp $ \h -> VU.mapM_ (load h) tokens
    where load h t = do l <- SI.readPostingList h t
                        let !filtered = VU.filter useDocId l
                        f filtered

outputSizes :: VU.Vector Word32 -> IO ()
outputSizes = putStrLn . (L.intercalate "\t") . map show . reverse . L.sort . VU.toList

run :: FilePath -> VU.Vector Token -> Cl.Clustering DocId -> (DocId -> Bool) -> IO ()
run fp ts cl useDocId = runPostingLists fp ts useDocId (outputSizes . clusterSizes cl)

countTerms :: Int -> [Token] -> VU.Vector Word32
countTerms size tokens = VU.accum (+) newV tokens'
    where newV = VU.replicate size 0
          tokens' = map (\(Token t) -> (fromIntegral t, 1)) tokens

main :: IO ()
main = do
    args <- A.cmdArgs clusterSizesArgs
    putInfo "Loading Document collection..."
    !coll <- C.loadDocCollection (collectionDir args)
    putInfo "Loading term frequencies..."
    !tfs <- case (queryLogFile args) of
              Just f -> do putInfo "Loading Dictionary..."
                           !dict <- liftM D.buildWordMap $ C.loadTokenMap coll
                           putInfo "Loading queries..."
                           --Somewhere TIO.readFile is slow on large files, so
                           --read with ByteString instead
                           !queries <- liftM E.decodeUtf8 $ BS.readFile f
                           putInfo "Filtering query words..."
                           let terms = concatMap extractWords $ T.lines queries
                           let tokens = catMaybes $ map (D.lookupWord dict) terms
                           --return $ calcProbs (C.dictionarySize coll) tokens
                           return $ countTerms (C.dictionarySize coll) tokens
              Nothing -> let (S.TFCounter arr) = C.tfCounter coll
                         in return $ arr
                         --return $ calcProbsFromTFs (C.tfCounter coll)
    let sortedTerms = reverse $ L.sortBy (compare `on` snd) $ VU.toList $ VU.indexed tfs
    let topTerms = VU.fromList $ map (Token . fromIntegral . fst) $ take (numTerms args) sortedTerms
    putInfo "Loading clustering..."
    !cl <- decodeFile $ clusterFile args
    putInfo "Running for top terms"
    run (indexFile args) topTerms cl (cl `Cl.contains`)
    putInfo "Done..."
