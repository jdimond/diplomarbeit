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
import Search.Processing (extractWords)
import qualified Search.Foreign.ClusterLookupFFI as CLF
import qualified Search.Intersect.Lookup as LU

import System.Random (randomRIO)

import qualified Data.HashMap.Strict as HM
import qualified Data.HashSet as HS

import Control.Monad

import Control.DeepSeq

import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Unboxed as VU

import qualified Data.List as L
import Data.Maybe

import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Data.Binary


pickV :: VG.Vector v a => v a -> IO a
pickV v
    | VG.length v == 0 = error "empty vector"
    | otherwise       = liftM (v VG.!) $ randomRIO (0, VG.length v - 1)

readLine :: Bool -> D.WordMap -> T.Text -> [(Token, Token)]
readLine expandQueries dict line =
    let terms = extractWords line
        tokens = mapM (D.lookupWord dict) terms
    in case tokens of
         Just ts ->
           let queries = [(x,y) | x <- ts, y <- ts, x<y]
           in if expandQueries || L.length queries == 1 then queries else []
         Nothing -> []

pickRandomQuery :: C.DocCollection a -> IO (Token, Token)
pickRandomQuery coll =
    do did <- liftM (DocId . fromIntegral) $ randomRIO (0, C.collectionSize coll - 1)
       doc <- C.loadTokenDoc coll did
       let docV = vectorFromDoc $ tokenDoc doc
       if VU.length docV == 0
          then pickRandomQuery coll
          else do t1 <- pickV docV
                  t2 <- pickV docV
                  return (t1,t2)

data BenchArgs = BenchArgs
    { indexFile :: FilePath
    , collectionDir :: FilePath
    , clusterFile :: FilePath
    , randomQueries :: Maybe Int
    , numRuns :: Int
    , expandQueries :: Bool
    } deriving (Eq, Show, A.Data, A.Typeable)

benchArgs :: BenchArgs
benchArgs = BenchArgs
    { indexFile = A.def &= A.typ "SEARCH_INDEX" &= A.argPos 0
    , collectionDir = A.def &= A.typ "COLLECTION" &= A.argPos 1
    , clusterFile = A.def &= A.typ "CLUSTER_FILE" &= A.argPos 2
    , randomQueries = Nothing &= A.typ "NUM" &= A.help "use n randomly generated queries"
    , numRuns = 1 &= A.help "number of iterations for benchmark (default=1)"
    , expandQueries = False &= A.explicit &= A.name "expand-queries" &= A.help "expand n term queries into multiple 2-term queries"
    }

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

type PostingListMap = HM.HashMap Token (VU.Vector DocId)
type LookupMap = HM.HashMap Token LU.Lookup
type ComplexityMap = HM.HashMap Token (VU.Vector Word32)
data ClusterLookup = ClusterLookup !CLF.InvertedIndex !(HM.HashMap Token Token)

instance NFData ClusterLookup
instance NFData LU.Lookup

newClusterLookup :: VU.Vector Token -> Cl.Clustering DocId -> IO ClusterLookup
newClusterLookup ts cl =
    do let remap = HM.fromList $ zip (VU.toList ts) (map Token [0..])
       ccl <- CLF.newClustering cl
       i <- CLF.newInvertedIndex ccl $ VU.length ts
       return $! ClusterLookup i remap

loadLookup :: (DocId-> DocId) -> Token -> LookupMap -> VU.Vector DocId -> IO LookupMap
loadLookup remap t m v =
    do let !l = LU.new $ VU.map unDocId $ VU.map remap v
       return $! HM.insert t l m

loadClusterLookup :: ClusterLookup -> Token -> () -> VU.Vector DocId -> IO ()
loadClusterLookup (ClusterLookup ii remap) t () v =
    do let newtoken = remap HM.! t
       CLF.setDocs ii newtoken v

clusterSizes :: Cl.Clustering DocId -> VU.Vector DocId -> VU.Vector Word32
clusterSizes cl =
    let clmap = Cl.mapping cl
        clusterRange = 1 + (fromIntegral $ Cl.unClusterId $ maximum $ map snd $ Cl.clusteringToList cl)
        emptyCount = VU.replicate clusterRange 0
        dtoc did = (fromIntegral $ Cl.unClusterId $ fromJust $ clmap did, 1)
    in (VU.accumulate (+) emptyCount) . (VU.map dtoc)

type LoadFunction a = (Token -> a -> VU.Vector DocId -> IO a) -> a -> IO a

loadPostingLists :: FilePath -> VU.Vector Token -> (DocId -> Bool) -> LoadFunction a
loadPostingLists fp tokens useDocId f e =
    SI.withSearchIndexFile fp $ \h -> VU.foldM' (load h) e tokens
    where load h a t = do l <- SI.readPostingList h t
                          let !filtered = VU.filter useDocId l
                          f t a filtered

lookupComplexity :: ComplexityMap -> (Token, Token) -> (Int, Int)
lookupComplexity m (t1,t2) =
    case (HM.lookup t1 m, HM.lookup t2 m) of
      (Just l1, Just l2) ->
         let !intersectComplexity = VU.sum $ VU.map fromIntegral $ VU.zipWith min l1 l2
             !clusterComplexity = VU.sum $ VU.map fromIntegral $ VU.zipWith boolmin l1 l2
         in force $ (intersectComplexity, clusterComplexity)
      _ -> error $ "Term " ++ show t1 ++ " and " ++ show t2 ++ " not loaded"
    where boolmin a b = min 1 (min a b)

benchQueryLookup :: LookupMap -> (Token, Token) -> IO (Double, VU.Vector DocId)
benchQueryLookup m (t1,t2) =
    case (HM.lookup t1 m, HM.lookup t2 m) of
      (Just l1, Just l2) ->
          do (t,v) <- LU.benchIntersect l1 l2
             return (t, VU.map DocId v)
      _ -> error $ "Term " ++ show t1 ++ " and " ++ show t2 ++ " not loaded"

benchQueryCluster :: ClusterLookup -> (Token, Token) -> IO (Double, VU.Vector DocId)
benchQueryCluster (ClusterLookup i rm) (t1,t2) =
    let t1' = rm HM.! t1
        t2' = rm HM.! t2
    in CLF.benchIntersect i t1' t2'

benchLookup :: LookupMap -> VU.Vector (Token, Token) -> IO (VU.Vector Double)
benchLookup l = VU.mapM (liftM fst . benchQueryLookup l)

benchClusterLookup :: ClusterLookup -> VU.Vector (Token, Token) -> IO (VU.Vector Double)
benchClusterLookup l = VU.mapM (liftM fst . benchQueryCluster l)

resultSizes :: LookupMap -> VU.Vector (Token, Token) -> IO (VU.Vector Int)
resultSizes l = VU.mapM (liftM (VU.length . snd) . benchQueryLookup l)

postingListSizes :: PostingListMap -> VU.Vector (Token, Token) -> VU.Vector (Int, Int)
postingListSizes pm qs = VU.map lens qs
    where len t = VU.length $ pm HM.! t
          lens (t1,t2) = (len t1, len t2)

runNormalBenchmark :: Int -> LoadFunction LookupMap -> VU.Vector (Token, Token) -> IO [BenchmarkResult]
runNormalBenchmark runs load qs =
    do putInfo "Loading normal index..."
       !lm <- load (loadLookup id) HM.empty
       putInfo "Benchmarking..."
       res <- forM [0..runs-1] $ \i ->
                do ts <- benchLookup lm qs
                   return $ genResultDouble ("normal-"++ show i) ts
       putInfo "Benchmarking sizes..."
       sizes <- resultSizes lm qs
       return $ (genResultInt "result-size" sizes):res

runRelabeledBenchmark :: Int -> LoadFunction LookupMap -> Cl.Clustering DocId -> VU.Vector (Token, Token) -> IO [BenchmarkResult]
runRelabeledBenchmark runs load cl qs =
    do putInfo "Loading relabeled index..."
       !lm <- load (loadLookup (Cl.unsafeRemap cl)) HM.empty
       putInfo "Benchmarking..."
       forM [0..runs-1] $ \i ->
         do ts <- benchLookup lm qs
            return $ genResultDouble ("relabeled-"++ show i) ts

runClusterBenchmark :: Int -> LoadFunction () -> VU.Vector Token -> Cl.Clustering DocId -> VU.Vector (Token, Token) -> IO [BenchmarkResult]
runClusterBenchmark runs load ts cl qs =
    do putInfo "Loading cluster index..."
       ii <- newClusterLookup ts cl
       load (loadClusterLookup ii) ()
       putInfo "Benchmarking..."
       forM [0..runs-1] $ \i ->
           do times <- benchClusterLookup ii qs
              return $ genResultDouble ("cluster-"++ show i) times

runSizesBenchmark :: LoadFunction PostingListMap -> Cl.Clustering DocId -> VU.Vector (Token, Token) -> IO [BenchmarkResult]
runSizesBenchmark load cl qs =
    do putInfo "Loading index..."
       let loadList t m v = return $! HM.insert t v m
       plm <- load loadList HM.empty
       putInfo "Getting posting list sizes..."
       let sizes = postingListSizes plm qs
       let !sizesSmall = VU.map (uncurry min) sizes
       let !sizesBig = VU.map (uncurry max) sizes
       putInfo "Building complexity map..."
       let !csizes = force $ HM.map (clusterSizes cl) plm
       putInfo "Calculating complexities..."
       -- the first bang is important to enforce strict evaluation
       let !(!intersect, !cluster) = VU.unzip $ VU.map (lookupComplexity csizes) qs
       return [ genResultInt "size-small" sizesSmall
              , genResultInt "size-big" sizesBig
              , genResultInt "intersect-comp" intersect
              , genResultInt "cluster-comp" cluster]


data BenchmarkData =   BenchmarkDataDouble !(VU.Vector Double)
                     | BenchmarkDataInt !(VU.Vector Int)

benchmarkLength :: BenchmarkData -> Int
benchmarkLength (BenchmarkDataDouble v) = VU.length v
benchmarkLength (BenchmarkDataInt v) = VU.length v

showDataElem :: BenchmarkData -> Int -> String
showDataElem (BenchmarkDataDouble v) i = show $ v VU.! i
showDataElem (BenchmarkDataInt v) i = show $ v VU.! i

genResultInt :: String -> VU.Vector Int -> BenchmarkResult
genResultInt header dat = BenchmarkResult header (BenchmarkDataInt dat)

genResultDouble :: String -> VU.Vector Double -> BenchmarkResult
genResultDouble header dat = BenchmarkResult header (BenchmarkDataDouble dat)

data BenchmarkResult =
    BenchmarkResult
    { benchmarkHeader :: !String
    , benchmarkData :: !BenchmarkData
    }

runBenchmark :: FilePath -> Int -> VU.Vector Token -> Cl.Clustering DocId -> (DocId -> Bool) -> VU.Vector (Token, Token) -> IO [BenchmarkResult]
runBenchmark fp runs ts cl useDocId qs =
    do sizes <- runSizesBenchmark loadFunc cl qs
       normal <- runNormalBenchmark runs loadFunc qs
       relabeled <- runRelabeledBenchmark runs loadFunc cl qs
       cluster <- runClusterBenchmark runs loadFunc ts cl qs
       putInfo "Done Benchmarking..."
       return $ concat [sizes, normal, relabeled, cluster]
    where loadFunc = loadPostingLists fp ts useDocId

benchmarkToLines :: [BenchmarkResult] -> [String]
benchmarkToLines bench = header : datalines
    where numQueries = benchmarkLength $ benchmarkData $ head bench
          toColumns = L.intercalate "\t"
          header = toColumns $ map benchmarkHeader bench
          dataline i = toColumns $ map (\b -> showDataElem (benchmarkData b) i) bench
          datalines = map dataline [0..numQueries-1]

main :: IO ()
main = do
    args <- A.cmdArgs benchArgs
    putInfo "Loading Document collection..."
    !coll <- C.loadDocCollection (collectionDir args)
    putInfo "Loading queries..."
    !queries <- case (randomQueries args) of
                  Just num -> do replicateM num (pickRandomQuery coll)
                  Nothing -> do putInfo "Loading dictionary..."
                                !dict <- liftM D.buildWordMap $ C.loadTokenMap coll
                                putInfo "Reading standard input..."
                                ls <- liftM T.lines TIO.getContents
                                return $!! concatMap (readLine (expandQueries args) dict) ls
    let !tokens = VU.fromList $ L.sort $ HS.toList $ HS.fromList $ uncurry (++) $ unzip queries
    let !queryv = VU.fromList queries
    putInfo "Loading clustering..."
    !cl <- decodeFile $ clusterFile args
    benchmark <- runBenchmark (indexFile args) (numRuns args) tokens cl (cl `Cl.contains`) queryv
    mapM_ putStrLn $ benchmarkToLines benchmark
    putInfo "Done..."
