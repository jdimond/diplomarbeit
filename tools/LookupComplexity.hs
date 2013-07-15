{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
import GHC.Conc (numCapabilities)

import System.FilePath.Posix ((</>))
import System.Directory (getDirectoryContents)

import System.IO (hPutStrLn, stderr)

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Search.Common
import qualified Search.Dictionary as D
import qualified Search.Collection as C
import qualified Search.SearchIndex as SI
import Search.Processing (extractWords)

import Control.Monad (liftM, replicateM)
import Control.Concurrent.ThreadPool (concurrentMapN)
import Control.DeepSeq

import qualified Data.List as L

import qualified Data.Vector.Generic as VG
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU

import qualified Data.IntMap.Strict as I

import System.Random (randomRIO)

import Data.Maybe (mapMaybe)

import qualified Data.Text as T
import qualified Data.Text.IO as TIO

import Data.Binary

isSearchIndexFile :: String -> Bool
isSearchIndexFile str | "searchindex_" `L.isPrefixOf` str = True
                      | otherwise = False

type SearchIndexSizes = I.IntMap Int

data PostingsLengths = PostingsLengths
    { totalLength1 :: !Int
    , totalLength2 :: !Int
    , lengths1s :: !(VU.Vector Int)
    , lengths2s :: !(VU.Vector Int)
    }

postingsLengths :: V.Vector (SearchIndexSizes) -> Token -> Token -> PostingsLengths
postingsLengths sizes t1 t2 =
    PostingsLengths
    { totalLength1 = VU.sum $ l1s
    , totalLength2 = VU.sum $ l2s
    , lengths1s = l1s
    , lengths2s = l2s
    }
    where lens (Token t) = I.findWithDefault 0 (fromIntegral t)
          l1s = V.convert $ V.map (lens t1) sizes
          l2s = V.convert $ V.map (lens t2) sizes

type ComplexityFunc = Int -> Int -> Double

minFunc :: ComplexityFunc
minFunc x y = fromIntegral $ min x y

cacheFunc :: ComplexityFunc
cacheFunc x y = if minV == 0
                   then 0
                   else (fromIntegral minV) * (-350*(fromIntegral minV/ fromIntegral maxV) + 400)
    where minV = min x y
          maxV = max x y

logFunc :: ComplexityFunc
logFunc x y = if minV == 0
                 then 0
                 else (fromIntegral minV) * log (fromIntegral minV/ fromIntegral maxV)
    where minV = min x y
          maxV = max x y

sum' :: Num a => [a] -> a
sum' = L.foldl' (+) 0

printSpeedups :: [PostingsLengths] -> IO ()
printSpeedups pls =
    do putStrLn $ "Average speedup: " ++ (show $ sum' speedups / fromIntegral (length speedups))
       let totalComp = sum' $ map complexityTotal pls
       let clusterComp = sum' $ map (complexityCluster True) pls
       let clusterComp' = sum' $ map (complexityCluster False) pls
       putStrLn $ "Total speedup: " ++ show (speedup totalComp clusterComp) ++
                  "(" ++ show totalComp ++ "/" ++ show clusterComp ++ ")"
       putStrLn $ "Total speedup (only list intersection)" ++ show (speedup totalComp clusterComp')
       let clusterContainments = map (VU.length . nonEmpty) pls
       putStrLn $ "Average cluster containment: " ++ show ((fromIntegral $ sum' clusterContainments) / (fromIntegral $ length pls))
    where complexity = minFunc
          complexityTotal pl = complexity (totalLength1 pl) (totalLength2 pl)
          zippedLengths pl = VU.zip (lengths1s pl) (lengths2s pl)
          nonEmpty pl = VU.filter (\(a,b) -> a /= 0 && b /= 0) $ zippedLengths pl
          complexityCluster merge pl =
              let c1 = VU.sum $ VU.map (uncurry complexity) (nonEmpty pl)
                  c2 = complexity (VU.length $ zippedLengths pl) (VU.length $ nonEmpty pl)
              in c1 + (if merge then c2 else 0)
          speedup x y = if y == 0 then 1 else x/y
          speedups = map (\pl -> speedup (complexityTotal pl) (complexityCluster True pl)) pls

printRatios :: [PostingsLengths] -> IO ()
printRatios pls = mapM_ printRatio pls
    where printRatio pl =
              do let op = ratioOp pl
                 putStr $ show $ op (totalLength1 pl) (totalLength2 pl)
                 putStr " "
                 let zipped = VU.zip (lengths1s pl) (lengths2s pl)
                 let printSingle (l1,l2) = do putStr $ (show $ op l1 l2) ++ " "
                 VU.mapM_ printSingle zipped
                 putStrLn ""
          safeDiv x y = if y == 0
                           then 0
                           else (fromIntegral x) / (fromIntegral y)
          ratioOp :: PostingsLengths -> Int -> Int -> Double
          ratioOp pl = if (totalLength1 pl > totalLength2 pl)
                          then flip safeDiv
                          else safeDiv

printSizes :: [PostingsLengths] -> IO ()
printSizes pls = mapM_ printSize pls
    where printSize pl =
              do putStrLn $ showSizes (totalLength1 pl) (totalLength2 pl)
                 let zipped = V.convert $ VU.zip (lengths1s pl) (lengths2s pl)
                 putStrLn $ show $ V.map (uncurry showSizes) zipped
              where showSizes a b = show a ++ "/" ++ show b

pickV :: VG.Vector v a => v a -> IO a
pickV v
    | VG.length v == 0 = error "empty vector"
    | otherwise       = liftM (v VG.!) $ randomRIO (0, VG.length v - 1)

readLine :: D.WordMap -> T.Text -> Maybe (Token, Token)
readLine dict line =
    let terms = extractWords line
        tokens = L.nub $ mapMaybe (D.lookupWord dict) terms
    in if length tokens /= 2
          then Nothing
          else Just $ (head tokens, tokens !! 1)

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

data StatsMode = SMSpeedup | SMClusterRatios | SMClusterSizes
    deriving (Eq, Show, A.Data, A.Typeable)

data LArgs = LArgs
    { indexDir :: FilePath
    , collectionDir :: FilePath
    , randomQueries :: Maybe Int
    , statsMode :: StatsMode
    } deriving (Eq, Show, A.Data, A.Typeable)

lArgs :: LArgs
lArgs = LArgs
    { indexDir = A.def &= A.typ "SEARCH_INDEX_DIR" &= A.argPos 0
    , collectionDir = A.def &= A.typ "COLLECTION" &= A.argPos 1
    , randomQueries = Nothing &= A.typ "NUM" &= A.help "use n randomly generated queries"
    , statsMode = A.enum [ SMSpeedup &= A.explicit &= A.name "speedup" &= A.help "show speedup stats"
                         , SMClusterRatios &= A.explicit &= A.name "ratio" &= A.help "show ratios of list length"
                         , SMClusterSizes &= A.explicit &= A.name "sizes" &= A.help "show list sizes"
                         ]
    }

loadSearchIndexSizes :: FilePath -> IO SearchIndexSizes
loadSearchIndexSizes path =
    do putInfo $ "Loading index file " ++ show path
       si <- decodeFile path
       let !sizesMap = VU.foldl' add I.empty $ VU.indexed $ SI.numDocsPerTerm si
       return sizesMap
    where add m (i,v)
            | v == 0    = m
            | otherwise = I.insert i v m

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

main :: IO ()
main = do
    args <- A.cmdArgs lArgs
    let baseDir = indexDir args
    putInfo $ "Loading Document collection"
    !coll <- C.loadDocCollection (collectionDir args)
    putInfo $ "Loading queries"
    !queries <- case (randomQueries args) of
                  Just num -> do replicateM num (pickRandomQuery coll)
                  Nothing -> do !dict <- liftM D.buildWordMap $ C.loadTokenMap coll
                                ls <- liftM T.lines TIO.getContents
                                return $!! mapMaybe (readLine dict) ls
    putInfo $ "Loading indexes"
    files <- getDirectoryContents baseDir
    let indexFiles = map (baseDir </>) $ filter isSearchIndexFile files
    putInfo $ "Number of clusters: " ++ (show $ length indexFiles)
    !ci <- liftM V.fromList $ concurrentMapN numCapabilities loadSearchIndexSizes indexFiles
    let lens = map (uncurry $ postingsLengths ci) queries
    case (statsMode args) of
        SMSpeedup -> printSpeedups lens
        SMClusterRatios -> printRatios lens
        SMClusterSizes -> printSizes lens
