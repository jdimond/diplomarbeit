{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE ScopedTypeVariables #-}

import Prelude hiding (iterate)

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import System.Random.MWC

import Search.Common
import Search.Util
import qualified Search.Dictionary as D
import qualified Search.Collection as C
import Search.Processing (extractWords)
import qualified Search.Cluster as Cl
import qualified Search.Stats as S

import qualified Data.List as L

import qualified Data.Text as T
import qualified Data.Text.Encoding as E

import qualified Data.ByteString as BS

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as MVU
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Mutable as MV

import qualified Data.IntMap.Strict as IM
import Data.Ord (comparing)

import Data.Maybe (catMaybes)

import Control.Monad.Primitive

import Control.Monad
import Control.Concurrent
import qualified Control.Concurrent.ThreadPool as TP

import Data.Binary

import qualified Search.Foreign.FFIInterface as FFI

import Criterion.Measurement

data ClusteringMode = CMNormal | CMTopDown
        deriving (Eq, Show, A.Data, A.Typeable)

data FMFArgs = FMFArgs
    { collection :: FilePath
    , clusterFiles :: [FilePath]
    , numClusters :: Int
    , shrinkFactor :: Double
    , exactScoring :: Bool
    , clusteringMode :: ClusteringMode
    , queryLogFile :: Maybe FilePath
    , numTerms :: Maybe Int
    , docLimit :: Maybe Int
    , randomDocs :: Maybe Int
    } deriving (Eq, Show, A.Data, A.Typeable)

fmfArgs :: FMFArgs
fmfArgs = FMFArgs
    { collection = A.def &= A.typ "COLLECTION" &= A.argPos 0
    , clusterFiles = A.def &= A.typ "CLUSTERFILE" &= A.args
    , numClusters = 100 &= A.help "number of output clusters (default 100)"
    , shrinkFactor = 0.33 &= A.help "shrinking factor for FMF algorithm (default 0.33)"
    , exactScoring = False &= A.explicit &= A.name "exact" &= A.help "slower exact scoring function"
    , clusteringMode = A.enum [ CMNormal &= A.explicit &= A.name "normal" &= A.help "normal clustering"
                              , CMTopDown &= A.explicit &= A.name "topdown" &= A.help "top down clustering" ]
    , queryLogFile = Nothing &= A.explicit &= A.name "use-query-log" &= A.help "use query logs to cluster instead of term frequencies"
    , numTerms = Nothing &= A.explicit &= A.name "num-terms" &= A.help "only the most [num-terms] frequent terms should be used for clustering"
    , docLimit = Nothing &= A.explicit &= A.name "doc-limit" &= A.help "cluster first [doc-limit] documents"
    , randomDocs = Nothing &= A.explicit &= A.name "random-docs" &= A.help "cluster [doc-limit] random documents"
    }

data TermProbabilities = TermProbabilities (VU.Vector Double) Double

countTerms :: Int -> [Token] -> VU.Vector Word32
countTerms size tokens = VU.accum (+) newV tokens'
    where newV = VU.replicate size 0
          tokens' = map (\(Token t) -> (fromIntegral t, 1)) tokens

calcProbs :: Int -> [Token] -> TermProbabilities
calcProbs size tokens = TermProbabilities (VU.map (\x -> x / numTokens) $ VU.accum (+) newV tokens') 0
    where newV = VU.replicate size 0
          tokens' = map (\(Token t) -> (fromIntegral t, 1)) tokens
          numTokens = fromIntegral $ L.length tokens

calcProbsFromTFs :: S.TFCounter -> TermProbabilities
calcProbsFromTFs (S.TFCounter arr) =
    let doublearr :: VU.Vector Double
        doublearr = VU.map fromIntegral arr
        total = VU.sum doublearr
    in TermProbabilities (VU.map (\x -> x / total) doublearr) (1/total)

data FMFClusterConfig cca cc i d =
    FMFClusterConfig
    { fmfNewAccum :: cca
    , fmfAccum :: cca -> d -> cca
    , fmfFreezeAccum :: cca -> cc
    , fmfLoadData :: i -> IO d
    , fmfScore :: d -> cc -> Double
    , fmfScoreAccum :: d -> cca -> Double
    }

minIndexRandV :: (Ord a, PrimMonad m) => Gen (PrimState m) -> V.Vector a -> m Int
{-# INLINE minIndexRandV #-}
minIndexRandV g v =
    do i <- uniformR (0, V.length mins - 1) g
       return $ mins V.! i
  where
    m = V.minimum v
    mins = V.elemIndices m v

fmfClusterInit :: FMFClusterConfig cca cc i d
               -> V.Vector i
               -> IO (V.Vector cca, V.Vector Cl.ClusterId)
fmfClusterInit conf elems =
    do datas <- V.mapM (fmfLoadData conf) elems
       let s = fromIntegral $ V.length elems
       let as = V.map Cl.ClusterId $ V.fromList [0..s-1]
       let accums = V.map (fmfAccum conf (fmfNewAccum conf)) datas
       return (accums, as)

fmfAssignRound :: (Show d, Show cc) => FMFClusterConfig cca cc i d
               -> GenIO
               -> Either (V.Vector cc) (V.Vector cca)
               -> V.Vector i
               -> IO (Double, V.Vector cca, V.Vector Cl.ClusterId)
fmfAssignRound conf gen eCenters elems =
    do let nc = case eCenters of
                  Left c -> V.length c
                  Right a -> V.length a
       accums <- case eCenters of
                   Left _ -> V.replicateM nc (newMVar $ fmfNewAccum conf)
                   Right accs -> V.mapM newMVar accs
       let scoreFunc d = case eCenters of
                         Left c -> return $ V.map (fmfScore conf d) c
                         Right _ -> do accs <- V.mapM readMVar accums
                                       return $ V.map (fmfScoreAccum conf d) accs
       totalScoreM <- newMVar 0
       usedM <- MVU.replicate nc (0 :: Int)
       let assign e = do
             d <- fmfLoadData conf e
             scores <- scoreFunc d
             !mi <- minIndexRandV gen scores
             let score = scores V.! mi
             modifyMVar_ (accums V.! mi) (\cca -> return $! fmfAccum conf cca d)
             modifyMVar_ totalScoreM (\s -> return $! s + score)
             x <- MVU.read usedM mi
             MVU.write usedM mi (x+1)
             return $ Cl.ClusterId $ fromIntegral mi
       as <- V.mapM assign elems
       used <- VU.freeze usedM
       print used
       !endScore <- readMVar totalScoreM
       !newAccums <- V.mapM readMVar accums
       return (endScore, newAccums, as)

fmfClusterRound :: (Show i, Show d, Show cc) => FMFClusterConfig cca cc i d
                -> Int
                -> GenIO
                -> V.Vector i
                -> IO (V.Vector cca, V.Vector Cl.ClusterId)
fmfClusterRound conf nc gen elems =
    do let setSize = V.length elems
       let sf = (0.33 :: Double) -- TODO: make configurable
       let sampleSize = max nc (round $ sf * fromIntegral setSize)
       if setSize > nc
          then do shuffledIndexes <- shuffleV gen $ V.generate setSize id
                  let shuffled = V.backpermute elems shuffledIndexes
                  let (sample, rest) = V.splitAt sampleSize shuffled
                  (subaccums, sampleas) <- fmfClusterRound conf nc gen sample
                  putStrLn $ "################ Running with size: " ++ show setSize
                  if setSize < nc*100
                     then do (score, accums, restas) <- fmfAssignRound conf gen (Right subaccums) rest
                             putStrLn $ "Score: " ++ show score
                             return (accums, fromElems $ V.zip shuffledIndexes (sampleas V.++ restas))
                     else do (score, accums, _) <- fmfAssignRound conf gen (Left $ centers subaccums) elems
                             putStrLn $ "Score: " ++ show score
                             -- run a second round to let the score adapt to bigger cluster
                             -- sizes
                             (score2, accums2, as2) <- fmfAssignRound conf gen (Left $ centers accums) elems
                             putStrLn $ "Score: " ++ show score2
                             iterate score2 accums2 as2
          else fmfClusterInit conf elems
    where iterate score accums as =
              do (newScore, newAccums, newAs) <- fmfAssignRound conf gen (Left $ centers accums) elems
                 putStrLn $ "$$$" ++ show score ++ "->" ++ show newScore
                 if newScore < score*0.999
                    then iterate newScore newAccums newAs
                    else if newScore <= score
                            then return (newAccums, newAs)
                            else return (accums, as)
          centers = V.map (fmfFreezeAccum conf)

topDownRound :: (Show cc, Show d, Show i) => FMFClusterConfig cca cc i d
                -> Int
                -> GenIO
                -> Bool
                -> MVar Int
                -> Int
                -> V.Vector i
                -> IO (V.Vector Cl.ClusterId)
topDownRound conf minClusterSize gen useThreads progMVar total elems =
    do let partitionSize' = 8
       let partitionSize = min partitionSize' (V.length elems `div` minClusterSize)
       let numElems = V.length elems
       if numElems < minClusterSize || partitionSize <= 1
          then do putStrLn $ "Smallest cluster size reached: " ++ (show $ V.length elems)
                  progress <- takeMVar progMVar
                  let newProgress = progress + numElems
                  putStrLn $ "Progress: " ++ show newProgress ++ "/" ++ show total
                  putMVar progMVar newProgress
                  return $ V.map (const $ Cl.ClusterId 0) elems
          else do (_, as) <- fmfClusterRound conf partitionSize gen elems
                  let partitionedIs = partitionElems partitionSize as
                  let partitionedEs = V.map (V.map (elems V.!)) partitionedIs
                  let subRound = topDownRound conf minClusterSize gen False progMVar total
                  subas <- if useThreads
                              then do numProcessors <- getNumCapabilities
                                      pool <- TP.newThreadPool numProcessors
                                      TP.concurrentPoolMap pool subRound partitionedEs
                              else V.mapM subRound partitionedEs
                  --subas <- V.forM partitionedEs $ topDownRound conf minClusterSize gen progMVar total
                  let offsets = V.prescanl' (+) 0 $ V.map ( (+1) . maxCid ) subas
                  let withOffsets = V.map (uncurry withOffset) $ V.zip subas offsets
                  return $ fromElems $ V.zip (flatten partitionedIs) (flatten withOffsets)
    where partitionElems :: Int -> V.Vector Cl.ClusterId -> V.Vector (V.Vector Int)
          partitionElems s as = V.generate s $ flip V.elemIndices as . Cl.ClusterId . fromIntegral
          maxCid :: V.Vector Cl.ClusterId -> Word32
          maxCid v = if V.null v
                      then -1
                      else Cl.unClusterId $ V.maximum v
          flatten = V.foldl1 (V.++)
          withOffset :: V.Vector Cl.ClusterId -> Word32 -> V.Vector Cl.ClusterId
          withOffset v o = V.map (Cl.ClusterId . (+o) . Cl.unClusterId) v

fromElems :: V.Vector (Int, a) -> V.Vector a
{-# INLINE fromElems #-}
fromElems v = V.update empty v
    where empty = V.replicate (V.length v) undefined


fmfClustering :: (Identifier i, Show i, Show d, Show cc)
              => FMFClusterConfig cca cc i d
              -> ClusteringMode
              -> Int
              -> [i]
              -> IO (Cl.Clustering i)
fmfClustering conf mode nc elems =
    do gen <- withSystemRandom (return :: (GenIO -> IO (GenIO)))
       let !elemsV = V.fromList elems
       let numElems = V.length elemsV
       (nc', as) <- case mode of
                      CMNormal -> do (_, as) <- fmfClusterRound conf nc gen elemsV
                                     return (nc, as)
                      CMTopDown -> do pm <- newMVar 0
                                      as <- topDownRound conf nc gen True pm numElems elemsV
                                      let (Cl.ClusterId mcid) = V.maximum as
                                      return (fromIntegral $ mcid + 1, as)
       Cl.clusteringFromList nc' $ V.toList $ V.zip elemsV as

newtype FMFAccum = FMFAccum (IM.IntMap Int)
    deriving (Eq, Show)
data FMFState = FMFState (VU.Vector Double) Int
    deriving (Eq)

instance Show FMFState where
    show (FMFState _ t) = show t

prefixSums :: TermProbabilities -> FMFAccum -> FMFState
prefixSums (TermProbabilities probs _) (FMFAccum m) =
    FMFState (VU.replicate (VU.length probs) totalProb VU.// pfs) total
    where
      freqs = reverse $ L.sortBy (comparing snd) $ IM.toList m
      total = L.foldl' (+) 0 $ map snd freqs
      (pfs,totalProb) = prefixes freqs [] 0 0 0
      prob k = probs VU.! k
      prefixes [] acc _ curSum freqSum = (acc, curSum + freqSum)
      prefixes ((t,freq):ts) acc lastFreq curSum freqSum =
          if lastFreq == freq
             then prefixes ts ((t, curSum):acc) freq curSum (freqSum + prob t)
             else prefixes ts ((t, curSum+freqSum):acc) freq (curSum+freqSum) (prob t)

psScore :: TermProbabilities -> TokenDoc -> FMFState -> Double
psScore (TermProbabilities probs _) doc (FMFState pfs _) =
    L.foldl' (+) 0 $ map innerSum $ tokensFromDoc doc
    where
      innerSum (Token t) = (probs VU.! fromIntegral t) * (pfs VU.! fromIntegral t)

psScoreAccum :: TermProbabilities -> TokenDoc -> FMFAccum -> Double
psScoreAccum (TermProbabilities probs _) doc (FMFAccum m) =
    L.foldl' (+) 0 $ map innerSum $ tokensFromDoc doc
    where
      innerSum t = (probs VU.! fromIntegral (unToken t)) * prefixSum t
      clusterTs = reverse $ L.sortBy (comparing snd) $ IM.toList m
      prefixSumTotal = sum $ map (prob. fromIntegral . fst) clusterTs
      moreTs f = sum $ map (prob . fromIntegral . fst) $ L.takeWhile (\(_, f') -> f' > f) clusterTs
      prefixSum (Token t) = prefixSum' (IM.lookup (fromIntegral t) m)
      prefixSum' Nothing = prefixSumTotal
      prefixSum' (Just f) = moreTs f
      prob k = probs VU.! k


tokenDocAccum :: FMFAccum -> TokenDoc -> FMFAccum
tokenDocAccum (FMFAccum m) = FMFAccum . L.foldl' upd m . tokensFromDoc
    where upd m' (Token t) = IM.insertWith (+) (fromIntegral t) 1 m'

fmfConf :: C.DocCollection a
        -> TermProbabilities
        -> FMFClusterConfig FMFAccum FMFState DocId TokenDoc
fmfConf coll probs = FMFClusterConfig
    { fmfNewAccum = FMFAccum IM.empty
    , fmfAccum = tokenDocAccum
    , fmfFreezeAccum = prefixSums probs
    , fmfLoadData = liftM tokenDoc . C.loadTokenDoc coll
    , fmfScore = psScore probs
    , fmfScoreAccum = psScoreAccum probs
    }

main :: IO ()
main = do
    opts <- A.cmdArgs fmfArgs
    when (length (clusterFiles opts) == 0) $ error "At least one clusterfile has to be given!"
    putInfo $ "Loading Document collection in " ++ (show $ collection opts)
    !coll <- C.loadDocCollection $ collection opts
    !tps <- case (queryLogFile opts) of
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
                         in return arr
                         --return $ calcProbsFromTFs (C.tfCounter coll)

    elemsV <- case (docLimit opts,randomDocs opts) of
                (Just _, Just _) -> error "invalid configuration: use either doc-limit or random-docs"
                (Just limit, Nothing) -> return $! VU.fromList $ take limit $ C.enumerateDocIds coll
                (Nothing, Just rDocs) ->
                    do gen <- withSystemRandom (return :: (GenIO -> IO (GenIO)))
                       let allDocIds = VU.fromList $ C.enumerateDocIds coll
                       shuffled <- shuffleV gen allDocIds
                       return $! VU.fromList $ L.sort $ VU.toList $ VU.take rDocs shuffled
                (Nothing, Nothing) -> return $! VU.fromList $ C.enumerateDocIds coll
    putInfo "Loading collection..."
    clustercoll <- FFI.loadCollection coll elemsV tps (numTerms opts)
    let useTopDown = case (clusteringMode opts) of { CMNormal -> False; CMTopDown -> True }
    putInfo "Clustering..."
    forM_ (clusterFiles opts) $ \file ->
        do putInfo $ "Running clustering for " ++ file
           (time, as) <- time $ FFI.cluster clustercoll (numClusters opts) (shrinkFactor opts) useTopDown (not $ exactScoring opts)
           let actualNumClusters = 1 + (fromIntegral $ Cl.unClusterId $ VU.maximum as)
           clustering <- Cl.clusteringFromList actualNumClusters $ VU.toList $ VU.zip elemsV as
           putInfo $ "Clustering took " ++ (show time) ++ "s"
           putInfo "Saving clustering..."
           encodeFile file clustering
    putInfo "Done..."
