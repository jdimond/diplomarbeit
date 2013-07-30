{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import System.IO

import Search.Common
import Search.Util
import qualified Search.Collection as C
import qualified Search.Cluster as Cl
import qualified Search.SearchIndex as SI

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as MVU
import qualified Data.Vector.Algorithms.Intro as VA

import qualified Data.List as L

import Control.Monad.ST

import Data.Binary

data CompressionArgs = CompressionArgs
    { indexFile :: FilePath
    , collectionDir :: FilePath
    , clusterFile :: FilePath
    } deriving (Eq, Show, A.Data, A.Typeable)

compressionArgs :: CompressionArgs
compressionArgs = CompressionArgs
    { indexFile = A.def &= A.typ "SEARCH_INDEX" &= A.argPos 0
    , collectionDir = A.def &= A.typ "COLLECTION" &= A.argPos 1
    , clusterFile = A.def &= A.typ "CLUSTER_FILE" &= A.argPos 2
    }

log2 :: Integral a => a -> Double
{-# INLINE log2 #-}
log2 a = log (fromIntegral a) / log 2

intSize :: Int -> Int
intSize 0 = error "invalid number"
intSize x = 2 + (ceiling $ log2 x)

eliasGammaSize :: Int -> Int
eliasGammaSize 0 = error "invalid number"
eliasGammaSize x = 1 + 2*(floor $ log2 x)

eliasDeltaSize :: Int -> Int
eliasDeltaSize 0 = error "invalid number"
eliasDeltaSize x = 1 + 2*(floor $ log (log2 $ 2*x) / log 2) + (floor $ log2 x)

golombSize :: Int -> Int -> Int
golombSize _ 0 = error "invalid number"
golombSize b v = (q+1) + bitsr
    where q = (v-1) `div` b
          r = v - q*b - 1
          lb = floor $ log2 b
          bitsr = if (fromIntegral r) < 2^(lb-1)
                     then lb
                     else ceiling $ log2 b


listGolombSize :: Int -> VU.Vector Int -> Int
listGolombSize b = VU.sum . VU.map (golombSize b)

listEliasGammaSize :: VU.Vector Int -> Int
listEliasGammaSize = VU.sum . VU.map eliasGammaSize

listEliasDeltaSize :: VU.Vector Int -> Int
listEliasDeltaSize = VU.sum . VU.map eliasDeltaSize

listIntSize :: VU.Vector Int -> Int
listIntSize = VU.sum . VU.map intSize

golombFactor :: Int -> Int -> Int
golombFactor _ 0 = 2
golombFactor n l = max 2 (round $ (n'/l')*log 2)
    where n' :: Double
          n' = fromIntegral n
          l' = fromIntegral l

hybridSize :: Int -> VU.Vector Int -> Int
hybridSize blocksize v =
    if totalLength == 0
       then 0
       else go 0 0
    where go !o !s
            | o < totalLength =
                let l = min blocksize (totalLength - o)
                    v' = VU.unsafeSlice o l v
                    n = VU.sum v'
                    b = golombFactor n l
                    blockBits = (eliasGammaSize (b-1)) + (listGolombSize b v')
                in go (o+l) (s+blockBits)
            | otherwise = s
          totalLength = VU.length v

toDeltas :: VU.Vector DocId -> VU.Vector Int
toDeltas v
    | VU.length v == 0 = VU.empty
    | otherwise = runST $
        do mv <- VU.thaw $ VU.map (fromIntegral . unDocId) v
           VA.sort mv
           let go !i
                 | i > 0 =
                     do v1 <- mv `MVU.unsafeRead` i
                        v2 <- mv `MVU.unsafeRead` (i-1)
                        MVU.unsafeWrite mv i (v1-v2)
                        go (i-1)
                 | otherwise = return ()
           go ((MVU.length mv)-1)
           v0 <- mv `MVU.unsafeRead` 0
           MVU.unsafeWrite mv 0 (v0+1)
           VU.freeze mv

{-
postingListSize :: VU.Vector DocId -> Int
postingListSize x = fst $ L.foldl' accum (0,0) $ L.sort $ VU.toList x
    where accum (!size, !last) (DocId cur) = (size + gSize (cur+1-last), cur+1)
          me = fromIntegral $ unDocId $ VU.maximum x
          l = fromIntegral $ VU.length x
          gSize = intSize
          {-gSize = if l > 0
                     then golombSize (max 2 (round $ (me/l)*log 2))
                     else golombSize 2 -}
-}

outputTabbed :: [String] -> IO ()
outputTabbed = putStrLn . L.intercalate "\t"

type Config = (String, VU.Vector Int -> Int)

outputPostingListSizes :: FilePath -> C.DocCollection a -> [Config] -> (Cl.Clustering DocId) -> IO ()
outputPostingListSizes fp coll cfgs cl =
    SI.withSearchIndexFile fp $ \h ->
        do hSetBuffering stdout LineBuffering
           putStrLn $ L.intercalate "\t" $
                  ["Size"]
               ++ (map (cfgToHeader "Normal") cfgs)
               ++ (map (cfgToHeader "Cluster") cfgs)
           mapM_ (output h remap) $ SI.handleIndexedTokens h
    where output h r1 t = do l <- SI.readPostingList h t
                             let filtered = VU.filter useDocId l
                                 deltasNormal = toDeltas filtered
                                 deltasCluster = toDeltas $ VU.map r1 filtered
                                 funcs = map snd cfgs
                                 applyFuncs v = map (\f -> f v) funcs
                                 sizesNormal = map show $ applyFuncs deltasNormal
                                 sizesCluster = map show $ applyFuncs deltasCluster
                             outputTabbed ((show $ VU.length filtered):(sizesNormal ++ sizesCluster))
          useDocId = Cl.contains cl
          remap = Cl.unsafeRemap cl
          cfgToHeader prefix (cfgName,_) = prefix ++ "-" ++ cfgName

main :: IO ()
main = do
    args <- A.cmdArgs compressionArgs
    putInfo "Loading Document collection..."
    !coll <- C.loadDocCollection (collectionDir args)
    putInfo "Loading clustering..."
    !cl <- decodeFile $ clusterFile args
    putInfo "Finished loading"
    let cfgs = [ ("Approx", listIntSize)
               , ("Elias-Gamma", listEliasGammaSize)
               , ("Elias-Delta", listEliasDeltaSize)
               , ("Golomb", \v -> listGolombSize (golombFactor (VU.sum v) (VU.length v)) v)
               , ("Hybrid-8", hybridSize 8)
               , ("Hybrid-16", hybridSize 16)
               , ("Hybrid-32", hybridSize 32)
               , ("Hybrid-64", hybridSize 64)
               ]
    outputPostingListSizes (indexFile args) coll cfgs cl
    putInfo "Done..."
