{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}

module Search.Cluster
    (
      ClusterId(ClusterId), unClusterId
    , Clustering
    , clusterToList, enumerateClusterIds
    , clusteringFromList, clusteringToList
    , mapping, contains, maxElem
    , remap, unsafeRemap, unsafeRemapPow
    ) where

import Search.Common

import qualified Search.IndexedSet as SI

import Data.Tuple (swap)
import Data.Maybe
import Data.Function

import Data.List (sortBy, groupBy, mapAccumL)

import Data.Binary
import qualified Data.Binary.Put as P
import qualified Data.Binary.Get as G

import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as MVU

import Control.DeepSeq
import Control.Arrow
import Control.Monad.ST
import Control.Monad.Primitive

_INVALID_CLUSTER_ID_ :: ClusterId
_INVALID_CLUSTER_ID_ = ClusterId (-1)


newtype ClusterId = ClusterId { unClusterId :: Word32 }
    deriving (Show, Eq, Ord, NFData, VG.Vector VU.Vector, VGM.MVector VU.MVector, VU.Unbox)

instance Identifier ClusterId where
    unboxId (ClusterId c) = c
    boxId = ClusterId
    putId (ClusterId c) = P.putWord32be c
    getId = fmap ClusterId G.getWord32be

type Clustering i = SI.IndexedSet ClusterId i

clusteringFromList :: (Identifier i) => Int -> [(i, ClusterId)] -> IO (Clustering i)
clusteringFromList size cs = do
    builder <- SI.newIndexedSetBuilder size
    mapM_ (uncurry (SI.addToIndex builder) . swap) cs
    SI.buildIndex builder

maxElem :: Identifier i => Clustering i -> i
maxElem = VU.maximum . SI.allElems

fillClusterIds :: (PrimMonad m, Identifier i) => ClusterId -> VU.Vector i -> MVU.MVector (PrimState m) ClusterId -> m ()
fillClusterIds cid is v = do
    let numIds = VU.length is
    let fill i
          | i < numIds =
              do let oldid = fromIntegral $ unboxId $ is VU.! i
                 MVU.unsafeWrite v oldid cid
                 fill (i+1)
          | otherwise = return ()
    fill 0
{-# INLINE fillClusterIds #-}

mapping :: Identifier i => Clustering i -> (i -> Maybe ClusterId)
mapping cl =
    let maxId = fromIntegral $ unboxId $ VU.maximum $ SI.allElems cl
        indexV = runST $ do
                    mids <- MVU.replicate (maxId+1) _INVALID_CLUSTER_ID_
                    let update cid = fillClusterIds cid (SI.lookupV cl cid) mids
                    mapM_ update $ enumerateClusterIds cl
                    VU.freeze mids
    in \i -> case indexV VU.!? (fromIntegral $ unboxId i) of
               Nothing -> Nothing
               Just cid -> if cid == _INVALID_CLUSTER_ID_
                              then Nothing
                              else Just cid
{-# INLINE mapping #-}

contains :: Identifier i => Clustering i -> i -> Bool
contains cl = isJust . mapping cl
{-# INLINE contains #-}

clusteringToList :: (Identifier i) => Clustering i -> [(i, ClusterId)]
clusteringToList cl = concatMap tuples $ enumerateClusterIds cl
    where tuples cid = map (\i -> (i,cid)) $ clusterToList cl cid

clusterToList :: (Identifier i) => Clustering i -> ClusterId -> [i]
clusterToList = SI.lookup

enumerateClusterIds :: Clustering i -> [ClusterId]
enumerateClusterIds = SI.enumerateIndexKeys

remap :: (Identifier i) => Clustering i -> i -> i
remap cl = (m V.!) . fromIntegral . unboxId
    where tuples = sortBy (compare `on` snd) $ clusteringToList cl
          ids = map (fromIntegral . unboxId . fst) tuples
          updates = zip ids (map boxId [0..])
          maxId = maximum ids
          m = V.replicate (maxId+1) (error "Identifier not mapped") V.// updates
{-# INLINE remap #-}

sortByV :: (VG.Vector v a) => VA.Comparison a -> v a -> v a
sortByV c v = runST $ do
    m <- VG.thaw v
    VA.sortBy c m
    VG.freeze m
{-# INLINE sortByV #-}

fillRelabeling :: (PrimMonad m, Identifier i) => VU.Vector i -> m (VU.Vector i)
fillRelabeling ids = do
    let size = (fromIntegral $ unboxId $ VU.maximum ids) + 1
    let numIds = VU.length ids
    mids <- MVU.replicate size $ boxId (-1)
    let fill i
          | i < numIds =
              do let oldid = fromIntegral $ unboxId $ ids VU.! i
                 MVU.unsafeWrite mids oldid $ boxId $ fromIntegral i
                 fill (i+1)
          | otherwise = return ()
    fill 0
    VU.freeze mids
{-# INLINE fillRelabeling #-}


unsafeRemap :: (Identifier i) => Clustering i -> i -> i
unsafeRemap cl = (VU.unsafeIndex m) . fromIntegral . unboxId
    where elems = SI.allElems cl
          clmap = mapping cl
          ids = sortByV (compare `on` clmap) elems
          m = runST $ fillRelabeling $ sortByV (compare `on` clmap) ids
{-# INLINE unsafeRemap #-}

remapRange :: Identifier i => (Int, Int) -> [i] -> [(i,i)]
remapRange (l,t) is
    | len >= range = error "to small range to remap"
    | otherwise = map (\(i,c) -> (i, remapElem c)) $ zip is [0..]
  where len = length is
        range = t - l
        remapElem i = boxId $ fromIntegral $ ((i * range) `div` len) + l

unsafeRemapPow :: (Identifier i) => Clustering i -> i -> i
unsafeRemapPow cl = (m VU.!) . fromIntegral . unboxId
    where tuples = sortBy (compare `on` snd) $ clusteringToList cl
          clusters = map (map fst) $ groupBy ((==) `on` snd) $ tuples
          maxSize = maximum $ map length clusters
          bucketRange = 2 ^ (ceiling (log (fromIntegral maxSize) / log 2 :: Double) :: Integer)
          remapBucket o = remapRange (o,o+bucketRange)
          updates = concat $ snd $ mapAccumL (\o is -> (o+bucketRange, remapBucket o is)) 0 clusters
          maxId = fromIntegral $ unboxId $ maximum $ map fst updates
          m = VU.replicate (maxId+1) (boxId $ -1) VU.// map (first (fromIntegral . unboxId)) updates
{-# INLINE unsafeRemapPow #-}
