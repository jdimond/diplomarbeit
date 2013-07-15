{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# OPTIONS_GHC -fno-cse -fno-full-laziness #-} -- recommended by GHC manual
{-# OPTIONS_GHC -Wwarn #-}

#include "clusterlookup.h"

module Search.Foreign.ClusterLookupFFI
    (
      InvertedIndex
    , newClustering, newInvertedIndex, setDocs, intersect
    , benchIntersect
    ) where

import Data.Word
import Foreign.Ptr
import Foreign.ForeignPtr

import Control.Monad

import qualified Data.Vector.Storable as SV
import qualified Data.Vector.Storable.Mutable as MSV
import qualified Data.Vector.Unboxed as VU

import Search.Common
import qualified Search.Cluster as Cl

import Criterion.Measurement

data ClusteringPtr
data InvertedIndexPtr

type Clustering = ForeignPtr ClusteringPtr
data InvertedIndex = InvertedIndex (ForeignPtr InvertedIndexPtr) Clustering

foreign import ccall unsafe "new_clustering" cNewClustering
    :: Ptr Word16
    -> (#type size_t)
    -> IO (Ptr ClusteringPtr)

foreign import ccall unsafe "&free_clustering" cFreeClustering
    :: FunPtr (Ptr ClusteringPtr -> IO ())

foreign import ccall unsafe "new_inverted_index" cNewInvertedIndex
    :: Ptr ClusteringPtr
    -> (#type size_t)
    -> IO (Ptr InvertedIndexPtr)

foreign import ccall unsafe "&free_inverted_index" cFreeInvertedIndex
    :: FunPtr (Ptr InvertedIndexPtr -> IO ())

foreign import ccall unsafe "set_docs" cSetDocs
    :: Ptr InvertedIndexPtr
    -> Word32
    -> Ptr Word32
    -> (#type size_t)
    -> IO ()

foreign import ccall unsafe "intersect" cIntersect
    :: Ptr InvertedIndexPtr
    -> Word32
    -> Word32
    -> Ptr Word32
    -> IO (#type size_t)

foreign import ccall unsafe "list_size" cListSize
    :: Ptr InvertedIndexPtr
    -> Word32
    -> IO (#type size_t)

newClustering :: Cl.Clustering DocId -> IO Clustering
newClustering cl =
    do ccl <- SV.unsafeWith mappingV $ \mappingPtr -> cNewClustering mappingPtr numMappings
       if ccl == nullPtr
          then error "Failed to create Clustering"
          else newForeignPtr cFreeClustering ccl
    where maxClusterId = fromIntegral $ Cl.unClusterId $ maximum $ Cl.enumerateClusterIds cl
          numElems = 1 + (fromIntegral $ unDocId $ Cl.maxElem cl)
          numMappings = fromIntegral $ SV.length mappingV
          mapping = Cl.mapping cl
          dtoc d = maybe (maxClusterId+1) (fromIntegral . Cl.unClusterId) $ mapping $ DocId $ fromIntegral d
          mappingV = SV.generate numElems dtoc

newInvertedIndex :: Clustering -> Int -> IO InvertedIndex
newInvertedIndex cl nt =
    do ii <- withForeignPtr cl $ \clptr ->
               cNewInvertedIndex clptr $ fromIntegral nt
       if ii == nullPtr
          then error "Failed to create inverted index"
          else do iiFPtr <- newForeignPtr cFreeInvertedIndex ii
                  return $ InvertedIndex iiFPtr cl

withInvertedIndex :: InvertedIndex -> (Ptr InvertedIndexPtr -> IO a) -> IO a
withInvertedIndex (InvertedIndex ii cl) action =
    do withForeignPtr cl $ \_ ->
         withForeignPtr ii action

setDocs :: InvertedIndex -> Token -> VU.Vector DocId -> IO ()
setDocs i (Token term) docids =
    do withInvertedIndex i $ \iptr ->
         SV.unsafeWith unboxed $ \docidptr ->
           cSetDocs iptr (fromIntegral term) docidptr (fromIntegral $ SV.length unboxed)
    where unboxed = SV.convert $ VU.map unDocId docids


size :: InvertedIndex -> Token -> IO Int
size i (Token t) = withInvertedIndex i $ \ptr ->
                     liftM fromIntegral $ cListSize ptr t

intersect :: InvertedIndex -> Token -> Token -> IO (VU.Vector DocId)
{-# INLINE intersect #-}
intersect i t1 t2 =
    do s1 <- (size i t1)
       s2 <- (size i t2)
       v <- MSV.unsafeNew $ min s1 s2
       s <- MSV.unsafeWith v $ \outptr ->
              withInvertedIndex i $ \iptr ->
                cIntersect iptr (unToken t1) (unToken t2) outptr
       frozen <- SV.freeze $ MSV.take (fromIntegral s) v
       return $ VU.map DocId $ VU.convert frozen

benchIntersect :: InvertedIndex -> Token -> Token -> IO (Double, VU.Vector DocId)
benchIntersect i t1 t2 =
    do s1 <- (size i t1)
       s2 <- (size i t2)
       v <- MSV.unsafeNew $ min s1 s2
       (t, s) <- MSV.unsafeWith v $ \outptr ->
                   withInvertedIndex i $ \iptr ->
                     time $ cIntersect iptr (unToken t1) (unToken t2) outptr
       frozen <- SV.freeze $ MSV.take (fromIntegral s) v
       return (t, VU.map DocId $ VU.convert frozen)
