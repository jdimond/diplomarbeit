{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fno-cse -fno-full-laziness #-} -- recommended by GHC manual
{-# OPTIONS_GHC -Wwarn #-}

#include "search.h"
#include "fmf.h"

module Search.Foreign.FFIInterface
    (
      CollectionHandle
    , cluster, loadCollection
    ) where

import Data.Word
import Foreign.Ptr
import Foreign.ForeignPtr hiding (unsafeForeignPtrToPtr)

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Storable as SV
import qualified Data.Vector.Storable.Mutable as MSV

import Data.Maybe (fromJust, isJust)
import qualified Data.List as L

import Search.Common
import Search.Cluster (ClusterId(..))
import qualified Search.Collection as C

data Collection
type CollectionHandle = ForeignPtr Collection

data ClusterDocuments = ClusterDocuments
                     { clusterCollection :: CollectionHandle
                     , clusterTermFreqs :: VU.Vector Word32
                     , clusterDocIds :: VU.Vector DocId
                     }

data DocStruct
data DocArray = DocArray
                { enclosingCollection :: CollectionHandle
                , numDocs :: Word32
                , docArray :: Ptr DocStruct
                }

foreign import ccall "new_collection" cNewCollection :: IO (Ptr Collection)
foreign import ccall "&free_collection" cFreeCollection :: FunPtr (Ptr Collection -> IO ())

newCollection :: IO CollectionHandle
newCollection =
    do p <- cNewCollection
       newForeignPtr cFreeCollection p

foreign import ccall "add_to_collection" cAddToCollection :: Ptr Collection -> Word32 -> Ptr (Word32) -> IO ()

addToCollection :: CollectionHandle -> TokenDoc -> IO ()
addToCollection ch d =
    do let terms = V.convert $ VU.map unToken $ vectorFromDoc d
       withForeignPtr ch $ \ptr ->
           SV.unsafeWith terms $ \termptr ->
               cAddToCollection ptr (fromIntegral $ SV.length terms) termptr

foreign import ccall "num_docs_in_collection" cNumDocsInCollection :: Ptr Collection -> IO Word32

numDocsInCollection :: CollectionHandle -> IO Word32
numDocsInCollection ch = withForeignPtr ch $ cNumDocsInCollection

foreign import ccall "get_docs_in_collection" cGetDocsInCollection :: Ptr Collection -> IO (Ptr DocStruct)

docsInCollection :: CollectionHandle -> IO (DocArray)
docsInCollection ch =
    withForeignPtr ch $ \h ->
        do docs <- cGetDocsInCollection h
           nd <- cNumDocsInCollection h
           return $ DocArray ch nd docs

foreign import ccall "fmf_clustering"
    cFmfClustering :: Word32
                   -> Ptr DocStruct
                   -> Word32
                   -> Double
                   -> Int
                   -> Int
                   -> Ptr (Word32)
                   -> Word32
                   -> Ptr (Word64)
                   -> IO ()

loadCollection :: C.DocCollection a -> VU.Vector DocId -> VU.Vector Word32 -> (Maybe Int) -> IO ClusterDocuments
loadCollection coll dids tfreqs mnumterms =
    do ccoll <- newCollection
       V.mapM_ (addDocs ccoll) $ chunkV 100000 dids
       return $ ClusterDocuments
                { clusterCollection = ccoll
                , clusterTermFreqs = newTermFreqs
                , clusterDocIds = dids
                }
    where addDoc ccoll doc =
            let newTokens = V.map (\(Token t) -> termMapping V.! (fromIntegral t)) $ V.convert $ vectorFromDoc doc
                filteredDoc = docFromVector $ V.convert $ V.map (fromJust) $ V.filter isJust newTokens
            in addToCollection ccoll filteredDoc
          addDocs ccoll ds = do
              docs <- C.loadTokenDocs coll (VU.toList ds)
              mapM_ (addDoc ccoll . tokenDoc) docs
          chunkV size = V.unfoldr (\v -> if VU.null v then Nothing else Just $ VU.splitAt size v)
          filteredFreqs = case mnumterms of
                            Just n -> let compareFreq (_,a) (_,b) = compare b a
                                          sorted = L.sortBy compareFreq $ VU.toList $ VU.indexed tfreqs
                                      in VU.fromList $ take n $ sorted
                            Nothing -> VU.indexed tfreqs
          newTermFreqs = VU.map snd $ filteredFreqs
          nothingTerms = V.replicate (VU.length tfreqs) Nothing
          termMapping = V.update nothingTerms $ V.map (\(i,t) -> (t, Just $ Token $ fromIntegral i)) $ V.convert $ VU.indexed $ VU.map fst filteredFreqs

cluster :: ClusterDocuments -> Int -> Double -> Bool -> Bool -> IO (VU.Vector ClusterId)
cluster coll clusterSizeParam shrinkFactor useTopDown fastScoring =
    do mclusters <- MSV.new $ VU.length $ clusterDocIds coll
       docs <- docsInCollection $ clusterCollection coll
       let (ctfreqs :: SV.Vector Word64) = SV.map fromIntegral $ SV.convert $ clusterTermFreqs coll
       let numTerms = fromIntegral $ SV.length ctfreqs
       let useTopDown' = if useTopDown then 1 else 0
       let fastScoring' = if fastScoring then 1 else 0
       MSV.unsafeWith mclusters $ \clusterptr ->
          SV.unsafeWith ctfreqs $ \freqptr ->
             cFmfClustering (numDocs docs) (docArray docs) (fromIntegral clusterSizeParam) shrinkFactor useTopDown' fastScoring' clusterptr numTerms freqptr
       touchForeignPtr $ clusterCollection coll
       frozen <- SV.freeze mclusters
       return $ VU.map (ClusterId) $ V.convert frozen
