{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE OverloadedStrings #-}

-- # Standard Library
import Control.Applicative
import qualified Data.List as L
import Control.Monad

-- # Site Packages
import qualified Data.Vector.Unboxed as VU

-- # LOCAL
import Search.Common
import qualified Search.Foreign.ClusterLookupFFI as LF

import qualified Search.Cluster as Cl

-- # HTF
import Test.Framework

-- # DEBUG
import Debug.Trace

toIds :: (Identifier i, Integral a) => [a] -> [i]
toIds = map (boxId . fromIntegral)

clustering :: DocId -> Cl.ClusterId -> IO (Cl.Clustering DocId)
clustering (DocId did) (Cl.ClusterId cid) =
    Cl.clusteringFromList (fromIntegral cid +1) $ zip (toIds [0..did]) (cycle $ toIds [0..cid])

test_clustering1 :: IO ()
test_clustering1 =
    do cl <- clustering (DocId 100000) (Cl.ClusterId 100)
       _ <- LF.newClustering cl
       assertBool True

test_clustering2 :: IO ()
test_clustering2 =
    do cl <- clustering (DocId 100000) (Cl.ClusterId 0)
       _ <- LF.newClustering cl
       assertBool True

test_clustering3 :: IO ()
test_clustering3 =
    do cl <- clustering (DocId 100000) (Cl.ClusterId 100)
       _ <- LF.newClustering cl
       assertBool True

defaultLookup :: Int -> IO LF.InvertedIndex
defaultLookup size =
    do cl <- clustering (DocId 100000) (Cl.ClusterId 10)
       ccl <- LF.newClustering cl
       LF.newInvertedIndex ccl size

test_invertedindex :: IO ()
test_invertedindex =
    do _ <- defaultLookup 10000
       assertBool True

test_addingdocuments :: IO ()
test_addingdocuments =
    do let numTerms = 1000
       i <- defaultLookup numTerms
       forM_ [0..numTerms-1] $ \t ->
           LF.setDocs i (Token $ fromIntegral t) $ VU.fromList $ toIds [0..t]
       assertBool True

test_intersect_simple :: IO ()
test_intersect_simple =
    do cl <- clustering (DocId 20) (Cl.ClusterId 5)
       ccl <- LF.newClustering cl
       i <- LF.newInvertedIndex ccl 2
       let docs1 = map DocId [0,1,4,7,8,9,11,12,13,15,16,18,19]
       let docs2 = map DocId [1,2,3,4,8,10,11,12,13,16,17,19]
       LF.setDocs i (Token 0) $ VU.fromList docs1
       LF.setDocs i (Token 1) $ VU.fromList docs2
       let normal = L.sort $ L.intersect docs1 docs2
       lookup <- liftM (L.sort . VU.toList) $ LF.intersect i (Token 0) (Token 1)
       assertEqual normal lookup

test_intersect :: IO ()
test_intersect =
    do let maxDocId = 100000
       let numTerms = 50
       cl <- clustering (DocId maxDocId) (Cl.ClusterId 27)
       ccl <- LF.newClustering cl
       i <- LF.newInvertedIndex ccl numTerms
       let terms = map (Token . fromIntegral) [1..numTerms-1]
       let dids (Token t) = VU.fromList $ toIds [0,(t+1)..(min (maxDocId-1) (t+1)*500)]
       forM_ terms $ \t ->
         LF.setDocs i t $ dids t
       intersectedLookup <- sequence [liftM (L.sort . VU.toList) $ LF.intersect i t1 t2 | t1 <- terms, t2 <- terms]
       let intersectedNormal = [L.sort $ L.intersect (VU.toList $ dids t1) (VU.toList $ dids t2) | t1 <- terms, t2 <- terms]
       assertEqual intersectedNormal intersectedLookup

main = htfMain htf_thisModulesTests
