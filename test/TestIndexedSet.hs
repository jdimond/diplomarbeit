{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleInstances #-}

-- # Standard Library
import Data.Maybe
import Control.Applicative
import Control.Monad

import Prelude hiding (lookup)

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V
import qualified Data.Vector.Generic.Base as VG
import qualified Data.Vector.Generic.Mutable as VGM

import Control.DeepSeq
import Control.Exception (finally)

import System.IO
import System.Directory

-- # Site Packages
import Test.QuickCheck.Monadic

-- # LOCAL
import Search.Common
import Search.IndexedSet

-- # HTF
import Test.Framework

-- # DEBUG
import Debug.Trace

newtype Id = Id { unId :: IdType }
    deriving (Show, Eq, Ord, VG.Vector VU.Vector, VGM.MVector VU.MVector, VU.Unbox, NFData)

instance Identifier Id where
    unboxId = unId
    boxId = Id
    putId (Id i) = putWord32be i
    getId = Id <$> getWord32be

instance Arbitrary Id where
    arbitrary = Id <$> arbitrary

instance Arbitrary (IndexedSet Id Id) where
    arbitrary = do
        size <- choose (0,1000)
        idLists <- replicateM size $ listOf arbitrary
        return $ indexedSetFromList idLists

prop_serialization :: IndexedSet Id Id -> Bool
prop_serialization m = m == runGet get (runPut $ put m)

prop_ordered :: IndexedSet Id Id -> Bool
prop_ordered i = all isStrictMonotone $ map (lookup i) $ enumerateIndexKeys i
    where
      isStrictMonotone [] = True
      isStrictMonotone (x:[]) = True
      isStrictMonotone (x:y:zs) =
          if x < y
             then isStrictMonotone $ y:zs
             else False

prop_setSizes :: IndexedSet Id Id -> Bool
prop_setSizes i = setSizes i == (VU.fromList $ map length $ map (lookup i) $ enumerateIndexKeys i)

withTmpFile :: (FilePath -> IO a) -> IO a
withTmpFile func = do
      tempdir <- getTemporaryDirectory
      (tempfile, temph) <- openTempFile tempdir ""
      hClose temph
      finally (func tempfile) (removeFile tempfile)

mapIds :: [[Word32]] -> [[Id]]
mapIds = map (map Id)

prop_fileRead :: IndexedSet Id Id -> Property
prop_fileRead i = monadicIO $
    do let keys = enumerateIndexKeys i
       listsFile <- run $ withTmpFile $ \file ->
           do encodeFile file i
              withIndexedSetFile file $ \h ->
                forM keys $ readSet h
       let lists = map (lookupV i) keys
       assert $ lists == listsFile

test_fileJoin :: IO ()
test_fileJoin =
    do let set1 = indexedSetFromList $ mapIds $ [[0,10],    [1,2,3], [4,5], []] :: IndexedSet Id Id
           set2 = indexedSetFromList $ mapIds $ [[],        [7],     [],    []] :: IndexedSet Id Id
           set3 = indexedSetFromList $ mapIds $ [[1,2,3,4], [4,5],   [],    []] :: IndexedSet Id Id
           all = indexedSetFromList $ mapIds $ [[0,1,2,3,4,10], [1,2,3,4,5,7], [4,5], []] :: IndexedSet Id Id
       fromfile <- withTmpFile $ \f1 ->
                    withTmpFile $ \f2 ->
                     withTmpFile $ \f3 ->
                      withTmpFile $ \a ->
                       do encodeFile f1 set1
                          encodeFile f2 set2
                          encodeFile f3 set3
                          joinFiles [f1,f2,f3] a
                          decodeFile a
       assertEqual all fromfile

main = htfMain htf_thisModulesTests
