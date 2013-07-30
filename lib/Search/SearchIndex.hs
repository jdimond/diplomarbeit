{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

module Search.SearchIndex
    (
      SearchIndex, SearchIndexBuilder
    , newSearchIndexBuilder, indexTokenDoc
    , buildSearchIndex
    , search, numDocsPerTerm
    , SearchIndexHandle, withSearchIndexFile, readPostingList, joinIndexes
    , indexedTokens, handleIndexedTokens
    ) where

import Control.Monad.Primitive

import Control.Arrow (second)

import Search.Common
import qualified Search.Collection as C
import qualified Search.IndexedSet as I

import qualified Data.Vector.Unboxed as VU

type SearchIndex = I.IndexedSet Token DocId

type SearchIndexBuilder s = I.IndexedSetBuilder s Token DocId

newSearchIndexBuilder :: (PrimMonad m) => C.DocCollection a -> m (SearchIndexBuilder (PrimState m))
newSearchIndexBuilder coll = I.newIndexedSetBuilder (C.dictionarySize coll)

indexTokenDoc :: (PrimMonad m) => SearchIndexBuilder (PrimState m) -> IndexedTokenDoc -> m ()
indexTokenDoc ib (IndexedTokenDoc did doc) = do
    let tfs = termFrequencies doc
    let toIndex = map (second $ const did) tfs
    mapM_ (uncurry $ I.addToIndex ib) toIndex

buildSearchIndex :: (PrimMonad m) => SearchIndexBuilder (PrimState m) -> m SearchIndex
buildSearchIndex = I.buildIndex

search :: SearchIndex -> [Token] -> [DocId]
search _ [] = []
search index ts = foldl1 intersectSorted $ map (I.lookup index) ts

numDocsPerTerm :: SearchIndex -> VU.Vector Int
numDocsPerTerm = I.setSizes

intersectSorted :: (Ord a) => [a] -> [a] -> [a]
intersectSorted _ [] = []
intersectSorted [] _ = []
intersectSorted xs@(x:xs') ys@(y:ys')
    | x < y  = intersectSorted xs' ys
    | x == y = x:intersectSorted xs' ys'
    | x > y  = intersectSorted xs  ys'
    | otherwise = undefined

type SearchIndexHandle = I.IndexedSetHandle Token DocId

withSearchIndexFile :: FilePath -> (SearchIndexHandle -> IO a) -> IO a
withSearchIndexFile = I.withIndexedSetFile

readPostingList :: SearchIndexHandle -> Token -> IO (VU.Vector DocId)
readPostingList = I.readSet

joinIndexes :: [FilePath] -> FilePath -> IO ()
joinIndexes = I.joinFiles

indexedTokens :: SearchIndex -> [Token]
indexedTokens = I.enumerateIndexKeys

handleIndexedTokens :: SearchIndexHandle -> [Token]
handleIndexedTokens = I.enumerateIndexHandleKeys
