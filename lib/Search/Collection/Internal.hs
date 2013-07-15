{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}

module Search.Collection.Internal
    (
      indexFilePath, metaFilePath, metaOffsetsFilePath, statsFilePath, containerFilePath, dictionaryFilePath
    ) where

import System.FilePath.Posix ((</>))

indexFilePath :: FilePath -> FilePath
indexFilePath b = b </> "index"

metaFilePath :: FilePath -> FilePath
metaFilePath b = b </> "meta"

metaOffsetsFilePath :: FilePath -> FilePath
metaOffsetsFilePath b = b </> "metaoffsets"

statsFilePath :: FilePath -> FilePath
statsFilePath b = b </> "tfs"

containerFilePath :: FilePath -> Int -> FilePath
containerFilePath b i = b </> "docs_" ++ show i

dictionaryFilePath :: FilePath -> FilePath
dictionaryFilePath b = b </> "dictionary"
