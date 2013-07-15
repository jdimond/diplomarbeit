{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

import System.IO (stderr, hPutStrLn)
import System.Exit

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Search.Common ()
import qualified Search.SearchIndex as SI
import qualified Search.Dictionary as D
import Search.Processing (extractWords)

import qualified Data.List as L

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import qualified Data.Text.Lazy.IO as LTIO
import qualified Data.Text.Lazy as LT

import Data.Maybe (fromMaybe)

import Control.Monad (when, liftM)

import Data.Binary

data QueryOptions = QueryOptions
    { dictionary :: FilePath
    , search_indexes :: [FilePath]
    , threshold :: Maybe Int
    , min_query_tokens :: Maybe Int
    , max_query_tokens :: Maybe Int
    } deriving (Eq, Show, A.Data, A.Typeable)

queryOptions :: QueryOptions
queryOptions = QueryOptions
    { dictionary = A.def &= A.typ "DICTIONARY" &= A.argPos 0
    , search_indexes = A.def &= A.typ "SEARCH_INDEX" &= A.args
    , threshold = A.def &= A.help "minimum number of documents per query"
    , min_query_tokens = A.def &= A.help "minimum number of tokens in a query"
    , max_query_tokens = A.def &= A.help "maximum number of tokens in a query"
    } &=
    A.help "Filter queries by number of documents in the index"

longerThan :: [a] -> Int -> Bool
longerThan l c
    | c < 0 = True
    | otherwise =
        case l of
          [] -> False
          (_:xs) -> longerThan xs (c-1)

type ClusterSearchIndex = [SI.SearchIndex]

check :: (a -> Bool) -> Maybe a -> Bool
check = maybe True

hasDocs :: ClusterSearchIndex -> D.WordMap -> QueryOptions -> T.Text -> Bool
hasDocs cs dict opts t =
    let terms = extractWords t
        tokens = mapM (D.lookupWord dict) terms
        results = maybe [] (\ts -> concatMap (`SI.search` ts) cs) tokens
    in check (length terms >=) (min_query_tokens opts) &&
       check (length terms <=) (max_query_tokens opts) &&
       check (\x -> results `longerThan` (x-1)) (threshold opts)

decodeFile' :: (Binary a) => FilePath -> IO a
decodeFile' p = do
    !x <- decodeFile p
    return x

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

main :: IO ()
main = do
    opts <- A.cmdArgs queryOptions
    when (L.null $ search_indexes opts) $ putInfo "No search indexes given" >> exitWith (ExitFailure 1)
    putInfo "Loading index files..."
    let indexFiles = search_indexes opts
    csi <- mapM (decodeFile' :: FilePath -> IO SI.SearchIndex) indexFiles
    putInfo "Loading dictionary..."
    let dictFile = dictionary opts
    !dict <- liftM D.buildWordMap $ decodeFile dictFile
    putInfo "Done..."
    lines <- liftM (map LT.toStrict . LT.lines) LTIO.getContents
    let loop :: Int -> [T.Text] -> IO ()
        loop _ [] = putInfo "Done filtering"
        loop i (l:ls) =
            do when (hasDocs csi dict opts l) $ TIO.putStrLn l
               when (i `mod` 1000 == 0) $ hPutStrLn stderr $ "Processed " ++ show i ++ " query lines"
               loop (i+1) ls
    loop 0 lines
