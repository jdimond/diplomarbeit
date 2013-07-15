{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

import System.Console.Editline.Readline (readline, addHistory)

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Data.Binary

import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import Data.Text.Encoding
import Data.Text.Encoding.Error (lenientDecode)

import qualified Data.ByteString.Lazy as BSL

import Control.Monad (liftM)

import Codec.Compression.GZip (decompress)

import Search.Common
import qualified Search.Collection as C
import qualified Search.SearchIndex as SI
import qualified Search.Dictionary as D
import Search.Processing (extractWords)
import Search.Processing.GOV2

showDoc :: C.FragmentDocCollection -> Bool -> DocId -> IO ()
showDoc coll useGzip did = do
    frag <- C.loadDocMeta coll did
    let path = C.fragmentFile frag
    putStrLn $ "######### " ++ path ++ " ########"
    filecontents <- BSL.readFile path
    let bs = if useGzip then decompress filecontents else filecontents
    let o = fromIntegral $ C.fragmentOffset frag
    let l = fromIntegral $ C.fragmentLength frag
    let string = decodeUtf8With lenientDecode $ BSL.toStrict $ BSL.take l $ BSL.drop o bs
    TIO.putStrLn string
    putStrLn $ "##########" ++ replicate (length path) '#' ++ "#########"

showGov2 :: C.FragmentDocCollection -> DocId -> IO ()
showGov2 coll did = do
    frag <- C.loadDocMeta coll did
    let path = C.fragmentFile frag
    let o = fromIntegral $ C.fragmentOffset frag
    let l = fromIntegral $ C.fragmentLength frag
    text <- govText path o l
    let delimiter = "###############################"
    putStrLn delimiter
    TIO.putStrLn text
    putStrLn delimiter


lineToQuery :: D.WordMap -> T.Text -> Maybe [Token]
lineToQuery dict line =
    let terms = extractWords line
    in mapM (D.lookupWord dict) terms

queryLoop :: D.WordMap -> C.FragmentDocCollection -> Mode -> [SI.SearchIndex] -> IO ()
queryLoop dict coll mode indexes = loop
    where
      loop = do
          line <- readline "Search: "
          case line of
            Nothing -> return ()
            Just l -> do
                addHistory l
                let q = lineToQuery dict $ T.pack l
                case q of
                  Nothing -> putStrLn "Query contains unknown terms" >> loop
                  Just [] -> putStrLn "Query empty or only stop words" >> loop
                  Just ts -> do
                      let docIds = concatMap (`SI.search` ts) indexes
                      let showing = take 3 docIds
                      let showFunc = case mode of
                                       ModeText -> showDoc coll False
                                       ModeGOV2 -> showGov2 coll
                                       ModeGZip -> showDoc coll True
                      mapM_ showFunc showing
                      putStrLn $ "Found " ++ show (length docIds) ++ " Documents"
                      loop

decodeFile' :: (Binary a) => FilePath -> IO a
decodeFile' p = do
    !x <- decodeFile p
    return x

data Mode = ModeText | ModeGOV2 | ModeGZip
        deriving (Eq, Show, A.Data, A.Typeable)

data CmdArgs = CmdArgs
    { argCollectionDir :: FilePath
    , argSearchIndexes :: [FilePath]
    , argMode :: Mode
    } deriving (Eq, Show, A.Data, A.Typeable)

cmdArgs :: CmdArgs
cmdArgs = CmdArgs
    { argCollectionDir = A.def &= A.typ "COLLECTION_DIR" &= A.argPos 0
    , argSearchIndexes = A.def &= A.typ "SEARCH_INDEX" &= A.args
    , argMode = A.enum [ ModeText &= A.explicit &= A.name "text" &= A.help "treat collection data as raw text"
                       , ModeGOV2 &= A.explicit &= A.name "gov2" &= A.help "treat collection data as GOV2"
                       , ModeGZip &= A.explicit &= A.name "gzip" &= A.help "treat collection data as gzipped text"]
    }


main :: IO ()
main = do
    args <- A.cmdArgs cmdArgs
    putStrLn "Loading document information..."
    !coll <- C.loadDocCollection (argCollectionDir args)
    putStrLn "Loading dictionary..."
    !dictionary <- liftM D.buildWordMap $ C.loadTokenMap coll
    putStrLn "Loading index..."
    !indexes <- mapM (decodeFile' :: FilePath -> IO SI.SearchIndex) (argSearchIndexes args)
    queryLoop dictionary coll (argMode args) indexes
