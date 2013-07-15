{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}
import GHC.Conc (numCapabilities)

import qualified System.FilePath.Find as FP
import System.FilePath.Find ((==?))
import System.Directory (doesDirectoryExist)

import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))

import Control.Exception

import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TCBQueue

import Control.Concurrent.MVar

import Control.Monad

import Control.Concurrent.Async

import qualified Data.Text.IO as TIO
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Text.Encoding.Error (lenientDecode)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Binary.Get

import Codec.Compression.GZip (decompress)

import qualified Search.Dictionary as D
import qualified Search.Collection.Writer as W
import qualified Search.Collection as C
import Search.Common
import Search.Processing
import Search.Processing.GOV2

data InputMode = ModeText | ModeSentences | ModeGOV2 | ModeLengthDelimited | ModeGZipLengthDelimited
        deriving (Eq, Show, A.Data, A.Typeable)

data CmdArgs = CmdArgs
    { argDataDir :: FilePath
    , argCollectionDir :: FilePath
    , argInputMode :: InputMode
    } deriving (Eq, Show, A.Data, A.Typeable)

cmdArgs :: CmdArgs
cmdArgs = CmdArgs
    { argDataDir = A.def &= A.typ "DATADIR" &= A.argPos 0
    , argCollectionDir = A.def &= A.typ "INDEXDIR" &= A.argPos 1
    , argInputMode = A.enum [ ModeText &= A.explicit
                                       &= A.name "text"
                                       &= A.help "input are raw text files"
                            , ModeSentences &= A.explicit
                                            &= A.name "sentences"
                                            &= A.help "raw text file with sentences"
                            , ModeGOV2 &= A.explicit &= A.name "gov2" &= A.help "input are compressed GOV2 archives"
                            , ModeLengthDelimited &= A.explicit
                                                  &= A.name "length-delimited"
                                                  &= A.help "input are length delimited UTF-8 strings"
                            , ModeGZipLengthDelimited &= A.explicit
                                                      &= A.name "length-delimited-gzip"
                                                      &= A.help "input is gzip compressed file with length delimited UTF-8 strings"]
    }

tokenizeFile :: MVar D.WordMap -> FilePath -> IO [(C.FileFragment, TokenDoc)]
tokenizeFile wmRef path =
    do contents <- TIO.readFile path
       doc <- tokenizeWords wmRef $ extractWords contents
       let frag = C.FileFragment
                  { C.fragmentFile = path
                  , C.fragmentOffset = 0
                  , C.fragmentLength = fromIntegral $ T.length contents
                  }
       return [(frag, doc)]

tokenizeGov2 :: MVar D.WordMap -> FilePath -> IO [(C.FileFragment, TokenDoc)]
tokenizeGov2 wmRef path =
    do pages <- govFileToTexts path
       tokenized <- mapM (tokenizeWords wmRef . extractWords) $ map gov2PageText pages
       let frags = map page2Frag pages
       return $ zip frags tokenized
    where page2Frag p = C.FileFragment
                        { C.fragmentFile = path
                        , C.fragmentOffset = gov2PageOffset p
                        , C.fragmentLength = gov2PageLength p
                        }

tokenizeLengthDelimited :: MVar D.WordMap -> Bool -> FilePath -> IO [(C.FileFragment, TokenDoc)]
tokenizeLengthDelimited wmRef useGzip path =
    do bytes <- BS.readFile path
       let lazybs = if useGzip
                       then decompress $ BSL.fromStrict bytes
                       else BSL.fromStrict $ bytes
       let texts = runGetOrFail extractTexts lazybs
       case texts of
         Left (_,_,err) -> do putStrLn $ "Error decoding file " ++ path ++ ":" ++ err
                              return []
         Right (_,_,ts) -> do mapM (\(f,t) -> liftM ((,) f) $ tokenizeWords wmRef t) ts
    where extractTexts =
              do empty <- isEmpty
                 if empty
                    then return []
                    else do len <- getWord32be
                            offset <- bytesRead
                            next <- getByteString $ fromIntegral len
                            let text = extractWords $ decodeUtf8With lenientDecode next
                            rest <- extractTexts
                            let frag = C.FileFragment
                                       { C.fragmentFile = path
                                       , C.fragmentOffset = fromIntegral $ offset
                                       , C.fragmentLength = len
                                       }
                            return $ (frag, text) : rest

tokenizeSentences :: MVar D.WordMap -> FilePath -> IO [(C.FileFragment, TokenDoc)]
tokenizeSentences wmRef path =
    do contents <- TIO.readFile path
       let sentences = T.split (`elem` "!.?") contents
       let frags = zip (fragments sentences) (map extractWords sentences)
       let filtered = filter (\(_,s) -> length s > 1) frags
       mapM (\(f,s) -> tokenizeWords wmRef s >>= \d -> return (f,d)) filtered
    where fragments s =
              let lengths = map T.length s
                  offsets = scanl (\o l -> o + 1 + l) 0 lengths
              in zipWith (curry fragment) offsets lengths
          fragment (o,l) =
              C.FileFragment
              { C.fragmentFile = path
              , C.fragmentOffset = fromIntegral o
              , C.fragmentLength = fromIntegral l
              }

seqList :: [a] -> ()
seqList [] = ()
seqList (x:xs) = x `seq` seqList xs

tokenizeWords :: MVar D.WordMap -> [T.Text] -> IO TokenDoc
tokenizeWords wmRef ws =
    do wm <- readMVar wmRef
       (_, rts) <- foldM handleWord (wm,[]) ws
       let !doc = docFromTokens $ reverse rts
       return doc
    where
       handleWord (!wm,!ts) w =
           do let (t', mwm) = D.addToken wm w
              case mwm of
                Nothing -> return (wm, t':ts)
                Just _ -> do owm <- takeMVar wmRef
                             let (t, mwm') = D.addToken owm w
                             let nwm = maybe owm id mwm'
                             putMVar wmRef nwm
                             return (nwm, t:ts)

fileWorker :: C.DocMeta a => (FilePath -> IO [(a, TokenDoc)]) -> TCBQueue FilePath -> TCBQueue (a, TokenDoc) -> IO ()
fileWorker tfunc fpQueue docQueue = loop
    where
      loop = do queued <- atomically $ readTCBQueue fpQueue
                case queued of
                  TClosed -> putStrLn "Finishing worker thread..."
                  TQueued fp -> do docs <- tfunc fp
                                   mapM_ (atomically . writeTCBQueue docQueue) docs
                                   loop

docIndexWriter :: C.DocMeta a => MVar D.WordMap -> TCBQueue (a, TokenDoc) -> FilePath -> IO ()
docIndexWriter wmRef docQueue docDir =
    W.withCollectionWriter docDir $ \writer ->
    loop writer 0
    where loop :: W.DocCollectionWriter -> Int -> IO D.WordMap
          loop w i =
            do when (i `mod` 1000 == 0) $ putStrLn $ "Processed " ++ show i ++ " Docs"
               queued <- atomically $ readTCBQueue docQueue
               case queued of
                 TClosed -> readMVar wmRef
                 TQueued (m, d) ->
                     do W.saveDoc w (m, d)
                        loop w (i+1)

_NUM_THREADS_ :: Int
_NUM_THREADS_ = numCapabilities

_CHANNEL_SIZE_ :: Int
_CHANNEL_SIZE_ = 1024

async' :: String -> IO a -> IO (Async a)
async' msg a = async $ catch a (\e -> putStrLn (msg ++ "! Exception occured: " ++ (show (e :: SomeException))) >> undefined)

main :: IO ()
main =
  do args <- A.cmdArgs cmdArgs
     putStrLn "Reading files..."
     documentPaths <- FP.find FP.always (FP.fileType ==? FP.RegularFile) (argDataDir args)
     let indexDir = argCollectionDir args
     indexDirExists <- doesDirectoryExist indexDir
     unless indexDirExists $ error $ "Index directory \"" ++ indexDir ++ "\" does not exist. Please create!"
     wmRef <- newMVar D.emptyWordMap
     fpQueue <- newTCBQueueIO _CHANNEL_SIZE_
     docQueue <- newTCBQueueIO _CHANNEL_SIZE_
     let tfunc = case (argInputMode args) of
                   ModeText -> tokenizeFile wmRef
                   ModeSentences -> tokenizeSentences wmRef
                   ModeGOV2 -> tokenizeGov2 wmRef
                   ModeLengthDelimited -> tokenizeLengthDelimited wmRef False
                   ModeGZipLengthDelimited -> tokenizeLengthDelimited wmRef True
     fileWorkers <- replicateM _NUM_THREADS_ $ async' "FileWorker" $ fileWorker tfunc fpQueue docQueue
     indexWriter <- async' "IndexWriter" $ docIndexWriter wmRef docQueue indexDir
     mapM_ (atomically . writeTCBQueue fpQueue) documentPaths
     atomically $ closeTCBQueue fpQueue
     mapM_ wait fileWorkers
     atomically $ closeTCBQueue docQueue
     wait indexWriter
     putStrLn "All done!"
