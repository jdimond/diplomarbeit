{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

import System.IO
import qualified System.Console.CmdArgs as A
import System.Console.CmdArgs ((&=))


import qualified Search.Collection as C
import qualified Search.Stats as S

import qualified Data.Vector.Unboxed as VU

import qualified Data.List as L

import Data.Word

putInfo :: String -> IO ()
putInfo = hPutStrLn stderr

sum' :: Num a => [a] -> a
sum' = L.foldl' (+) 0

sumLarge' :: Integral a => [a] -> Word64
sumLarge' = L.foldl' (+) 0 . (map fromIntegral)

avg :: Integral a => [a] -> Double
avg as =
  let rs = map fromIntegral as
  in  (sum' rs) / (fromIntegral $ length as)

data CmdArgs = CmdArgs
    { argCollectionDir :: FilePath
    } deriving (Eq, Show, A.Data, A.Typeable)

cmdArgs :: CmdArgs
cmdArgs = CmdArgs
    { argCollectionDir = A.def &= A.typ "COLLECTION_DIR" &= A.argPos 0
    }

main :: IO ()
main = do
    args <- A.cmdArgs cmdArgs
    putInfo "Loading document information..."
    !coll <- C.loadDocCollection $ argCollectionDir args
    let dids = C.enumerateDocIds coll
    putStrLn $ "Number of documents: " ++ show (length dids)
    let avgDocSize = avg $ map (C.documentLength coll) dids :: Double
    putStrLn $ "Average document size: " ++ show avgDocSize
    let (S.TFCounter tfc) = C.tfCounter coll
    let tfcs = VU.toList tfc
    let numTerms = fromIntegral $ VU.length tfc
    let totalTerms = sumLarge' tfcs
    let sorted = reverse $ L.sort tfcs
    putStrLn $ "Number of different words: " ++ show numTerms
    putStrLn $ "Average occurences per term: " ++ show (avg tfcs)
    putStrLn $ "Median occurrences per term: " ++ show (sorted L.!! (numTerms `div` 2))
    putStrLn $ "Max occurrences per term: " ++ show (L.maximum tfcs)
    putStrLn $ "Number of total term occurences:" ++ show totalTerms
    let occN n = sumLarge' $ take n sorted
    putStrLn $ "Number of total occurences for the first 1000 Terms:" ++ show (occN 1000)
    putStrLn $ "Number of total occurences for the first 10000 Terms:" ++ show (occN 10000)
    putStrLn $ "Number of total occurences for the first 100000 Terms:" ++ show (occN 100000)
    putStrLn $ "Number of total occurences for the first 1000000 Terms:" ++ show (occN 1000000)
