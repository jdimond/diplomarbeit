{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
module Search.Dictionary
    (
      TokenMap, WordMap
    , emptyWordMap, addToken, buildTokenMap, lookupWord, lookupToken
    , buildWordMap
    ) where

import Prelude hiding (words)

import Control.Arrow (second)

import qualified Data.Text as T
import Data.Text.Encoding (encodeUtf8)
import Data.Text.Lazy.Encoding (decodeUtf8)
import Data.Text.Lazy (toStrict)
import Data.Tuple (swap)
import qualified Data.Vector as V

import Data.Binary
import Data.Binary.Put (putByteString)
import Data.Binary.Get (getRemainingLazyByteString)

import qualified Data.ByteString.Lazy as BL
import qualified Data.HashMap.Strict as H

import Search.Common

type WordHashMap = H.HashMap T.Text Token

data WordMap = WordMap !WordHashMap Token
    deriving (Show, Eq)

data TokenMap = TokenMap !(V.Vector T.Text)
    deriving (Show, Eq)

instance Binary TokenMap where
  put (TokenMap v) =
    V.foldM_ addBytes () v
    where
      addBytes _ t =
        do putByteString $ encodeUtf8 t
           putWord8 0
  get = do bs <- getRemainingLazyByteString
           let word x = let w = toStrict $ decodeUtf8 x in w `seq` w
           let words [] = []
               words ws = map word $ init ws
           return $ TokenMap $! V.fromList $ words $ BL.split 0 bs

addToken :: WordMap -> T.Text -> (Token, Maybe WordMap)
addToken (WordMap hm ot@(Token otid)) w =
    case H.lookup w hm of
      Just tid -> (tid, Nothing)
      Nothing -> (ot, Just $ WordMap nhm ntid)
        where ntid = Token $ otid + 1
              nhm = H.insert w (Token otid) hm

emptyWordMap :: WordMap
emptyWordMap = WordMap H.empty (Token 0)

lookupWord :: WordMap -> T.Text -> Maybe Token
lookupWord (WordMap hm _) w = H.lookup w hm

lookupToken :: TokenMap -> Token -> T.Text
lookupToken (TokenMap v) (Token t) = v V.! fromIntegral t

buildTokenMap :: WordMap -> TokenMap
buildTokenMap (WordMap hm (Token curId)) = TokenMap m
    where m = empty V.// tokenList
          empty = V.replicate (fromIntegral curId) ""
          tokenList = map (swap . second (fromIntegral . unToken)) (H.toList hm)

buildWordMap :: TokenMap -> WordMap
buildWordMap (TokenMap v) =
    let mapping = zip (V.toList v) $ map Token [0..]
        maxToken = Token $ fromIntegral $ V.length v
    in WordMap (H.fromList mapping) maxToken
