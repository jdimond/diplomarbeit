{-# LANGUAGE OverloadedStrings #-}
module Search.Processing
    (
      extractWords, isStopWord
    ) where

import qualified Data.Text as T

import qualified Data.HashSet as HS

extractWords :: T.Text -> [T.Text]
extractWords str = filter (not . isStopWord) $ map T.toCaseFold $ filter (not . T.null) $ T.split isTokenSeperator str

isTokenSeperator :: Char -> Bool
isTokenSeperator c =
    c <= '\x20' ||
    c >= '\x20' && c <= '\x29' ||
    c >= '\x2a' && c <= '\x2f' ||
    c >= '\x3a' && c <= '\x40' ||
    c >= '\x5b' && c <= '\x60' ||
    c >= '\x7b' && c <= '\x7e' ||
    c == '\xa0' ||
    c == '\xb0' ||
    c == '\NUL'

isStopWord :: T.Text -> Bool
isStopWord = flip HS.member stopWordSet

stopWordSet :: HS.HashSet T.Text
stopWordSet = HS.fromList stopWords

stopWords :: [T.Text]
stopWords =
    ["a", "able", "about", "across", "after", "all", "almost", "also", "am", "among",
     "an", "and", "any", "are", "as", "at", "be", "because", "been", "but", "by", "can",
     "cannot", "could", "dear", "did", "do", "does", "either", "else", "ever", "every",
     "for", "from", "get", "got", "had", "has", "have", "he", "her", "hers", "him",
     "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just",
     "least", "let", "like", "likely", "may", "me", "might", "most", "must", "my",
     "neither", "no", "nor", "not", "of", "off", "often", "on", "only", "or", "other",
     "our", "own", "rather", "said", "say", "says", "she", "should", "since", "so",
     "some", "than", "that", "the", "their", "them", "then", "there", "these", "they",
     "this", "tis", "to", "too", "us", "wants", "was", "we", "were", "what",
     "when", "where", "which", "while", "who", "whom", "why", "will", "with",
     "would", "yet", "you", "your"]
