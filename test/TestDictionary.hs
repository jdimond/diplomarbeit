{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE OverloadedStrings #-}

-- # Standard Library
import Data.Maybe
import Control.Applicative
import qualified Data.Text as T
import qualified Data.List as L

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

-- # Site Packages

-- # LOCAL
import Search.Common
import qualified Search.Dictionary as D

-- # HTF
import Test.Framework

-- # DEBUG
import Debug.Trace

test_newToken :: IO ()
test_newToken =
    do let new = D.emptyWordMap
       let (token, newMap) = D.addToken new "word"
       assertBool $ isJust newMap
       assertEqual token (Token 0)

test_doubleAdd :: IO ()
test_doubleAdd =
    do let new = D.emptyWordMap
       let (token1, map1) = D.addToken new "word"
       assertBool $ isJust map1
       let (token2, map2) = D.addToken (fromJust map1) "word"
       assertEqual token1 token2
       assertBool $ isNothing map2

test_lookup :: IO ()
test_lookup =
    do let new = D.emptyWordMap
       let (token1 ,Just map1) = D.addToken new "word"
       assertEqual (Just token1) (D.lookupWord map1 "word")
       assertEqual Nothing (D.lookupWord map1 "antiword")

addAll :: D.WordMap -> [T.Text] -> D.WordMap
addAll m ts = foldl add m ts
    where
      add m t =
        case D.addToken m t of
          (_, Just nm) -> nm
          (_, Nothing) -> m

instance Arbitrary T.Text where
    arbitrary = T.filter (/= '\NUL') . T.pack <$> arbitrary

instance Arbitrary D.WordMap where
    arbitrary = addAll D.emptyWordMap <$> arbitrary

instance Arbitrary D.TokenMap where
    arbitrary = D.buildTokenMap <$> arbitrary

prop_noDoubleAdd :: [T.Text] -> Bool
prop_noDoubleAdd ts = addAll D.emptyWordMap ts == addAll (addAll D.emptyWordMap ts) ts

prop_addAll :: [T.Text] -> Bool
prop_addAll ts =
    let m = addAll D.emptyWordMap ts
        uniqs = L.nub ts
    in length uniqs == (length $ mapMaybe (D.lookupWord m) uniqs)

prop_tokenMapConversion :: D.WordMap -> Bool
prop_tokenMapConversion wm = wm == (D.buildWordMap $ D.buildTokenMap wm)

prop_tokenMapLookup :: [T.Text] -> Bool
prop_tokenMapLookup ts =
    let wm = addAll D.emptyWordMap ts
        tm = D.buildTokenMap wm
    in ts == (map (D.lookupToken tm) $ mapMaybe (D.lookupWord wm) ts)

prop_serialization :: D.TokenMap -> Bool
prop_serialization m = m == runGet get (runPut $ put m)

main = htfMain htf_thisModulesTests
