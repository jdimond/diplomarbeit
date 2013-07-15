{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE FlexibleContexts #-}
module Search.Common
    (
      Identifier, IdType, unboxId, boxId, putId, getId
    , Token(Token), DocId(DocId), unToken, unDocId
    , TokenDoc, docFromTokens, tokensFromDoc, tokenDocLength, termFrequencies, vectorFromDoc, docFromVector
    , IndexedTokenDoc(IndexedTokenDoc), docId, tokenDoc
    ) where

import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector as V
import qualified Data.Vector.Generic.Base as VG
import qualified Data.Vector.Generic.Mutable as VGM

import Data.Binary
import qualified Data.Binary.Put as P
import qualified Data.Binary.Get as G

import qualified Data.List as L

import Data.Hashable

import Control.DeepSeq

import Control.Monad (liftM)

type IdType = Word32

class (Eq t,
       Ord t,
       NFData t,
       VU.Unbox t,
       VG.Vector VU.Vector t,
       VGM.MVector VU.MVector t) => Identifier t where
    unboxId :: t -> IdType
    boxId :: IdType -> t
    putId :: t -> Put
    getId :: Get t

newtype Token = Token { unToken :: IdType }
    deriving (Show, Eq, Ord, VG.Vector VU.Vector, VGM.MVector VU.MVector, VU.Unbox, Hashable)

instance NFData Token where
    rnf (Token t) = t `seq` ()

instance Identifier Token where
    unboxId (Token t) = t
    boxId = Token
    putId (Token t) = P.putWord32be t
    getId = fmap Token G.getWord32be

newtype DocId = DocId { unDocId :: IdType }
    deriving (Show, Eq, Ord, NFData, VG.Vector VU.Vector, VGM.MVector VU.MVector, VU.Unbox, Hashable)

instance Identifier DocId where
    unboxId (DocId d) = d
    boxId = DocId
    putId (DocId d) = P.putWord32be d
    getId = fmap DocId G.getWord32be

newtype TokenDoc = TokenDoc (VU.Vector Token)
    deriving (Show, Eq, NFData)

data IndexedTokenDoc = IndexedTokenDoc { docId :: !DocId, tokenDoc :: !TokenDoc }
    deriving (Show, Eq)

instance NFData IndexedTokenDoc where
    rnf (IndexedTokenDoc a b) = rnf a `seq` rnf b `seq` ()

instance Binary TokenDoc where
    put doc = mapM_ putId $ tokensFromDoc doc
    get = liftM docFromTokens getTokenList
        where getTokenList = do
                  empty <- G.isEmpty
                  if empty
                     then return []
                     else do token <- getId
                             rest <- getTokenList
                             return $ token : rest

tokenDocLength :: TokenDoc -> Int
tokenDocLength (TokenDoc doc) = VU.length doc
{-# INLINE tokenDocLength #-}

docFromTokens :: [Token] -> TokenDoc
docFromTokens = TokenDoc . VU.fromList
{-# INLINE docFromTokens #-}

tokensFromDoc :: TokenDoc -> [Token]
tokensFromDoc (TokenDoc doc) = VU.toList doc
{-# INLINE tokensFromDoc #-}

vectorFromDoc :: TokenDoc -> VU.Vector Token
vectorFromDoc (TokenDoc doc) = V.convert doc
{-# INLINE vectorFromDoc #-}

docFromVector :: VU.Vector Token -> TokenDoc
docFromVector = TokenDoc
{-# INLINE docFromVector #-}

termFrequencies :: TokenDoc -> [(Token, Int)]
termFrequencies = map (\(x:xs) -> (x, length xs + 1)) . L.group . L.sort . tokensFromDoc
