{-# LANGUAGE FlexibleContexts #-}
module Search.Intersect.Lookup
    (
      Lookup
    , new, intersect, size
    , benchIntersect
    ) where

import qualified Search.Foreign.LookupFFI as LF

import qualified Data.Vector.Generic as V

import Data.Word

newtype Lookup = Lookup (LF.Lookup)

new :: (V.Vector v Word32) => v Word32 -> Lookup
new elems = Lookup $ LF.new $ V.convert elems

intersect :: (V.Vector v Word32) => Lookup -> Lookup -> v Word32
intersect (Lookup l1) (Lookup l2) = V.convert $ LF.intersect l1 l2

size :: Lookup -> Int
size (Lookup l) = LF.size l

benchIntersect :: (V.Vector v Word32) => Lookup -> Lookup -> IO (Double, v Word32)
benchIntersect (Lookup l1) (Lookup l2) =
    do (t, v) <- LF.benchIntersect l1 l2
       return $ (t, V.convert v)
