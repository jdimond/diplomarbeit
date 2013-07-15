{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# OPTIONS_GHC -fno-cse -fno-full-laziness #-} -- recommended by GHC manual
{-# OPTIONS_GHC -Wwarn #-}

#include "lookup.h"

module Search.Foreign.LookupFFI
    (
      Lookup
    , new, size, intersect
    , benchIntersect
    ) where

import Data.Word
import System.IO.Unsafe (unsafePerformIO)
import Foreign.Ptr
import Foreign.ForeignPtr

import qualified Data.Vector.Storable as SV
import qualified Data.Vector.Storable.Mutable as MSV

import Criterion.Measurement

data LookupPtr
type Lookup = ForeignPtr LookupPtr

foreign import ccall unsafe "lookup_new_set" cLookupNewSet
    :: Ptr Word32
    -> (#type size_t)
    -> IO (Ptr LookupPtr)

foreign import ccall unsafe "&lookup_destroy" cLookupDestroy
    :: FunPtr (Ptr LookupPtr -> IO ())

foreign import ccall unsafe "lookup_set_size" cLookupSetSize
    :: Ptr LookupPtr
    -> (#type size_t)

foreign import ccall unsafe "lookup_intersect" cLookupIntersect
    :: Ptr LookupPtr
    -> Ptr LookupPtr
    -> Ptr Word32
    -> (#type size_t)

size :: Lookup -> Int
size l = unsafePerformIO $ withForeignPtr l $ return . fromIntegral . cLookupSetSize

new :: SV.Vector Word32 -> Lookup
new setelems =
    unsafePerformIO $
    SV.unsafeWith setelems $ \elemsptr ->
        do l <- cLookupNewSet elemsptr (fromIntegral $ SV.length setelems)
           if l == nullPtr
              then error "Failed to create Lookup"
              else newForeignPtr cLookupDestroy l

intersect :: Lookup -> Lookup -> SV.Vector Word32
intersect z1 z2 =
    unsafePerformIO $
      do v <- MSV.unsafeNew $ min (size z1) (size z2)
         s <- MSV.unsafeWith v $ \outptr ->
                   withForeignPtr z1 $ \ptr1 ->
                     withForeignPtr z2 $ \ptr2 ->
                       return $ cLookupIntersect ptr1 ptr2 outptr
         SV.freeze $ MSV.take (fromIntegral s) v

benchIntersect :: Lookup -> Lookup -> IO (Double, SV.Vector Word32)
benchIntersect z1 z2 =
  do v <- MSV.unsafeNew $ min (size z1) (size z2)
     (t, s) <- MSV.unsafeWith v $ \outptr ->
               withForeignPtr z1 $ \ptr1 ->
                 withForeignPtr z2 $ \ptr2 ->
                   time $ return $! cLookupIntersect ptr1 ptr2 outptr
     frozen <- SV.freeze $ MSV.take (fromIntegral s) v
     return (t,frozen)
