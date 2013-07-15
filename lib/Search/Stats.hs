{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
module Search.Stats
    (
      MTFCounter, TFCounter(TFCounter)
    , newMTFCounter, countTermFrequencies
    , tfFreeze, tfSize
    ) where

import qualified Data.Vector.Unboxed.Mutable as MVU
import qualified Data.Vector.Unboxed as VU

import Data.Binary
import qualified Data.Binary.Put as P
import qualified Data.Binary.Get as G

import Control.Monad (foldM, liftM)

import Control.Concurrent.MVar

import Search.Common

type MCounterArray = MVU.IOVector Word32
data MTFCounter = MTFCounter (MVar MCounterArray) (MVar Int)

newtype TFCounter = TFCounter (VU.Vector Word32)

tfFreeze :: MTFCounter -> IO TFCounter
tfFreeze (MTFCounter vref mref) = do
    m <- takeMVar mref
    v <- takeMVar vref
    frozen <- VU.freeze $ MVU.take (m+1) v
    putMVar vref v
    putMVar mref m
    return $ TFCounter frozen

instance Binary TFCounter where
    put (TFCounter v) = VU.mapM_ P.putWord32be v
    get = liftM (TFCounter . VU.fromList) freqs
        where freqs = do
                  empty <- G.isEmpty
                  if empty
                     then return []
                     else do freq <- G.getWord32be
                             rest <- freqs
                             return $ freq : rest

_DEFAULT_SIZE_ :: Int
_DEFAULT_SIZE_ = 1024

newMTFCounter :: IO MTFCounter
newMTFCounter = do
    vref <- newMVar =<< MVU.replicate _DEFAULT_SIZE_ 0
    mref <- newMVar 0
    return $ MTFCounter vref mref

countTokenDoc' :: Int -> MCounterArray -> TokenDoc -> IO (Int, MCounterArray)
countTokenDoc' curMax curV doc = foldM add (curMax, curV) $ tokensFromDoc doc
    where add (cm,cv) (Token tw) = do
              let t = fromIntegral tw
              let nm = max t cm
              !nv <- do let l = MVU.length cv
                        if t >= l then enlarge cv (max l (t-l+1)) else return cv
              cur <- MVU.unsafeRead nv t
              let new = if (cur+1) < cur -- Overflow
                           then cur
                           else cur+1
              MVU.unsafeWrite nv t new
              return (nm, nv)
          enlarge v by = do
              v' <- MVU.replicate (n+by) 0
              MVU.unsafeCopy (MVU.unsafeSlice 0 n v') v
              return v'
              where
                n = MVU.length v

countTermFrequencies :: MTFCounter -> TokenDoc -> IO ()
countTermFrequencies (MTFCounter vref mref) doc = do
    curm <- takeMVar mref
    curv <- takeMVar vref
    (!newm, !newv) <- countTokenDoc' curm curv doc
    putMVar vref newv
    putMVar mref newm

tfSize :: TFCounter -> Int
tfSize (TFCounter v) = VU.length v
