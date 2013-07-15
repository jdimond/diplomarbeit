{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Search.IndexedSet
    (
      IndexedSet, IndexedSetBuilder
    , newIndexedSetBuilder, addToIndex
    , enumerateIndexKeys
    , buildIndex
    , indexedSetFromList
    , lookup, lookupV, setSizes, allElems
    , IndexedSetHandle, withIndexedSetFile, readSet, joinFiles
    ) where

import Prelude hiding (lookup)

import Control.Monad
import Control.Monad.ST
import Control.Monad.Primitive

import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as MVU
import qualified Data.Vector.Mutable as MV

import qualified Data.List as L

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

import System.IO.Unsafe (unsafePerformIO)
import qualified System.IO as IO

import qualified Data.ByteString.Lazy as BSL

import Search.Common

type AppendVector s t = MVU.MVector s t

newAppendVector :: (PrimMonad m, Identifier t) => m (AppendVector (PrimState m) t)
newAppendVector =
    do v <- MVU.unsafeNew 1
       MVU.unsafeWrite v 0 $ boxId 0
       return v

append :: (PrimMonad m, Identifier t)
       => AppendVector (PrimState m) t
       -> t
       -> m (AppendVector (PrimState m) t)
append v t =
    do oldSize <- liftM unboxId $ MVU.unsafeRead v 0
       let newSize = oldSize + 1
       let len = fromIntegral $ MVU.length v
       v' <- if (newSize > len - 1)
                then MVU.grow v (fromIntegral len*2)
                else return v
       MVU.unsafeWrite v' 0 $ boxId newSize
       MVU.unsafeWrite v' (fromIntegral newSize) t
       return v'

freeze :: (PrimMonad m, Identifier t)
       => AppendVector (PrimState m) t
       -> m (VU.Vector t)
freeze v =
    do size <- liftM (fromIntegral . unboxId) $ MVU.unsafeRead v 0
       VU.freeze $ MVU.slice 1 size v

data IndexedSetBuilder s t0 t1 = IndexedSetBuilder !(MV.MVector s (AppendVector s t1))
data IndexedSet t0 t1 = IndexedSet !(VU.Vector Word64) !(VU.Vector t1)
    deriving (Show, Eq)

newIndexedSetBuilder :: (PrimMonad m, Identifier t0, Identifier t1) => Int -> m (IndexedSetBuilder (PrimState m) t0 t1)
newIndexedSetBuilder size = do
    v <- MV.replicateM size newAppendVector
    return $ IndexedSetBuilder v

addToIndex :: (PrimMonad m, Identifier t0, Identifier t1)
           => IndexedSetBuilder (PrimState m) t0 t1 -> t0 -> t1 -> m ()
addToIndex (IndexedSetBuilder vector) key e = do
    let k = fromIntegral $ unboxId key
    old <- MV.read vector k
    new <- append old e
    MV.write vector k new

nubSort :: (Eq a, Ord a) => [a] -> [a]
nubSort [] = []
nubSort l = nub' (head sorted) (tail sorted)
  where
    sorted = L.sort l
    nub' e [] = [e]
    nub' e1 (e2:es)
        | e1 == e2 = nub' e2 es
        | otherwise = e1 : nub' e2 es

buildIndex :: (PrimMonad m, Identifier t0, Identifier t1)
           => IndexedSetBuilder (PrimState m) t0 t1
           -> m (IndexedSet t0 t1)
buildIndex (IndexedSetBuilder vector) = do
    frozen <- V.freeze vector
    let go i v acc
           | i >= V.length v = return $ V.fromList $ reverse acc
           | otherwise =
               do f <- freeze $ v V.! i
                  let !nubSorted = VU.fromList $ nubSort $ VU.toList f
                  go (i+1) v (nubSorted:acc)
    sorted <- go 0 frozen []
    let !offsets = VU.scanl' (+) (0 :: Word64) $ V.convert $ V.map (fromIntegral . VU.length) sorted
    return $ IndexedSet offsets (VU.concat $ V.toList sorted)

indexedSetFromList :: (Identifier t0, Identifier t1) => [[t1]] -> IndexedSet t0 t1
indexedSetFromList l = runST $
    do builder <- newIndexedSetBuilder len
       forM_ (zip ids l) $ \(i :: t0, is) ->
           mapM (addToIndex builder i) is
       buildIndex builder
    where len = length l
          ids = map (boxId . fromIntegral) [0..len-1]

enumerateIndexKeys :: (Identifier t0) => IndexedSet t0 t1 -> [t0]
enumerateIndexKeys (IndexedSet offsets _) = map (boxId . fromIntegral) [0..VU.length offsets-2]

lookup :: (Identifier t0, Identifier t1) => IndexedSet t0 t1 -> t0 -> [t1]
lookup s i = VU.toList $ lookupV s i

lookupV :: (Identifier t0, Identifier t1) => IndexedSet t0 t1 -> t0 -> VU.Vector t1
lookupV (IndexedSet offsets ids) k =
    let ku = fromIntegral $ unboxId k
        offset = fromIntegral $ offsets VU.! ku
        len = setSize offsets ku
    in VU.slice offset len ids

allElems :: (Identifier t0, Identifier t1) => IndexedSet t0 t1 -> VU.Vector t1
allElems (IndexedSet _ elems) = elems

setSize :: VU.Vector Word64 -> Int -> Int
{-# INLINE setSize #-}
setSize ov i =
    let offset = ov VU.! i
    in fromIntegral $ (ov VU.! (i + 1)) - offset

setSizes' :: VU.Vector Word64 -> VU.Vector Int
setSizes' offsets = VU.generate n (setSize offsets)
    where n = VU.length offsets - 1

setSizes :: (Identifier t0) => IndexedSet t0 t1 -> VU.Vector Int
setSizes (IndexedSet offsets _) = setSizes' offsets

lift :: IO b -> Get b
lift = return .unsafePerformIO

data IndexedSetHandle t0 t1 = IndexedSetHandle IO.Handle !(VU.Vector Word64) !Word64
    deriving (Eq)

indexedSetHandle :: (Identifier t0, Identifier t1) => IO.Handle -> IO (IndexedSetHandle t0 t1)
indexedSetHandle handle =
      do bs <- BSL.hGetContents handle
         let (offsetv, readBytes) = flip runGet bs $
                do ov <- getOffsets
                   l <- bytesRead
                   return (ov, l)
         return $! IndexedSetHandle handle offsetv (fromIntegral readBytes)

closeIndexedSetHandle :: (Identifier t0, Identifier t1) => IndexedSetHandle t0 t1 -> IO ()
closeIndexedSetHandle (IndexedSetHandle handle _ _) = IO.hClose handle

withIndexedSetFile :: (Identifier t0, Identifier t1)
                   => FilePath
                   -> (IndexedSetHandle t0 t1 -> IO a)
                   -> IO a
withIndexedSetFile path action =
    IO.withBinaryFile path IO.ReadMode $ \handle ->
        indexedSetHandle handle >>= action

readSet :: (Identifier t0, Identifier t1) => IndexedSetHandle t0 t1 -> t0 -> IO (VU.Vector t1)
readSet (IndexedSetHandle handle offsets fileOffset) k =
    do IO.hSeek handle IO.AbsoluteSeek offset
       bs <- BSL.hGet handle (len*4)
       return $! runGet (getIds len) bs
    where ku = fromIntegral $ unboxId k
          listOffset = fromIntegral $ offsets VU.! ku
          offset = fromIntegral $ fileOffset + 4*listOffset
          len = setSize offsets ku

allTheSame :: (Eq a) => [a] -> Bool
allTheSame xs = and $ map (== head xs) (tail xs)

joinOffsets :: [VU.Vector Word64] -> VU.Vector Word64
joinOffsets = L.foldl1' (VU.zipWith (+))

joinSets :: Identifier t0 => [VU.Vector t0] -> VU.Vector t0
joinSets ss =
    let new = VU.fromList $ nubSort $ concatMap VU.toList ss
        origlength = sum $ map VU.length ss
    in if VU.length new /= origlength
          then error "One or more element was in multiple sets"
          else new

joinFiles :: [FilePath]
          -> FilePath
          -> IO ()
joinFiles paths outpath =
    do filehandles <- mapM (flip IO.openBinaryFile IO.ReadMode) paths
       sethandles <- mapM (indexedSetHandle) filehandles :: IO [IndexedSetHandle DocId DocId] -- Use dummy type
       let ovs = map (\(IndexedSetHandle _ ov _) -> ov) sethandles
       let sizes = map VU.length ovs
       unless (allTheSame sizes) $ error "files do not match"
       outhandle <- IO.openBinaryFile outpath IO.WriteMode
       let newov = joinOffsets ovs
       BSL.hPut outhandle $ runPut $ putOffsets newov
       let numElems = fromIntegral $ VU.length newov - 1
       forM_ (map boxId [0..numElems-1]) $ \i -> do
         do sets <- mapM (flip readSet i) sethandles
            let joined = joinSets sets
            let bs = runPut $ VU.mapM_ (putWord32be . unboxId) joined
            BSL.hPut outhandle bs
       IO.hClose outhandle
       mapM_ closeIndexedSetHandle sethandles

getOffsets :: Get (VU.Vector Word64)
getOffsets = do
    totalSize <- liftM fromIntegral getWord32be
    numLists <- liftM fromIntegral getWord32be :: Get Integer
    -- OFFSETS - Inspired by Don Stewart --
    mlengths <- lift $ MVU.replicate totalSize 0
    let fillOffset i
          | i < numLists =
              do e <- liftM fromIntegral getWord32be
                 l <- liftM fromIntegral getWord32be
                 (unsafePerformIO $ MVU.unsafeWrite mlengths e l) `seq` return ()
                 fillOffset (i+1)
          | otherwise = return ()
    fillOffset 0
    lengths <- lift $ VU.unsafeFreeze mlengths
    return $ VU.scanl' (+) 0 lengths

getIds :: Identifier t1 => Int -> Get (VU.Vector t1)
getIds numIds = do
    -- IDS - Inspired by Don Stewart --
    mids <- lift $ MVU.new numIds
    let fillIds i
          | i < numIds =
              do w <- liftM boxId getWord32be
                 (unsafePerformIO $ MVU.unsafeWrite mids i w) `seq` return ()
                 fillIds (i+1)
          | otherwise = return ()
    fillIds 0
    lift $ VU.unsafeFreeze mids

putOffsets :: VU.Vector Word64 -> Put
putOffsets offsets =
    do let filtered = VU.filter ((> 0) . snd) $ VU.indexed $ setSizes' offsets
       putWord32be $ fromIntegral $ VU.length offsets - 1
       putWord32be $ fromIntegral $ VU.length filtered
       VU.mapM_ (\(i,l) -> putWord32be (fromIntegral i) >> putWord32be (fromIntegral l)) filtered


instance (Identifier t0, Identifier t1) => Binary (IndexedSet t0 t1) where
    put (IndexedSet offsets ids) = do
        putOffsets offsets
        VU.mapM_ (putWord32be . unboxId) ids
    get = do
        offsetv <- getOffsets
        let numIds = fromIntegral $ VU.last offsetv
        idv <- getIds numIds
        empty <- isEmpty
        if empty
           then return $ IndexedSet offsetv idv
           else error "not a correct IndexedSet"
