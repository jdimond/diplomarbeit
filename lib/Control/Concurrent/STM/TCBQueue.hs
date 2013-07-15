{-# LANGUAGE DeriveDataTypeable #-}
module Control.Concurrent.STM.TCBQueue
    (
      TCBQueue, TQueueResult(TQueued, TClosed)
    , newTCBQueue, newTCBQueueIO, readTCBQueue, tryReadTCBQUeue
    , peekTCBQueue, tryPeekTCBQueue, writeTCBQueue, tryWriteTCBQueue
    , closeTCBQueue, safeCloseTCBQueue
    , isEmptyTCBQueue, isClosedTCBQueue, isFinishedTCBQueue
    ) where

import Control.Concurrent.STM

import Control.Monad (when, liftM)

import Data.Typeable


data TCBQueue a
   = TCBQueue {-# UNPACK #-} !(TBQueue a)
              {-# UNPACK #-} !(TVar Bool)
     deriving (Typeable, Eq)

data TQueueResult a
   = TQueued !a
   | TClosed

newTCBQueue :: Int -> STM (TCBQueue a)
newTCBQueue size = do
    closed <- newTVar False
    queue <- newTBQueue size
    return (TCBQueue queue closed)

newTCBQueueIO :: Int -> IO (TCBQueue a)
newTCBQueueIO size = do
    closed <- newTVarIO False
    queue <- newTBQueueIO size
    return (TCBQueue queue closed)

whenOpen :: (TVar Bool) -> STM (TQueueResult a) -> STM (TQueueResult a)
whenOpen tclosed s = do
    closed <- readTVar tclosed
    if not closed
       then s
       else return TClosed

readTCBQueue :: TCBQueue a -> STM (TQueueResult a)
readTCBQueue (TCBQueue queue tclosed) = do
    melem <- tryReadTBQueue queue
    case melem of
      Just a -> return $ TQueued a
      Nothing -> whenOpen tclosed retry

tryReadTCBQUeue :: TCBQueue a -> STM (TQueueResult (Maybe a))
tryReadTCBQUeue (TCBQueue queue tclosed) = do
    melem <- tryReadTBQueue queue
    case melem of
      Just a -> return $ TQueued $ Just a
      Nothing -> whenOpen tclosed $ return $ TQueued Nothing

peekTCBQueue :: TCBQueue a -> STM (TQueueResult a)
peekTCBQueue (TCBQueue _ _) = undefined

tryPeekTCBQueue :: TCBQueue a -> STM (TQueueResult (Maybe a))
tryPeekTCBQueue = undefined

writeTCBQueue :: TCBQueue a -> a -> STM ()
writeTCBQueue (TCBQueue queue tclosed) a = do
    closed <- readTVar tclosed
    if closed
       then error "wrote to closed stm queue"
       else writeTBQueue queue a

tryWriteTCBQueue :: TCBQueue a -> a -> STM Bool
tryWriteTCBQueue (TCBQueue queue tclosed) a = do
    open <- liftM not $ readTVar tclosed
    when open $ writeTBQueue queue a
    return open

isEmptyTCBQueue :: TCBQueue a -> STM Bool
isEmptyTCBQueue (TCBQueue queue _) = isEmptyTBQueue queue

isClosedTCBQueue :: TCBQueue a -> STM Bool
isClosedTCBQueue (TCBQueue _ tclosed) = readTVar tclosed

isFinishedTCBQueue :: TCBQueue a -> STM Bool
isFinishedTCBQueue t = do
    closed <- isClosedTCBQueue t
    empty <- isEmptyTCBQueue t
    return $ closed && empty

safeCloseTCBQueue :: TCBQueue a -> STM ()
safeCloseTCBQueue (TCBQueue _ tclosed) = writeTVar tclosed True

closeTCBQueue :: TCBQueue a -> STM ()
closeTCBQueue (TCBQueue _ tclosed) = do
    closed <- readTVar tclosed
    if closed
       then error "queue already closed"
       else writeTVar tclosed True
