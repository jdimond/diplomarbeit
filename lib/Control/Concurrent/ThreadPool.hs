{-# LANGUAGE BangPatterns #-}

module Control.Concurrent.ThreadPool
    (
      ThreadPool, Future
    , newThreadPool
    , schedule
    , concurrentMap, concurrentMap_
    , concurrentPoolMap, concurrentPoolMap_
    , concurrentMapN, concurrentMapN_
    ) where

import Control.Concurrent
import Control.Concurrent.STM

import Control.Monad

import qualified Data.Traversable as T

data ThreadPool = ThreadPool (TQueue (IO ()))

data Future a = Future (MVar a)

newThreadPool :: Int -> IO ThreadPool
newThreadPool size =
    do queue <- newTQueueIO
       forM_ [1..size] (const $ forkIO $ worker queue)
       return $ ThreadPool queue
    where
      worker queue = forever $
                     do action <- atomically $ readTQueue queue
                        action

schedule :: ThreadPool -> IO a -> IO (Future a)
schedule (ThreadPool queue) action =
    do mvar <- newEmptyMVar
       let a = do !result <- action
                  putMVar mvar result
       atomically $ writeTQueue queue a
       return $ Future mvar
       {--
       _ <- forkIO $ do atomically $ waitTSem sem
                        result <- action
                        atomically $ putTMVar tmvar result
                        atomically $ signalTSem sem
       return $ Future tmvar
       --}

wait :: Future a -> IO a
wait (Future mvar) = takeMVar mvar

concurrentPoolMap_ :: (T.Traversable t) => ThreadPool -> (a -> IO b) -> t a -> IO ()
concurrentPoolMap_ pool action t = void $ concurrentPoolMap pool (void . action) t

concurrentPoolMap :: (T.Traversable t) => ThreadPool -> (a -> IO b) -> t a -> IO (t b)
concurrentPoolMap pool action t =
    do futures <- T.mapM (schedule pool . action) t
       T.mapM wait futures

concurrentMapN_ :: (T.Traversable t) => Int -> (a -> IO b) -> t a -> IO ()
concurrentMapN_ n f t =
    do pool <- newThreadPool n
       concurrentPoolMap_ pool f t

concurrentMapN :: (T.Traversable t) => Int -> (a -> IO b) -> t a -> IO (t b)
concurrentMapN n f t =
    do pool <- newThreadPool n
       concurrentPoolMap pool f t

concurrentMap_ :: (T.Traversable t) => (a -> IO b) -> t a -> IO ()
concurrentMap_ action t = void $ concurrentMap (void . action) t

concurrentMap :: (T.Traversable t) => (a -> IO b) -> t a -> IO (t b)
concurrentMap action t =
    do futures <- T.mapM forkMVar t
       T.mapM takeMVar futures
    where forkMVar b =
              do mvar <- newEmptyMVar
                 _ <- forkIO $ action b >>= putMVar mvar
                 return mvar
