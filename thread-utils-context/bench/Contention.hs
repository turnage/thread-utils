{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}

-- | Contention & scaling benchmark for ThreadStorageMap.
--
-- Measures aggregate throughput at increasing thread counts to demonstrate
-- the implementation scales with capability count. Reports speedup relative
-- to single-threaded baseline.
--
-- Uses -A128m nursery (baked into .cabal) to isolate data-structure contention
-- from GC stop-the-world effects.
module Main (main) where

import Control.Concurrent
import qualified Control.Concurrent.Thread.Storage as S
import Control.Monad (forM_, replicateM, replicateM_, unless, void, when)
import Data.IORef
import Data.List (maximumBy)
import Data.Ord (comparing)
import GHC.Clock (getMonotonicTimeNSec)
import Prelude hiding (lookup)
import System.IO (hFlush, stdout)
import Text.Printf (printf)


itersPerThread :: Int
itersPerThread = 2_000_000


main :: IO ()
main = do
  caps <- getNumCapabilities
  printf "thread-utils-context — contention & scaling\n"
  printf "Capabilities: %d · Iters/thread: %d · Best of 5\n" caps itersPerThread
  putStrLn (replicate 60 '=')

  let ns = threadCounts caps

  printf "\nWarming up CPU (%d threads)...\n\n" caps
  replicateM_ 3 $
    void $ timed caps $ \tsm _ref -> do
      let go 0 = pure ()
          go !i = do { !_ <- S.lookup tsm; go (i - 1) }
      go itersPerThread

  section "Read-only (fused CMM lookup)" ns $ \tsm _ref -> do
    let go 0 = pure ()
        go !i = do { !_ <- S.lookup tsm; go (i - 1) }
    go itersPerThread

  section "Write, no alloc (update → IORef write)" ns $ \tsm _ref -> do
    let !val = (42 :: Int)
    let go 0 = pure ()
        go !i = do
          S.update tsm $ \_ -> (Just val, ())
          go (i - 1)
    go itersPerThread

  section "Read+write, no alloc (lookup + 2× update)" ns $ \tsm _ref -> do
    let !val = (42 :: Int)
    let go 0 = pure ()
        go !i = do
          !_ <- S.lookup tsm
          S.update tsm $ \_ -> (Just val, ())
          S.update tsm $ \_ -> (Just val, ())
          go (i - 1)
    go itersPerThread

  section "Span lifecycle (lookup + 2× update, alloc)" ns $ \tsm _ref -> do
    let go 0 = pure ()
        go !i = do
          !_ <- S.lookup tsm
          S.update tsm $ \old -> (Just $! maybe 1 (+ 1) old, ())
          S.update tsm $ \old -> (Just $! maybe 0 (subtract 1) old, ())
          go (i - 1)
    go itersPerThread

  section "Attach/detach cycle (reattach path)" ns $ \tsm _ref -> do
    let go 0 = pure ()
        go !i = do
          void $ S.attach tsm i
          void $ S.detach tsm
          go (i - 1)
    go itersPerThread

  section "Cached IORef (read + 2× write, no probe)" ns $ \_tsm ref -> do
    let go 0 = pure ()
        go !i = do
          !_ <- S.readRef ref
          S.writeRef ref $! i
          S.writeRef ref $! i - 1
          go (i - 1)
    go itersPerThread

  section "Ref-based (CMM probe + read + 2× write)" ns $ \tsm _ref -> do
    let go 0 = pure ()
        go !i = do
          (!_tid, mref) <- S.lookupRefFast tsm
          case mref of
            Just r -> do
              !_ <- S.readRef r
              S.writeRef r $! i
              S.writeRef r $! i - 1
            Nothing -> pure ()
          go (i - 1)
    go itersPerThread


---------------------------------------------------------------------------

type Bench = S.ThreadStorageMap Int -> IORef Int -> IO ()


threadCounts :: Int -> [Int]
threadCounts caps
  | caps <= 0 = [1]
  | otherwise =
      let powers = takeWhile (<= caps) (iterate (* 2) 1)
      in if last powers == caps then powers else powers ++ [caps]


section :: String -> [Int] -> Bench -> IO ()
section name ns bench = do
  printf "── %s ──\n" name
  printf "  %4s  %8s  %14s  %8s  %8s\n"
    ("N" :: String) ("ns/op" :: String) ("total ops/s" :: String)
    ("speedup" :: String) ("ideal" :: String)
  baseRef <- newIORef (0.0 :: Double)
  forM_ ns $ \n -> do
    void $ timed n bench
    results <- replicateM 5 (timed n bench)
    let (nsOp, total, _) = maximumBy (comparing (\(_, t, _) -> t)) results
    b <- readIORef baseRef
    when (b == 0) $ writeIORef baseRef total
    base <- readIORef baseRef
    let speedup = total / base
        ideal = fromIntegral n :: Double
    printf "  %4d  %8.0f  %14.0f  %7.1fx  %7.1fx\n" n nsOp total speedup ideal
    hFlush stdout
  printf "\n"


timed :: Int -> Bench -> IO (Double, Double, Double)
timed numThreads bench = do
  tsm <- S.newThreadStorageMap
  readyRef <- newIORef (0 :: Int)
  goRef <- newIORef False
  doneRef <- newIORef (0 :: Int)

  replicateM_ numThreads $ forkIO $ do
    tid <- myThreadId
    let !tw = fromIntegral (S.getThreadId tid) :: Int
    ref <- S.ensureRef tsm tid tw (0 :: Int)
    atomicModifyIORef' readyRef (\x -> (x + 1, ()))
    let spin = readIORef goRef >>= \go -> unless go (yield >> spin)
    spin
    bench tsm ref
    atomicModifyIORef' doneRef (\x -> (x + 1, ()))

  let waitReady = readIORef readyRef >>= \c ->
        when (c < numThreads) (yield >> waitReady)
  waitReady

  t0 <- getMonotonicTimeNSec
  writeIORef goRef True

  let waitDone = readIORef doneRef >>= \c ->
        when (c < numThreads) (yield >> waitDone)
  waitDone

  t1 <- getMonotonicTimeNSec

  let ops = numThreads * itersPerThread
      wall = fromIntegral (t1 - t0) :: Double
      nsOp = wall / fromIntegral ops
      total = fromIntegral ops / (wall / 1e9)
      perThr = total / fromIntegral numThreads
  pure (nsOp, total, perThr)
