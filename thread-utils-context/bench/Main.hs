{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE NumericUnderscores #-}

module Main (main) where

import Control.Concurrent
import qualified Control.Concurrent.Thread.Storage as S
import Control.Monad (replicateM, void, when)
import Data.IORef
import GHC.Clock (getMonotonicTimeNSec)
import Prelude hiding (lookup)
import System.IO (hFlush, stdout)
import Text.Printf (printf)


itersPerThread :: Int
itersPerThread = 200_000


main :: IO ()
main = do
  caps <- getNumCapabilities
  printf "=== TLS contention benchmark (capabilities: %d) ===\n\n" caps

  putStrLn "======== High-level API (fused CMM probe) ========"
  putStrLn ""

  putStrLn "--- lookup (read-only, fused CMM probe) ---"
  mapM_ (\n -> runCompat n benchLookupHL) threadCounts

  putStrLn "\n--- full cycle: lookup + 2x update (fused CMM) ---"
  mapM_ (\n -> runCompat n benchFullCycleHL) threadCounts

  putStrLn ""
  putStrLn "======== Compat API (lookupRaw / updateRaw) ========"
  putStrLn ""

  putStrLn "--- lookupRaw (read-only) ---"
  mapM_ (\n -> runCompat n benchLookupRaw) threadCounts

  putStrLn "\n--- full cycle: lookupRaw + 2x updateRaw ---"
  mapM_ (\n -> runCompat n benchFullCycleCompat) threadCounts

  putStrLn ""
  putStrLn "======== Ref-based API (per-thread IORef) ========"
  putStrLn ""

  putStrLn "--- lookupRef (probe + IORef deref, read-only) ---"
  mapM_ (\n -> runRef n benchLookupRef) threadCounts

  putStrLn "\n--- cached ref (readIORef, zero probe) ---"
  mapM_ (\n -> runRef n benchCachedRead) threadCounts

  putStrLn "\n--- full cycle: lookupRef + read + 2x write ---"
  mapM_ (\n -> runRef n benchFullCycleRef) threadCounts

  putStrLn "\n--- full cycle: cached ref (read + 2x write, zero probe) ---"
  mapM_ (\n -> runRef n benchCachedCycle) threadCounts

  putStrLn "\n--- fused CMM probe (lookupRefFast: tid + slot + key in one CMM call) ---"
  mapM_ (\n -> runRef n benchFusedProbe) threadCounts

  putStrLn "\n--- fused CMM full cycle (lookupRefFast + read + 2x write) ---"
  mapM_ (\n -> runRef n benchFusedCycle) threadCounts

  putStrLn "\n--- getCurrentThreadId (CMM, no ThreadId alloc) ---"
  mapM_ (\n -> runRef n benchGetTid) threadCounts

  putStrLn "\n--- updateRaw (compat path, should be zero-CAS for Just->Just) ---"
  mapM_ (\n -> runRef n benchUpdateRaw) threadCounts


threadCounts :: [Int]
threadCounts = [1, 2, 4, 8, 16]


---------------------------------------------------------------------------
-- Runners
---------------------------------------------------------------------------

runCompat :: Int -> (S.ThreadStorageMap Int -> IO ()) -> IO ()
runCompat numThreads benchFn = do
  tsm <- S.newThreadStorageMap
  runBench numThreads $ \done -> do
    void $ S.attach tsm (0 :: Int)
    benchFn tsm
    atomicModifyIORef' done (\n -> (n + 1, ()))


runRef :: Int -> (S.ThreadStorageMap Int -> IORef Int -> IO ()) -> IO ()
runRef numThreads benchFn = do
  tsm <- S.newThreadStorageMap
  runBench numThreads $ \done -> do
    tid <- myThreadId
    let !tw = fromIntegral (S.getThreadId tid) :: Int
    ref <- S.ensureRef tsm tid tw (0 :: Int)
    benchFn tsm ref
    atomicModifyIORef' done (\n -> (n + 1, ()))


runBench :: Int -> (IORef Int -> IO ()) -> IO ()
runBench numThreads worker = do
  goRef <- newIORef False
  doneRef <- newIORef (0 :: Int)
  let totalOps = numThreads * itersPerThread

  _workers <- replicateM numThreads $ forkIO $ do
    let waitGo = readIORef goRef >>= \go -> when (not go) (yield >> waitGo)
    waitGo
    worker doneRef

  threadDelay 5_000

  wallStart <- getMonotonicTimeNSec
  writeIORef goRef True

  let waitDone = readIORef doneRef >>= \n -> when (n < numThreads) (yield >> waitDone)
  waitDone

  wallEnd <- getMonotonicTimeNSec

  let wallNS = fromIntegral (wallEnd - wallStart) :: Integer
      nsPerOp = wallNS `div` fromIntegral totalOps
      throughput = fromIntegral totalOps / (fromIntegral wallNS / 1e9 :: Double)

  printf "  N=%-2d  %5d ns/op  %12.0f ops/s\n" numThreads nsPerOp throughput
  hFlush stdout


---------------------------------------------------------------------------
-- High-level API benchmarks (fused CMM probe)
---------------------------------------------------------------------------

benchLookupHL :: S.ThreadStorageMap Int -> IO ()
benchLookupHL tsm = do
  let go 0 = pure ()
      go !i = do { !_ <- S.lookup tsm; go (i - 1) }
  go itersPerThread

benchFullCycleHL :: S.ThreadStorageMap Int -> IO ()
benchFullCycleHL tsm = do
  let go 0 = pure ()
      go !i = do
        !_ <- S.lookup tsm
        S.update tsm $ \old -> (Just $! maybe 1 (+1) old, ())
        S.update tsm $ \old -> (Just $! maybe 0 (subtract 1) old, ())
        go (i - 1)
  go itersPerThread


---------------------------------------------------------------------------
-- Compat API benchmarks
---------------------------------------------------------------------------

benchLookupRaw :: S.ThreadStorageMap Int -> IO ()
benchLookupRaw tsm = do
  tid <- myThreadId
  let !tw = S.getThreadId tid
  let go 0 = pure ()
      go !i = do { !_ <- S.lookupRaw tsm tw; go (i - 1) }
  go itersPerThread

benchFullCycleCompat :: S.ThreadStorageMap Int -> IO ()
benchFullCycleCompat tsm = do
  tid <- myThreadId
  let !tw = S.getThreadId tid
  let go 0 = pure ()
      go !i = do
        !_ <- S.lookupRaw tsm tw
        S.updateRaw tsm tid tw $ \old -> (Just $! maybe 1 (+1) old, ())
        S.updateRaw tsm tid tw $ \old -> (Just $! maybe 0 (subtract 1) old, ())
        go (i - 1)
  go itersPerThread


---------------------------------------------------------------------------
-- Ref-based benchmarks
---------------------------------------------------------------------------

benchLookupRef :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchLookupRef tsm _ref = do
  tid <- myThreadId
  let !tw = fromIntegral (S.getThreadId tid) :: Int
  let go 0 = pure ()
      go !i = do
        mref <- S.lookupRef tsm tw
        case mref of
          Just r -> do { !_ <- S.readRef r; pure () }
          Nothing -> pure ()
        go (i - 1)
  go itersPerThread

benchCachedRead :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchCachedRead _tsm ref = do
  let go 0 = pure ()
      go !i = do { !_ <- S.readRef ref; go (i - 1) }
  go itersPerThread

benchFullCycleRef :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchFullCycleRef tsm _ref = do
  tid <- myThreadId
  let !tw = fromIntegral (S.getThreadId tid) :: Int
  let go 0 = pure ()
      go !i = do
        mref <- S.lookupRef tsm tw
        case mref of
          Just r -> do
            !_ <- S.readRef r
            S.writeRef r $! i
            S.writeRef r $! i - 1
          Nothing -> pure ()
        go (i - 1)
  go itersPerThread

benchCachedCycle :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchCachedCycle _tsm ref = do
  let go 0 = pure ()
      go !i = do
        !_ <- S.readRef ref
        S.writeRef ref $! i
        S.writeRef ref $! i - 1
        go (i - 1)
  go itersPerThread

benchFusedProbe :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchFusedProbe tsm _ref = do
  let go 0 = pure ()
      go !i = do
        (!_tid, mref) <- S.lookupRefFast tsm
        case mref of
          Just r -> do { !_ <- S.readRef r; pure () }
          Nothing -> pure ()
        go (i - 1)
  go itersPerThread

benchFusedCycle :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchFusedCycle tsm _ref = do
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

benchGetTid :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchGetTid _tsm _ref = do
  let go 0 = pure ()
      go !i = do { !_ <- S.getCurrentThreadId; go (i - 1) }
  go itersPerThread

benchUpdateRaw :: S.ThreadStorageMap Int -> IORef Int -> IO ()
benchUpdateRaw tsm _ref = do
  tid <- myThreadId
  let !tw = S.getThreadId tid
  let go 0 = pure ()
      go !i = do
        S.updateRaw tsm tid tw $ \old -> (Just $! maybe 1 (+1) old, ())
        go (i - 1)
  go itersPerThread
