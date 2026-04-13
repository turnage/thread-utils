# Changelog for thread-utils-context

## 0.4.0.0

- Replace striped-IntMap internals with a flat open-addressed hash table
  backed by per-thread IORefs. Reads and writes on the hot path are now
  plain IORef operations, with zero CAS and zero contention.
- Add CMM primops (`stg_getCurrentThreadId`, `stg_probeThreadSlot`) to
  eliminate ThreadId allocation and FFI overhead on the hot path.
- New construction function: `newThreadStorageMapWith` for explicit capacity.
- New `getCurrentThreadId` reads `CurrentTSO.id` directly via CMM.
- New ref-based API for instrumentation hot loops: `ensureRef`,
  `ensureRefFast`, `lookupRef`, `lookupRefFast`, `readRef`, `writeRef`,
  `modifyRef`.
- Remove `containers` dependency.
- Requires `cabal-version: 3.0` (for `cmm-sources`).
- Backwards compatible: all previously exported symbols retain their
  original type signatures.

## 0.3.0.4

- Fix compilation on GHC 8.12

## 0.3.0.3

- Fix compilation of purgeDeadThreads on GHC 9.6

## Unreleased changes
