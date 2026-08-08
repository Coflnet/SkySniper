# perf(sniper): optimize the snipe-finding path (~10× throughput, bit-exact)

## What
Optimizes `SniperService`'s snipe-finding hot path (`TestNewAuction → FindFlip → FoundAFlip`), which prices every incoming Hypixel auction and emits snipes. This is the squashed **production** change of an 11-round, measurement-driven campaign; the full per-round history, the benchmarking harness, and the profiling write-ups live on `perf/sniper-snipe-path` (separate follow-up PR).

## Results (snipe replay vs the round-1 baseline)
| metric | baseline | now | Δ |
|---|---|---|---|
| throughput | 236 /s | 2,269 /s | **~9.6×** |
| p50 | 2.77 ms | 0.32 ms | **−88%** |
| p99 | 38.2 ms | 2.1 ms | **−94.5%** |
| alloc / auction | 1.61 MB | ~0.02 MB | **−98.8%** |

Also: warm alloc ~115 KB → ~24 KB, rare gen2 GCs 9 → 0, cold p99 38 ms → ~1.3 ms. Machine-readable per-round record: [`benchmarks/speedups.json`](benchmarks/speedups.json).

## Core changes
- **Columnar dominance/closest kernels** (`DominatorIndex`, `ClosestScoreKernel`, `ClosestCandidateIndex`) with an interned `ulong` mask prefilter, replacing per-candidate LINQ scans over `ConcurrentDictionary`.
- **De-LINQ'd hot path**: `IsHigherValue`, `GetReduced`, `CheckCombined`, `ApplyAntiMarketManipulation`, `UpdateMedian` → explicit loops + pooled/`[ThreadStatic]` scratch.
- **Clean-price**: partial-selection (quickselect) instead of two full sorts + an entry-epoch-stamped recompute memo.
- **Parse/key path**: lazy-memoized hash on the immutable `AuctionKey`; O(M²)→O(M) modifier cache-key hashing; one parse per auction (share `basekey` into the closest finder).
- **Throughput**: `ShardedAuctionDispatcher` (tag-sharded, per-tag-serial) for near-linear core scaling.

## Correctness — bit-exact
Emitted snipe set / prices / medians / `GetPrice` are byte-identical to before. Gated by **432 tests + 4 runtime parity guards** (`SNIPER_VERIFY_{DOMINATOR,CLOSEST,CLEANPRICE_MEMO,PARSE}`) + fuzz oracles (40k dominance, 40k clean-price, 60k AMM, 50k GetReduced, 25k key-extraction) + snipe-set/`GetPrice`/`GetCleanItemPrice`/`UpdateMedian` goldens. All green; the guards are verification-only (off in production).

## Rollout / risk
- **New behavior is off by default**: the cross-auction parse memo ships disabled (`SNIPER_PARSE_MEMO`, 0% relist hit on the synthetic workload — gated on the shipped `SNIPER_PARSE_MEMO_COUNT` production counter); the shard-balance metrics are additive (`sky_sniper_shard_{queue_depth,processed_total}`).
- Perf numbers are **benchmark-derived** (synthetic replay + micro-benchmarks); correctness is **oracle-proven**. The throughput/shard-balance wins are **instrumented but not yet observed in production** — a canary is advisable.
- **Heads-up (pre-existing, not introduced here):** `BazaarHasMedian` is a wall-clock-boundary flake (a bazaar entry seeded at `UtcNow.AddHours(-24)` ages in/out of the retention window); it fails identically on the pre-campaign baseline. Worth pinning its clock in a separate fix so it doesn't flake CI.

## Not in this PR
The benchmarking harness (`benchmarks/`), profiling tooling, and the round-by-round `PIPELINE_PROFILE.md` — a separate PR off `perf/sniper-snipe-path`. The regenerable BenchmarkDotNet `*-report-full.json` dumps are untracked; `speedups.json` is the compact record.
