# M1 Benchmark Baseline

Spec §7.1 requires a measured baseline before any optimization, so that every
later change has a before and after. These numbers are the M1 reference. M8
ratchets them down; M2 onward must not regress them.

**Recorded:** 2026-09-07
**Machine:** Apple M4 Pro, macOS, arm64
**Toolchain:** rustc 1.96.0, `--release` with `lto = "fat"`, `codegen-units = 1`
**Command:** `cargo bench -p tasker-bench`
**Fixture seed:** `0x5EED_1234_ABCD_0001`, 64 slots of 8 000 millicores / 32 GiB / 4 GPUs

| Benchmark | Fixture | Time (mean, 95% CI) | Per element |
|---|---|---|---|
| `priority/score_one_job` | — | 1.20 ns [1.19, 1.22] | — |
| `scheduler/submit` | 1,000 pending | 17.2 µs [17.2, 17.3] | 17.2 ns/job |
| `scheduler/submit` | 10,000 pending | 208 µs [207, 208] | 20.8 ns/job |
| `cycle/full` | 1,000 pending / 100 running | 576 µs [574, 578] | 576 ns/job |
| `cycle/full` | **10,000 pending / 1,000 running** | **1.83 ms [1.82, 1.84]** | 183 ns/job |

The bold row is the spec §7.1 fixture and the number M8 optimizes against.

## Observations

- **Heap admission scales as O(log n).** 10× the jobs costs 1.34× per job;
  log₂(10 000) / log₂(1 000) ≈ 1.33. The indexed heap behaves as designed.
- **`score_one_job` is at the measurement floor.** Every divisor in `factor()`
  is a configuration constant, so under fat LTO the divisions lower to
  multiply-and-shift on opaque inputs. 1.2 ns is close to criterion's own
  `iter` overhead. Use this row as a regression tripwire, not as the cost of
  scoring.
- **Per-job cycle cost falls with scale** (576 → 183 ns/job). Pure sorting would
  do the opposite, so a fixed or early-terminating cost dominates: packing stops
  at the head job, and most backfill candidates are cleared by the cheap
  `ends_at <= reservation` branch without recomputing the reservation. This is
  the first thing M8 should put under a flamegraph.
- **1.83 ms fits inside the spec §6 default 5 ms tick**, with margin. It is far
  from the microsecond bound §7.1 aspires to; closing that gap is M8's scope.

## M2 re-measurement (2026-09-08)

`ready_set/admit` became `scheduler/submit`, which additionally runs
`DependencyTracker::register` and the `Submitted → Ready` transition per job.
Per-job cost roughly doubled (8.3 → 17.2 ns at 1,000; 11.1 → 20.8 ns at
10,000), which is the expected price of the tracker's two-pass register.
Submission is not on the cycle hot path.

`cycle/full` at 10 000 × 1 000 measured **1.71 ms [1.69, 1.73]** against the M1
figure of 1.83 ms — criterion reports −5.6 % (p < 0.05) versus its saved M1 run.
Nothing in M2 touches a cycle with no completions, so this is run-to-run
variance across sessions, not an M2 improvement. For regression purposes the
cycle baseline is **unchanged at ~1.8 ms**; the 10 % guard is measured from
there.

`priority/score_one_job`: 1.18 ns, unchanged.

## Notes

Criterion reports mean with a confidence interval, not p99. The p99 figure spec
§7.1 actually asks for needs the `hdrhistogram` open-loop harness, which arrives
with the end-to-end path in M3 — a cycle benchmark has no arrival process to be
late relative to. Until then the mean here is the regression guard.

`easy_backfill` is O(candidates × slots × running) in the worst case, because
each admission that cannot be cleared by the walltime test recomputes the head
reservation. The measured numbers show this worst case is not being hit on the
synthetic fixture; a fixture engineered to hit it belongs in M8.

Criterion reported 4–12 % outliers per benchmark on this run. For a baseline
that is acceptable; a ratchet comparison should use `--save-baseline` /
`--baseline` on a quiet machine.

## M3 end-to-end latency (2026-09-08)

Parent §7.3: submit→start, open loop, coordinated omission accounted for (the
payload carries the *intended* send time; a stalled client shows up as latency).

**Command:** `cargo run --release -p tasker-bench --bin loadgen -- --rate 2000 --seconds 10`
**Fixture:** daemon + 4 in-process sleep workers (64 000 millicores each), 100-millicore
jobs sleeping 10 ms, 5 ms tick. 20000 samples, 0 rejected.

| Quantile | Latency |
|---|---|
| p50 | 1.42 ms |
| p90 | 2.12 ms |
| p99 | 3.49 ms |
| p99.9 | 19.20 ms |
| p99.99 | 26.19 ms |
| max | 26.93 ms |
| mean | 1.51 ms |

**Reading it.** p50 is *below* half a tick: an inbox push unparks the scheduler,
so a submit triggers an on-demand cycle instead of waiting for the 5 ms timer.
The 1.4 ms median is therefore gRPC round trip + drain + one cycle + dispatch +
stream delivery + task spawn. p99 at 3.5 ms is mild queueing under a 2 000/s
arrival rate. The step from p99 to p99.9 (3.5 → 19 ms) is the tail this project
exists to hunt: runtime scheduling of the gRPC handlers, dispatcher wake-ups,
and tokio timer coalescing in the workers. M8 measures that gap and attacks it.

## M4 cluster smoke (2026-09-08)

Parent §9.3: the scaler endpoint drives the worker Deployment to the
scheduler's demand. `docker-desktop`, KEDA 2.x, `ScaledObject` with
`minReplicaCount 1`, `maxReplicaCount 8`, HPA scale-down stabilization 30 s.

**Command:** `make -C deploy/k8s smoke` — 40 jobs × 3 000 millicores × 20 s
against 4 000-millicore workers (one job per worker at a time).

| Time (s from submit) | replicas | desired | Event |
|---|---|---|---|
| −5 | 1 | 1 | baseline, one warm worker |
| 0 | 1 | 8 | 40 jobs submitted; demand clamps to max |
| +5 | 5 | 8 | HPA's first sync after KEDA's poll |
| +21 | 8 | 8 | full demand met |
| +107 | 8 | 6 | queue draining; demand tracks remaining work |
| +127 | 6 | 1 | stabilization window elapsed; scale-down begins |
| +142 | 3 | 1 | |
| ~+160 | 1 | 1 | back to the warm minimum |

**Reading it.** Scale-up latency (~21 s to full demand) is the sum of KEDA's
poll, the HPA's 15 s sync period, and pod scheduling + image start on a warm
node; none of it is the scheduler. Scale-down is governed entirely by the HPA
stabilization window we set (30 s) plus its sync period — the daemon's demand
figure dropped the instant the queue emptied. The 40 jobs completed on 8
workers in 5 rounds of 20 s, as the arithmetic predicts. Draining workers
finished their in-flight job before exiting; no job was requeued during
scale-down (`worker left ... requeued=0` in the daemon log).

## M5 fair-share (2026-09-09)

Same fixture as M1 (`synthetic(10_000, 1_000, 64)`, seed `0x5EED_1234_ABCD_0001`),
now with fair-share weight 1 000 and 16 accounts. Recorded on a **loaded**
machine (load average 5.4–6.1; Docker Desktop, the `tasker` cluster, and a
browser running), which the M1 numbers were not. Two consecutive runs differed
by 5 % on the large cycle, so the cycle rows below are provisional until
re-measured quiet; the two new micro-benchmarks are stable across runs.

| Benchmark | M1 | M5 run 1 | M5 run 2 | Note |
|---|---|---|---|---|
| `priority/score_one_job` | 1.20 ns | 1.34 ns | 1.34 ns | one `min` and one multiply-add more; real |
| `fairshare/refresh_16_accounts` | — | 47.4 ns | — | new: touch 16 ledgers, 16 factors, one 5 ms tick |
| `cycle/full` 1,000 × 100 | 576 µs | 626 µs | 610 µs | under load; noise band ≈ ±5 % |
| `cycle/full` 10,000 × 1,000 | 1.83 ms | 2.03 ms | 1.93 ms | under load; noise band ≈ ±5 % |

`refresh` costs about 3 ns per account, not the 100 ns the design budgeted.
The `u128` division in the factor takes LLVM's 64-bit fast path while
`usage_total × shares` fits in 64 bits, and the sixteen ledgers are
independent, so their divisions overlap in the out-of-order window. The
per-candidate cost added to rescoring is one indexed load of the account's
factor. `run_cycle` still allocates nothing (`tests/alloc.rs`).

To refresh the cycle rows on a quiet machine:

```
cargo bench -p tasker-bench --bench scheduling_cycle -- "cycle/full"
```

## M6 durability (2026-09-09)

Same load-generator fixture as M3 (`--rate 2000 --seconds 10`, 4 in-process
sleep workers, 100-millicore 10 ms jobs, 5 ms tick), now with the in-process
daemon writing a WAL under `--data-dir`. Same loaded machine as the M5 rows
(load average 5.7–6.1); the in-memory row is the same-session control.

| Mode | p50 | p90 | p99 | p99.9 | mean |
|---|---|---|---|---|---|
| in memory (control) | 1.37 ms | 2.13 ms | 4.38 ms | 8.52 ms | 1.47 ms |
| `--wal-sync none` | 1.43 ms | 2.26 ms | 5.03 ms | 17.1 ms | 1.57 ms |
| `--wal-sync data` | 10.98 ms | 13.28 ms | 18.56 ms | 23.68 ms | 11.18 ms |
| `--wal-sync full` | 10.20 ms | 12.57 ms | 16.25 ms | 22.18 ms | 10.36 ms |

`data` and `full` are indistinguishable on macOS: Rust's `sync_data` and
`sync_all` both issue `F_FULLFSYNC`, which forces the drive's cache. One such
sync costs about 4 ms here, and every tick's batch pays one. `none` adds
roughly 60 µs at p50 over the control: the extra hop through the writer
thread, which fires the acks and releases the dispatches.

**The number that mattered was the one before this table.** The first run with
`data` measured **p50 = 271 ms**. The commit channel between the engine and the
writer was 64 deep; with a 4 ms sync per 5 ms tick the writer saturates, the
channel fills, and steady-state latency is 64 syncs. Shrinking the channel to
depth 1 (`COMMIT_QUEUE_DEPTH`) makes later ticks merge into one pending batch
while the writer is busy, so a saturated disk grows the batch instead of the
queue: 271 ms became 11 ms with no other change. That is group commit doing
its job, and it only does it when the queue in front of the syncer is shallow.
