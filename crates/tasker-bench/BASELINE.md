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
| `ready_set/admit` | 1,000 pending | 8.26 µs [8.22, 8.34] | 8.3 ns/job |
| `ready_set/admit` | 10,000 pending | 111 µs [110, 113] | 11.1 ns/job |
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
