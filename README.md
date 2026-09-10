# TaskerRT

An HPC-style batch scheduler in Rust. The scheduling core is a pure library
with no async runtime, no I/O, and no wall clock, which makes it deterministic
to simulate and cheap to benchmark. Around it sit a gRPC control plane, a
worker that runs tasks as sleeps or subprocesses and drains cleanly on
SIGTERM, a CLI, an open-loop load generator, and Kubernetes manifests where
KEDA reads the scheduler's own demand signal to scale the worker pool.
Delivered so far: M1 scheduler core, M2 DAG dependencies and simulation,
M3 execution, M4 Kubernetes and autoscaling, M5 fair-share, M6 durability
with worker reconciliation, M7 preemption. Remaining: latency hardening.

**New here?** [GUIDE.md](GUIDE.md) walks through building, running, submitting
jobs, watching them, fair-share, durability, the Kubernetes deployment, and
every flag, with expected output at each step.

## Layout

```
crates/
├── tasker-core/           the scheduler (library)
│   ├── src/
│   │   ├── domain/        entities and value objects — depends on nothing
│   │   ├── cluster/       the capacity model — depends on domain
│   │   ├── policy/        the algorithms — depends on domain + cluster
│   │   ├── engine/        the scheduling cycle — depends on everything below
│   │   └── lib.rs         re-exports; public paths are flat (`tasker_core::Job`)
│   └── tests/
│       ├── unit/          one file per source module, public API only
│       └── property/      randomized invariant checks (proptest)
├── tasker-sim/            deterministic simulation harness (library)
│   ├── src/               virtual clock, event queue, scenario generator, invariants
│   └── tests/             determinism, invariant, and dependency properties
├── tasker-proto/          .proto files + generated gRPC types (control API, worker protocol)
├── taskerd/               the control plane: inbox ring, scheduler thread, dispatcher, gRPC
│   ├── src/lib.rs         everything; `main.rs` is ten lines of wiring
│   └── tests/e2e.rs       daemon + in-process workers over loopback gRPC
├── tasker-worker/         attaches to the daemon, runs tasks under a walltime timeout
├── tasker-cli/            submit | status | cancel | queue | nodes
└── tasker-bench/          synthetic workloads + criterion benchmarks
    └── BASELINE.md        the recorded baseline numbers
└── deploy/                Dockerfile, Kubernetes manifests, KEDA ScaledObject, smoke test
```

Dependencies point inward only: `engine → policy → cluster → domain`.
Every `use crate::...` line in the source is a check of that rule.

## Running it

The short version is below; [GUIDE.md](GUIDE.md) has the full walkthrough.

Two terminals. The daemon:

```bash
cargo run -p taskerd
```

A worker (sleep executor; a task's payload is its duration in nanoseconds):

```bash
cargo run -p tasker-worker
```

Both log `attached slot=0`. Ctrl-C the daemon: it closes worker streams, drains
the dispatcher, and joins the scheduler thread before exiting.

A worker that runs real commands instead of sleeping:

```bash
cargo run -p tasker-worker -- --executor command
```

### CLI

The payload must match the worker's executor: `--sleep` for a sleep worker,
`--cmd` for a command worker.

```bash
cargo run -q -p tasker-cli -- submit --sleep 50
```

```bash
cargo run -q -p tasker-cli -- submit --cmd echo hello
```

```bash
cargo run -q -p tasker-cli -- status 0
```

```bash
cargo run -q -p tasker-cli -- queue
```

```bash
cargo run -q -p tasker-cli -- nodes
```

```bash
cargo run -q -p tasker-cli -- accounts
```

`--dep <id>` (repeatable) declares a dependency; `--account <n>` charges the
job to an account (default 0); `cancel <id>` cancels a job and everything
downstream of it; `accounts` lists per-account usage in core-seconds and the
fair-share factor.

### Fair-share

Every job is charged to its account: `cpu_millis × milliseconds held`, accrued
continuously and halved every half-life (`taskerd --half-life-secs`, default
3600). The priority formula's fair-share term is `2^(-U/S)`: 1 for an account
that has used nothing, ½ for one using exactly its share of the cluster, and
falling toward 0 beyond that. Shares come from `--shares 0=3,1=1`; unlisted
accounts weigh 1. The arithmetic is integer-only (a compile-time Q48 table of
`2^(-k/1024)`), so the simulator's determinism guarantee covers it.
`tasker accounts` and the `tasker_account_*` gauges show the ledgers.

### Preemption

A job that cannot be placed and is at or above `--preempt-min-class` (default
`urgent`) evicts strictly lower-class running jobs on one slot: lowest class
first, youngest first, fewest victims. Workers stop tasks cooperatively within
`--preempt-grace-secs`; victims requeue and become immune after `--preempt-max`
evictions. No new plan is made while an eviction is in flight.

### Durability

`taskerd --data-dir ./data` turns on the write-ahead log. Every accepted submit,
dispatch, and lifecycle transition is framed, checksummed, and appended; a
writer thread syncs each tick's batch (`--wal-sync full|data|none`, default
`data`) and only then acknowledges the submits and releases the dispatches that
depended on it. Restart replays the log through the scheduler's own lifecycle
API, so job ids survive and clients' handles stay valid; workers that kept
their tasks running reconnect and reclaim them instead of rerunning. The log is compacted
by a snapshot at every start and whenever it passes `--wal-rotate-bytes`;
terminal jobs are forgotten after `--retain-secs` (default 300). Without
`--data-dir` the daemon runs in memory only.

### Kubernetes (OrbStack or Docker Desktop, plus KEDA)

`deploy/` holds a multi-stage `Dockerfile` and plain-YAML manifests. The daemon
implements KEDA's external scaler protocol: KEDA polls it for how many workers
it wants (`ceil((cpu_ready + cpu_running) / worker_cpu)`, clamped to
`[min, max]`) and, through a Kubernetes HPA, sets the worker Deployment's
replicas to exactly that number. Scale-down speed is the HPA stabilization
window set in `scaledobject.yaml`. A worker that receives SIGTERM tells the
daemon it is draining, finishes what it is running, and exits.

Prerequisites: a local Kubernetes sharing your `docker` daemon (OrbStack, or
Docker Desktop with Kubernetes enabled), `kubectl`, and KEDA
(`helm install keda kedacore/keda -n keda --create-namespace`).

```bash
make -C deploy/k8s build
```

```bash
make -C deploy/k8s apply
```

```bash
make -C deploy/k8s smoke
```

`make status` shows deployments, pods, and the `ScaledObject`; `make logs` tails
the daemon; `make delete` removes everything. Metrics are on the `taskerd`
Service, port 9090 (`kubectl -n tasker port-forward svc/taskerd 9090:9090`,
then `curl localhost:9090/metrics`).

### End-to-end latency

Hosts a daemon and workers in-process and drives an open-loop client:

```bash
cargo run --release -p tasker-bench --bin loadgen -- --rate 2000 --seconds 10
```

## Prerequisites

`rustup` and `protoc` (`brew install protobuf`). The toolchain is pinned in `rust-toolchain.toml`; rustup installs
and selects it automatically the first time you run `cargo` here.

## Everyday commands

Build everything:

```bash
cargo build --workspace
```

Run every test (unit + property):

```bash
cargo test --workspace
```

Only the unit tests, or only the property tests:

```bash
cargo test -p tasker-core --test unit
```

```bash
cargo test -p tasker-core --test property
```

One test by name (substring match):

```bash
cargo test -p tasker-core --test unit reused_slot
```

Property tests with a deeper random search (default is 256 cases):

```bash
PROPTEST_CASES=20000 cargo test -p tasker-core --test property
```

If a property test fails, proptest prints the minimal failing input and saves
it under `crates/tasker-core/proptest-regressions/`. Commit that file: it is a
permanent regression test.

Run the simulation suite alone, or with a deeper random search:

```bash
cargo test -p tasker-sim
```

```bash
PROPTEST_CASES=1000 cargo test -p tasker-sim --test invariants
```

## Quality gate (run before every commit)

```bash
cargo fmt --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

`clippy::pedantic` is on. Lints that fire on prose (product names in doc
comments) are handled once in `clippy.toml`, not with `#[allow]` at each site.

Two architectural rules are enforced mechanically:

- `tasker-core` and `tasker-sim` never depend on an async runtime or I/O crate. Verify:

  ```bash
  cargo tree -p tasker-core -i tokio
  ```

  ```bash
  cargo tree -p tasker-sim -i tokio
  ```

  The expected result is an **error** — `tokio` is not in the graph.

- Nothing in the workspace reads the wall clock or sleeps. `clippy.toml` bans
  `SystemTime::now`, `Instant::now`, and `thread::sleep` outright.

## Benchmarks

```bash
cargo bench -p tasker-bench
```

The first build is slow: release profile uses `lto = "fat"` and
`codegen-units = 1` so the numbers reflect real cross-crate inlining.
Results go to `target/criterion/`. Compare against a saved run with:

```bash
cargo bench -p tasker-bench -- --save-baseline before
```

```bash
cargo bench -p tasker-bench -- --baseline before
```

Profiling on macOS:

```bash
cargo install flamegraph && cargo flamegraph --bench scheduling_cycle -p tasker-bench -- --bench
```

## RustRover

The IDE reads `Cargo.toml` directly; there is no project file to generate.
Open the repository root, and it discovers both crates.

- **Run a single test:** click the ▶ gutter icon beside any `#[test]`.
- **Run a whole test file:** ▶ beside the file's first line, or right-click the
  file → *Run*.
- **Run configurations:** `.run/` ships daemon, worker, and load-generator
  configurations plus a `daemon + worker` compound; they appear in the Run
  widget on open. To add your own: *Run → Edit Configurations → + → Cargo*, then set the
  command line to any of the commands above (e.g. `test --workspace`, or
  `clippy --workspace --all-targets -- -D warnings`). Save one per command.
- **Property test depth:** in the Cargo run configuration, add
  `PROPTEST_CASES=20000` under *Environment variables*.
- `target/` is excluded from indexing automatically.
