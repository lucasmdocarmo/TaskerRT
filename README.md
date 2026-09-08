# TaskerRT

An HPC-style task scheduler in Rust. The scheduling core is a pure library:
no async runtime, no I/O, no wall clock. Execution on Kubernetes comes in
later milestones.

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

`--dep <id>` (repeatable) declares a dependency; `cancel <id>` cancels a job and
everything downstream of it.

### Kubernetes (docker-desktop + KEDA)

`deploy/` holds a multi-stage `Dockerfile` and plain-YAML manifests. The daemon
implements KEDA's external scaler protocol: every 5 s KEDA asks it how many
workers it wants (`ceil((cpu_ready + cpu_running) / worker_cpu)`, clamped to
`[min, max]`) and sets the worker Deployment's replicas to exactly that. A
worker that receives SIGTERM tells the daemon it is draining, finishes what it
is running, and exits.

Prerequisites: Docker Desktop with Kubernetes enabled, `kubectl`, and KEDA
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
- **Run configurations:** *Run → Edit Configurations → + → Cargo*, then set the
  command line to any of the commands above (e.g. `test --workspace`, or
  `clippy --workspace --all-targets -- -D warnings`). Save one per command.
- **Property test depth:** in the Cargo run configuration, add
  `PROPTEST_CASES=20000` under *Environment variables*.
- `target/` is excluded from indexing automatically.
