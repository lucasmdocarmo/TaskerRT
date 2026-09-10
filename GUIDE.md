# TaskerRT Guide

How to build, run, and use TaskerRT, from a first job on your laptop to an
autoscaled worker pool on Kubernetes. The [README](README.md) covers what the
project is and how the code is laid out; this document is the operator's view.

## 1. What you get

| Binary | Crate | Role |
|---|---|---|
| `taskerd` | `crates/taskerd` | The control plane: gRPC in, scheduler thread, dispatcher out, Prometheus metrics, KEDA scaler, write-ahead log |
| `tasker-worker` | `crates/tasker-worker` | Attaches to the daemon, runs tasks as sleeps or subprocesses, reconnects if the daemon goes away, drains on SIGTERM |
| `tasker-cli` | `crates/tasker-cli` | Submits and inspects jobs |
| `loadgen` | `crates/tasker-bench` | Hosts a daemon and workers in one process and measures submit-to-start latency |

Everything talks gRPC on one port (default `7070`). Metrics are plain HTTP on a
second port (default `9090`).

## 2. Prerequisites

- **Rust 1.96.** `rust-toolchain.toml` pins it; `rustup` installs it on the first
  `cargo` command.
- **protoc** (the Protocol Buffers compiler), for the generated gRPC types:
  `brew install protobuf` on macOS, `apt install protobuf-compiler` on Debian
  or Ubuntu.
- For section 9 only: a local Kubernetes that shares your `docker` CLI's
  daemon, so locally built images need no registry: Docker Desktop with
  Kubernetes enabled (what M4 was smoke-tested on), OrbStack, or Rancher
  Desktop in dockerd mode. Plus `kubectl` and `helm`.

## 3. Build and check

```bash
cargo build --workspace
```

The full quality gate, which every change in this repository passes:

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

The first release build is slow on purpose (`lto = "fat"`,
`codegen-units = 1`); debug builds are quick.

## 4. Quick start

RustRover users: the repository ships run configurations in `.run/`, so the
Run widget offers `taskerd (in memory)`, `taskerd (durable)`, `worker (sleep)`,
`worker (command)`, `loadgen`, and a compound `daemon + worker` that starts the
first two together. Each opens its own Run tab with the process log. The
commands below are the same thing from a terminal.

Three terminals. First the daemon:

```bash
cargo run -p taskerd
```

It logs `taskerd listening addr=127.0.0.1:7070` and `metrics listening`. Then
a worker:

```bash
cargo run -p tasker-worker
```

Both sides log `attached slot=0`. Now submit a job that sleeps for 50 ms:

```bash
cargo run -q -p tasker-cli -- submit --sleep 50
```

```
0 Ready
```

The first number is the job id; the word is the state the scheduler gave it
on arrival. A moment later:

```bash
cargo run -q -p tasker-cli -- status 0
```

```
0 Completed
```

Ctrl-C the daemon to stop: it closes the worker streams, drains the
dispatcher, and joins the scheduler thread before exiting. The worker notices
its stream closing and exits too.

Set `RUST_LOG=debug` on either binary for per-job logging.

## 5. Submitting work

### Executors

A worker runs one kind of task, chosen with `--executor`:

| Executor | Payload | Submit with |
|---|---|---|
| `sleep` (default) | duration in nanoseconds | `--sleep <milliseconds>` |
| `command` | NUL-separated argv | `--cmd <program> [args...]` |

The payload must match the worker. A sleep worker handed a command payload
reports the task as failed.

```bash
cargo run -p tasker-worker -- --executor command
```

```bash
cargo run -q -p tasker-cli -- submit --cmd echo hello
```

### Describing a job

| Flag | Default | Meaning |
|---|---|---|
| `--cpu-millis` | `1000` | CPU request in millicores; the scheduler packs by this |
| `--mem-bytes` | `0` | Memory request |
| `--gpus` | `0` | GPU request |
| `--walltime` | `60` | Seconds before the worker kills the task and reports failure; backfill plans around it |
| `--class` | `normal` | Priority tier: `low`, `normal`, `high`, `urgent` |
| `--account` | `0` | Account charged for the job (section 7) |
| `--dep <id>` | none | A job this one waits for; repeat for several |

A job with dependencies arrives `Blocked` and becomes `Ready` when the last
dependency completes. If a dependency fails or is cancelled, the dependent is
cancelled too, transitively.

```bash
cargo run -q -p tasker-cli -- submit --sleep 200
```

```bash
cargo run -q -p tasker-cli -- submit --sleep 10 --dep 1
```

```
2 Blocked
```

### Cancelling

```bash
cargo run -q -p tasker-cli -- cancel 1
```

Prints the cancelled id and the ids cancelled downstream of it. A running job
is killed on its worker.

### Job states

`Submitted → Blocked | Ready → Running → Completed | Failed | Cancelled`.
`Preempted` is the transient state a running job passes through when its worker
disappears; the job is then `Ready` again and runs elsewhere.

## 6. Watching it

Queue counts and the cycle counter:

```bash
cargo run -q -p tasker-cli -- queue
```

```
ready 0  blocked 1  running 1  cycles 4821
```

Attached workers and their free capacity:

```bash
cargo run -q -p tasker-cli -- nodes
```

Per-account usage and fair-share factors:

```bash
cargo run -q -p tasker-cli -- accounts
```

Prometheus metrics:

```bash
curl -s localhost:9090/metrics | grep '^tasker_'
```

| Metric | Meaning |
|---|---|
| `tasker_jobs_ready`, `tasker_jobs_blocked`, `tasker_jobs_running` | queue depth by state |
| `tasker_workers_attached`, `tasker_workers_desired` | pool size now, and what the scheduler wants |
| `tasker_cycle_seconds` | scheduling cycle duration histogram |
| `tasker_account_usage_core_seconds{account}` | decayed usage per account |
| `tasker_account_fairshare{account}` | fair-share factor per account, 0 to 1 |

To point the CLI at a daemon elsewhere, every subcommand takes
`--daemon http://host:7070`.

## 7. Fair-share

Each job is charged to its account: `cpu_millis × milliseconds held`, accrued
continuously while it runs and halved every half-life. The priority formula's
fair-share term is `2^(-U/S)`: 1 for an account that has used nothing, one half
for an account using exactly its share of the cluster, falling toward 0 beyond
that. Shares default to 1 per account.

```bash
cargo run -p taskerd -- --shares 0=3,1=1 --half-life-secs 600
```

To see it act, attach one worker and submit a batch of identical jobs from
two accounts; the dispatch order follows the share ratio, and `accounts` shows
the factors moving:

```bash
for i in 1 2 3 4 5 6; do cargo run -q -p tasker-cli -- submit --sleep 2000 --account 0; cargo run -q -p tasker-cli -- submit --sleep 2000 --account 1; done
```

```bash
cargo run -q -p tasker-cli -- accounts
```

```
account   shares   core_seconds   running_mc  fairshare
0              3            9.0         1000      0.625
1              1            5.0            0      0.397
```

Accounts are numbers below 1024; a submit naming a larger one is rejected.

## 8. Durability

Without `--data-dir` the daemon keeps everything in memory and forgets it on
exit. With it, every accepted submit, dispatch, and state transition is written
to a checksummed log before the client is acknowledged or a worker is told to
start, and a restart replays the log.

```bash
cargo run -p taskerd -- --data-dir ./data
```

Try it: submit a few jobs including a long sleep, Ctrl-C the daemon while one is
running, start it again with the same `--data-dir`, and ask:

```bash
cargo run -q -p tasker-cli -- status 0
```

Completed jobs are still completed, blocked jobs are still blocked on the same
dependencies, and job ids are the same ids you held before the restart. A job
that was running is held for its worker: workers keep their tasks across a
lost stream and reconnect with backoff, announcing what they still run, and the
daemon binds that work to the worker's new slot with no rerun. This holds for
a crash and for a clean stop alike: Ctrl-C leaves running jobs running. A job
nobody claims within twice the heartbeat timeout (20 s by default) is queued
again. One known gap: a task that finishes in the instant between the daemon
starting to stop and its streams closing is reported to a daemon that no
longer applies it, and will run again.

What is in the directory:

```
data/
├── snapshot-000002.bin   full state at the last compaction
└── wal-000002.log        every transition since
```

Every start compacts the log into a fresh snapshot; the log is also rotated
when it passes `--wal-rotate-bytes` (default 64 MiB). Finished jobs leave the
arena `--retain-secs` after they finish (default 300), so a job can only be
named as a dependency within that window.

| `--wal-sync` | What happens per batch | Submit latency (macOS, measured) |
|---|---|---|
| `none` | write only; the OS page cache decides | about the same as in memory |
| `data` (default) | `fdatasync` on Linux; `F_FULLFSYNC` on macOS | p50 ≈ 11 ms |
| `full` | `fsync`; identical to `data` on macOS | p50 ≈ 10 ms |

The cost is one disk sync per scheduler tick, shared by every submit in that
tick. Semantics to know: a completion lost in a crash makes the job run again,
and a client whose acknowledgement was lost may resubmit and create a duplicate.

## 9. Kubernetes

The manifests target whichever cluster your current `kubectl` context points
at, and `make build` builds into whichever daemon your current `docker`
context points at. Those two must belong to the same tool (OrbStack or Docker
Desktop) so the images are visible without a registry:

```bash
docker context ls && kubectl config get-contexts
```

Install KEDA once:

```bash
helm repo add kedacore https://kedacore.github.io/charts && helm repo update
```

```bash
helm install keda kedacore/keda -n keda --create-namespace
```

Build both images and deploy:

```bash
make -C deploy/k8s build
```

```bash
make -C deploy/k8s apply
```

Reach the daemon from your shell:

```bash
kubectl -n tasker port-forward svc/taskerd 7070:7070 9090:9090
```

With that running, every `tasker-cli` command above works unchanged, and
`curl localhost:9090/metrics` shows the gauges.

Autoscaling: the daemon serves KEDA's external scaler protocol. KEDA polls it
for the number of workers the scheduler wants, `ceil((cpu_ready + cpu_running) /
worker_cpu)` clamped to `--min-workers..--max-workers`, and sets the worker
Deployment's replicas through a Kubernetes HPA. Submit a burst and watch:

```bash
kubectl -n tasker get pods,hpa -w
```

The smoke test does exactly that and asserts on the replica count:

```bash
make -C deploy/k8s smoke
```

Other targets: `make -C deploy/k8s status`, `logs`, and `delete`, which removes
the namespace. The daemon's flags are set in `deploy/k8s/taskerd.yaml`; the
`ScaledObject` and its scale-down window are in `deploy/k8s/scaledobject.yaml`.

A worker that receives SIGTERM, as pods do on scale-down, tells the daemon it is
draining, finishes what it is running, and exits; `--drain-timeout` (default
50 s) bounds the wait.

## 10. Load and benchmarks

The load generator hosts a daemon and sleep workers in one process and submits
at a fixed rate regardless of how the system keeps up, so a stall shows up as
latency rather than as a gap in the data:

```bash
cargo run --release -p tasker-bench --bin loadgen -- --rate 2000 --seconds 10
```

| Flag | Default | Meaning |
|---|---|---|
| `--rate` | `2000` | submissions per second |
| `--seconds` | `10` | run length |
| `--workers`, `--worker-cpu` | `4`, `64000` | in-process workers and their millicores |
| `--job-cpu`, `--task-ms` | `100`, `10` | per-job request and sleep |
| `--tick-ms` | `5` | scheduler tick |
| `--data-dir`, `--wal-sync` | off, `data` | turn on the WAL to measure durability's cost |

Scheduler micro-benchmarks, with Criterion comparing against the last saved run:

```bash
cargo bench -p tasker-bench
```

Recorded numbers and the conditions they were measured under are in
[`crates/tasker-bench/BASELINE.md`](crates/tasker-bench/BASELINE.md).

## 11. Simulation

`crates/tasker-sim` drives the scheduling core on a virtual clock with a seeded
scenario generator. The same seed always produces the same trace, and every
event is checked against the scheduler's invariants. Its tests are the fastest
way to see a scheduling policy behave:

```bash
cargo test -p tasker-sim -- --nocapture
```

`tests/fairshare.rs` in that crate, for example, prints the dispatch order two
accounts get under equal and 3:1 shares.

## 12. Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `protoc` not found while building `tasker-proto` | install protobuf (section 2) |
| `address already in use` on start | a previous daemon or a `port-forward` is on 7070 or 9090; pass `--listen` / `--metrics-listen` or stop it |
| `make smoke` says `demand did not rise` or `port ... in use` | a local `taskerd` from section 4 is still running, so the port-forward could not bind and the jobs went to it; stop the local daemon and rerun |
| a job goes `Failed` immediately | payload kind does not match the worker's executor (section 5) |
| `submit` rejected with a dependency error | the dependency id is unknown, names the job itself, or was forgotten after `--retain-secs` |
| `submit` rejected with an account error | `--account` must be below 1024 |
| `cargo test` seems to hang on macOS | macOS has no `timeout`; use `perl -e 'alarm 120; exec @ARGV' cargo test ...` to bound a run |
| KEDA `ScaledObject` not `Ready` | KEDA is not installed or the daemon Service is not up; `make -C deploy/k8s status` and `kubectl -n keda get pods` |
| bench numbers differ from `BASELINE.md` | check the load average; the recorded rows state the machine conditions |

## 13. Flag reference

### `taskerd`

| Flag | Default |
|---|---|
| `--listen` | `127.0.0.1:7070` |
| `--metrics-listen` | `127.0.0.1:9090` |
| `--worker-cpu-millis` | `4000` (one worker's capacity, for the KEDA demand figure) |
| `--min-workers`, `--max-workers` | `1`, `8` |
| `--half-life-secs` | `3600` |
| `--shares` | none; `account=shares,...` |
| `--data-dir` | none (in memory) |
| `--wal-sync` | `data` (`full`, `data`, `none`) |
| `--wal-rotate-bytes` | `67108864` |
| `--retain-secs` | `300` |

### `tasker-worker`

| Flag | Default |
|---|---|
| `--daemon` | `http://127.0.0.1:7070` |
| `--name` | `worker` |
| `--cpu-millis`, `--mem-bytes`, `--gpus` | `4000`, `8589934592`, `0` |
| `--executor` | `sleep` (`sleep`, `command`) |
| `--drain-timeout` | `50` seconds |

A worker that loses its stream keeps its tasks running and reconnects with
backoff, 200 ms doubling to 5 s, reporting the jobs it still holds. It exits
only after a SIGTERM drain.

### `tasker-cli`

`submit`, `status <id>`, `cancel <id>`, `queue`, `nodes`, `accounts`; all
accept `--daemon <url>`. Submit flags are in section 5.

## 14. Where to read next

- [`README.md`](README.md): what the project is, the crate layout, the
  architectural rules, and the quality gate.
- [`crates/tasker-bench/BASELINE.md`](crates/tasker-bench/BASELINE.md): every
  recorded measurement, milestone by milestone, with the reasoning.
- [`deploy/`](deploy/): the Dockerfile, manifests, Makefile, and smoke test.
