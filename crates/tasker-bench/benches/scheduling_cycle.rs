//! Spec §7.1: p99 of a full scheduling cycle with 10,000 pending jobs and
//! 1,000 running jobs. This file establishes the baseline; M8 ratchets it.

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use tasker_bench::{bench_config, synthetic};
use tasker_core::{DispatchDecision, Scheduler, VirtualTime, score};

const SEED: u64 = 0x5EED_1234_ABCD_0001;
const SLOTS: u32 = 64;

fn bench_priority_score(c: &mut Criterion) {
    let workload = synthetic(1_000, 0, SLOTS, SEED);
    let config = bench_config();
    let now = VirtualTime::from_nanos(3_600_000_000_000);
    let job = workload.jobs.get(workload.admitted[0]).expect("job exists");

    c.bench_function("priority/score_one_job", |b| {
        b.iter(|| black_box(score(black_box(job), black_box(now), &config.priority)));
    });
}

fn bench_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler/submit");
    for pending in [1_000_usize, 10_000] {
        let config = bench_config();
        group.throughput(Throughput::Elements(pending as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(pending),
            &pending,
            |b, &pending| {
                b.iter_batched(
                    || synthetic(pending, 0, SLOTS, SEED),
                    |mut workload| {
                        let mut scheduler = Scheduler::with_slots(workload.jobs.capacity_slots());
                        // `&workload.admitted` and `&mut workload.jobs` are disjoint
                        // fields, so both borrows may coexist.
                        for id in &workload.admitted {
                            scheduler
                                .submit(*id, &mut workload.jobs, VirtualTime::ZERO, &config)
                                .expect("fresh Submitted job");
                        }
                        black_box(scheduler.pending())
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_full_cycle(c: &mut Criterion) {
    let mut group = c.benchmark_group("cycle/full");
    for (pending, running) in [(1_000_usize, 100_usize), (10_000, 1_000)] {
        let config = bench_config();
        group.throughput(Throughput::Elements(pending as u64));
        group.bench_with_input(
            BenchmarkId::new("pending_running", format!("{pending}x{running}")),
            &(pending, running),
            |b, &(pending, running)| {
                b.iter_batched(
                    || {
                        // Setup is excluded from the measurement: a fresh
                        // workload and a scheduler already holding every job.
                        let mut workload = synthetic(pending, running, SLOTS, SEED);
                        let mut scheduler = Scheduler::with_slots(workload.jobs.capacity_slots());
                        for id in &workload.admitted {
                            scheduler
                                .submit(*id, &mut workload.jobs, VirtualTime::ZERO, &config)
                                .expect("fresh Submitted job");
                        }
                        let decisions: Vec<DispatchDecision> = Vec::with_capacity(pending);
                        (workload, scheduler, decisions)
                    },
                    |(mut workload, mut scheduler, mut decisions)| {
                        let outcome = scheduler.run_cycle(
                            &mut workload.jobs,
                            &mut workload.inventory,
                            &workload.running,
                            VirtualTime::from_nanos(3_600_000_000_000),
                            &config,
                            &mut decisions,
                        );
                        black_box(outcome)
                    },
                    criterion::BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_priority_score,
    bench_submit,
    bench_full_cycle
);
criterion_main!(benches);
