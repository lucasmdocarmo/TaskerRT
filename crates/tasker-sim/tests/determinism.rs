//! Same seed, same trace. This is the property that makes every simulation
//! failure reproducible.

use tasker_core::{
    CycleConfig, PackBudget, PriorityConfig, PriorityWeights, ResourceRequest, Resources,
    SlotInventory, VirtualDuration,
};
use tasker_sim::{ScenarioParams, Simulation, Trace, generate};

fn config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights::default(),
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(8_000, 0, 0),
        ),
        budget: PackBudget::default(),
        max_candidates: 256,
    }
}

fn run(seed: u64) -> (Trace, bool) {
    let mut s = Simulation::new(
        SlotInventory::from_uniform(8, Resources::new(8_000, 0, 0)),
        config(),
        VirtualDuration::from_secs(1),
    );
    s.check_invariants = true;
    for (at, event) in generate(seed, &ScenarioParams::default()) {
        s.schedule(at, event);
    }
    let quiescent = s
        .run_to_quiescence(100_000)
        .expect("no invariant violation");
    (s.trace().clone(), quiescent)
}

#[test]
fn the_same_seed_produces_an_identical_trace() {
    for seed in [1, 42, 0xDEAD_BEEF] {
        let (a, qa) = run(seed);
        let (b, qb) = run(seed);
        assert!(qa && qb, "seed {seed} must drain");
        assert!(!a.is_empty());
        assert_eq!(a, b, "seed {seed} diverged between runs");
    }
}

#[test]
fn different_seeds_produce_different_traces() {
    assert_ne!(run(1).0, run(2).0);
}
