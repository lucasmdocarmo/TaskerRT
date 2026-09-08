//! Property: for any seed and any shape, every invariant holds after every
//! event, the scenario drains, and every job ends terminal.

use proptest::prelude::*;
use tasker_core::{
    CycleConfig, PackBudget, PriorityConfig, PriorityWeights, ResourceRequest, Resources,
    SlotInventory, VirtualDuration,
};
use tasker_sim::{ScenarioParams, Shape, Simulation, generate};

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

proptest! {
    // Each case runs a whole simulation; 64 cases keeps the suite fast.
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn random_scenarios_keep_every_invariant(
        seed in any::<u64>(),
        jobs in 4_usize..40,
        shape_index in 0_usize..4,
        width in 1_usize..5,
        fail_permille in 0_u32..200,
        cancel_permille in 0_u32..100,
    ) {
        let shape = match shape_index {
            0 => Shape::Independent,
            1 => Shape::Chain,
            2 => Shape::Tree { fan_out: width },
            _ => Shape::Layered { width },
        };
        let params = ScenarioParams { jobs, shape, fail_permille, cancel_permille, ..ScenarioParams::default() };

        let mut sim = Simulation::new(
            SlotInventory::from_uniform(4, Resources::new(8_000, 0, 0)),
            config(),
            VirtualDuration::from_secs(1),
        );
        sim.check_invariants = true;
        for (at, event) in generate(seed, &params) {
            sim.schedule(at, event);
        }

        // `map_err` turns a SimError into a proptest failure with its message.
        let quiescent = sim
            .run_to_quiescence(200_000)
            .map_err(|e| TestCaseError::fail(e.to_string()))?;
        prop_assert!(quiescent, "scenario never drained");

        for (id, job) in sim.jobs.iter() {
            prop_assert!(job.state.is_terminal(), "{id:?} ended in {:?}", job.state);
        }
    }
}
