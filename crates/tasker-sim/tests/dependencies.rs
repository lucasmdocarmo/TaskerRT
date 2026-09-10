//! Property: a job is never dispatched before every dependency has completed.
//! Verified from the trace alone, with the runtime invariant checker disabled.

use proptest::prelude::*;
use tasker_core::{
    CycleConfig, FairShareConfig, PackBudget, PriorityConfig, PriorityWeights, ResourceRequest,
    Resources, SlotInventory, VirtualDuration,
};
use tasker_sim::{Action, ScenarioParams, Shape, Simulation, generate};

fn config() -> CycleConfig {
    CycleConfig {
        priority: PriorityConfig::new(
            PriorityWeights::default(),
            VirtualDuration::from_secs(3_600),
            ResourceRequest::new(8_000, 0, 0),
        ),
        fairshare: FairShareConfig::default(),
        budget: PackBudget::default(),
        max_candidates: 256,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn no_job_runs_before_its_dependencies_complete(seed in any::<u64>(), width in 1_usize..5) {
        let params = ScenarioParams {
            jobs: 24,
            shape: Shape::Layered { width },
            fail_permille: 100,
            cancel_permille: 50,
            ..ScenarioParams::default()
        };
        let mut sim = Simulation::new(
            SlotInventory::from_uniform(4, Resources::new(8_000, 0, 0)),
            config(),
            VirtualDuration::from_secs(1),
        );
        for (at, event) in generate(seed, &params) {
            sim.schedule(at, event);
        }
        sim.run_to_quiescence(200_000).map_err(|e| TestCaseError::fail(e.to_string()))?;

        let records = sim.trace().records();
        for (pos, record) in records.iter().enumerate() {
            let (Some(id), Action::Dispatched { .. }) = (record.job, record.action) else {
                continue;
            };
            let job = sim.jobs.get(id).expect("dispatched jobs stay in the arena");
            for dep in &job.deps {
                // `records[..pos]` is everything strictly before this dispatch.
                let completed_first = records[..pos]
                    .iter()
                    .any(|r| r.job == Some(*dep) && r.action == Action::Completed);
                prop_assert!(completed_first, "{id:?} ran at trace #{pos} before {dep:?} completed");
            }
        }
    }
}
