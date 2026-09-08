//! Property: no admitted backfill job pushes the head job's reservation later
//! (spec §9.1).

use proptest::prelude::*;
use tasker_core::{
    AccountId, Arena, BackfillScratch, DispatchDecision, Disposition, Job, JobId, JobState,
    PackBudget, PriorityClass, ReadyEntry, ResourceRequest, Resources, RunningJob, SlotInventory,
    VirtualDuration, VirtualTime, easy_backfill, reservation_time,
};

const SECOND: u64 = 1_000_000_000;
const SLOT_CPU: u32 = 4_000;

#[derive(Debug, Clone)]
struct Scenario {
    running: Vec<(u32, u32, u64)>, // (slot, cpu, ends_at_secs)
    queued: Vec<(u32, u64)>,       // (cpu, walltime_secs)
}

fn scenario_strategy() -> impl Strategy<Value = Scenario> {
    (
        prop::collection::vec((0_u32..3, 500_u32..3_000, 1_u64..200), 0..8),
        prop::collection::vec((500_u32..SLOT_CPU, 1_u64..300), 1..10),
    )
        .prop_map(|(running, queued)| Scenario { running, queued })
}

proptest! {
    #[test]
    fn backfill_never_delays_the_head(scenario in scenario_strategy()) {
        let mut inventory = SlotInventory::from_uniform(3, Resources::new(SLOT_CPU, 0, 0));
        let mut running: Vec<RunningJob> = Vec::new();

        for (n, (slot, cpu, ends)) in scenario.running.iter().enumerate() {
            let resources = Resources::new(*cpu, 0, 0);
            if inventory.try_allocate(*slot, &resources).is_ok() {
                running.push(RunningJob {
                    job: JobId::from_bits(n as u64 + 10_000),
                    slot: *slot,
                    resources,
                    ends_at: VirtualTime::from_nanos(ends * SECOND),
                });
            }
        }

        let mut arena: Arena<Job> = Arena::new();
        let mut candidates: Vec<ReadyEntry> = Vec::new();
        for (rank, (cpu, walltime)) in scenario.queued.iter().enumerate() {
            let mut job = Job::new(
                AccountId::new(0),
                PriorityClass::Normal,
                VirtualTime::ZERO,
                ResourceRequest::new(*cpu, 0, 0),
                VirtualDuration::from_secs(*walltime),
            );
            job.try_transition(JobState::Ready).unwrap();
            let id = arena.insert(job);
            candidates.push(ReadyEntry {
                class: PriorityClass::Normal,
                job: id,
                score: (scenario.queued.len() - rank) as u64,
            });
        }

        let now = VirtualTime::ZERO;
        let head_index = 0;
        let head_request = arena.get(candidates[head_index].job).unwrap().request;

        let mut scratch = BackfillScratch::new();
        let before = reservation_time(&head_request, &inventory, &running, now, &mut scratch);

        let mut disposition = vec![Disposition::Pending; candidates.len()];
        let mut decisions: Vec<DispatchDecision> = Vec::new();
        let outcome = easy_backfill(
            &candidates,
            head_index,
            &arena,
            &mut inventory,
            &running,
            now,
            PackBudget::default(),
            &mut disposition,
            &mut decisions,
            &mut scratch,
        );

        // Rebuild the running set with everything backfill admitted, then ask
        // again when the head can start.
        let mut after_running = running.clone();
        for decision in &decisions {
            let job = arena.get(decision.job).unwrap();
            after_running.push(RunningJob {
                job: decision.job,
                slot: decision.slot,
                resources: job.request,
                ends_at: now.saturating_add(job.walltime_limit),
            });
        }
        let after =
            reservation_time(&head_request, &inventory, &after_running, now, &mut scratch);

        match (before, after) {
            (Some(before), Some(after)) => prop_assert!(
                after <= before,
                "head reservation slipped from {before:?} to {after:?} after {} backfills",
                outcome.backfilled
            ),
            (None, _) => prop_assert_eq!(outcome.backfilled, 0, "no reservation means no backfill"),
            (Some(_), None) => prop_assert!(false, "backfill made the head unschedulable"),
        }

        // The head job itself is never dispatched by a backfill pass.
        prop_assert_ne!(disposition[head_index], Disposition::Dispatched);
    }
}
