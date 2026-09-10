//! Properties: a plan frees enough for the head on one slot and breaks no guard.

use proptest::prelude::*;
use tasker_core::{
    AccountId, Arena, Job, JobState, PreemptConfig, PriorityClass, ResourceRequest, Resources,
    RunningJob, SlotInventory, VirtualDuration, VirtualTime, plan_preemption,
};

const CAP: u32 = 4_000;

fn class(i: usize) -> PriorityClass {
    PriorityClass::ALL[i % PriorityClass::COUNT]
}

proptest! {
    #[test]
    fn a_plan_frees_the_head_and_respects_every_guard(
        slots in 1_u32..4,
        specs in prop::collection::vec((0_usize..4, 500_u32..3_000, 0_u8..5, 0_u32..4, 0_u64..60), 0..8),
        head_class in 0_usize..4, head_cpu in 500_u32..=CAP, min_class in 0_usize..4, max in 1_u8..4,
    ) {
        let mut jobs = Arena::new();
        let mut inv = SlotInventory::from_uniform(slots, Resources::new(CAP, 0, 0));
        let mut running = Vec::new();
        for (c, cpu, evictions, slot, started) in specs {
            let slot = slot % slots;
            let mut job = Job::new(AccountId::new(0), class(c), VirtualTime::ZERO, ResourceRequest::new(cpu, 0, 0), VirtualDuration::from_secs(60));
            job.state = JobState::Running;
            job.preemptions = evictions;
            let resources = job.request;
            // Only place what still fits; the rest is dropped from the scenario.
            if inv.try_allocate(slot, &resources).is_err() { continue; }
            let id = jobs.insert(job);
            running.push(RunningJob { job: id, slot, resources, ends_at: VirtualTime::from_nanos(started * 1_000_000_000).saturating_add(VirtualDuration::from_secs(60)) });
        }
        let head = Job::new(AccountId::new(0), class(head_class), VirtualTime::ZERO, ResourceRequest::new(head_cpu, 0, 0), VirtualDuration::from_secs(60));
        let cfg = PreemptConfig { min_class: Some(class(min_class)), max_preemptions: max, grace: VirtualDuration::from_secs(5) };
        let mut out = Vec::new();
        let now = VirtualTime::from_nanos(100_000_000_000);
        if let Some(slot) = plan_preemption(&head, &running, &jobs, &inv, now, &cfg, &mut out) {
            prop_assert!(!out.is_empty());
            let mut freed = inv.free(slot).unwrap();
            for e in &out {
                prop_assert_eq!(e.slot, slot);
                let victim = jobs.get(e.job).unwrap();
                prop_assert!(victim.priority_class < head.priority_class);
                prop_assert!(victim.preemptions < max);
                freed = freed.saturating_add(&victim.request);
            }
            prop_assert!(head.request.fits_within(&freed));
        } else {
            prop_assert!(out.is_empty());
        }
    }
}
