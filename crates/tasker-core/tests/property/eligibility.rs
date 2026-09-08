//! Property: the reverse-index tracker promotes exactly the jobs a full scan
//! would, for any DAG and any completion order.

use proptest::prelude::*;
use std::collections::BTreeSet;
use tasker_core::{
    AccountId, Arena, DependencyTracker, Job, JobId, JobState, PriorityClass, Readiness,
    ResourceRequest, VirtualDuration, VirtualTime,
};

fn job_with_deps(deps: &[JobId]) -> Job {
    let mut j = Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(1, 0, 0),
        VirtualDuration::from_secs(1),
    );
    j.deps.extend_from_slice(deps);
    j
}

/// A DAG as an edge list over `n` nodes, every edge pointing to a lower index,
/// plus a permutation giving the completion order.
fn dag_strategy() -> impl Strategy<Value = (usize, Vec<(usize, usize)>, Vec<usize>)> {
    (2_usize..24).prop_flat_map(|n| {
        let edges = prop::collection::vec((1..n, 0..n), 0..(n * 2)).prop_map(move |raw| {
            raw.into_iter()
                .filter_map(|(to, from)| (from < to).then_some((from, to)))
                .collect::<Vec<_>>()
        });
        let order = Just((0..n).collect::<Vec<_>>()).prop_shuffle();
        (Just(n), edges, order)
    })
}

proptest! {
    #[test]
    fn tracker_matches_a_full_scan((n, edges, order) in dag_strategy()) {
        let mut arena: Arena<Job> = Arena::new();
        let mut tracker = DependencyTracker::with_slots(n);
        let mut ids = Vec::with_capacity(n);

        // Build nodes in index order so every dependency already exists.
        for node in 0..n {
            let deps: Vec<JobId> = edges
                .iter()
                .filter(|(_, to)| *to == node)
                .map(|(from, _)| ids[*from])
                .collect();
            let id = arena.insert(job_with_deps(&deps));
            ids.push(id);
            let readiness = tracker.register(id, arena.get(id).unwrap(), &arena).unwrap();
            let next = match readiness {
                Readiness::Ready => JobState::Ready,
                Readiness::Blocked => JobState::Blocked,
                Readiness::Cancelled => unreachable!("no terminal jobs in this scenario"),
            };
            arena.get_mut(id).unwrap().try_transition(next).unwrap();
        }

        let mut completed: BTreeSet<usize> = BTreeSet::new();
        let mut promoted = Vec::new();

        for node in order {
            // Only jobs whose deps are all done can complete; skip the rest
            // (a random order will include some that are still blocked).
            let deps_done = edges.iter().filter(|(_, to)| *to == node).all(|(from, _)| completed.contains(from));
            if !deps_done {
                continue;
            }
            let id = ids[node];
            let job = arena.get_mut(id).unwrap();
            if job.state == JobState::Ready {
                job.try_transition(JobState::Running).unwrap();
                job.try_transition(JobState::Completed).unwrap();
            }
            completed.insert(node);

            promoted.clear();
            tracker.on_completed(id, &mut promoted);

            // Reference: every Blocked job whose deps are now all complete.
            let mut expected: Vec<JobId> = (0..n)
                .filter(|m| arena.get(ids[*m]).unwrap().state == JobState::Blocked)
                .filter(|m| edges.iter().filter(|(_, to)| to == m).all(|(from, _)| completed.contains(from)))
                .map(|m| ids[m])
                .collect();
            expected.sort();
            let mut got = promoted.clone();
            got.sort();
            prop_assert_eq!(got, expected, "after completing node {}", node);

            for p in &promoted {
                arena.get_mut(*p).unwrap().try_transition(JobState::Ready).unwrap();
            }
        }
    }
}
