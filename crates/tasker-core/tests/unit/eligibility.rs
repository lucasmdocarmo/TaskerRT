use tasker_core::{
    AccountId, Arena, DependencyTracker, Job, JobId, JobState, PriorityClass, Readiness,
    ResourceRequest, VirtualDuration, VirtualTime,
};

fn job() -> Job {
    Job::new(
        AccountId::new(0),
        PriorityClass::Normal,
        VirtualTime::ZERO,
        ResourceRequest::new(100, 0, 0),
        VirtualDuration::from_secs(60),
    )
}

fn job_depending_on(deps: &[JobId]) -> Job {
    let mut j = job();
    j.deps.extend_from_slice(deps);
    j
}

/// Inserts a job, drives it to `state`, and returns its handle.
fn insert_in_state(arena: &mut Arena<Job>, state: JobState) -> JobId {
    let mut j = job();
    let path: &[JobState] = match state {
        JobState::Submitted => &[],
        JobState::Ready => &[JobState::Ready],
        JobState::Blocked => &[JobState::Blocked],
        JobState::Running => &[JobState::Ready, JobState::Running],
        JobState::Completed => &[JobState::Ready, JobState::Running, JobState::Completed],
        JobState::Failed => &[JobState::Ready, JobState::Running, JobState::Failed],
        JobState::Cancelled => &[JobState::Cancelled],
        JobState::Preempted => &[JobState::Ready, JobState::Running, JobState::Preempted],
    };
    for s in path {
        j.try_transition(*s).unwrap();
    }
    arena.insert(j)
}

#[test]
fn no_dependencies_is_ready() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let id = arena.insert(job());
    let r = tracker
        .register(id, arena.get(id).unwrap(), &arena)
        .unwrap();
    assert_eq!(r, Readiness::Ready);
    assert_eq!(tracker.unsatisfied(id), Some(0));
}

#[test]
fn a_pending_dependency_blocks() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let dep = insert_in_state(&mut arena, JobState::Ready);
    let id = arena.insert(job_depending_on(&[dep]));
    let r = tracker
        .register(id, arena.get(id).unwrap(), &arena)
        .unwrap();
    assert_eq!(r, Readiness::Blocked);
    assert_eq!(tracker.unsatisfied(id), Some(1));
}

#[test]
fn a_completed_dependency_counts_as_satisfied() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let done = insert_in_state(&mut arena, JobState::Completed);
    let id = arena.insert(job_depending_on(&[done]));
    assert_eq!(
        tracker
            .register(id, arena.get(id).unwrap(), &arena)
            .unwrap(),
        Readiness::Ready
    );
}

#[test]
fn a_failed_or_cancelled_dependency_dooms_the_job() {
    for terminal in [JobState::Failed, JobState::Cancelled] {
        let mut arena = Arena::new();
        let mut tracker = DependencyTracker::with_slots(8);
        let dead = insert_in_state(&mut arena, terminal);
        let pending = insert_in_state(&mut arena, JobState::Ready);
        let id = arena.insert(job_depending_on(&[pending, dead]));
        let r = tracker
            .register(id, arena.get(id).unwrap(), &arena)
            .unwrap();
        assert_eq!(r, Readiness::Cancelled, "{terminal:?}");
        assert!(!tracker.is_tracked(id), "a doomed job is not tracked");
        // No reverse edge must have been left on the pending dependency.
        let mut promoted = Vec::new();
        tracker.on_completed(pending, &mut promoted);
        assert!(promoted.is_empty());
    }
}

#[test]
fn an_unknown_dependency_is_rejected() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let ghost = arena.insert(job());
    arena.remove(ghost);
    let id = arena.insert(job_depending_on(&[ghost]));
    let err = tracker
        .register(id, arena.get(id).unwrap(), &arena)
        .unwrap_err();
    assert_eq!(err.job, id);
    assert_eq!(err.dependency, ghost);
}

#[test]
fn a_job_may_not_depend_on_itself() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    // Insert first to learn the handle, then rewrite the job to depend on it.
    let id = arena.insert(job());
    arena.get_mut(id).unwrap().deps.push(id);
    let err = tracker
        .register(id, arena.get(id).unwrap(), &arena)
        .unwrap_err();
    assert_eq!(err.dependency, id);
    assert!(
        !tracker.is_tracked(id),
        "a rejected registration leaves no trace"
    );
}

#[test]
fn completing_the_last_dependency_promotes() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let a = insert_in_state(&mut arena, JobState::Ready);
    let b = insert_in_state(&mut arena, JobState::Ready);
    let c = arena.insert(job_depending_on(&[a, b]));
    assert_eq!(
        tracker.register(c, arena.get(c).unwrap(), &arena).unwrap(),
        Readiness::Blocked
    );

    let mut promoted = Vec::new();
    tracker.on_completed(a, &mut promoted);
    assert!(promoted.is_empty(), "one of two still pending");
    assert_eq!(tracker.unsatisfied(c), Some(1));

    tracker.on_completed(b, &mut promoted);
    assert_eq!(promoted, vec![c]);
    assert_eq!(tracker.unsatisfied(c), Some(0));
}

#[test]
fn termination_cascades_transitively_and_visits_each_job_once() {
    // Diamond: root -> {left, right} -> sink. sink must appear once.
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let root = insert_in_state(&mut arena, JobState::Ready);
    let left = arena.insert(job_depending_on(&[root]));
    let right = arena.insert(job_depending_on(&[root]));
    let sink = arena.insert(job_depending_on(&[left, right]));
    for id in [left, right, sink] {
        tracker
            .register(id, arena.get(id).unwrap(), &arena)
            .unwrap();
    }

    let mut cascade = Vec::new();
    tracker.on_terminated(root, &mut cascade);
    cascade.sort();
    let mut expected = vec![left, right, sink];
    expected.sort();
    assert_eq!(cascade, expected);
    for id in [root, left, right, sink] {
        assert!(
            !tracker.is_tracked(id),
            "{id:?} should be retired after cascade"
        );
    }
}

#[test]
fn retire_clears_the_slot_for_reuse() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let a = insert_in_state(&mut arena, JobState::Ready);
    let b = arena.insert(job_depending_on(&[a]));
    tracker.register(b, arena.get(b).unwrap(), &arena).unwrap();
    tracker.retire(b);
    assert!(!tracker.is_tracked(b));

    // Completing `a` must not promote the retired `b`.
    let mut promoted = Vec::new();
    tracker.on_completed(a, &mut promoted);
    assert!(promoted.is_empty());
}

#[test]
fn a_stale_handle_sharing_an_index_is_not_confused_with_the_live_one() {
    let mut arena = Arena::new();
    let mut tracker = DependencyTracker::with_slots(8);
    let a = insert_in_state(&mut arena, JobState::Ready);
    let old = arena.insert(job_depending_on(&[a]));
    tracker
        .register(old, arena.get(old).unwrap(), &arena)
        .unwrap();
    tracker.retire(old);
    arena.remove(old);
    let new = arena.insert(job()); // reuses old's index, new generation
    assert_eq!(old.index(), new.index());
    tracker
        .register(new, arena.get(new).unwrap(), &arena)
        .unwrap();

    assert!(!tracker.is_tracked(old));
    assert!(tracker.is_tracked(new));
    assert_eq!(tracker.unsatisfied(old), None);
}
