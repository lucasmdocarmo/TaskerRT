use tasker_core::{
    AccountId, Job, JobState, PriorityClass, ResourceRequest, TransitionError, VirtualDuration,
    VirtualTime,
};

fn a_job() -> Job {
    Job::new(
        AccountId::new(1),
        PriorityClass::Normal,
        VirtualTime::from_nanos(0),
        ResourceRequest::new(1_000, 1 << 30, 0),
        VirtualDuration::from_secs(60),
    )
}

#[test]
fn a_new_job_starts_submitted() {
    assert_eq!(a_job().state, JobState::Submitted);
}

#[test]
fn submitted_may_go_blocked_ready_or_cancelled() {
    assert!(JobState::Submitted.can_transition_to(JobState::Blocked));
    assert!(JobState::Submitted.can_transition_to(JobState::Ready));
    assert!(JobState::Submitted.can_transition_to(JobState::Cancelled));
    assert!(!JobState::Submitted.can_transition_to(JobState::Running));
}

#[test]
fn preempted_may_requeue_finish_or_be_cancelled() {
    assert!(JobState::Preempted.can_transition_to(JobState::Ready));
    // A task can finish inside its eviction grace; the result is kept.
    assert!(JobState::Preempted.can_transition_to(JobState::Completed));
    assert!(JobState::Preempted.can_transition_to(JobState::Failed));
    assert!(JobState::Preempted.can_transition_to(JobState::Cancelled));
    assert!(!JobState::Preempted.can_transition_to(JobState::Running));
}

#[test]
fn terminal_states_have_no_successors() {
    for terminal in [JobState::Completed, JobState::Failed, JobState::Cancelled] {
        assert!(terminal.is_terminal());
        for next in JobState::ALL {
            assert!(
                !terminal.can_transition_to(next),
                "{terminal:?} must not reach {next:?}"
            );
        }
    }
}

#[test]
fn cancellation_is_reachable_from_blocked_ready_and_running() {
    for from in [JobState::Blocked, JobState::Ready, JobState::Running] {
        assert!(from.can_transition_to(JobState::Cancelled));
    }
}

#[test]
fn try_transition_rejects_illegal_moves() {
    let mut job = a_job();
    assert!(job.try_transition(JobState::Ready).is_ok());
    assert_eq!(job.state, JobState::Ready);
    let err = job.try_transition(JobState::Completed).unwrap_err();
    assert_eq!(
        err,
        TransitionError {
            from: JobState::Ready,
            to: JobState::Completed
        }
    );
    assert_eq!(
        job.state,
        JobState::Ready,
        "a rejected transition must not mutate"
    );
}

#[test]
fn priority_classes_are_ordered_low_to_urgent() {
    assert!(PriorityClass::Low < PriorityClass::Normal);
    assert!(PriorityClass::Normal < PriorityClass::High);
    assert!(PriorityClass::High < PriorityClass::Urgent);
    assert_eq!(PriorityClass::COUNT, 4);
    assert_eq!(PriorityClass::Urgent.ordinal(), 3);
}
