use bytes::Bytes;
use tasker_core::{JobState, PriorityClass, Resources, VirtualTime};
use tasker_proto::convert::{
    ConvertError, job_from_submit, job_state_to_proto, priority_class_from_proto,
    priority_class_to_proto, resources_from_proto, resources_to_proto,
};
use tasker_proto::v1;

#[test]
fn priority_class_round_trips() {
    for class in PriorityClass::ALL {
        assert_eq!(
            priority_class_from_proto(priority_class_to_proto(class)).unwrap(),
            class
        );
    }
    assert_eq!(
        priority_class_from_proto(v1::PriorityClass::Unspecified).unwrap_err(),
        ConvertError::UnspecifiedPriorityClass
    );
}

#[test]
fn every_job_state_has_a_distinct_wire_value() {
    let mut seen = std::collections::BTreeSet::new();
    for state in JobState::ALL {
        let wire = job_state_to_proto(state);
        assert_ne!(wire, v1::JobState::Unspecified);
        assert!(seen.insert(wire as i32), "{state:?} collides");
    }
}

#[test]
fn resources_round_trip_and_reject_wide_gpus() {
    let r = Resources::new(4_000, 8 << 30, 2);
    assert_eq!(resources_from_proto(&resources_to_proto(r)).unwrap(), r);
    let bad = v1::Resources {
        cpu_millis: 1,
        mem_bytes: 1,
        gpus: 300,
    };
    assert_eq!(
        resources_from_proto(&bad).unwrap_err(),
        ConvertError::GpusOutOfRange(300)
    );
}

#[test]
fn a_submit_request_becomes_a_submitted_job() {
    let req = v1::SubmitRequest {
        account: 7,
        priority_class: v1::PriorityClass::High as i32,
        request: Some(v1::Resources {
            cpu_millis: 500,
            mem_bytes: 1 << 20,
            gpus: 0,
        }),
        walltime_nanos: 60_000_000_000,
        deps: vec![0x0000_0001_0000_0002],
        payload: Bytes::from_static(b"hi"),
    };
    let job = job_from_submit(&req, VirtualTime::from_nanos(5)).unwrap();
    assert_eq!(job.state, JobState::Submitted);
    assert_eq!(job.account.get(), 7);
    assert_eq!(job.priority_class, PriorityClass::High);
    assert_eq!(job.request.cpu_millis, 500);
    assert_eq!(job.walltime_limit.as_nanos(), 60_000_000_000);
    assert_eq!(job.deps.len(), 1);
    assert_eq!(job.deps[0].index(), 2);
    assert_eq!(job.deps[0].generation(), 1);
    assert_eq!(&job.payload[..], b"hi");
    assert_eq!(job.submit_time, VirtualTime::from_nanos(5));
}

#[test]
fn a_submit_request_with_defaults_is_rejected() {
    let req = v1::SubmitRequest::default();
    assert_eq!(
        job_from_submit(&req, VirtualTime::ZERO).unwrap_err(),
        ConvertError::UnspecifiedPriorityClass
    );
    let req = v1::SubmitRequest {
        priority_class: v1::PriorityClass::Normal as i32,
        ..v1::SubmitRequest::default()
    };
    assert_eq!(
        job_from_submit(&req, VirtualTime::ZERO).unwrap_err(),
        ConvertError::MissingResources
    );
}
