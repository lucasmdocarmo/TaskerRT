//! Properties: any record sequence round-trips; any cut of the file is a prefix.

use bytes::Bytes;
use proptest::prelude::*;
use tasker_core::{
    AccountId, Job, JobId, JobState, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime,
};
use tasker_wal::{LOG_MAGIC, Record, parse_log};

fn job_strategy() -> impl Strategy<Value = Job> {
    (
        any::<u32>(),
        0_usize..PriorityClass::COUNT,
        any::<u64>(),
        any::<u32>(),
        any::<u64>(),
        any::<u8>(),
        any::<u64>(),
        prop::collection::vec(any::<u64>(), 0..6),
        prop::collection::vec(any::<u8>(), 0..64),
        0_usize..JobState::ALL.len(),
        any::<u8>(),
    )
        .prop_map(
            |(
                account,
                class,
                submit,
                cpu,
                mem,
                gpus,
                walltime,
                deps,
                payload,
                state,
                evictions,
            )| {
                let mut j = Job::new(
                    AccountId::new(account),
                    PriorityClass::ALL[class],
                    VirtualTime::from_nanos(submit),
                    ResourceRequest::new(cpu, mem, gpus),
                    VirtualDuration::from_nanos(walltime),
                );
                j.deps.extend(deps.into_iter().map(JobId::from_bits));
                j.payload = Bytes::from(payload);
                j.state = JobState::ALL[state];
                j.preemptions = evictions;
                j
            },
        )
}

fn record_strategy() -> impl Strategy<Value = Record> {
    let event = |make: fn(JobId, VirtualTime) -> Record| {
        (any::<u64>(), any::<u64>())
            .prop_map(move |(id, at)| make(JobId::from_bits(id), VirtualTime::from_nanos(at)))
    };
    prop_oneof![
        (any::<u64>(), job_strategy()).prop_map(|(id, job)| Record::Submitted {
            id: JobId::from_bits(id),
            job,
        }),
        event(|id, at| Record::Dispatched { id, at }),
        event(|id, at| Record::Completed { id, at }),
        event(|id, at| Record::Failed { id, at }),
        event(|id, at| Record::Cancelled { id, at }),
        event(|id, at| Record::Requeued { id, at }),
        event(|id, at| Record::Preempted { id, at }),
        any::<u64>().prop_map(|id| Record::Forgotten {
            id: JobId::from_bits(id),
        }),
    ]
}

proptest! {
    #[test]
    fn sequences_round_trip(records in prop::collection::vec(record_strategy(), 0..20)) {
        let mut buf = LOG_MAGIC.to_vec();
        for r in &records {
            r.encode(&mut buf);
        }
        let replay = parse_log(&buf).unwrap();
        prop_assert_eq!(replay.records, records);
        prop_assert_eq!(replay.torn_at, None);
    }

    #[test]
    fn any_cut_yields_a_prefix(
        records in prop::collection::vec(record_strategy(), 1..10),
        cut in 0_usize..4_096,
    ) {
        let mut buf = LOG_MAGIC.to_vec();
        for r in &records {
            r.encode(&mut buf);
        }
        let cut = cut.min(buf.len());
        match parse_log(&buf[..cut]) {
            Ok(replay) => prop_assert!(records.starts_with(&replay.records)),
            // Only a cut inside the magic is an error; everything after is a torn tail.
            Err(_) => prop_assert!(cut < LOG_MAGIC.len()),
        }
    }
}
