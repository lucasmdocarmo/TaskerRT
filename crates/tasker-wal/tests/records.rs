use bytes::Bytes;
use tasker_core::{
    AccountId, Job, JobId, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime,
};
use tasker_wal::{LOG_MAGIC, Record, encode_submitted, parse_log};

fn sample_job() -> Job {
    let mut j = Job::new(
        AccountId::new(7),
        PriorityClass::High,
        VirtualTime::from_nanos(42),
        ResourceRequest::new(1_500, 1 << 30, 2),
        VirtualDuration::from_secs(90),
    );
    j.deps.push(JobId::from_bits(5));
    j.deps.push(JobId::from_bits(1 << 40));
    j.payload = Bytes::from_static(b"hello");
    j.preemptions = 2;
    j
}

#[test]
fn every_record_kind_round_trips() {
    let id = JobId::from_bits(0x0000_0003_0000_0009);
    let at = VirtualTime::from_nanos(123_456_789);
    let records = vec![
        Record::Submitted {
            id,
            job: sample_job(),
        },
        Record::Dispatched { id, at },
        Record::Completed { id, at },
        Record::Failed { id, at },
        Record::Cancelled { id, at },
        Record::Requeued { id, at },
        Record::Forgotten { id },
        Record::Preempted { id, at },
    ];
    let mut buf = LOG_MAGIC.to_vec();
    for r in &records {
        r.encode(&mut buf);
    }
    let replay = parse_log(&buf).unwrap();
    assert_eq!(replay.records, records);
    assert_eq!(replay.torn_at, None);
}

#[test]
fn encode_submitted_matches_the_owned_record() {
    let id = JobId::from_bits(1);
    let job = sample_job();
    let mut borrowed = LOG_MAGIC.to_vec();
    encode_submitted(&mut borrowed, id, &job);
    let mut owned = LOG_MAGIC.to_vec();
    Record::Submitted { id, job }.encode(&mut owned);
    assert_eq!(borrowed, owned);
}

#[test]
fn an_unknown_kind_is_a_codec_error_not_a_panic() {
    let mut buf = LOG_MAGIC.to_vec();
    // Hand-build a frame whose payload starts with kind 0.
    tasker_wal::frame(&mut buf, |b| b.extend_from_slice(&[0_u8; 9]));
    assert!(parse_log(&buf).is_err());
}
