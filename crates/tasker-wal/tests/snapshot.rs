use bytes::Bytes;
use tasker_core::{
    AccountId, Arena, Job, JobId, Ledger, PriorityClass, ResourceRequest, VirtualDuration,
    VirtualTime,
};
use tasker_wal::{WalError, decode_snapshot, encode_snapshot};

fn sample_job() -> Job {
    let mut j = Job::new(
        AccountId::new(7),
        PriorityClass::High,
        VirtualTime::from_nanos(42),
        ResourceRequest::new(1_500, 1 << 30, 2),
        VirtualDuration::from_secs(90),
    );
    j.deps.push(JobId::from_bits(5));
    j.payload = Bytes::from_static(b"hello");
    j
}

#[test]
fn a_snapshot_restores_an_arena_that_hands_out_the_same_ids() {
    let mut arena: Arena<Job> = Arena::new();
    let ids: Vec<JobId> = (0..4).map(|_| arena.insert(sample_job())).collect();
    arena.remove(ids[2]);
    let running = vec![ids[0]];
    let ledgers = vec![Ledger::from_parts(
        5,
        1_000,
        2,
        VirtualTime::from_nanos(7),
        VirtualTime::from_nanos(3),
    )];
    let mut buf = Vec::new();
    encode_snapshot(
        &mut buf,
        VirtualTime::from_nanos(99),
        arena.slots(),
        arena.free_head(),
        &running,
        &ledgers,
    );
    let snap = decode_snapshot(&buf).unwrap();
    assert_eq!(snap.taken_at, VirtualTime::from_nanos(99));
    assert_eq!(snap.running, running);
    assert_eq!(snap.ledgers, ledgers);
    let mut restored = Arena::from_parts(snap.slots, snap.free_head).unwrap();
    assert_eq!(restored.len(), 3);
    assert_eq!(restored.next_id(), arena.next_id());
    assert_eq!(restored.insert(sample_job()), arena.insert(sample_job()));
    assert_eq!(restored.get(ids[1]).unwrap(), arena.get(ids[1]).unwrap());
}

#[test]
fn a_corrupted_snapshot_is_refused() {
    let mut buf = Vec::new();
    encode_snapshot(
        &mut buf,
        VirtualTime::ZERO,
        Arena::<Job>::new().slots(),
        None,
        &[],
        &[],
    );
    let last_body_byte = buf.len() - 5;
    buf[last_body_byte] ^= 0x01;
    assert!(matches!(
        decode_snapshot(&buf),
        Err(WalError::SnapshotChecksum)
    ));
    assert!(matches!(
        decode_snapshot(b"short"),
        Err(WalError::BadMagic { .. })
    ));
}
