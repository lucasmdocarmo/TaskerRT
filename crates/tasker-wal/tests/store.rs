use std::path::PathBuf;

use tasker_core::{Arena, Job, JobId, VirtualTime};
use tasker_wal::{Record, Store, SyncPolicy, encode_snapshot};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("taskerrt-wal-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn recover_then_rotate_compacts_and_replays() {
    let dir = scratch("store");
    let first = Store::recover(&dir).unwrap();
    assert_eq!(first.generation, 0);
    assert!(first.snapshot.is_none());
    assert!(first.records.is_empty());

    let mut store = Store::resume(&dir, 0).unwrap();
    let mut frames = Vec::new();
    Record::Forgotten {
        id: JobId::from_bits(1),
    }
    .encode(&mut frames);
    store.append(&frames).unwrap();
    store.sync(SyncPolicy::Data).unwrap();
    drop(store);

    let again = Store::recover(&dir).unwrap();
    assert_eq!(again.generation, 0);
    assert_eq!(again.records.len(), 1);

    // Rotating onto generation 1 writes a snapshot, opens an empty log, and
    // removes generation 0.
    let mut store = Store::resume(&dir, again.generation).unwrap();
    let mut snap = Vec::new();
    encode_snapshot(
        &mut snap,
        VirtualTime::ZERO,
        Arena::<Job>::new().slots(),
        None,
        &[],
        &[],
    );
    store.rotate(&snap).unwrap();
    assert_eq!(store.generation(), 1);
    assert!(dir.join("snapshot-000001.bin").exists());
    assert!(dir.join("wal-000001.log").exists());
    assert!(!dir.join("wal-000000.log").exists());

    let third = Store::recover(&dir).unwrap();
    assert_eq!(third.generation, 1);
    assert!(third.snapshot.is_some());
    assert!(third.records.is_empty());
}
