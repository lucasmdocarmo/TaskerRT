use std::path::PathBuf;

use tasker_core::JobId;
use tasker_wal::{FRAME_HEADER, LogWriter, Record, SyncPolicy, WalError, read_log, truncate_log};

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("taskerrt-wal-{}-{name}", std::process::id()));
    std::fs::remove_dir_all(&dir).ok();
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Three `Forgotten` frames, synced. Returns the path and one frame's length.
fn three_frames(dir: &std::path::Path) -> (PathBuf, u64) {
    let path = dir.join("wal-000000.log");
    let mut writer = LogWriter::open(&path).unwrap();
    let mut buf = Vec::new();
    for i in 0..3_u64 {
        Record::Forgotten {
            id: JobId::from_bits(i),
        }
        .encode(&mut buf);
    }
    writer.append(&buf).unwrap();
    writer.sync(SyncPolicy::Full).unwrap();
    (path, (buf.len() / 3) as u64)
}

#[test]
fn a_cut_tail_keeps_the_prefix_and_reports_the_offset() {
    let (path, frame) = three_frames(&scratch("torn-cut"));
    let full = std::fs::metadata(&path).unwrap().len();
    // Cut the third frame in half.
    truncate_log(&path, full - frame / 2).unwrap();
    let replay = read_log(&path).unwrap();
    assert_eq!(replay.records.len(), 2);
    assert_eq!(replay.torn_at, Some(8 + 2 * frame));
}

#[test]
fn a_flipped_byte_stops_at_that_record() {
    let (path, frame) = three_frames(&scratch("torn-flip"));
    let mut bytes = std::fs::read(&path).unwrap();
    // Inside the second frame's payload, past its header and kind byte.
    let victim = 8 + usize::try_from(frame).unwrap() + FRAME_HEADER + 3;
    bytes[victim] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();
    let replay = read_log(&path).unwrap();
    assert_eq!(replay.records.len(), 1);
    assert_eq!(replay.torn_at, Some(8 + frame));
}

#[test]
fn a_foreign_file_is_rejected() {
    let dir = scratch("torn-magic");
    let path = dir.join("wal-000000.log");
    std::fs::write(&path, b"definitely not a log").unwrap();
    assert!(matches!(read_log(&path), Err(WalError::BadMagic { .. })));
}
