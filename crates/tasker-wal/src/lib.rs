//! Append-only write-ahead log for the scheduler: framed, checksummed records,
//! whole-state snapshots, and the directory that holds them. Synchronous
//! `std::fs` only: no runtime, no clock.

pub mod codec;
pub mod log;
pub mod record;
pub mod snapshot;
pub mod store;

pub use codec::CodecError;
pub use log::{LOG_MAGIC, LogWriter, Replay, SyncPolicy, parse_log, read_log, truncate_log};
pub use record::{FRAME_HEADER, Record, encode_submitted, frame};
pub use snapshot::{SNAPSHOT_MAGIC, Snapshot, decode_snapshot, encode_snapshot};
pub use store::{Recovered, Store};

/// Anything that can go wrong reading or writing the log.
#[derive(Debug, thiserror::Error)]
pub enum WalError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("not a TaskerRT {kind} file (bad magic)")]
    BadMagic { kind: &'static str },
    #[error("snapshot checksum mismatch")]
    SnapshotChecksum,
    #[error(transparent)]
    Codec(#[from] CodecError),
}
