//! The data directory: one snapshot and one log per generation, rotated together.

use std::fs::File;
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use crate::log::{LogWriter, Replay, SyncPolicy, read_log, truncate_log};
use crate::snapshot::{Snapshot, decode_snapshot};
use crate::{Record, WalError};

/// Everything found on disk at startup.
#[derive(Debug)]
pub struct Recovered {
    pub generation: u64,
    pub snapshot: Option<Snapshot>,
    pub records: Vec<Record>,
    /// Offset of a torn tail that was cut off, if any.
    pub torn_at: Option<u64>,
}

/// The directory plus the open log of the current generation.
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    generation: u64,
    log: LogWriter,
}

fn snapshot_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("snapshot-{generation:06}.bin"))
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("wal-{generation:06}.log"))
}

/// `snapshot-000012.bin` or `wal-000012.log` → 12.
fn generation_of(name: &str) -> Option<u64> {
    let digits = if let Some(rest) = name.strip_prefix("snapshot-") {
        rest.strip_suffix(".bin")?
    } else if let Some(rest) = name.strip_prefix("wal-") {
        rest.strip_suffix(".log")?
    } else {
        return None;
    };
    digits.parse().ok()
}

fn ignore_missing(result: std::io::Result<()>) -> std::io::Result<()> {
    match result {
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
        other => other,
    }
}

impl Store {
    /// Reads the highest generation: its snapshot and log, either of which may
    /// be absent. A torn tail is cut off the log in place.
    ///
    /// # Errors
    /// I/O or format errors in either file.
    pub fn recover(dir: &Path) -> Result<Recovered, WalError> {
        std::fs::create_dir_all(dir)?;
        let mut generation = 0;
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            if let Some(g) = entry.file_name().to_str().and_then(generation_of) {
                generation = generation.max(g);
            }
        }
        let snapshot = match std::fs::read(snapshot_path(dir, generation)) {
            Ok(bytes) => Some(decode_snapshot(&bytes)?),
            Err(e) if e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        let log = log_path(dir, generation);
        let replay = if log.exists() {
            read_log(&log)?
        } else {
            Replay::default()
        };
        if let Some(at) = replay.torn_at {
            truncate_log(&log, at)?;
        }
        Ok(Recovered {
            generation,
            snapshot,
            records: replay.records,
            torn_at: replay.torn_at,
        })
    }

    /// Opens generation `generation`'s log for appending.
    ///
    /// # Errors
    /// I/O failure or a foreign file at the log's path.
    pub fn resume(dir: &Path, generation: u64) -> Result<Self, WalError> {
        let log = LogWriter::open(&log_path(dir, generation))?;
        Ok(Self {
            dir: dir.to_path_buf(),
            generation,
            log,
        })
    }

    /// Writes `snapshot` as the next generation, opens its empty log, deletes the
    /// old pair. Ordered so a crash at any point leaves a loadable generation.
    ///
    /// # Errors
    /// I/O failure.
    pub fn rotate(&mut self, snapshot: &[u8]) -> Result<(), WalError> {
        let next = self.generation + 1;
        let final_path = snapshot_path(&self.dir, next);
        let tmp = final_path.with_extension("bin.tmp");
        {
            let mut file = File::create(&tmp)?;
            file.write_all(snapshot)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &final_path)?;
        // The rename is directory metadata; sync the directory so it survives power loss.
        File::open(&self.dir)?.sync_all()?;
        self.log = LogWriter::open(&log_path(&self.dir, next))?;
        let old = self.generation;
        self.generation = next;
        ignore_missing(std::fs::remove_file(snapshot_path(&self.dir, old)))?;
        ignore_missing(std::fs::remove_file(log_path(&self.dir, old)))?;
        Ok(())
    }

    /// Appends framed bytes to the current log.
    pub fn append(&mut self, frames: &[u8]) -> std::io::Result<()> {
        self.log.append(frames)
    }

    /// Syncs the current log per `policy`.
    pub fn sync(&mut self, policy: SyncPolicy) -> std::io::Result<()> {
        self.log.sync(policy)
    }

    /// Bytes in the current log.
    #[must_use]
    pub const fn log_len(&self) -> u64 {
        self.log.len()
    }

    /// The current generation number.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}
