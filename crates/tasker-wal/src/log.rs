//! One log file: magic, frames, and the torn-tail rule on read.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

use crate::record::FRAME_HEADER;
use crate::{Record, WalError};

/// First eight bytes of every log file.
pub const LOG_MAGIC: &[u8; 8] = b"TKRWAL02";

/// How hard each batch is pushed toward the disk.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum SyncPolicy {
    /// `sync_all`: data and metadata.
    Full,
    /// `sync_data`: fdatasync on Linux; `F_FULLFSYNC` on macOS, same as `Full`.
    #[default]
    Data,
    /// No sync; durability is whatever the OS page cache gives.
    None,
}

/// An open log file positioned at its end.
#[derive(Debug)]
pub struct LogWriter {
    file: File,
    len: u64,
}

impl LogWriter {
    /// Opens or creates `path` for appending; a new file gets the magic.
    ///
    /// # Errors
    /// I/O failure, or an existing file that is not a log.
    pub fn open(path: &Path) -> Result<Self, WalError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        let len = file.metadata()?.len();
        if len == 0 {
            file.write_all(LOG_MAGIC)?;
            file.sync_all()?;
            return Ok(Self {
                file,
                len: LOG_MAGIC.len() as u64,
            });
        }
        // Append mode fixes the write offset at the end; reads still start at zero.
        let mut magic = [0_u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != LOG_MAGIC {
            return Err(WalError::BadMagic { kind: "log" });
        }
        Ok(Self { file, len })
    }

    /// Appends already-framed bytes. Not durable until `sync`.
    pub fn append(&mut self, frames: &[u8]) -> std::io::Result<()> {
        self.file.write_all(frames)?;
        self.len += frames.len() as u64;
        Ok(())
    }

    /// Pushes appended bytes to disk per `policy`.
    pub fn sync(&mut self, policy: SyncPolicy) -> std::io::Result<()> {
        match policy {
            SyncPolicy::Full => self.file.sync_all(),
            SyncPolicy::Data => self.file.sync_data(),
            SyncPolicy::None => Ok(()),
        }
    }

    /// Bytes in the file, magic included.
    #[must_use]
    pub const fn len(&self) -> u64 {
        self.len
    }

    /// True when the file holds only its magic: no frames yet.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len <= LOG_MAGIC.len() as u64
    }
}

/// Records recovered from a log, and where a torn tail began if there was one.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Replay {
    pub records: Vec<Record>,
    pub torn_at: Option<u64>,
}

/// Parses a whole log image. Stops at the first frame that is cut short or
/// fails its checksum and reports that offset; a frame that checksums but does
/// not decode is an error, not a torn write.
///
/// # Errors
/// `BadMagic`, or `Codec` for an intact but undecodable frame.
pub fn parse_log(bytes: &[u8]) -> Result<Replay, WalError> {
    if bytes.len() < LOG_MAGIC.len() || &bytes[..LOG_MAGIC.len()] != LOG_MAGIC {
        return Err(WalError::BadMagic { kind: "log" });
    }
    let mut records = Vec::new();
    let mut pos = LOG_MAGIC.len();
    while pos < bytes.len() {
        // `let ... else` breaks out when the slice is too short: a torn frame.
        let Some(header) = bytes.get(pos..pos + FRAME_HEADER) else {
            break;
        };
        let len = u32::from_le_bytes(header[..4].try_into().expect("four bytes")) as usize;
        let crc = u32::from_le_bytes(header[4..].try_into().expect("four bytes"));
        let Some(payload) = bytes.get(pos + FRAME_HEADER..pos + FRAME_HEADER + len) else {
            break;
        };
        if crc32fast::hash(payload) != crc {
            break;
        }
        records.push(Record::decode(payload)?);
        pos += FRAME_HEADER + len;
    }
    let torn_at = (pos < bytes.len()).then_some(pos as u64);
    Ok(Replay { records, torn_at })
}

/// Reads and parses the file at `path`.
///
/// # Errors
/// I/O failure or what `parse_log` reports.
pub fn read_log(path: &Path) -> Result<Replay, WalError> {
    parse_log(&std::fs::read(path)?)
}

/// Cuts a torn tail off so the next append starts at a frame boundary.
///
/// # Errors
/// I/O failure.
pub fn truncate_log(path: &Path, len: u64) -> std::io::Result<()> {
    let file = OpenOptions::new().write(true).open(path)?;
    file.set_len(len)?;
    file.sync_all()
}
