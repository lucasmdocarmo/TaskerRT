//! A full copy of the durable state, written whole and read whole.

use tasker_core::{Job, JobId, Ledger, SlotState, VirtualTime};

use crate::WalError;
use crate::codec::{Cursor, put_opt_u32, put_u8, put_u32, put_u64};
use crate::record::{get_job, put_job};

/// First eight bytes of every snapshot file.
pub const SNAPSHOT_MAGIC: &[u8; 8] = b"TKRSNP01";

/// A decoded snapshot.
#[derive(Debug)]
pub struct Snapshot {
    pub taken_at: VirtualTime,
    pub slots: Vec<SlotState<Job>>,
    pub free_head: Option<u32>,
    pub running: Vec<JobId>,
    pub ledgers: Vec<Ledger>,
}

/// Encodes magic, body, and the body's CRC-32 into `buf`.
pub fn encode_snapshot<'a>(
    buf: &mut Vec<u8>,
    taken_at: VirtualTime,
    slots: impl ExactSizeIterator<Item = SlotState<&'a Job>>,
    free_head: Option<u32>,
    running: &[JobId],
    ledgers: &[Ledger],
) {
    buf.extend_from_slice(SNAPSHOT_MAGIC);
    let body_start = buf.len();
    put_u64(buf, taken_at.as_nanos());
    put_u32(
        buf,
        u32::try_from(slots.len()).expect("more than u32::MAX slots"),
    );
    for slot in slots {
        match slot {
            SlotState::Occupied { generation, value } => {
                put_u8(buf, 1);
                put_u32(buf, generation);
                put_job(buf, value);
            }
            SlotState::Vacant {
                generation,
                next_free,
            } => {
                put_u8(buf, 0);
                put_u32(buf, generation);
                put_opt_u32(buf, next_free);
            }
        }
    }
    put_opt_u32(buf, free_head);
    put_u32(
        buf,
        u32::try_from(running.len()).expect("more than u32::MAX running"),
    );
    for id in running {
        put_u64(buf, id.to_bits());
    }
    put_u32(
        buf,
        u32::try_from(ledgers.len()).expect("more than u32::MAX ledgers"),
    );
    for l in ledgers {
        put_u64(buf, l.usage);
        put_u64(buf, l.running_cpu);
        put_u32(buf, l.shares);
        put_u64(buf, l.accrued_to().as_nanos());
        put_u64(buf, l.decayed_to().as_nanos());
    }
    let crc = crc32fast::hash(&buf[body_start..]);
    put_u32(buf, crc);
}

/// Decodes a whole snapshot image.
///
/// # Errors
/// `BadMagic`, `SnapshotChecksum`, or `Codec`.
pub fn decode_snapshot(bytes: &[u8]) -> Result<Snapshot, WalError> {
    // Magic (8) plus trailing CRC (4) is the smallest possible file.
    if bytes.len() < 12 || &bytes[..8] != SNAPSHOT_MAGIC {
        return Err(WalError::BadMagic { kind: "snapshot" });
    }
    let (body, crc_bytes) = bytes[8..].split_at(bytes.len() - 12);
    let crc = u32::from_le_bytes(crc_bytes.try_into().expect("four bytes"));
    if crc32fast::hash(body) != crc {
        return Err(WalError::SnapshotChecksum);
    }
    let mut c = Cursor::new(body);
    let taken_at = VirtualTime::from_nanos(c.u64()?);
    let slot_count = c.u32()? as usize;
    // Cap the pre-allocation: the count is data, not a promise.
    let mut slots = Vec::with_capacity(slot_count.min(1 << 16));
    for _ in 0..slot_count {
        if c.tag(1)? == 1 {
            let generation = c.u32()?;
            let value = get_job(&mut c)?;
            slots.push(SlotState::Occupied { generation, value });
        } else {
            let generation = c.u32()?;
            let next_free = c.opt_u32()?;
            slots.push(SlotState::Vacant {
                generation,
                next_free,
            });
        }
    }
    let free_head = c.opt_u32()?;
    let running_count = c.u32()? as usize;
    let mut running = Vec::with_capacity(running_count.min(1 << 16));
    for _ in 0..running_count {
        running.push(JobId::from_bits(c.u64()?));
    }
    let ledger_count = c.u32()? as usize;
    let mut ledgers = Vec::with_capacity(ledger_count.min(1 << 10));
    for _ in 0..ledger_count {
        let usage = c.u64()?;
        let running_cpu = c.u64()?;
        let shares = c.u32()?;
        let accrued_to = VirtualTime::from_nanos(c.u64()?);
        let decayed_to = VirtualTime::from_nanos(c.u64()?);
        ledgers.push(Ledger::from_parts(
            usage,
            running_cpu,
            shares,
            accrued_to,
            decayed_to,
        ));
    }
    Ok(Snapshot {
        taken_at,
        slots,
        free_head,
        running,
        ledgers,
    })
}
