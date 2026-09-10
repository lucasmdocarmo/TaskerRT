//! The seven record kinds and their byte layout. Every field is little-endian.

use bytes::Bytes;
use tasker_core::{
    AccountId, Job, JobId, JobState, PriorityClass, ResourceRequest, VirtualDuration, VirtualTime,
};

use crate::codec::{CodecError, Cursor, put_bytes, put_u8, put_u32, put_u64};

/// One state transition the engine made.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Record {
    Submitted { id: JobId, job: Job },
    Dispatched { id: JobId, at: VirtualTime },
    Completed { id: JobId, at: VirtualTime },
    Failed { id: JobId, at: VirtualTime },
    Cancelled { id: JobId, at: VirtualTime },
    Requeued { id: JobId, at: VirtualTime },
    Forgotten { id: JobId },
    Preempted { id: JobId, at: VirtualTime },
}

const SUBMITTED: u8 = 1;
const DISPATCHED: u8 = 2;
const COMPLETED: u8 = 3;
const FAILED: u8 = 4;
const CANCELLED: u8 = 5;
const REQUEUED: u8 = 6;
const FORGOTTEN: u8 = 7;
const PREEMPTED: u8 = 8;

/// Frame header: payload length, then CRC-32 of the payload.
pub const FRAME_HEADER: usize = 8;

/// Appends one frame, `len | crc | payload`, with `write` producing the payload.
pub fn frame(buf: &mut Vec<u8>, write: impl FnOnce(&mut Vec<u8>)) {
    let start = buf.len();
    // Reserve the header; it is filled in once the payload is known.
    buf.extend_from_slice(&[0; FRAME_HEADER]);
    write(buf);
    let payload = &buf[start + FRAME_HEADER..];
    let len = u32::try_from(payload.len()).expect("record longer than u32::MAX");
    let crc = crc32fast::hash(payload);
    buf[start..start + 4].copy_from_slice(&len.to_le_bytes());
    buf[start + 4..start + FRAME_HEADER].copy_from_slice(&crc.to_le_bytes());
}

/// Frames a `Submitted` record straight from a borrowed job: no clone on the hot path.
pub fn encode_submitted(buf: &mut Vec<u8>, id: JobId, job: &Job) {
    frame(buf, |b| {
        put_u8(b, SUBMITTED);
        put_u64(b, id.to_bits());
        put_job(b, job);
    });
}

fn put_event(b: &mut Vec<u8>, kind: u8, id: JobId, at: VirtualTime) {
    put_u8(b, kind);
    put_u64(b, id.to_bits());
    put_u64(b, at.as_nanos());
}

impl Record {
    /// Frames this record onto `buf`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        frame(buf, |b| match self {
            Self::Submitted { id, job } => {
                put_u8(b, SUBMITTED);
                put_u64(b, id.to_bits());
                put_job(b, job);
            }
            Self::Dispatched { id, at } => put_event(b, DISPATCHED, *id, *at),
            Self::Completed { id, at } => put_event(b, COMPLETED, *id, *at),
            Self::Failed { id, at } => put_event(b, FAILED, *id, *at),
            Self::Cancelled { id, at } => put_event(b, CANCELLED, *id, *at),
            Self::Requeued { id, at } => put_event(b, REQUEUED, *id, *at),
            Self::Forgotten { id } => {
                put_u8(b, FORGOTTEN);
                put_u64(b, id.to_bits());
            }
            Self::Preempted { id, at } => put_event(b, PREEMPTED, *id, *at),
        });
    }

    /// Decodes one frame's payload (the bytes after the header).
    ///
    /// # Errors
    /// `CodecError` when the payload is short or carries an unknown tag.
    pub fn decode(payload: &[u8]) -> Result<Self, CodecError> {
        let mut c = Cursor::new(payload);
        let kind = c.tag(PREEMPTED)?;
        let id = JobId::from_bits(c.u64()?);
        Ok(match kind {
            SUBMITTED => Self::Submitted {
                id,
                job: get_job(&mut c)?,
            },
            FORGOTTEN => Self::Forgotten { id },
            // Every other kind is `id` then `at`.
            _ => {
                let at = VirtualTime::from_nanos(c.u64()?);
                match kind {
                    DISPATCHED => Self::Dispatched { id, at },
                    COMPLETED => Self::Completed { id, at },
                    FAILED => Self::Failed { id, at },
                    CANCELLED => Self::Cancelled { id, at },
                    REQUEUED => Self::Requeued { id, at },
                    PREEMPTED => Self::Preempted { id, at },
                    tag => return Err(CodecError::BadTag { tag, at: 0 }),
                }
            }
        })
    }

    /// The job this record is about.
    #[must_use]
    pub const fn id(&self) -> JobId {
        match self {
            Self::Submitted { id, .. }
            | Self::Dispatched { id, .. }
            | Self::Completed { id, .. }
            | Self::Failed { id, .. }
            | Self::Cancelled { id, .. }
            | Self::Requeued { id, .. }
            | Self::Preempted { id, .. }
            | Self::Forgotten { id } => *id,
        }
    }
}

fn state_tag(state: JobState) -> u8 {
    let index = JobState::ALL
        .iter()
        .position(|s| *s == state)
        .expect("every state is listed in ALL");
    u8::try_from(index).expect("fewer than 256 states")
}

/// Job layout: account, class, submit time, request, walltime, deps, payload, state, evictions.
pub(crate) fn put_job(b: &mut Vec<u8>, job: &Job) {
    put_u32(b, job.account.get());
    put_u8(b, job.priority_class.ordinal());
    put_u64(b, job.submit_time.as_nanos());
    put_u32(b, job.request.cpu_millis);
    put_u64(b, job.request.mem_bytes);
    put_u8(b, job.request.gpus);
    put_u64(b, job.walltime_limit.as_nanos());
    put_u32(
        b,
        u32::try_from(job.deps.len()).expect("more than u32::MAX dependencies"),
    );
    for dep in &job.deps {
        put_u64(b, dep.to_bits());
    }
    put_bytes(b, &job.payload);
    put_u8(b, state_tag(job.state));
    put_u8(b, job.preemptions);
}

pub(crate) fn get_job(c: &mut Cursor<'_>) -> Result<Job, CodecError> {
    let account = AccountId::new(c.u32()?);
    let class_at = c.position();
    let class_tag = c.u8()?;
    let class = PriorityClass::ALL
        .get(usize::from(class_tag))
        .copied()
        .ok_or(CodecError::BadTag {
            tag: class_tag,
            at: class_at,
        })?;
    let submit_time = VirtualTime::from_nanos(c.u64()?);
    let cpu = c.u32()?;
    let mem = c.u64()?;
    let gpus = c.u8()?;
    let walltime = VirtualDuration::from_nanos(c.u64()?);
    let mut job = Job::new(
        account,
        class,
        submit_time,
        ResourceRequest::new(cpu, mem, gpus),
        walltime,
    );
    let deps = c.u32()?;
    for _ in 0..deps {
        job.deps.push(JobId::from_bits(c.u64()?));
    }
    // `copy_from_slice`: the log buffer is transient, the job outlives it.
    job.payload = Bytes::copy_from_slice(c.bytes()?);
    let state_at = c.position();
    let state_tag = c.u8()?;
    job.state = JobState::ALL
        .get(usize::from(state_tag))
        .copied()
        .ok_or(CodecError::BadTag {
            tag: state_tag,
            at: state_at,
        })?;
    job.preemptions = c.u8()?;
    Ok(job)
}
