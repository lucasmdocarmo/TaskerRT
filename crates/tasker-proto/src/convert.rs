//! Wire ↔ core conversions. Every `UNSPECIFIED` enum value is a conversion
//! error, never a silent default.

use tasker_core::{
    AccountId, Job, JobId, JobState, PriorityClass, ResourceRequest, Resources, VirtualDuration,
    VirtualTime,
};

use crate::v1;

/// A wire value that has no core meaning.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
pub enum ConvertError {
    #[error("priority class is unspecified")]
    UnspecifiedPriorityClass,
    #[error("resource request is missing")]
    MissingResources,
    #[error("gpus {0} exceeds u8")]
    GpusOutOfRange(u32),
    #[error("walltime must be positive")]
    ZeroWalltime,
}

/// Core → wire. Total: every class has a wire value.
#[must_use]
pub fn priority_class_to_proto(class: PriorityClass) -> v1::PriorityClass {
    match class {
        PriorityClass::Low => v1::PriorityClass::Low,
        PriorityClass::Normal => v1::PriorityClass::Normal,
        PriorityClass::High => v1::PriorityClass::High,
        PriorityClass::Urgent => v1::PriorityClass::Urgent,
    }
}

/// Wire → core. `Unspecified` is an error.
///
/// # Errors
/// `UnspecifiedPriorityClass`.
pub fn priority_class_from_proto(class: v1::PriorityClass) -> Result<PriorityClass, ConvertError> {
    match class {
        v1::PriorityClass::Low => Ok(PriorityClass::Low),
        v1::PriorityClass::Normal => Ok(PriorityClass::Normal),
        v1::PriorityClass::High => Ok(PriorityClass::High),
        v1::PriorityClass::Urgent => Ok(PriorityClass::Urgent),
        v1::PriorityClass::Unspecified => Err(ConvertError::UnspecifiedPriorityClass),
    }
}

/// Core → wire. Total.
#[must_use]
pub fn job_state_to_proto(state: JobState) -> v1::JobState {
    match state {
        JobState::Submitted => v1::JobState::Submitted,
        JobState::Blocked => v1::JobState::Blocked,
        JobState::Ready => v1::JobState::Ready,
        JobState::Running => v1::JobState::Running,
        JobState::Completed => v1::JobState::Completed,
        JobState::Failed => v1::JobState::Failed,
        JobState::Preempted => v1::JobState::Preempted,
        JobState::Cancelled => v1::JobState::Cancelled,
    }
}

/// Core → wire.
#[must_use]
pub fn resources_to_proto(r: Resources) -> v1::Resources {
    v1::Resources {
        cpu_millis: r.cpu_millis,
        mem_bytes: r.mem_bytes,
        gpus: u32::from(r.gpus),
    }
}

/// Wire → core. `gpus` must fit a `u8`.
///
/// # Errors
/// `GpusOutOfRange`.
pub fn resources_from_proto(r: &v1::Resources) -> Result<Resources, ConvertError> {
    let gpus = u8::try_from(r.gpus).map_err(|_| ConvertError::GpusOutOfRange(r.gpus))?;
    Ok(Resources::new(r.cpu_millis, r.mem_bytes, gpus))
}

/// Builds a `Submitted` job from a request. `deps` are raw `JobId` bits; the
/// scheduler validates them at submit.
///
/// # Errors
/// Any `ConvertError`.
pub fn job_from_submit(req: &v1::SubmitRequest, now: VirtualTime) -> Result<Job, ConvertError> {
    // `try_from(i32)` rejects unknown enum numbers; `.unwrap_or` maps them to Unspecified.
    let class =
        v1::PriorityClass::try_from(req.priority_class).unwrap_or(v1::PriorityClass::Unspecified);
    let class = priority_class_from_proto(class)?;
    let request: ResourceRequest =
        resources_from_proto(req.request.as_ref().ok_or(ConvertError::MissingResources)?)?;
    if req.walltime_nanos == 0 {
        return Err(ConvertError::ZeroWalltime);
    }
    let mut job = Job::new(
        AccountId::new(req.account),
        class,
        now,
        request,
        VirtualDuration::from_nanos(req.walltime_nanos),
    );
    job.deps
        .extend(req.deps.iter().map(|bits| JobId::from_bits(*bits)));
    // `Bytes::clone` is a refcount bump, not a copy.
    job.payload = req.payload.clone();
    Ok(job)
}
