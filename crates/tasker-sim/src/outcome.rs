//! What happens to a job once it is running.

use tasker_core::VirtualDuration;

/// Scripted fate of a dispatched job, relative to its dispatch time.
/// An `after` beyond the walltime limit is clamped and becomes a failure.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Outcome {
    Completes { after: VirtualDuration },
    Fails { after: VirtualDuration },
}
