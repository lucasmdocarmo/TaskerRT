//! The one wall-clock read in the workspace. Everything else receives time.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tasker_core::VirtualTime;

static START: OnceLock<Instant> = OnceLock::new();
/// Added to every reading, so time continues past the last logged instant after a restart.
static ORIGIN: AtomicU64 = AtomicU64::new(0);

/// Continues virtual time from `origin`. Call before the engine thread starts;
/// the origin only ever rises, so the clock stays monotonic.
pub fn set_origin(origin: VirtualTime) {
    // Relaxed: the engine thread is spawned after this store, and spawning synchronizes.
    ORIGIN.fetch_max(origin.as_nanos(), Ordering::Relaxed);
}

/// Origin plus nanoseconds since the daemon first asked. Monotonic.
#[must_use]
// The single permitted `Instant::now`: the ban in clippy.toml exists so this
// is the only place time originates.
#[allow(clippy::disallowed_methods)]
pub fn now() -> VirtualTime {
    // `get_or_init` runs `Instant::now` exactly once, on first call.
    let start = START.get_or_init(Instant::now);
    let elapsed = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    VirtualTime::from_nanos(ORIGIN.load(Ordering::Relaxed).saturating_add(elapsed))
}
