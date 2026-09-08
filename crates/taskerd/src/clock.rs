//! The one wall-clock read in the workspace. Everything else receives time.

use std::sync::OnceLock;
use std::time::Instant;

use tasker_core::VirtualTime;

static START: OnceLock<Instant> = OnceLock::new();

/// Nanoseconds since the daemon first asked, as core time. Monotonic.
#[must_use]
// The single permitted `Instant::now`: the ban in clippy.toml exists so this
// is the only place time originates.
#[allow(clippy::disallowed_methods)]
pub fn now() -> VirtualTime {
    // `get_or_init` runs `Instant::now` exactly once, on first call.
    let start = START.get_or_init(Instant::now);
    let nanos = u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    VirtualTime::from_nanos(nanos)
}
