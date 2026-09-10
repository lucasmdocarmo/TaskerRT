//! Properties: decay only ever lowers usage and keeps accounts in order; more
//! usage never raises an account's factor; extreme inputs never panic.

use proptest::prelude::*;
use tasker_core::{
    AccountId, DECAY_ONE, FACTOR_SCALE, FairShare, FairShareConfig, VirtualDuration, VirtualTime,
    decay_factor,
};

const MILLI: u64 = 1_000_000;

fn ms(n: u64) -> VirtualTime {
    VirtualTime::from_nanos(n.saturating_mul(MILLI))
}

/// Two accounts, both dispatched at zero and released at `hold_ms`, with the
/// given cores each, so both ledgers see identical touch times.
fn pair(cpu0: u32, cpu1: u32, hold_ms: u64, shares0: u32, cfg: FairShareConfig) -> FairShare {
    let mut fs = FairShare::new();
    fs.set_shares(AccountId::new(0), shares0).unwrap();
    fs.ensure(AccountId::new(1)).unwrap();
    for (i, cpu) in [(0_u32, cpu0), (1, cpu1)] {
        fs.on_dispatch(AccountId::new(i), cpu, VirtualTime::ZERO, &cfg);
    }
    for (i, cpu) in [(0_u32, cpu0), (1, cpu1)] {
        fs.on_release(AccountId::new(i), cpu, ms(hold_ms), &cfg);
    }
    fs.refresh(ms(hold_ms), &cfg);
    fs
}

proptest! {
    #[test]
    fn decay_factor_is_monotone_and_bounded(a in any::<u64>(), b in any::<u64>()) {
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        prop_assert!(decay_factor(lo) >= decay_factor(hi));
        prop_assert!(decay_factor(lo) <= DECAY_ONE);
    }

    #[test]
    fn more_usage_never_raises_the_factor(
        cpu in 0_u32..4_000, extra in 0_u32..4_000, other in 0_u32..4_000,
        hold_ms in 1_u64..1 << 32, shares in 1_u32..64,
    ) {
        // A very long half-life: usage is cpu × hold with no decay in the way.
        let cfg = FairShareConfig::new(VirtualDuration::from_nanos(u64::MAX));
        let less = pair(cpu, other, hold_ms, shares, cfg).factor(AccountId::new(0));
        let more = pair(cpu.saturating_add(extra), other, hold_ms, shares, cfg)
            .factor(AccountId::new(0));
        prop_assert!(more <= less, "more={more} less={less}");
        prop_assert!(less <= FACTOR_SCALE);
    }

    #[test]
    fn decay_never_increases_usage_and_preserves_order(
        cpu0 in 0_u32..4_000, cpu1 in 0_u32..4_000,
        hold_ms in 1_u64..1 << 30, elapsed_ms in 0_u64..1 << 34,
    ) {
        let cfg = FairShareConfig::new(VirtualDuration::from_secs(3_600));
        let mut fs = pair(cpu0, cpu1, hold_ms, 1, cfg);
        let before = (
            fs.ledger(AccountId::new(0)).unwrap().usage,
            fs.ledger(AccountId::new(1)).unwrap().usage,
        );
        fs.refresh(ms(hold_ms.saturating_add(elapsed_ms)), &cfg);
        let after = (
            fs.ledger(AccountId::new(0)).unwrap().usage,
            fs.ledger(AccountId::new(1)).unwrap().usage,
        );
        prop_assert!(after.0 <= before.0 && after.1 <= before.1);
        // Both ledgers decay by the same steps; a monotone map keeps their order.
        if before.0 <= before.1 {
            prop_assert!(after.0 <= after.1, "before={before:?} after={after:?}");
        }
    }

    #[test]
    fn extreme_times_never_panic(now in any::<u64>(), half_life in any::<u64>()) {
        let cfg = FairShareConfig::new(VirtualDuration::from_nanos(half_life));
        let mut fs = FairShare::new();
        fs.ensure(AccountId::new(0)).unwrap();
        fs.on_dispatch(AccountId::new(0), u32::MAX, VirtualTime::ZERO, &cfg);
        fs.refresh(VirtualTime::from_nanos(now), &cfg);
        fs.on_release(AccountId::new(0), u32::MAX, VirtualTime::from_nanos(now), &cfg);
        prop_assert!(fs.factor(AccountId::new(0)) <= FACTOR_SCALE);
    }
}
