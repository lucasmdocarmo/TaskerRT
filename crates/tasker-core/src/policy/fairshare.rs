//! Fair-share: each account's decayed usage and the priority factor it earns.
//! Integer-only arithmetic, so every machine computes the same bits.

use crate::domain::{AccountId, VirtualDuration, VirtualTime};
use crate::policy::priority::FACTOR_SCALE;

/// Usage unit: `cpu_millis × milliseconds held`. One core-second is 1 000 000.
pub type Usage = u64;

/// `Usage` units in one core-second.
pub const USAGE_PER_CORE_SECOND: u64 = 1_000_000;

/// Account ids are dense indices below this bound.
pub const MAX_ACCOUNTS: u32 = 1_024;

/// Decay resolution: one halving is this many table steps.
pub const DECAY_STEPS: u64 = 1_024;

/// Q48 fixed point: this value is 1.0. Multiply two values, then shift right 48.
pub const DECAY_ONE: u64 = 1 << 48;

const TABLE_LEN: usize = 1_024;
const NANOS_PER_MILLI: u64 = 1_000_000;

// A compile-time check that the two spellings of the table size agree.
const _: () = assert!(TABLE_LEN as u64 == DECAY_STEPS);

/// `2^(-k / DECAY_STEPS)` in Q48 for every `k` below `DECAY_STEPS`.
static DECAY_TABLE: [u64; TABLE_LEN] = build_decay_table();

/// Q48 product. `u128` keeps the 64-bit product exact before the shift.
#[allow(clippy::cast_possible_truncation)] // inputs ≤ 2^64 and ≤ 2^48: the shifted product fits
const fn mul_q48(a: u64, b: u64) -> u64 {
    ((a as u128 * b as u128) >> 48) as u64
}

/// `base^exp` in Q48 by squaring: at most 2·log2(exp) multiplies.
const fn pow_q48(mut base: u64, mut exp: u64) -> u64 {
    let mut acc = DECAY_ONE;
    while exp > 0 {
        // An odd exponent folds the current base into the result.
        if exp & 1 == 1 {
            acc = mul_q48(acc, base);
        }
        base = mul_q48(base, base);
        exp >>= 1;
    }
    acc
}

/// Evaluated once at compile time: no floats, no runtime cost, identical everywhere.
const fn build_decay_table() -> [u64; TABLE_LEN] {
    // Binary search the largest r with r^DECAY_STEPS <= 1/2, i.e. 2^(-1/DECAY_STEPS).
    let mut lo = DECAY_ONE / 2;
    let mut hi = DECAY_ONE;
    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        if pow_q48(mid, DECAY_STEPS) <= DECAY_ONE / 2 {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    let root = lo;
    let mut table = [0_u64; TABLE_LEN];
    let mut k = 0_usize;
    // `while`, not `for`: iterators are not usable in `const fn`.
    while k < TABLE_LEN {
        table[k] = pow_q48(root, k as u64);
        k += 1;
    }
    table
}

/// `2^(-steps / DECAY_STEPS)` in Q48. Whole halvings are exact shifts.
#[must_use]
pub fn decay_factor(steps: u64) -> u64 {
    let halvings = steps / DECAY_STEPS;
    if halvings >= 48 {
        return 0;
    }
    #[allow(clippy::cast_possible_truncation)] // the remainder is below DECAY_STEPS
    let fine = DECAY_TABLE[(steps % DECAY_STEPS) as usize];
    fine >> halvings
}

/// Applies `steps` of decay to a usage value.
fn decay(usage: Usage, steps: u64) -> Usage {
    let halvings = steps / DECAY_STEPS;
    if halvings >= 64 {
        return 0;
    }
    #[allow(clippy::cast_possible_truncation)] // the remainder is below DECAY_STEPS
    let fine = DECAY_TABLE[(steps % DECAY_STEPS) as usize];
    mul_q48(usage >> halvings, fine)
}

/// How fast usage is forgotten.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct FairShareConfig {
    /// Usage halves every `half_life`. Zero forgets everything but running work.
    pub half_life: VirtualDuration,
}

impl Default for FairShareConfig {
    fn default() -> Self {
        Self::new(VirtualDuration::from_secs(3_600))
    }
}

impl FairShareConfig {
    /// Wraps a half-life.
    #[must_use]
    pub const fn new(half_life: VirtualDuration) -> Self {
        Self { half_life }
    }

    /// Nanoseconds per decay step; never zero, so division is always safe.
    #[must_use]
    pub const fn quantum_nanos(&self) -> u64 {
        let q = self.half_life.as_nanos() / DECAY_STEPS;
        if q == 0 { 1 } else { q }
    }
}

/// An account id at or above `MAX_ACCOUNTS`.
#[derive(Clone, Copy, PartialEq, Eq, Debug, thiserror::Error)]
#[error("account {0:?} is not below MAX_ACCOUNTS")]
pub struct AccountError(pub AccountId);

/// One account's usage record.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Ledger {
    /// Decayed usage as of `decayed_to`, plus undecayed accrual since.
    pub usage: Usage,
    /// Σ `cpu_millis` over the account's `Running` jobs.
    pub running_cpu: u64,
    /// Relative entitlement; default 1.
    pub shares: u32,
    accrued_to: VirtualTime,
    decayed_to: VirtualTime,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            usage: 0,
            running_cpu: 0,
            shares: 1,
            accrued_to: VirtualTime::ZERO,
            decayed_to: VirtualTime::ZERO,
        }
    }
}

impl Ledger {
    /// Rebuilds a ledger from a snapshot.
    #[must_use]
    pub const fn from_parts(
        usage: Usage,
        running_cpu: u64,
        shares: u32,
        accrued_to: VirtualTime,
        decayed_to: VirtualTime,
    ) -> Self {
        Self {
            usage,
            running_cpu,
            shares,
            accrued_to,
            decayed_to,
        }
    }

    /// Charged up to here, in whole milliseconds.
    #[must_use]
    pub const fn accrued_to(&self) -> VirtualTime {
        self.accrued_to
    }

    /// Decayed up to here, in whole quanta.
    #[must_use]
    pub const fn decayed_to(&self) -> VirtualTime {
        self.decayed_to
    }

    /// Brings the ledger to `now`: decay first, then charge what ran since.
    fn touch(&mut self, now: VirtualTime, config: FairShareConfig) {
        // Whole quanta only; the remainder waits so tiny ticks never round decay away.
        let quantum = config.quantum_nanos();
        let steps = now.saturating_sub_time(self.decayed_to).as_nanos() / quantum;
        if steps > 0 {
            self.usage = decay(self.usage, steps);
            let consumed = VirtualDuration::from_nanos(steps.saturating_mul(quantum));
            self.decayed_to = self.decayed_to.saturating_add(consumed);
        }
        // Whole milliseconds, same carry trick. Fresh work is charged undecayed.
        let held_ms = now.saturating_sub_time(self.accrued_to).as_nanos() / NANOS_PER_MILLI;
        if held_ms > 0 {
            self.usage = self
                .usage
                .saturating_add(self.running_cpu.saturating_mul(held_ms));
            let consumed = VirtualDuration::from_nanos(held_ms * NANOS_PER_MILLI);
            self.accrued_to = self.accrued_to.saturating_add(consumed);
        }
    }
}

/// `FACTOR_SCALE × 2^(-(usage/usage_total) / (shares/shares_total))`.
fn factor_for(usage: Usage, shares: u32, usage_total: u128, shares_total: u64) -> u64 {
    if usage_total == 0 {
        return FACTOR_SCALE;
    }
    if shares == 0 {
        return 0;
    }
    // The exponent in 1/DECAY_STEPS units, so the decay table evaluates 2^-x.
    // u128: usage (2^64) × shares_total (2^42) × steps (2^10) stays inside.
    let num = u128::from(usage) * u128::from(shares_total) * u128::from(DECAY_STEPS);
    let den = usage_total * u128::from(shares);
    let x = u64::try_from(num / den).unwrap_or(u64::MAX);
    mul_q48(FACTOR_SCALE, decay_factor(x))
}

/// Every account's ledger and the factor it earned at the last refresh.
#[derive(Clone, Debug, Default)]
pub struct FairShare {
    ledgers: Vec<Ledger>,
    /// Parallel to `ledgers`; the hot read in rescoring touches only this.
    factors: Vec<u64>,
    shares_total: u64,
}

impl FairShare {
    /// No accounts yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn index(account: AccountId) -> usize {
        // u32 → usize is lossless on every supported target.
        account.get() as usize
    }

    /// Creates ledgers up to and including `account`. The only growth path;
    /// callers invoke it at submit, never inside a cycle.
    ///
    /// # Errors
    /// `AccountError` when the id is not below `MAX_ACCOUNTS`.
    pub fn ensure(&mut self, account: AccountId) -> Result<(), AccountError> {
        if account.get() >= MAX_ACCOUNTS {
            return Err(AccountError(account));
        }
        let index = Self::index(account);
        if index >= self.ledgers.len() {
            let added = index + 1 - self.ledgers.len();
            self.ledgers.resize(index + 1, Ledger::default());
            self.factors.resize(index + 1, FACTOR_SCALE);
            // New ledgers carry the default share of 1 each.
            self.shares_total += added as u64;
        }
        Ok(())
    }

    /// Sets one account's shares, creating it if needed.
    ///
    /// # Errors
    /// `AccountError` when the id is not below `MAX_ACCOUNTS`.
    pub fn set_shares(&mut self, account: AccountId, shares: u32) -> Result<(), AccountError> {
        self.ensure(account)?;
        let ledger = &mut self.ledgers[Self::index(account)];
        self.shares_total = self.shares_total - u64::from(ledger.shares) + u64::from(shares);
        ledger.shares = shares;
        Ok(())
    }

    /// Replaces every ledger with a snapshot's. Factors recompute on the next refresh.
    ///
    /// # Errors
    /// `AccountError` when there are more ledgers than `MAX_ACCOUNTS`.
    pub fn restore(&mut self, ledgers: Vec<Ledger>) -> Result<(), AccountError> {
        if ledgers.len() > MAX_ACCOUNTS as usize {
            return Err(AccountError(AccountId::new(MAX_ACCOUNTS)));
        }
        self.shares_total = ledgers.iter().map(|l| u64::from(l.shares)).sum();
        self.factors.clear();
        self.factors.resize(ledgers.len(), FACTOR_SCALE);
        self.ledgers = ledgers;
        Ok(())
    }

    /// A job of `account` started holding `cpu_millis` at `now`.
    pub fn on_dispatch(
        &mut self,
        account: AccountId,
        cpu_millis: u32,
        now: VirtualTime,
        config: &FairShareConfig,
    ) {
        if let Some(ledger) = self.ledgers.get_mut(Self::index(account)) {
            ledger.touch(now, *config);
            ledger.running_cpu = ledger.running_cpu.saturating_add(u64::from(cpu_millis));
        }
    }

    /// A job of `account` stopped holding `cpu_millis` at `now`.
    pub fn on_release(
        &mut self,
        account: AccountId,
        cpu_millis: u32,
        now: VirtualTime,
        config: &FairShareConfig,
    ) {
        if let Some(ledger) = self.ledgers.get_mut(Self::index(account)) {
            ledger.touch(now, *config);
            ledger.running_cpu = ledger.running_cpu.saturating_sub(u64::from(cpu_millis));
        }
    }

    /// Touches every ledger and recomputes every factor. O(accounts), once per cycle.
    pub fn refresh(&mut self, now: VirtualTime, config: &FairShareConfig) {
        let mut total: u128 = 0;
        for ledger in &mut self.ledgers {
            ledger.touch(now, *config);
            total += u128::from(ledger.usage);
        }
        // `zip` walks both vectors in lockstep; they are always the same length.
        for (ledger, factor) in self.ledgers.iter().zip(&mut self.factors) {
            *factor = factor_for(ledger.usage, ledger.shares, total, self.shares_total);
        }
    }

    /// The account's factor in `0..=FACTOR_SCALE`; max for an unseen account.
    #[must_use]
    pub fn factor(&self, account: AccountId) -> u64 {
        self.factors
            .get(Self::index(account))
            .copied()
            .unwrap_or(FACTOR_SCALE)
    }

    /// One account's ledger, if it has been seen.
    #[must_use]
    pub fn ledger(&self, account: AccountId) -> Option<&Ledger> {
        self.ledgers.get(Self::index(account))
    }

    /// Every ledger, indexed by account id.
    #[must_use]
    pub fn ledgers(&self) -> &[Ledger] {
        &self.ledgers
    }

    /// Every factor, indexed by account id.
    #[must_use]
    pub fn factors(&self) -> &[u64] {
        &self.factors
    }

    /// Σ shares over all accounts.
    #[must_use]
    pub const fn shares_total(&self) -> u64 {
        self.shares_total
    }

    /// Accounts seen so far.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ledgers.len()
    }

    /// True before any account has been seen.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ledgers.is_empty()
    }
}
