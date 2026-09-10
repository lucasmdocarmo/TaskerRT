use tasker_core::{
    AccountId, DECAY_ONE, DECAY_STEPS, FACTOR_SCALE, FairShare, FairShareConfig, MAX_ACCOUNTS,
    USAGE_PER_CORE_SECOND, VirtualDuration, VirtualTime, decay_factor,
};

const SECOND: u64 = 1_000_000_000;

fn at(secs: u64) -> VirtualTime {
    VirtualTime::from_nanos(secs * SECOND)
}

fn hour() -> FairShareConfig {
    FairShareConfig::new(VirtualDuration::from_secs(3_600))
}

#[test]
fn decay_table_endpoints_are_exact() {
    assert_eq!(decay_factor(0), DECAY_ONE);
    // Whole halvings are shifts, so they are exact.
    assert_eq!(decay_factor(DECAY_STEPS), DECAY_ONE / 2);
    assert_eq!(decay_factor(2 * DECAY_STEPS), DECAY_ONE / 4);
    assert_eq!(decay_factor(64 * DECAY_STEPS), 0);
}

#[test]
fn half_a_halving_squares_to_one_half() {
    let half_step = decay_factor(DECAY_STEPS / 2);
    // (2^-0.5)^2 = 0.5 up to Q48 truncation in the table build.
    let squared = (u128::from(half_step) * u128::from(half_step)) >> 48;
    let target = u128::from(DECAY_ONE / 2);
    assert!(
        squared.abs_diff(target) < 1 << 16,
        "squared={squared} target={target}"
    );
}

#[test]
fn one_half_life_halves_usage() {
    let cfg = hour();
    let mut fs = FairShare::new();
    let a = AccountId::new(0);
    fs.ensure(a).unwrap();
    // One core for one hour: 3 600 core-seconds, charged at release.
    fs.on_dispatch(a, 1_000, at(0), &cfg);
    fs.on_release(a, 1_000, at(3_600), &cfg);
    let before = fs.ledger(a).unwrap().usage;
    assert_eq!(before, 3_600 * USAGE_PER_CORE_SECOND);
    // Exactly one half-life later the shift path halves it exactly.
    fs.refresh(at(7_200), &cfg);
    assert_eq!(fs.ledger(a).unwrap().usage, before / 2);
}

#[test]
fn many_small_touches_decay_like_one_big_one() {
    let cfg = hour();
    let a = AccountId::new(0);
    let mut fine = FairShare::new();
    let mut coarse = FairShare::new();
    for fs in [&mut fine, &mut coarse] {
        fs.ensure(a).unwrap();
        fs.on_dispatch(a, 1_000, at(0), &cfg);
        fs.on_release(a, 1_000, at(3_600), &cfg);
    }
    // 3 600 one-second touches versus one touch after an hour. The carry makes
    // both consume the same 1 024 quanta; only per-step truncation differs.
    for s in 3_601..=7_200 {
        fine.refresh(at(s), &cfg);
    }
    coarse.refresh(at(7_200), &cfg);
    let (f, c) = (
        fine.ledger(a).unwrap().usage,
        coarse.ledger(a).unwrap().usage,
    );
    assert!(f <= c, "fine={f} coarse={c}");
    assert!(c - f <= 2_048, "fine={f} coarse={c}");
}

#[test]
fn accrual_charges_running_cpu_per_millisecond() {
    let cfg = hour();
    let mut fs = FairShare::new();
    let a = AccountId::new(2);
    fs.ensure(a).unwrap();
    fs.on_dispatch(a, 2_000, at(10), &cfg);
    fs.on_dispatch(a, 1_000, at(10), &cfg);
    assert_eq!(fs.ledger(a).unwrap().running_cpu, 3_000);
    // Ten seconds at three cores.
    fs.refresh(at(20), &cfg);
    assert_eq!(fs.ledger(a).unwrap().usage, 30 * USAGE_PER_CORE_SECOND);
    fs.on_release(a, 2_000, at(20), &cfg);
    assert_eq!(fs.ledger(a).unwrap().running_cpu, 1_000);
}

#[test]
fn factor_is_max_with_no_usage_and_half_at_exactly_one_share() {
    let cfg = hour();
    let mut fs = FairShare::new();
    let (a, b) = (AccountId::new(0), AccountId::new(1));
    // Ensuring id 1 creates ids 0 and 1.
    fs.ensure(b).unwrap();
    fs.refresh(at(0), &cfg);
    assert_eq!(fs.factor(a), FACTOR_SCALE);
    assert_eq!(fs.factor(b), FACTOR_SCALE);
    for acct in [a, b] {
        fs.on_dispatch(acct, 1_000, at(0), &cfg);
    }
    // Equal usage, equal shares: U/S = 1, so 2^-1 exactly.
    fs.refresh(at(10), &cfg);
    assert_eq!(fs.factor(a), FACTOR_SCALE / 2);
    assert_eq!(fs.factor(b), FACTOR_SCALE / 2);
}

#[test]
fn factor_quarters_when_usage_is_twice_the_share() {
    let cfg = hour();
    let mut fs = FairShare::new();
    let (a, b) = (AccountId::new(0), AccountId::new(1));
    fs.set_shares(a, 1).unwrap();
    fs.set_shares(b, 3).unwrap();
    for acct in [a, b] {
        fs.on_dispatch(acct, 1_000, at(0), &cfg);
    }
    fs.refresh(at(10), &cfg);
    // a: half the usage on a quarter of the shares → U/S = 2 → 2^-2 exactly.
    assert_eq!(fs.factor(a), FACTOR_SCALE / 4);
    // b: U/S = 2/3 → 2^(-2/3) ≈ 0.63, quantized to 1/1024ths of the exponent.
    let fb = fs.factor(b);
    assert!((629_000..631_000).contains(&fb), "{fb}");
}

#[test]
fn zero_shares_means_zero_factor_and_ids_past_the_cap_are_rejected() {
    let cfg = hour();
    let mut fs = FairShare::new();
    let (a, b) = (AccountId::new(0), AccountId::new(1));
    fs.set_shares(a, 0).unwrap();
    fs.ensure(b).unwrap();
    fs.on_dispatch(b, 1_000, at(0), &cfg);
    fs.refresh(at(1), &cfg);
    assert_eq!(fs.factor(a), 0);
    assert!(fs.ensure(AccountId::new(MAX_ACCOUNTS)).is_err());
    assert_eq!(fs.len(), 2);
}

#[test]
fn zero_half_life_forgets_everything_but_running_work() {
    let cfg = FairShareConfig::new(VirtualDuration::ZERO);
    let mut fs = FairShare::new();
    let a = AccountId::new(0);
    fs.ensure(a).unwrap();
    fs.on_dispatch(a, 1_000, at(0), &cfg);
    fs.on_release(a, 1_000, at(10), &cfg);
    assert_eq!(fs.ledger(a).unwrap().usage, 10 * USAGE_PER_CORE_SECOND);
    // A 1 ns quantum: one second is a billion steps, far past 64 halvings.
    fs.refresh(at(11), &cfg);
    assert_eq!(fs.ledger(a).unwrap().usage, 0);
}
