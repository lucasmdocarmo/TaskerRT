use tasker_core::Resources;

#[test]
fn fits_within_requires_every_dimension() {
    let capacity = Resources::new(4_000, 8 << 30, 2);
    assert!(Resources::new(4_000, 8 << 30, 2).fits_within(&capacity));
    assert!(
        !Resources::new(4_001, 0, 0).fits_within(&capacity),
        "cpu over"
    );
    assert!(
        !Resources::new(0, (8 << 30) + 1, 0).fits_within(&capacity),
        "mem over"
    );
    assert!(!Resources::new(0, 0, 3).fits_within(&capacity), "gpu over");
}

#[test]
fn zero_fits_within_anything() {
    assert!(Resources::ZERO.fits_within(&Resources::ZERO));
    assert!(Resources::ZERO.fits_within(&Resources::new(1, 1, 1)));
}

#[test]
fn saturating_sub_floors_at_zero() {
    let got = Resources::new(100, 100, 1).saturating_sub(&Resources::new(500, 500, 5));
    assert_eq!(got, Resources::ZERO);
}

#[test]
fn checked_add_detects_overflow() {
    let big = Resources::new(u32::MAX, u64::MAX, u8::MAX);
    assert_eq!(big.checked_add(&Resources::new(1, 0, 0)), None);
    assert_eq!(big.checked_add(&Resources::ZERO), Some(big));
}

#[test]
fn add_then_sub_roundtrips() {
    let base = Resources::new(1_000, 1 << 30, 1);
    let delta = Resources::new(250, 1 << 28, 1);
    assert_eq!(base.saturating_add(&delta).saturating_sub(&delta), base);
}
