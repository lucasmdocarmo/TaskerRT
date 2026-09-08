//! Wire types for TaskerRT. `v1` is generated from `proto/`; `convert` maps
//! between wire and core types.

/// Generated code. Exempt from the workspace lint policy: it is not ours to style.
#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    unreachable_pub,
    missing_debug_implementations,
    rust_2018_idioms
)]
pub mod v1 {
    tonic::include_proto!("tasker.v1");
}

/// KEDA's external scaler protocol, vendored from kedacore/keda.
#[allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    unreachable_pub,
    missing_debug_implementations,
    rust_2018_idioms
)]
pub mod externalscaler {
    tonic::include_proto!("externalscaler");
}

pub mod convert;

pub use convert::ConvertError;
