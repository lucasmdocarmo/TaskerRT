//! Deterministic simulation of the TaskerRT scheduling core.
//!
//! A `Simulation` owns a virtual clock, an event queue, and a scheduler, and
//! drives them tick by tick. Every run of the same seed produces the same
//! trace, which makes any failure exactly reproducible.

pub mod clock;
pub mod event;
pub mod invariants;
pub mod outcome;
pub mod rng;
pub mod scenario;
pub mod sim;
pub mod trace;

pub use clock::VirtualClock;
pub use event::{Event, EventQueue};
pub use invariants::Violation;
pub use outcome::Outcome;
pub use rng::SplitMix64;
pub use scenario::{ScenarioParams, Shape, generate};
pub use sim::{SimError, Simulation, StepReport};
pub use trace::{Action, Trace, TraceRecord};
