//! Durable mail delivery and supervised ACP agent sessions.
#![deny(clippy::unwrap_used)]

pub mod mailroom;
pub mod model;
pub mod swarm;

mod adapter;
mod adapters;
pub mod supervisor;
pub use adapter::{KindConfig, SandboxArgs};
