//! Durable mail delivery and supervised ACP and Infinity agent sessions.
#![deny(clippy::unwrap_used)]

pub mod mailroom;
pub mod model;
pub mod swarm;

mod adapter;
mod adapters;
pub mod supervisor;
pub use adapter::{KindConfig, SandboxArgs, Wire};

#[cfg(unix)]
pub mod surfaces;

pub mod egress;
pub mod sandbox;
