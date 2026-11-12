//! Durable mail delivery and supervised ACP and Infinity agent sessions.
#![deny(clippy::unwrap_used)]

#[cfg(test)]
mod allocator;

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
mod seccomp;
#[cfg(unix)]
pub mod verify;

mod credentials;
mod private_fs;

pub mod event_hub;
