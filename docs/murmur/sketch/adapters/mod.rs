//! One module per `adapter::Wire` variant. Nothing outside `adapter::AdapterSet::build`
//! names these types; the rest of the broker sees `Arc<dyn Adapter>` only. (One
//! exception: `surfaces::bindings` asks `InfinityAdapter::agent_for_root`, because only
//! this adapter can read Infinity resume tokens.)
//!
//! Each adapter keeps its `ResumeToken` shape private (`InfinityResume`, `AcpResume`,
//! `MspResume`) and is the only caller of `ResumeToken::{encode, decode}`.

pub mod acp;
pub mod infinity;
pub mod msp;
