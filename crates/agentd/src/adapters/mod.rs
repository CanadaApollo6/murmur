use std::sync::Arc;

use crate::adapter::{Adapter, KindConfig, Wire};

pub(crate) mod acp;
pub(crate) mod infinity;

pub(crate) struct AdapterSet;
impl AdapterSet {
    pub(crate) fn build(kind: KindConfig) -> Arc<dyn Adapter> {
        match kind.wire {
            Wire::Acp => Arc::new(acp::AcpAdapter(kind)),
            Wire::Infinity { .. } => Arc::new(infinity::InfinityAdapter(kind)),
        }
    }
}
