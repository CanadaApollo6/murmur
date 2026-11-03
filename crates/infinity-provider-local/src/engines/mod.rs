mod generic;
mod llama_cpp;
mod sglang;
mod tensorfold;
pub(crate) use tensorfold::context_limit as tensorfold_context_limit;
mod vllm;

use crate::errors::EngineError;
use serde::Deserialize;
use serde_json::Value;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Engine {
    #[serde(rename = "llama.cpp", alias = "llama-cpp", alias = "llamacpp")]
    LlamaCpp,
    Vllm,
    Sglang,
    TensorFold,
    Generic,
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Discovery {
    pub context: Option<u64>,
    pub slots: Option<usize>,
}
fn positive(value: &Value) -> Option<u64> {
    value.as_u64().filter(|v| *v > 0)
}
impl Engine {
    pub(crate) fn discover(self, model: &Value, auxiliary: &Value) -> Discovery {
        match self {
            Self::LlamaCpp => llama_cpp::discover(model, auxiliary),
            Self::Vllm => vllm::discover(model, auxiliary),
            Self::Sglang => sglang::discover(model, auxiliary),
            Self::TensorFold => tensorfold::discover(model, auxiliary),
            Self::Generic => generic::discover(model, auxiliary),
        }
    }
    pub(crate) fn overflow(self, error: &EngineError) -> bool {
        match self {
            Self::LlamaCpp => llama_cpp::classify(error),
            Self::Vllm => vllm::classify(error),
            Self::Sglang => sglang::classify(error),
            Self::TensorFold => tensorfold::classify(error),
            Self::Generic => generic::classify(error),
        }
    }
    pub(crate) fn reasoning_fields(self) -> &'static [&'static str] {
        match self {
            Self::LlamaCpp => llama_cpp::reasoning_fields(),
            Self::Vllm => vllm::reasoning_fields(),
            Self::Sglang => sglang::reasoning_fields(),
            Self::TensorFold => tensorfold::reasoning_fields(),
            Self::Generic => generic::reasoning_fields(),
        }
    }
    pub(crate) fn success_finishes(self) -> &'static [&'static str] {
        match self {
            Self::LlamaCpp => llama_cpp::success_finishes(),
            Self::Vllm => vllm::success_finishes(),
            Self::Sglang => sglang::success_finishes(),
            Self::TensorFold => tensorfold::success_finishes(),
            Self::Generic => generic::success_finishes(),
        }
    }
    pub(crate) fn default_reply_cap(self) -> Option<u64> {
        match self {
            Self::TensorFold => Some(4096),
            _ => None,
        }
    }
}
