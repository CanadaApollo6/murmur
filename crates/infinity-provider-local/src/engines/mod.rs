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
            Self::LlamaCpp => llama_cpp::default_reply_cap(),
            Self::Vllm => vllm::default_reply_cap(),
            Self::Sglang => sglang::default_reply_cap(),
            Self::TensorFold => tensorfold::default_reply_cap(),
            Self::Generic => generic::default_reply_cap(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn discovery_shapes_and_missing_values() {
        // llama.cpp a0e6dc7 tools/server/server-context.cpp:5133,5141.
        let llama = Engine::LlamaCpp.discover(
            &Value::Null,
            &json!({"default_generation_settings":{"n_ctx":32768},"total_slots":2}),
        );
        assert_eq!(llama.context, Some(32768));
        assert_eq!(llama.slots, Some(2));
        // vLLM v0.30.0 entrypoints/openai/models/serving.py and SGLang v0.5.23 http_server.py:2003-2006.
        assert_eq!(
            Engine::Vllm
                .discover(&json!({"max_model_len":8192}), &Value::Null)
                .context,
            Some(8192)
        );
        let sg = Engine::Sglang.discover(
            &json!({"max_model_len":8192}),
            &json!({"max_running_requests":null}),
        );
        assert_eq!(sg.slots, None);
        // TensorFold Python server/app.py:health and server/http.py:104-118, native status_routes.zig:35,89.
        let tf = Engine::TensorFold.discover(
            &json!({"id":"weights"}),
            &json!({"status":"ok","max_batch_size":4}),
        );
        assert_eq!(tf.context, None);
        assert_eq!(tf.slots, Some(4));
        let native =
            Engine::TensorFold.discover(&json!({"context_length":16384}), &json!({"status":"ok"}));
        assert_eq!(native.context, Some(16384));
        assert_eq!(native.slots, None);
        for e in [
            Engine::LlamaCpp,
            Engine::Vllm,
            Engine::Sglang,
            Engine::TensorFold,
            Engine::Generic,
        ] {
            let d=e.discover(&json!({"max_model_len":0,"context_length":0}),&json!({"total_slots":0,"max_batch_size":null,"max_running_requests":0,"default_generation_settings":{"n_ctx":0}}));
            assert_eq!(d.context, None);
            assert_eq!(d.slots, None);
        }
    }
}
