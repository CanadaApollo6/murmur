use super::*;
pub(super) fn discover(model: &Value, info: &Value) -> Discovery {
    Discovery {
        context: positive(&model["max_model_len"]),
        slots: positive(&info["max_running_requests"]).and_then(|n| usize::try_from(n).ok()),
    }
}
pub(super) fn classify(e: &EngineError) -> bool {
    e.message
        .contains("is longer than the model's context length")
        || e.message
            .contains("exceeds the model's maximum context length")
}
pub(super) fn reasoning_fields() -> &'static [&'static str] {
    &["reasoning_content"]
}
pub(super) fn success_finishes() -> &'static [&'static str] {
    &["stop", "tool_calls"]
}
// An omitted output cap is assumed to clamp to remaining room until the SGLang live probe runs.
