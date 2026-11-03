use super::*;
pub(super) fn discover(model: &Value, _: &Value) -> Discovery {
    Discovery {
        context: positive(&model["max_model_len"]),
        slots: None,
    }
}
pub(super) fn classify(e: &EngineError) -> bool {
    e.message
        .contains("is longer than the maximum model length")
        || e.message.contains("maximum context length is")
}
pub(super) fn reasoning_fields() -> &'static [&'static str] {
    &["reasoning_content", "reasoning"]
}
pub(super) fn success_finishes() -> &'static [&'static str] {
    &["stop", "tool_calls"]
}

pub(super) fn default_reply_cap() -> Option<u64> {
    None
}
