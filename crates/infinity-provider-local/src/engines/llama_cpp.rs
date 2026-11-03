use super::*;
pub(super) fn discover(_: &Value, props: &Value) -> Discovery {
    Discovery {
        context: positive(&props["default_generation_settings"]["n_ctx"]),
        slots: positive(&props["total_slots"]).and_then(|n| usize::try_from(n).ok()),
    }
}
pub(super) fn classify(e: &EngineError) -> bool {
    e.kind.as_deref() == Some("exceed_context_size_error")
}
pub(super) fn reasoning_fields() -> &'static [&'static str] {
    &["reasoning_content"]
}
pub(super) fn success_finishes() -> &'static [&'static str] {
    &["stop", "tool_calls"]
}

pub(super) fn default_reply_cap() -> Option<u64> {
    None
}
