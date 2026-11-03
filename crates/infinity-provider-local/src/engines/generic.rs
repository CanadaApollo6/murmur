use super::*;
pub(super) fn discover(_: &Value, _: &Value) -> Discovery {
    Discovery::default()
}
pub(super) fn classify(_: &EngineError) -> bool {
    false
}
pub(super) fn reasoning_fields() -> &'static [&'static str] {
    &["reasoning_content"]
}
pub(super) fn success_finishes() -> &'static [&'static str] {
    &["stop", "tool_calls"]
}
