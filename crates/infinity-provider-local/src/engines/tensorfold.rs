use super::*;
use regex::Regex;
use std::sync::LazyLock;
pub(super) fn discover(model: &Value, health: &Value) -> Discovery {
    Discovery {
        context: positive(&model["context_length"]),
        slots: positive(&health["max_batch_size"]).and_then(|n| usize::try_from(n).ok()),
    }
}
pub(super) fn classify(e: &EngineError) -> bool {
    e.code.as_deref() == Some("context_length_exceeded")
        || [
            "leaves no room for a reply",
            "exceeding the server's",
            "safe cache capacity",
            "this server's context window is",
        ]
        .iter()
        .any(|s| e.message.contains(s))
}
pub(super) fn reasoning_fields() -> &'static [&'static str] {
    &["reasoning_content"]
}
pub(super) fn success_finishes() -> &'static [&'static str] {
    &["stop", "tool_calls"]
}

pub(crate) fn context_limit(message: &str) -> Option<u64> {
    static PATTERNS: LazyLock<Vec<Regex>> = LazyLock::new(|| {
        [
            r"the server's (\d[\d,]*)-token (?:safe cache capacity|context window)",
            r"exceeding the server's (\d[\d,]*)-token",
            r"maximum context length is (\d[\d,]*) tokens",
            r"this server's context window is (\d[\d,]*)",
        ]
        .into_iter()
        .map(|s| Regex::new(s).expect("bug: static context regex"))
        .collect()
    });
    PATTERNS.iter().find_map(|p| {
        p.captures(message)
            .and_then(|c| c[1].replace(',', "").parse().ok())
    })
}
