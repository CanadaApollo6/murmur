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

pub(super) fn default_reply_cap() -> Option<u64> {
    Some(4096)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn python_limit_fixtures_with_decoys_and_commas() {
        for n in ["65,536", "262,144"] {
            // TensorFold Python 0.3.6.3 cuda/server.py:240-243 and restart hint :149-151.
            let uncapped = format!(
                "the rendered prompt has 300000 tokens and leaves no room for a reply in the server's {n}-token safe cache capacity (model window: 999999 tokens); shorten the prompt, or restart with --context 300001 or more (this memory admits up to 888888)"
            );
            // cuda/server.py:251-253, with the same restart hint.
            let capped = format!(
                "the rendered prompt has 60000 tokens and requests 16384 reply tokens, exceeding the server's {n}-token context window; reduce the prompt or reply length, or restart with --context 76384 or more (this memory admits up to 888888)"
            );
            // server/app.py:365-367, memory-fitted context formatted with commas.
            let app_uncapped = format!(
                "This server's maximum context length is {n} tokens, the most this server's memory budget fits, but the rendered prompt has 300,000 tokens and leaves no room for a reply. Compact or shorten the conversation."
            );
            // server/app.py:370-373.
            let app_capped = format!(
                "the rendered prompt has 60000 tokens and requests 16384 reply tokens; this server's context window is {n}. Reduce the prompt to at most 49152 prompt tokens or request at most 5536 reply tokens, including chat template and thinking tokens."
            );
            for message in [uncapped, capped, app_uncapped, app_capped] {
                let limit = context_limit(&message).expect("parse anchored context");
                assert_eq!(limit, n.replace(',', "").parse::<u64>().expect("number"));
                let error = EngineError::normalize(
                    Some(400),
                    &serde_json::json!({"error":{"message":message}}).to_string(),
                );
                assert!(classify(&error));
                assert_eq!(limit < 131072, n == "65,536");
            }
        }
        assert_eq!(
            context_limit("model window: 999999 --context 300001 admits up to 888888"),
            None
        );
    }
}
