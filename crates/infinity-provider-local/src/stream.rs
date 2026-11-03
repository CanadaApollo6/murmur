use crate::engines::Engine;
use futures_util::{Stream, StreamExt};
use infinity_provider_protocol::{
    CompletionError, ErrorClass, FinalResponse, Reasoning, StreamChunk, ToolCall, Usage,
};
use serde_json::Value;

pub(crate) fn events(
    response: reqwest::Response,
) -> impl Stream<Item = Result<String, CompletionError>> + Send {
    async_stream::try_stream! {
        let bytes = response.bytes_stream();
        tokio::pin!(bytes);
        let mut buffer = Vec::new();
        let mut data = Vec::new();
        while let Some(chunk) = bytes.next().await {
            buffer.extend_from_slice(&chunk.map_err(|e| CompletionError::provider(ErrorClass::Transient, format!("broken HTTP stream: {e}")))?);
            while let Some(end) = buffer.iter().position(|b| *b == b'\n') {
                let line: Vec<_> = buffer.drain(..=end).collect();
                let line = std::str::from_utf8(&line).map_err(|e| CompletionError::provider(ErrorClass::Transient, format!("invalid SSE UTF-8: {e}")))?.trim_end_matches(['\r', '\n']);
                if line.is_empty() {
                    if !data.is_empty() { yield data.join("\n"); data.clear(); }
                } else if let Some(value) = line.strip_prefix("data:") { data.push(value.strip_prefix(' ').unwrap_or(value).to_owned()); }
            }
        }
        if !buffer.is_empty() {
            let line = std::str::from_utf8(&buffer).map_err(|e| CompletionError::provider(ErrorClass::Transient, format!("invalid SSE UTF-8: {e}")))?.trim_end_matches('\r');
            if let Some(value) = line.strip_prefix("data:") { data.push(value.strip_prefix(' ').unwrap_or(value).to_owned()); }
        }
        if !data.is_empty() { yield data.join("\n"); }
    }
}

#[derive(Default)]
struct Call {
    index: u64,
    id: String,
    name: String,
    arguments: String,
}
#[derive(Default)]
pub(crate) struct Accumulator {
    reasoning: String,
    text: String,
    call: Option<Call>,
    finish: Option<String>,
    pub usage: Option<Usage>,
}
impl Accumulator {
    pub fn push(&mut self, engine: Engine, chunk: &Value) -> (bool, Vec<StreamChunk>) {
        if let Some(usage) = chunk.get("usage").filter(|v| v.is_object()) {
            let input_tokens = usage["prompt_tokens"].as_u64().unwrap_or(0);
            let output_tokens = usage["completion_tokens"].as_u64().unwrap_or(0);
            self.usage = Some(Usage {
                input_tokens,
                output_tokens,
                total_tokens: usage["total_tokens"]
                    .as_u64()
                    .unwrap_or_else(|| input_tokens.saturating_add(output_tokens)),
                cached_input_tokens: usage["prompt_tokens_details"]["cached_tokens"]
                    .as_u64()
                    .unwrap_or(0),
            });
        }
        let mut output = Vec::new();
        let mut generated = false;
        if let Some(choices) = chunk["choices"].as_array() {
            for choice in choices
                .iter()
                .filter(|c| c["index"].as_u64().unwrap_or(0) == 0)
            {
                let delta = &choice["delta"];
                if let Some(text) = delta["content"].as_str().filter(|s| !s.is_empty()) {
                    generated = true;
                    self.text.push_str(text);
                    output.push(StreamChunk::Text(text.to_owned()));
                }
                for field in engine.reasoning_fields() {
                    if let Some(text) = delta[*field].as_str().filter(|s| !s.is_empty()) {
                        generated = true;
                        self.reasoning.push_str(text);
                        output.push(StreamChunk::ReasoningDelta {
                            id: None,
                            text: text.to_owned(),
                        });
                    }
                }
                if let Some(calls) = delta["tool_calls"].as_array() {
                    for call in calls {
                        let index = call["index"].as_u64().unwrap_or(0);
                        let id = call["id"].as_str().unwrap_or("");
                        let name = call["function"]["name"].as_str().unwrap_or("");
                        let arguments = match &call["function"]["arguments"] {
                            Value::Null => String::new(),
                            Value::String(s) => s.clone(),
                            other => other.to_string(),
                        };
                        generated |= !id.is_empty() || !name.is_empty() || !arguments.is_empty();
                        if let Some(first) = &self.call
                            && first.index != index
                        {
                            tracing::warn!(
                                index,
                                "ignoring extra local tool call; first index wins"
                            );
                            continue;
                        }
                        let accumulated = self.call.get_or_insert_with(|| Call {
                            index,
                            ..Call::default()
                        });
                        accumulated.id.push_str(id);
                        accumulated.name.push_str(name);
                        accumulated.arguments.push_str(&arguments);
                    }
                }
                if let Some(reason) = choice["finish_reason"].as_str() {
                    self.finish = Some(reason.to_owned());
                }
            }
        }
        (generated, output)
    }
    pub fn finish(
        self,
        engine: Engine,
        server: &str,
        room: u64,
        cap: Option<u64>,
    ) -> Result<Vec<StreamChunk>, CompletionError> {
        let reason = self.finish.as_deref().ok_or_else(|| {
            CompletionError::provider(
                ErrorClass::Transient,
                "stream ended without a finish reason",
            )
        })?;
        if !engine.success_finishes().contains(&reason) {
            let message = match reason {
                "length" => {
                    let window_floor = self.usage.is_some_and(|u| {
                        u.input_tokens.saturating_add(u.output_tokens) as u128 * 100
                            >= room as u128 * 99
                    });
                    if window_floor {
                        "the context window filled during the reply; the conversation needs compaction".to_owned()
                    } else if self.usage.is_some()
                        && let Some(cap) = cap.or(engine.default_reply_cap())
                    {
                        format!(
                            "the reply hit the {cap}-token output cap; raise the profile's max_output_tokens"
                        )
                    } else {
                        "the reply was truncated by an output cap or the context window; check usage and max_output_tokens".to_owned()
                    }
                }
                "repetition" => "the model looped (finish reason repetition)".to_owned(),
                _ => format!("local generation stopped with finish reason {reason:?}"),
            };
            let class = if reason == "preempted" || (reason == "abort" && engine == Engine::Sglang)
            {
                ErrorClass::Transient
            } else {
                ErrorClass::Fatal
            };
            return Err(CompletionError::provider(class, message));
        }
        if self.text.is_empty() && self.call.is_none() {
            tracing::warn!(
                server,
                "empty local reply; TensorFold Python may have swallowed a malformed tool call"
            );
        }
        if ["<tool_call>", "<function=", "[TOOL_CALLS]", "<|tool_call|>"]
            .iter()
            .any(|m| self.text.contains(m))
        {
            tracing::warn!(
                server,
                "visible tool markup in local reply; check the engine's tool parser"
            );
        }
        let mut output = Vec::new();
        if !self.reasoning.is_empty() {
            output.push(StreamChunk::Reasoning(Reasoning::new_with_signature(
                self.reasoning,
                None,
            )));
        }
        if let Some(call) = self.call {
            let arguments = if call.arguments.is_empty() {
                serde_json::json!({})
            } else {
                serde_json::from_str(&call.arguments).map_err(|e| {
                    CompletionError::provider(
                        ErrorClass::Transient,
                        format!("invalid tool arguments on successful finish: {e}"),
                    )
                })?
            };
            let id = if call.id.is_empty() {
                format!("call_{}", uuid::Uuid::new_v4())
            } else {
                call.id
            };
            output.push(StreamChunk::ToolCall(ToolCall::new(
                id, call.name, arguments,
            )));
        }
        output.push(StreamChunk::Final(FinalResponse { usage: self.usage }));
        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn delta(value: Value) -> Value {
        json!({"choices":[{"index":0,"delta":value}]})
    }
    fn finish(acc: &mut Accumulator, reason: &str) {
        acc.push(
            Engine::TensorFold,
            &json!({"choices":[{"finish_reason":reason}]}),
        );
    }
    #[test]
    fn fragmented_and_whole_calls_reasoning_usage_and_empty_turns() {
        let mut acc = Accumulator::default();
        assert!(
            !acc.push(Engine::Vllm, &delta(json!({"role":"assistant"})))
                .0
        );
        assert!(acc.push(Engine::Vllm, &delta(json!({"reasoning":"why"}))).0);
        acc.push(Engine::Vllm,&delta(json!({"tool_calls":[{"index":0,"function":{"name":"read","arguments":"{\"x\":"}}]})));
        acc.push(Engine::Vllm,&delta(json!({"tool_calls":[{"index":0,"function":{"arguments":"1}"}},{"index":1,"id":"ignored","function":{"name":"bad","arguments":"broken"}}]})));
        finish(&mut acc, "stop");
        assert!(!acc.push(Engine::Vllm,&json!({"usage":{"prompt_tokens":10,"completion_tokens":3,"prompt_tokens_details":{"cached_tokens":4}}})).0);
        let output = acc.finish(Engine::Vllm, "v", 100, None).expect("finish");
        assert!(
            matches!(&output[0], StreamChunk::Reasoning(r) if r.first_text()==Some("why") && r.first_signature().is_none())
        );
        assert!(
            matches!(&output[1], StreamChunk::ToolCall(t) if t.id.starts_with("call_") && t.function.arguments==json!({"x":1}) && t.function.name=="read")
        );
        assert!(
            matches!(&output[2], StreamChunk::Final(f) if f.usage==Some(Usage { input_tokens:10,output_tokens:3,total_tokens:13,cached_input_tokens:4 }))
        );
        let mut acc = Accumulator::default();
        acc.push(
            Engine::TensorFold,
            &delta(
                json!({"tool_calls":[{"function":{"name":"whole","arguments":{"nested":[1,2]}}}]}),
            ),
        );
        finish(&mut acc, "tool_calls");
        assert!(
            matches!(&acc.finish(Engine::TensorFold,"tf",100,None).expect("whole")[0], StreamChunk::ToolCall(t) if t.function.arguments==json!({"nested":[1,2]}))
        );
        let mut acc = Accumulator::default();
        finish(&mut acc, "stop");
        assert_eq!(
            acc.finish(Engine::TensorFold, "tf", 100, None)
                .expect("empty success"),
            vec![StreamChunk::Final(FinalResponse { usage: None })]
        );
        let mut acc = Accumulator::default();
        acc.push(
            Engine::TensorFold,
            &delta(json!({"tool_calls":[{"function":{"name":"empty"}}]})),
        );
        finish(&mut acc, "stop");
        assert!(
            matches!(&acc.finish(Engine::TensorFold,"tf",100,None).expect("empty arguments")[0], StreamChunk::ToolCall(t) if t.function.arguments==json!({}))
        );
    }
    #[test]
    fn finish_classification_and_actual_window_diagnostics() {
        for (engine, reason, class) in [
            (Engine::Sglang, "abort", ErrorClass::Transient),
            // vLLM v0.30.0 vllm/v1/engine/__init__.py:35,49-58.
            (Engine::Vllm, "repetition", ErrorClass::Fatal),
            // TensorFold 0.3.6.3 server/app.py:443-449 replays preempted rounds internally. Defensive, not reachable on that wire.
            (Engine::TensorFold, "preempted", ErrorClass::Transient),
            (Engine::Generic, "unknown", ErrorClass::Fatal),
        ] {
            let mut acc = Accumulator::default();
            finish(&mut acc, reason);
            assert_eq!(
                acc.finish(engine, "server", 128, None)
                    .expect_err("failed finish")
                    .class(),
                class
            );
        }
        assert_eq!(
            Accumulator::default()
                .finish(Engine::Generic, "g", 128, None)
                .expect_err("DONE without reason")
                .class(),
            ErrorClass::Transient
        );
        let mut acc = Accumulator::default();
        acc.push(
            Engine::Generic,
            &delta(json!({"tool_calls":[{"function":{"arguments":"invalid"}}]})),
        );
        finish(&mut acc, "stop");
        assert_eq!(
            acc.finish(Engine::Generic, "g", 128, None)
                .expect_err("invalid JSON")
                .class(),
            ErrorClass::Transient
        );
        for (room, input, output, cap, expected) in [
            (262144, 250000, 12144, Some(16384), "context window filled"),
            (100000, 99000, 1000, None, "context window filled"),
            (262144, 1000, 16384, Some(16384), "16384-token output cap"),
            (262144, 1000, 4096, None, "4096-token output cap"),
        ] {
            let mut acc = Accumulator {
                usage: Some(Usage {
                    input_tokens: input,
                    output_tokens: output,
                    ..Usage::default()
                }),
                ..Default::default()
            };
            finish(&mut acc, "length");
            let error = acc
                .finish(Engine::TensorFold, "tf", room, cap)
                .expect_err("length fatal");
            assert_eq!(error.class(), ErrorClass::Fatal);
            assert!(error.to_string().contains(expected), "{error}");
        }
        let mut acc = Accumulator::default();
        finish(&mut acc, "length");
        assert!(
            acc.finish(Engine::Generic, "g", 128, None)
                .expect_err("neutral")
                .to_string()
                .contains("output cap or the context window")
        );
    }
}
