use crate::config::{Profile, ServerConfig};
use infinity_provider_protocol::{
    AssistantContent, CompletionRequest, Image, ImageSource, Message, ReasoningContent,
    ToolResultContent, UserContent,
};
use serde_json::{Value, json};

const OMITTED: &str = "[image omitted: model has no image input]";
fn text_part(text: &str) -> Value {
    json!({"type":"text", "text":text})
}
fn image_part(image: &Image, supported: bool) -> Value {
    if !supported {
        return text_part(OMITTED);
    }
    let url = match &image.data {
        ImageSource::Url(url) => url.clone(),
        ImageSource::Base64(data) => format!(
            "data:{};base64,{data}",
            image
                .media_type
                .map(|m| m.to_mime_type())
                .unwrap_or("image/png")
        ),
    };
    json!({"type":"image_url", "image_url":{"url":url}})
}
fn user_message(parts: Vec<Value>) -> Option<Value> {
    let parts: Vec<_> = parts
        .into_iter()
        .filter(|p| p["type"] != "text" || p["text"].as_str().is_some_and(|s| !s.is_empty()))
        .collect();
    if parts.is_empty() {
        return None;
    }
    let content = if parts.iter().all(|p| p["type"] == "text") {
        Value::String(
            parts
                .iter()
                .filter_map(|p| p["text"].as_str())
                .collect::<String>(),
        )
    } else {
        Value::Array(parts)
    };
    Some(json!({"role":"user", "content": content}))
}
fn parts(content: &Value) -> Vec<Value> {
    match content {
        Value::String(s) => vec![text_part(s)],
        Value::Array(a) => a.clone(),
        _ => vec![],
    }
}
fn append_string(target: &mut Value, other: &Value, separator: &str) {
    if let Some(s) = other.as_str().filter(|s| !s.is_empty()) {
        *target = Value::String(match target.as_str().filter(|s| !s.is_empty()) {
            Some(old) => format!("{old}{separator}{s}"),
            None => s.to_owned(),
        });
    }
}
pub(crate) fn messages(request: &CompletionRequest, image_support: bool) -> Vec<Value> {
    let mut mapped = Vec::new();
    if let Some(system) = request.preamble.as_ref().filter(|s| !s.is_empty()) {
        mapped.push(json!({"role":"system", "content":system}));
    }
    for (index, message) in request.chat_history.iter().enumerate() {
        match message {
            Message::Assistant { content }
                if index == 0 && matches!(content.as_slice(), [AssistantContent::Text(_)]) =>
            {
                let AssistantContent::Text(t) = &content[0] else {
                    unreachable!()
                };
                if let Some(message) = user_message(vec![text_part(&t.text)]) {
                    mapped.push(message);
                }
            }
            Message::User { content } => {
                let mut pending = Vec::new();
                for c in content {
                    match c {
                        UserContent::Text(t) => pending.push(text_part(&t.text)),
                        UserContent::Image(i) => pending.push(image_part(i, image_support)),
                        UserContent::ToolResult(t) => {
                            if let Some(message) = user_message(std::mem::take(&mut pending)) {
                                mapped.push(message);
                            }
                            let text = t
                                .content
                                .iter()
                                .map(|c| match c {
                                    ToolResultContent::Text(t) => t.text.as_str(),
                                    ToolResultContent::Image(_) => OMITTED,
                                })
                                .collect::<Vec<_>>()
                                .join("\n");
                            mapped
                                .push(json!({"role":"tool", "tool_call_id":t.id, "content":text}));
                        }
                    }
                }
                if let Some(message) = user_message(pending) {
                    mapped.push(message);
                }
            }
            Message::Assistant { content } => {
                let mut text = String::new();
                let mut reasoning = Vec::new();
                let mut calls = Vec::new();
                for c in content {
                    match c {
                        AssistantContent::Text(t) => text.push_str(&t.text),
                        AssistantContent::Reasoning(r) => reasoning.extend(r.content.iter().filter_map(|c| match c {
                            ReasoningContent::Text { text, .. } | ReasoningContent::Summary(text) if !text.is_empty() => Some(text.as_str()),
                            _ => None,
                        })),
                        AssistantContent::ToolCall(t) => calls.push(json!({"id":t.id, "type":"function", "function":{"name":t.function.name, "arguments":t.function.arguments.to_string()}})),
                    }
                }
                if text.is_empty() && reasoning.is_empty() && calls.is_empty() {
                    continue;
                }
                let mut message = json!({"role":"assistant", "content":if text.is_empty() { Value::Null } else { Value::String(text) }});
                if !reasoning.is_empty() {
                    message["reasoning_content"] = json!(reasoning.join("\n"));
                }
                if !calls.is_empty() {
                    message["tool_calls"] = json!(calls);
                }
                mapped.push(message);
            }
        }
    }
    let mut output: Vec<Value> = Vec::new();
    for next in mapped {
        if let Some(last) = output.last_mut()
            && last["role"] == next["role"]
        {
            if next["role"] == "assistant" {
                append_string(&mut last["content"], &next["content"], "");
                if next.get("reasoning_content").is_some() {
                    append_string(
                        &mut last["reasoning_content"],
                        &next["reasoning_content"],
                        "\n",
                    );
                }
                if let Some(calls) = next["tool_calls"].as_array() {
                    let old = last
                        .as_object_mut()
                        .expect("bug: mapped message")
                        .entry("tool_calls")
                        .or_insert_with(|| json!([]));
                    old.as_array_mut()
                        .expect("bug: mapped calls")
                        .extend(calls.clone());
                }
                continue;
            }
            if next["role"] == "user" {
                if last["content"].is_string() && next["content"].is_string() {
                    append_string(&mut last["content"], &next["content"], "\n\n");
                } else {
                    let mut combined = parts(&last["content"]);
                    combined.push(text_part("\n\n"));
                    combined.extend(parts(&next["content"]));
                    last["content"] = json!(combined);
                }
                continue;
            }
        }
        output.push(next);
    }
    output
}

pub(crate) fn body(
    server: &ServerConfig,
    profile: &Profile,
    model: &str,
    request: &CompletionRequest,
    cap: Option<u64>,
) -> Value {
    let mut body = json!(profile.extra);
    body["model"] = json!(model);
    body["messages"] = json!(messages(request, server.supports_image_input));
    body["stream"] = json!(true);
    if server.include_usage {
        body["stream_options"] = json!({"include_usage":true});
    }
    if !request.tools.is_empty() {
        body["tools"] = json!(request.tools.iter().map(|t| json!({"type":"function", "function":{"name":t.name, "description":t.description, "parameters":t.parameters}})).collect::<Vec<_>>());
        body["parallel_tool_calls"] = json!(false);
    }
    for (name, value) in [
        ("temperature", profile.temperature.map(|v| json!(v))),
        ("top_p", profile.top_p.map(|v| json!(v))),
        ("top_k", profile.top_k.map(|v| json!(v))),
    ] {
        if let Some(value) = value {
            body[name] = value;
        }
    }
    if let Some(thinking) = profile.thinking {
        if !body["chat_template_kwargs"].is_object() {
            body["chat_template_kwargs"] = json!({});
        }
        body["chat_template_kwargs"]["enable_thinking"] = json!(thinking);
    }
    if let Some(cap) = cap {
        body["max_tokens"] = json!(cap);
    }
    if server.engine == crate::engines::Engine::TensorFold && !profile.extra.contains_key("seed") {
        let bytes = *uuid::Uuid::new_v4().as_bytes();
        let seed = u64::from_le_bytes(bytes[..8].try_into().expect("bug: UUID has eight bytes"))
            & i64::MAX as u64;
        body["seed"] = json!(seed);
    }
    body
}

#[cfg(test)]
mod tests {
    use super::*;
    use infinity_provider_protocol::{
        ImageMediaType, Reasoning, ToolCall, ToolDefinition, ToolResult,
    };
    fn setup() -> (ServerConfig, Profile) {
        let config = crate::config::Config::parse("[[server]]\nname='local'\nengine='generic'\nbase_url='http://localhost:1'\ncontext_window=100\n[[server.profile]]\nid='p'\ndisplay_name='P'\ntemperature=0.6\ntop_p=0.95\ntop_k=20\nthinking=true\nextra={min_p=0.0}\n").expect("config");
        let s = config.server[0].clone();
        let p = s.profile[0].clone();
        (s, p)
    }
    fn image() -> Image {
        Image {
            data: ImageSource::Base64("aGVsbG8=".into()),
            media_type: Some(ImageMediaType::PNG),
        }
    }
    #[test]
    fn request_goldens() {
        let (mut server, profile) = setup();
        let mut req = CompletionRequest {
            preamble: Some("system".into()),
            chat_history: vec![Message::user("hello")],
            ..Default::default()
        };
        insta::assert_json_snapshot!("plain_turn", body(&server, &profile, "weights", &req, None));
        req.tools.push(ToolDefinition {
            name: "read".into(),
            description: "Read".into(),
            parameters: json!({"type":"object"}),
        });
        req.chat_history.extend([
            Message::Assistant {
                content: vec![AssistantContent::ToolCall(ToolCall::new(
                    "call",
                    "read",
                    json!({"path":"a"}),
                ))],
            },
            Message::User {
                content: vec![UserContent::ToolResult(ToolResult {
                    id: "call".into(),
                    call_id: None,
                    content: vec![ToolResultContent::text("result")],
                })],
            },
            Message::assistant("continuation"),
        ]);
        insta::assert_json_snapshot!(
            "tool_continuation",
            body(&server, &profile, "weights", &req, Some(128))
        );
        req.chat_history.insert(
            1,
            Message::Assistant {
                content: vec![AssistantContent::Reasoning(Reasoning {
                    id: None,
                    content: vec![
                        ReasoningContent::Text {
                            text: "thought".into(),
                            signature: None,
                        },
                        ReasoningContent::Summary("summary".into()),
                        ReasoningContent::Encrypted("drop".into()),
                        ReasoningContent::Redacted {
                            data: "drop".into(),
                        },
                    ],
                })],
            },
        );
        insta::assert_json_snapshot!(
            "reasoning_tool",
            body(&server, &profile, "weights", &req, None)
        );
        let snapshot = include_str!(
            "../../infinity-agent-core/src/system/snapshots/infinity_agent_core__system__tests__issue31_post_compaction_history.snap"
        );
        req.chat_history =
            serde_json::from_str(snapshot.splitn(3, "---").nth(2).expect("snapshot content"))
                .expect("real post-compaction history");
        insta::assert_json_snapshot!(
            "post_compaction",
            body(&server, &profile, "weights", &req, None)
        );
        req.chat_history = vec![
            Message::user("```rust\ncode\n```"),
            Message::Assistant {
                content: vec![AssistantContent::text("")],
            },
            Message::user("next input"),
        ];
        insta::assert_json_snapshot!(
            "batched_user_text",
            body(&server, &profile, "weights", &req, None)
        );
        req.chat_history = vec![
            Message::user("first"),
            Message::User {
                content: vec![UserContent::text("second"), UserContent::Image(image())],
            },
        ];
        server.supports_image_input = true;
        insta::assert_json_snapshot!(
            "batched_user_image",
            body(&server, &profile, "weights", &req, None)
        );
        req.chat_history = vec![Message::User {
            content: vec![UserContent::Image(image())],
        }];
        insta::assert_json_snapshot!(
            "image_supported",
            body(&server, &profile, "weights", &req, None)
        );
        server.supports_image_input = false;
        insta::assert_json_snapshot!(
            "image_omitted",
            body(&server, &profile, "weights", &req, None)
        );
    }
    #[test]
    fn seeds_and_owned_fields() {
        let (mut server, mut profile) = setup();
        server.engine = crate::engines::Engine::TensorFold;
        let req = CompletionRequest {
            additional_params: Some(json!({"model":"wrong"})),
            ..Default::default()
        };
        let first = body(&server, &profile, "weights", &req, None);
        let second = body(&server, &profile, "weights", &req, None);
        assert_ne!(first["seed"], second["seed"]);
        assert!(first["seed"].as_u64().expect("integer seed") < (1 << 63));
        assert!(first.get("max_tokens").is_none());
        assert_eq!(first["model"], "weights");
        profile.extra.insert("seed".into(), json!(-19));
        assert_eq!(body(&server, &profile, "weights", &req, None)["seed"], -19);
    }
}
