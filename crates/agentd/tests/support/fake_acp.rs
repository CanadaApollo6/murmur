//! Scriptable subprocess fixture, not an agentd entry point.
use std::fs::{File, OpenOptions};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use serde_json::{Value, json};

fn emit(value: Value) {
    println!("{value}");
    std::io::stdout().flush().expect("flush stdout");
}
fn reply(id: &Value, result: Value) {
    emit(json!({"jsonrpc":"2.0","id":id,"result":result}));
}
fn update(session: &str, text: &str) {
    emit(
        json!({"jsonrpc":"2.0","method":"session/update","params":{"sessionId":session,
        "update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":text}}}}),
    );
}
fn permission(session: &str, id: &str, reject: bool) {
    let mut options = vec![json!({"optionId":"yes","name":"Allow once","kind":"allow_once"})];
    if reject {
        options.push(json!({"optionId":"no","name":"Reject once","kind":"reject_once"}));
    }
    emit(
        json!({"jsonrpc":"2.0","id":id,"method":"session/request_permission",
        "params":{"sessionId":session,"toolCall":{"toolCallId":id,"title":"Write file","kind":"edit"},
        "options":options}}),
    );
}
fn main() {
    let root = PathBuf::from(std::env::var_os("FAKE_ROOT").expect("FAKE_ROOT"));
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("wire.jsonl"))
        .expect("wire log");
    writeln!(
        log,
        "{}",
        json!({"pid":std::process::id(),"cwd":std::env::current_dir().expect("cwd")})
    )
    .expect("log pid");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let line = line.expect("stdin line");
            let msg: Value = serde_json::from_str(&line).expect("JSON-RPC");
            if tx.send(msg).is_err() {
                break;
            }
        }
    });
    let session = "fake-session";
    let mut mode_set = false;
    let mut flight: Option<Value> = None;
    let mut held = false;
    let mut count = 0;
    let mut late_permissions = 0;
    let mut ignore_cancel = false;
    let mut permission_responses_remaining = None;
    loop {
        if held && root.join("release").exists() {
            std::fs::remove_file(root.join("release")).expect("release file");
            held = false;
            if let Some(id) = flight.take() {
                reply(&id, json!({"stopReason":"end_turn"}));
                if let Ok(bytes) = std::fs::read(root.join("tail-update.json")) {
                    let value: Value = serde_json::from_slice(&bytes).expect("tail update");
                    emit(json!({"jsonrpc":"2.0","method":"session/update",
                        "params":{"sessionId":session,"update":value}}));
                }
            }
        }
        let msg = match rx.recv_timeout(Duration::from_millis(2)) {
            Ok(msg) => msg,
            Err(mpsc::RecvTimeoutError::Timeout) => continue,
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        };
        writeln!(log, "{msg}").expect("write log");
        log.flush().expect("flush log");
        let id = &msg["id"];
        match msg["method"].as_str() {
            Some("initialize") => {
                if root.join("crash-open").exists() {
                    std::process::exit(19);
                }
                assert_eq!(
                    msg["params"]["clientCapabilities"],
                    json!({"fs":{"readTextFile":false,"writeTextFile":false},"terminal":false})
                );
                reply(
                    id,
                    json!({"protocolVersion":1,"agentCapabilities":{"loadSession": !root.join("no-resume").exists()}}),
                );
            }
            Some("session/new") => reply(id, json!({"sessionId":session})),
            Some("session/load") => {
                assert_eq!(msg["params"]["sessionId"], session);
                update(session, "history replay");
                if root.join("fail-load").exists() {
                    emit(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-1,"message":"load refused"}}),
                    );
                } else {
                    reply(id, json!({}));
                }
            }
            Some("session/set_mode") => {
                assert_eq!(
                    msg["params"]["modeId"].as_str(),
                    Some(std::env::var("FAKE_MODE").expect("mode").as_str())
                );
                mode_set = true;
                reply(id, json!({}));
            }
            Some("session/set_model") => reply(id, json!({})),
            Some("session/prompt") => {
                assert!(mode_set, "prompt before restrictive mode");
                assert!(flight.is_none(), "concurrent turns");
                count += 1;
                File::create(root.join(format!("prompt-{count}"))).expect("prompt marker");
                let text = msg["params"]["prompt"].to_string();
                if text.contains("die-before") {
                    std::process::exit(17);
                }
                if text.contains("protocol-error") {
                    emit(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-1,"message":"prompt refused"}}),
                    );
                    continue;
                }
                if text.contains("response-only") {
                    reply(id, json!({"stopReason":"end_turn"}));
                    continue;
                }
                if !text.contains("silent") {
                    update(session, "started");
                }
                if text.contains("ignore-cancel") || text.contains("late-permissions") {
                    ignore_cancel = text.contains("ignore-cancel");
                    late_permissions = usize::from(text.contains("late-permissions"));
                    flight = Some(id.clone());
                } else if text.contains("two-permissions") {
                    flight = Some(id.clone());
                    permission_responses_remaining = Some(2);
                    permission(session, "permission-1", false);
                    permission(session, "permission-2", false);
                } else if text.contains("die-after") {
                    std::process::exit(18);
                } else if text.contains("permission") {
                    flight = Some(id.clone());
                    permission(session, "permission-1", true);
                } else if text.contains("hold") {
                    flight = Some(id.clone());
                    held = true;
                } else {
                    reply(id, json!({"stopReason":"end_turn"}));
                }
            }
            Some("session/cancel") => {
                held = false;
                if let Some(remaining) = permission_responses_remaining.take() {
                    assert_eq!(remaining, 0, "cancel must answer every pending permission");
                }
                if late_permissions > 0 {
                    permission(session, "permission-late-1", true);
                } else if !ignore_cancel && let Some(id) = flight.take() {
                    reply(&id, json!({"stopReason":"cancelled"}));
                }
            }
            None if id
                .as_str()
                .is_some_and(|id| id.starts_with("permission-late-")) =>
            {
                assert_eq!(
                    msg["result"]["outcome"],
                    json!({"outcome":"cancelled"}),
                    "permission after cancel must be cancelled"
                );
                if late_permissions == 1 {
                    late_permissions = 2;
                    permission(session, "permission-late-2", true);
                } else {
                    late_permissions = 0;
                    reply(
                        &flight.take().expect("cancelled flight"),
                        json!({"stopReason":"cancelled"}),
                    );
                }
            }
            None if *id == "permission-1" || *id == "permission-2" => {
                if let Some(remaining) = &mut permission_responses_remaining {
                    assert_eq!(msg["result"]["outcome"], json!({"outcome":"cancelled"}));
                    *remaining -= 1;
                } else if let Some(id) = flight.take() {
                    reply(&id, json!({"stopReason":"end_turn"}));
                }
            }
            _ => panic!("unexpected message {msg}"),
        }
    }
}
