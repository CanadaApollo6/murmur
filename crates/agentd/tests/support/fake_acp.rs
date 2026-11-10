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
    let key = std::env::var("FAKE_OPTION_KEY").unwrap_or_else(|_| "yes".into());
    let title = std::env::var("FAKE_TITLE_TEXT").unwrap_or_else(|_| {
        "T".repeat(
            std::env::var("FAKE_TITLE_BYTES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
        )
    });
    let mut options = vec![json!({"optionId":key,"name":"Allow once","kind":"allow_once"})];
    if reject {
        options.push(json!({"optionId":"no","name":"Reject once","kind":"reject_once"}));
    }
    let bounded = std::env::var_os("FAKE_BOUND_OPTIONS").is_some();
    let (title, tool, padding) = if bounded {
        options=(0..16).map(|n|json!({"optionId":format!("{n:02}{}","'".repeat(1020)),"name":"\0'\\\"".repeat(1024),"kind":"allow_once"})).collect();
        ("\0'\\\"".repeat(4096), "t".repeat(2048), "\0".repeat(8192))
    } else {
        (title, "edit".into(), String::new())
    };
    emit(
        json!({"jsonrpc":"2.0","id":id,"method":"session/request_permission",
        "params":{"sessionId":session,"toolCall":{"toolCallId":id,"title":title,"kind":tool,"rawInput":padding},
        "options":options}}),
    );
}
struct Mcp {
    child: std::process::Child,
    input: std::process::ChildStdin,
    output: std::io::BufReader<std::process::ChildStdout>,
}
impl Mcp {
    fn open(entry: &Value) -> Self {
        let mut cmd = std::process::Command::new(entry["command"].as_str().expect("MCP command"));
        for arg in entry["args"].as_array().expect("MCP args") {
            cmd.arg(arg.as_str().expect("MCP arg"));
        }
        for env in entry["env"].as_array().expect("MCP env") {
            cmd.env(
                env["name"].as_str().expect("env name"),
                env["value"].as_str().expect("env value"),
            );
        }
        let mut child = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .spawn()
            .expect("MCP spawn");
        let mut mcp = Self {
            input: child.stdin.take().expect("MCP stdin"),
            output: std::io::BufReader::new(child.stdout.take().expect("MCP stdout")),
            child,
        };
        let init = mcp.call(json!({"jsonrpc":"2.0","id":"init","method":"initialize","params":{"protocolVersion":"2024-11-05"}}));
        assert_eq!(init["result"]["serverInfo"]["name"], "agentd");
        let tools = mcp.call(json!({"jsonrpc":"2.0","id":"tools","method":"tools/list"}));
        assert!(
            tools["result"]["tools"]
                .as_array()
                .expect("tools")
                .iter()
                .any(|t| t["name"] == "send_message")
        );
        mcp
    }
    fn call(&mut self, request: Value) -> Value {
        writeln!(self.input, "{request}").expect("MCP request");
        self.input.flush().expect("MCP flush");
        let mut line = String::new();
        assert!(
            self.output.read_line(&mut line).expect("MCP read") > 0,
            "MCP closed"
        );
        serde_json::from_str(&line).expect("MCP JSON")
    }
}
impl Drop for Mcp {
    fn drop(&mut self) {
        if self.child.try_wait().expect("MCP status").is_none() {
            self.child.kill().expect("MCP kill");
        }
        self.child.wait().expect("MCP reap");
    }
}
/// One `write_all` per line, so a kill mid-log never leaves a partial record:
/// `writeln!` on an unbuffered file issues one write per format fragment.
fn log_line(log: &mut File, value: &Value) {
    log.write_all(format!("{value}\n").as_bytes())
        .expect("write log");
}
fn main() {
    let root = if std::env::var_os("FAKE_ROOT_FROM_CWD").is_some() {
        std::env::current_dir().expect("cwd")
    } else {
        PathBuf::from(std::env::var_os("FAKE_ROOT").expect("FAKE_ROOT"))
    };
    let mut log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("wire.jsonl"))
        .expect("wire log");
    log_line(
        &mut log,
        &json!({"pid":std::process::id(),"cwd":std::env::current_dir().expect("cwd"),"args":std::env::args().collect::<Vec<_>>(),
            "agentd_env":std::env::vars_os().map(|(name, _)| name.to_string_lossy().into_owned()).filter(|name| name.starts_with("AGENTD_")).collect::<Vec<_>>()}),
    );
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
    let mut mcp = None;
    let mut broker_token = String::new();
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
        log_line(&mut log, &msg);
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
            Some("session/new") => {
                if let Some(entry) = msg["params"]["mcpServers"]
                    .as_array()
                    .and_then(|a| a.iter().find(|v| v["name"] == "agentd"))
                {
                    broker_token = entry["env"][0]["value"].as_str().unwrap_or("").to_owned();
                    mcp = Some(Mcp::open(entry));
                }
                reply(id, json!({"sessionId":session}));
            }
            Some("session/load") => {
                if let Some(entry) = msg["params"]["mcpServers"]
                    .as_array()
                    .and_then(|a| a.iter().find(|v| v["name"] == "agentd"))
                {
                    broker_token = entry["env"][0]["value"].as_str().unwrap_or("").to_owned();
                    mcp = Some(Mcp::open(entry));
                }
                assert_eq!(msg["params"]["sessionId"], session);
                update(session, "history replay");
                if root.join("fail-load").exists() {
                    emit(
                        json!({"jsonrpc":"2.0","id":id,"error":{"code":-1,"message":std::env::var("FAKE_LOAD_ERROR").unwrap_or_else(|_| "load refused".into())}}),
                    );
                } else {
                    reply(id, json!({}));
                }
            }
            Some("session/set_mode") => {
                assert_eq!(
                    msg["params"]["modeId"].as_str(),
                    Some(
                        std::fs::read_to_string(root.join("expected-mode"))
                            .unwrap_or_else(|_| std::env::var("FAKE_MODE").expect("mode"))
                            .as_str()
                    )
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
                if text.contains("isolation-probe") {
                    let status = std::process::Command::new("/usr/bin/python3")
                        .arg(root.join("probe.py"))
                        .arg(&root)
                        .status()
                        .expect("probe");
                    assert!(status.success(), "isolation probe failed");
                }
                if text.contains("mcp-send") {
                    let mcp = mcp.as_mut().expect("broker MCP entry");
                    let request = json!({"jsonrpc":"2.0","id":"peer-send","method":"tools/call","params":{"name":"send_message","arguments":{"to":"@b","body":"peer payload"}}});
                    let first = mcp.call(request.clone());
                    let retry = mcp.call(request);
                    assert_eq!(first["result"]["isError"], false, "{first}");
                    let list = mcp.call(json!({"jsonrpc":"2.0","id":"list","method":"tools/call","params":{"name":"list_agents","arguments":{}}}));
                    assert_eq!(list["result"]["isError"], false);
                    std::fs::write(root.join("mcp-retry.json"), retry.to_string())
                        .expect("MCP retry result");
                    std::fs::write(root.join("mcp-result.json"), first.to_string())
                        .expect("MCP result");
                }
                if text.contains("echo-token") {
                    update(session, &broker_token);
                    eprintln!("fixture secret {broker_token}");
                }
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
                    update(
                        session,
                        &std::env::var("FAKE_ACTIVITY_TEXT").unwrap_or_else(|_| "started".into()),
                    );
                }
                if text.contains("ignore-cancel") || text.contains("late-permissions") {
                    ignore_cancel = text.contains("ignore-cancel");
                    late_permissions = usize::from(text.contains("late-permissions"));
                    flight = Some(id.clone());
                } else if text.contains("max-permissions") {
                    flight = Some(id.clone());
                    for n in 0..64 {
                        permission(session, &format!("permission-max-{n}"), false);
                    }
                } else if text.contains("two-permissions") {
                    flight = Some(id.clone());
                    permission_responses_remaining = Some(2);
                    permission(session, "permission-1", false);
                    permission(session, "permission-2", false);
                } else if text.contains("die-after") {
                    std::process::exit(18);
                } else if text.contains("exit-after-turn") {
                    reply(id, json!({"stopReason":"end_turn"}));
                    std::thread::sleep(Duration::from_millis(5));
                    std::process::exit(0);
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
