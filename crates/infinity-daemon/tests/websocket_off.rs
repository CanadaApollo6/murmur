use futures_util::StreamExt;
use infinity_protocol::DaemonMessage;
use std::time::Duration;

#[tokio::test]
async fn websocket_off_reports_disabled_and_refuses_tcp() {
    let home = tempfile::tempdir().expect("home");
    let state = home.path().join(".infinity");
    std::fs::create_dir(&state).expect("state");
    let provider = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../agentd/tests/support/fake_provider.py");
    std::fs::write(
        state.join("providers.json"),
        serde_json::json!({
            "fake":{"command":["python3",provider,home.path().join("provider.sock")]}
        })
        .to_string(),
    )
    .expect("provider config");
    let port = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve port")
        .local_addr()
        .expect("address")
        .port();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_infinity-daemon"))
        .env("HOME", home.path())
        .env("INFINITY_WS", "off")
        .env("INFINITY_WS_PORT", port.to_string())
        .kill_on_drop(true)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("daemon process");
    let stream = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            assert!(
                child.try_wait().expect("daemon status").is_none(),
                "daemon exited before Welcome"
            );
            if let Ok(stream) = tokio::net::UnixStream::connect(state.join("daemon.sock")).await {
                break stream;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Unix listener");
    let mut framed =
        tokio_util::codec::Framed::new(stream, infinity_protocol::length_delimited_codec());
    let bytes = tokio::time::timeout(Duration::from_secs(15), framed.next())
        .await
        .expect("Welcome timeout")
        .expect("Welcome")
        .expect("frame");
    assert!(matches!(
        serde_json::from_slice::<DaemonMessage>(&bytes).expect("Welcome JSON"),
        DaemonMessage::Welcome {
            websocket_enabled: false,
            ..
        }
    ));
    assert!(
        matches!(tokio::net::TcpStream::connect(("127.0.0.1", port)).await, Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused)
    );
    child.kill().await.expect("kill daemon");
    child.wait().await.expect("reap daemon");
}
