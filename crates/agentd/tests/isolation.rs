#![cfg(target_os = "linux")]
use std::process::Command;

fn supported() -> bool {
    let output = Command::new("bwrap")
        .args([
            "--unshare-user",
            "--unshare-pid",
            "--unshare-net",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-cgroup",
            "--ro-bind",
            "/",
            "/",
            "--proc",
            "/proc",
            "--",
            "/bin/true",
        ])
        .output();
    match output {
        Ok(out) if out.status.success() => true,
        other => {
            assert!(
                std::env::var("AGENTD_REQUIRE_SANDBOX").as_deref() != Ok("1"),
                "required real sandbox gates unavailable: {other:?}"
            );
            eprintln!("SKIP real sandbox gates: bwrap/user namespaces unavailable: {other:?}");
            false
        }
    }
}
#[test]
fn real_agentd_sandbox_gates() {
    if !supported() {
        return;
    }
    for signal in ["--sigterm", "--sigkill"] {
        let output = Command::new("python3")
            .arg(format!(
                "{}/tests/support/isolation_driver.py",
                env!("CARGO_MANIFEST_DIR")
            ))
            .args([
                env!("CARGO_BIN_EXE_agentd"),
                env!("CARGO_BIN_EXE_agentd-fake-acp"),
                signal,
            ])
            .output()
            .expect("isolation driver");
        println!("{}", String::from_utf8_lossy(&output.stdout));
        assert!(
            output.status.success(),
            "public CONNECT check needs network access. {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[test]
fn startup_names_bwrap_remedy_and_warns_about_host_loopback() {
    let root = tempfile::tempdir().expect("root");
    let home = root.path().join("home");
    std::fs::create_dir(&home).expect("fake HOME");
    let config = root.path().join("kinds.toml");
    std::fs::write(
        &config,
        r#"
[bwrap]
path = "/missing/agentd-test-bwrap"
network = "host"
ro_binds = []
[kind.claude]
command = ["/bin/true"]
credentials = []
[kind.codex]
command = ["/bin/true"]
credentials = []
"#,
    )
    .expect("config");
    let output = Command::new(env!("CARGO_BIN_EXE_agentd"))
        .arg("--config")
        .arg(&config)
        .arg("--state-dir")
        .arg(root.path().join("state"))
        .env("HOME", &home)
        .output()
        .expect("startup");
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("bwrap") && error.contains("sandbox = \"none\""),
        "{error}"
    );
    assert!(
        error.contains("kind claude uses bwrap with host network")
            && error.contains("kind codex uses bwrap with host network"),
        "{error}"
    );
    assert!(
        error.contains("host loopback, including local servers, is reachable"),
        "{error}"
    );
}
