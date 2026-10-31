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
fn real_daemon_reopen_and_restart_reject_swapped_sources() {
    if !supported() {
        return;
    }
    let output = Command::new("python3")
        .arg(format!(
            "{}/tests/support/isolation_swap_driver.py",
            env!("CARGO_MANIFEST_DIR")
        ))
        .args([
            env!("CARGO_BIN_EXE_agentd"),
            env!("CARGO_BIN_EXE_agentd-fake-acp"),
        ])
        .output()
        .expect("swap driver");
    println!("{}", String::from_utf8_lossy(&output.stdout));
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn startup_names_bwrap_remedy_and_warns_about_host_loopback() {
    let root = tempfile::tempdir().expect("root");
    let home = root.path().join("home");
    std::fs::create_dir_all(home.join(".claude")).expect("fake HOME");
    std::fs::create_dir_all(home.join(".codex")).expect("codex parent");
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

#[test]
fn startup_names_bad_credential_component_and_rejects_its_parent_bind() {
    let root = tempfile::tempdir().expect("root");
    let home = root.path().join("human");
    std::fs::create_dir_all(home.join(".codex")).expect("codex");
    std::fs::create_dir_all(home.join(".claude")).expect("claude");
    let secret = root.path().join("secret");
    std::fs::write(&secret, b"{}").expect("secret");
    let leaf = home.join(".codex/auth.json");
    std::os::unix::fs::symlink(&secret, &leaf).expect("leaf link");
    let config = root.path().join("config.toml");
    for (case, binds, expected) in [
        ("leaf", "[]".to_owned(), "kind codex credential"),
        ("parent", "[]".to_owned(), "kind codex credential parent"),
        ("notdir", "[]".to_owned(), "kind codex credential"),
        (
            "bind",
            serde_json::to_string(&[home.join(".codex")]).expect("bind"),
            "protected paths",
        ),
    ] {
        if case == "parent" {
            std::fs::remove_file(&leaf).expect("remove link");
            std::fs::remove_dir(home.join(".codex")).expect("remove parent");
        } else if case == "notdir" {
            std::fs::write(home.join(".codex"), b"not a directory").expect("bad parent");
        } else if case == "bind" {
            std::fs::remove_file(home.join(".codex")).expect("bad parent removed");
            std::fs::create_dir(home.join(".codex")).expect("parent");
        }
        std::fs::write(&config, format!("[bwrap]\nro_binds = {binds}\n[kind.claude]\ncommand = ['/bin/true']\n[kind.codex]\ncommand = ['/bin/true']\n")).expect("config");
        let output = Command::new(env!("CARGO_BIN_EXE_agentd"))
            .arg("--config")
            .arg(&config)
            .arg("--state-dir")
            .arg(root.path().join(case))
            .env("HOME", &home)
            .output()
            .expect("startup");
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(expected), "{case}: {error}");
    }
}

#[test]
fn preflight_rejects_bwrap_without_fd_binds() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().expect("root");
    let stub = root.path().join("old-bwrap");
    std::fs::write(
        &stub,
        "#!/bin/sh\necho 'bubblewrap 0.9.0 --bind SRC DEST'\n",
    )
    .expect("stub");
    std::fs::set_permissions(&stub, std::fs::Permissions::from_mode(0o700)).expect("mode");
    let config = root.path().join("config.toml");
    std::fs::write(&config, format!("[bwrap]\npath = {}\nro_binds = []\n[kind.claude]\ncommand = ['/bin/true']\ncredentials = []\n[kind.codex]\ncommand = ['/bin/true']\ncredentials = []\n", serde_json::to_string(&stub).expect("path"))).expect("config");
    let output = Command::new(env!("CARGO_BIN_EXE_agentd"))
        .arg("--config")
        .arg(config)
        .arg("--state-dir")
        .arg(root.path().join("state"))
        .env("HOME", root.path())
        .output()
        .expect("preflight");
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(
        error.contains("--bind-fd") && error.contains("0.10.0"),
        "{error}"
    );
}
