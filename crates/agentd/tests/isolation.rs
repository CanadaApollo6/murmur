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
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
