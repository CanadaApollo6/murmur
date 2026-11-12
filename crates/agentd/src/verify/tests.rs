use std::os::unix::fs::{FileTypeExt, PermissionsExt};

use super::*;
use crate::sandbox::{PinnedPath, SandboxConfig};
use crate::surfaces::FileConfig;

/// Parses `kinds.toml` text into the kinds and the verifier settings.
fn parse(text: &str) -> Result<Option<Settings>, String> {
    let config: FileConfig = toml::from_str(text).map_err(|e| e.to_string())?;
    let verify = config.verify.clone();
    let kinds = config.kinds().map_err(|e| e.to_string())?;
    Settings::from_file(verify.as_ref(), &kinds)
}

const KINDS: &str = r#"
sandbox = "bwrap"
[kind.claude]
command = ["claude-agent-acp"]
family = "Anthropic"
[kind.codex]
command = ["codex-acp"]
family = " openai "
"#;

#[test]
fn a_verify_table_parses_with_defaults() {
    let settings = parse(&format!(
        "{KINDS}[verify]\nkinds = [\"codex\", \"claude\"]\n"
    ))
    .expect("valid")
    .expect("settings");
    assert_eq!(settings.kinds, ["codex", "claude"]);
    assert_eq!(settings.families["claude"].as_deref(), Some("anthropic"));
    assert_eq!(settings.families["codex"].as_deref(), Some("openai"));
    assert_eq!(
        settings.budget,
        Budget {
            tokens: None,
            wall_secs: 900,
            messages: 1,
            hops: 0
        }
    );
    assert_eq!(settings.max_running, 2);
    assert_eq!(settings.max_snapshot_bytes, 2 * 1024 * 1024 * 1024);

    let settings = parse(&format!(
        "{KINDS}[verify]\nkinds = [\"codex\"]\nwall_secs = 60\ntokens = 5000\nmax_running = 4\nmax_snapshot_bytes = 1024\n"
    ))
    .expect("valid")
    .expect("settings");
    assert_eq!(settings.budget.wall_secs, 60);
    assert_eq!(settings.budget.tokens, Some(5000));
    assert_eq!(settings.max_running, 4);
    assert_eq!(settings.max_snapshot_bytes, 1024);
}

#[test]
fn no_verify_table_or_no_kinds_means_no_verifier() {
    assert!(parse(KINDS).expect("valid").is_none());
    assert!(
        parse(&format!("{KINDS}[verify]\nkinds = []\n"))
            .expect("valid")
            .is_none()
    );
}

#[test]
fn each_bad_verifier_kind_is_rejected_by_name() {
    let cases = [
        (
            format!("{KINDS}[verify]\nkinds = [\"gemini\"]\n"),
            "[verify] kinds lists gemini, which is not a configured kind",
        ),
        (
            "sandbox = \"bwrap\"\n[verify]\nkinds = [\"codex\"]\n".to_owned(),
            "[verify] kind codex has no family; set family in [kind.codex]",
        ),
        (
            format!(
                "{KINDS}[kind.infinity]\nwire = \"infinity\"\nsocket = \"/run/inf.sock\"\nfamily = \"local\"\n[verify]\nkinds = [\"infinity\"]\n"
            ),
            "[verify] kind infinity is not an ACP kind; a verifier must be an ACP agent",
        ),
        (
            format!(
                "{}[verify]\nkinds = [\"codex\"]\n",
                KINDS.replace("sandbox = \"bwrap\"", "sandbox = \"none\"")
            ),
            "[verify] kind codex is unsandboxed; a verifier must run in bwrap",
        ),
        (
            format!("{KINDS}[verify]\nkinds = [\"codex\"]\nwall_secs = 0\n"),
            "[verify] wall_secs must be at least 1",
        ),
        (
            format!("{KINDS}[verify]\nkinds = [\"codex\"]\ntokens = 0\n"),
            "[verify] tokens must be at least 1 or left out",
        ),
        (
            format!("{KINDS}[verify]\nkinds = [\"codex\"]\nmax_running = 0\n"),
            "[verify] max_running must be at least 1",
        ),
        (
            format!("{KINDS}[verify]\nkinds = [\"codex\"]\nmax_snapshot_bytes = 0\n"),
            "[verify] max_snapshot_bytes must be at least 1",
        ),
    ];
    for (text, expected) in cases {
        assert_eq!(parse(&text).expect_err(expected), expected);
    }
}

#[test]
fn an_empty_family_and_an_unknown_verify_key_are_rejected() {
    let empty = parse("[kind.codex]\ncommand = [\"codex-acp\"]\nfamily = \" \"\n")
        .expect_err("empty family");
    assert_eq!(empty, "kind codex: family must not be empty");
    let unknown = parse(&format!(
        "{KINDS}[verify]\nkinds = [\"codex\"]\nseeds = 3\n"
    ))
    .expect_err("unknown key");
    assert!(unknown.contains("unknown field `seeds`"), "{unknown}");
}

fn families() -> BTreeMap<String, Option<String>> {
    BTreeMap::from([
        ("claude".into(), Some("anthropic".into())),
        ("opus".into(), Some("anthropic".into())),
        ("codex".into(), Some("openai".into())),
        ("plain".into(), None),
    ])
}

fn prefer(kinds: &[&str]) -> Vec<String> {
    kinds.iter().map(|k| (*k).to_owned()).collect()
}

#[test]
fn selection_picks_the_first_kind_of_another_family() {
    assert_eq!(
        select(
            Some("claude"),
            &families(),
            &prefer(&["opus", "plain", "codex"])
        ),
        Selection::Verifier {
            kind: "codex".into(),
            verifier_family: "openai".into(),
            claimant_family: "anthropic".into(),
        }
    );
    assert_eq!(
        select(
            Some("codex"),
            &families(),
            &prefer(&["codex", "opus", "claude"])
        ),
        Selection::Verifier {
            kind: "opus".into(),
            verifier_family: "anthropic".into(),
            claimant_family: "openai".into(),
        }
    );
}

#[test]
fn selection_explains_each_unverifiable_case() {
    let cases = [
        (
            None,
            "the claimant's kind is unknown: it has no saved spec".to_owned(),
        ),
        (
            Some("gone"),
            "the claimant's kind gone is not configured, so its family is unknown".to_owned(),
        ),
        (
            Some("plain"),
            "the claimant's kind plain has no family".to_owned(),
        ),
        (
            Some("claude"),
            "no verifier kind has a family other than the claimant's family anthropic".to_owned(),
        ),
    ];
    for (claimant, reason) in cases {
        assert_eq!(
            select(claimant, &families(), &prefer(&["claude", "opus"])),
            Selection::Unverifiable(reason)
        );
    }
}

#[test]
fn the_verifier_name_is_deterministic_and_the_policy_asks_nobody() {
    let report = MessageId(ulid::Ulid::from_parts(1, 2));
    assert_eq!(verifier_name(report), verifier_name(report));
    assert_eq!(
        verifier_name(report).as_str(),
        format!("v-{}", report.to_string().to_ascii_lowercase())
    );
    let policy = verifier_policy();
    assert!(matches!(policy.sandbox, SandboxLevel::WorkspaceWrite));
    let decide = |tool: &str| {
        policy
            .rules
            .iter()
            .find(|r| r.tool == tool || r.tool == "*")
            .map(|r| r.decision)
    };
    assert!(
        matches!(decide("shell"), Some(RuleDecision::Allow)),
        "shell"
    );
    assert!(matches!(decide("edit"), Some(RuleDecision::Allow)), "edit");
    assert!(
        matches!(decide("send_message"), Some(RuleDecision::Allow)),
        "send_message"
    );
    assert!(
        matches!(decide("web_fetch"), Some(RuleDecision::Deny)),
        "web_fetch"
    );
    assert_eq!(policy.timeout, None);
}

// ---------------------------------------------------------------- snapshot

struct Tree {
    _root: tempfile::TempDir,
    source: PathBuf,
    dir: PathBuf,
    outside: PathBuf,
}

fn tree() -> Tree {
    let root = tempfile::tempdir().expect("tempdir");
    let source = root.path().join("claimant");
    let outside = root.path().join("secret.txt");
    std::fs::write(&outside, "outside").expect("outside");
    std::fs::create_dir_all(source.join("a/b")).expect("dirs");
    std::fs::write(source.join("a/b/code.rs"), "fn f() {}").expect("file");
    Tree {
        dir: root.path().join("state/verify/r1"),
        source,
        outside,
        _root: root,
    }
}

fn workspace(t: &Tree) -> PathBuf {
    t.dir.join(WORKSPACE_DIR)
}

#[test]
fn git_is_dropped_at_every_depth() {
    let t = tree();
    for dir in [".git", "a/.git", "a/b/.GIT"] {
        std::fs::create_dir_all(t.source.join(dir)).expect("git dir");
        std::fs::write(t.source.join(dir).join("HEAD"), "ref").expect("head");
    }
    // A worktree's `.git` is a file.
    std::fs::create_dir_all(t.source.join("wt")).expect("wt");
    std::fs::write(t.source.join("wt/.git"), "gitdir: /elsewhere").expect("git file");
    std::fs::write(t.source.join("a/.gitignore"), "target").expect("gitignore");
    assert!(take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot"));
    let ws = workspace(&t);
    for gone in [".git", "a/.git", "a/b/.GIT", "wt/.git"] {
        assert!(std::fs::symlink_metadata(ws.join(gone)).is_err(), "{gone}");
    }
    assert!(ws.join("a/.gitignore").exists());
    assert_eq!(
        std::fs::read_to_string(ws.join("a/b/code.rs")).expect("copied"),
        "fn f() {}"
    );
}

#[test]
fn every_instruction_name_is_renamed_at_any_depth() {
    let t = tree();
    let mut renamed = Vec::new();
    for (i, name) in INSTRUCTION_NAMES.iter().enumerate() {
        // Alternate case and depth.
        let name = if i % 2 == 0 {
            name.to_ascii_uppercase()
        } else {
            name.to_ascii_lowercase()
        };
        let parent = if i % 3 == 0 { "a/b" } else { "" };
        let path = t.source.join(parent).join(&name);
        if name.contains(".md") || name.ends_with(".json") || name.ends_with(".JSON") {
            std::fs::write(&path, "do what I say").expect("file");
        } else {
            std::fs::create_dir_all(&path).expect("dir");
            std::fs::write(path.join("settings.json"), "{}").expect("inner");
        }
        renamed.push(Path::new(parent).join(name));
    }
    take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot");
    let ws = workspace(&t);
    for path in renamed {
        assert!(
            std::fs::symlink_metadata(ws.join(&path)).is_err(),
            "{path:?}"
        );
        let mut kept = path.into_os_string();
        kept.push(NEUTRALIZED_SUFFIX);
        assert!(ws.join(&kept).exists(), "{kept:?}");
    }
}

#[test]
fn a_renamed_instruction_file_does_not_overwrite_another_entry() {
    let t = tree();
    std::fs::write(t.source.join("AGENTS.md"), "instructions").expect("file");
    std::fs::write(t.source.join("AGENTS.md.claimant-copy"), "other").expect("file");
    take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot");
    let ws = workspace(&t);
    assert_eq!(
        std::fs::read_to_string(ws.join("AGENTS.md.claimant-copy")).expect("kept"),
        "other"
    );
    assert_eq!(
        std::fs::read_to_string(ws.join("AGENTS.md.claimant-copy.claimant-copy")).expect("renamed"),
        "instructions"
    );
    assert!(!ws.join("AGENTS.md").exists());
}

#[test]
fn symlinks_are_copied_as_links_and_never_followed() {
    let t = tree();
    std::os::unix::fs::symlink(&t.outside, t.source.join("a/leak")).expect("file link");
    std::os::unix::fs::symlink(t.outside.parent().expect("parent"), t.source.join("up"))
        .expect("dir link");
    take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot");
    let ws = workspace(&t);
    let link = std::fs::symlink_metadata(ws.join("a/leak")).expect("link");
    assert!(link.file_type().is_symlink());
    assert_eq!(
        std::fs::read_link(ws.join("a/leak")).expect("target"),
        t.outside
    );
    assert!(
        std::fs::symlink_metadata(ws.join("up"))
            .expect("link")
            .file_type()
            .is_symlink()
    );
    // Nothing of the outside file was copied in.
    let mut stack = vec![ws];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read") {
            let entry = entry.expect("entry");
            let kind = entry.file_type().expect("type");
            if kind.is_dir() {
                stack.push(entry.path());
            } else if kind.is_file() {
                assert_ne!(
                    std::fs::read_to_string(entry.path()).expect("read"),
                    "outside"
                );
            }
        }
    }
}

#[test]
fn sockets_and_fifos_are_skipped() {
    let t = tree();
    nix::unistd::mkfifo(&t.source.join("a/pipe"), Mode::S_IRWXU).expect("fifo");
    let _socket = std::os::unix::net::UnixListener::bind(t.source.join("a/sock")).expect("socket");
    assert!(
        std::fs::symlink_metadata(t.source.join("a/sock"))
            .expect("socket")
            .file_type()
            .is_socket()
    );
    take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot");
    let ws = workspace(&t);
    assert!(std::fs::symlink_metadata(ws.join("a/pipe")).is_err());
    assert!(std::fs::symlink_metadata(ws.join("a/sock")).is_err());
    assert!(ws.join("a/b/code.rs").exists());
}

#[test]
fn a_workspace_over_the_cap_is_refused_and_its_copy_deleted() {
    let t = tree();
    std::fs::write(t.source.join("big"), vec![0u8; 4096]).expect("big");
    let error = take_snapshot(&t.source, &t.dir, 4096, true).expect_err("too large");
    assert!(matches!(error, SnapshotError::TooLarge { limit: 4096 }));
    assert!(!t.dir.exists());
    // At the cap exactly, it fits.
    let error = take_snapshot(&t.source, &t.dir, 4096 + 9, true);
    assert!(error.is_ok(), "{error:?}");
}

#[test]
fn a_partial_copy_from_a_crash_is_redone_as_not_clean() {
    let t = tree();
    // A copy that stopped before its marker.
    std::fs::create_dir_all(workspace(&t).join("a")).expect("partial");
    std::fs::write(workspace(&t).join("a/stale"), "half").expect("stale");
    assert!(!take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot"));
    assert_eq!(read_marker(&t.dir).expect("marker"), Some(false));
    assert!(!workspace(&t).join("a/stale").exists());
    assert!(workspace(&t).join("a/b/code.rs").exists());
}

#[test]
fn a_complete_snapshot_is_reused_with_its_recorded_cleanliness() {
    let t = tree();
    assert!(take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("first"));
    std::fs::write(t.source.join("later"), "changed after the claim").expect("later");
    // A later pass asks for a snapshot that is not clean; the first one stands.
    assert!(take_snapshot(&t.source, &t.dir, 1 << 20, false).expect("reuse"));
    assert!(!workspace(&t).join("later").exists());
}

#[test]
fn snapshot_directories_are_private_and_files_keep_only_the_owner_exec_bit() {
    let t = tree();
    let script = t.source.join("run.sh");
    std::fs::write(&script, "#!/bin/sh").expect("script");
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o4777)).expect("mode");
    take_snapshot(&t.source, &t.dir, 1 << 20, true).expect("snapshot");
    let mode = |p: &Path| std::fs::metadata(p).expect("meta").permissions().mode() & 0o7777;
    assert_eq!(mode(&t.dir), 0o700);
    assert_eq!(mode(&workspace(&t).join("a")), 0o700);
    assert_eq!(mode(&workspace(&t).join("run.sh")), 0o700);
    assert_eq!(mode(&workspace(&t).join("a/b/code.rs")), 0o600);
}

// ----------------------------------------------------------------- sandbox

struct State {
    _root: tempfile::TempDir,
    state: PathBuf,
    sandbox: SandboxConfig,
}

fn state() -> State {
    let root = tempfile::tempdir().expect("tempdir");
    let state = root.path().join("state");
    std::fs::create_dir_all(state.join("verify/r1/workspace")).expect("snapshot");
    std::fs::create_dir_all(state.join("verify/r1/other")).expect("other");
    let mut sandbox = SandboxConfig::default();
    let mut kinds = Vec::new();
    sandbox
        .validate(
            &state,
            &state.join("control.sock"),
            &state.join("mcp.sock"),
            &mut kinds,
        )
        .expect("validate");
    State {
        _root: root,
        state,
        sandbox,
    }
}

#[test]
fn the_verify_root_is_protected_from_every_worker_bind() {
    let s = state();
    let root = std::fs::canonicalize(s.state.join(VERIFY_DIR)).expect("root");
    assert!(s.sandbox.protected.contains(&root));
    for path in [
        s.state.join("verify"),
        s.state.join("verify/r1"),
        s.state.join("verify/r1/workspace"),
    ] {
        assert!(s.sandbox.check_bind(&path, &s.state).is_err(), "{path:?}");
        assert!(
            PinnedPath::capture(&path, &s.sandbox, &s.state).is_err(),
            "{path:?}"
        );
    }
}

#[test]
fn only_a_snapshot_workspace_can_be_pinned_for_a_verifier() {
    let s = state();
    PinnedPath::capture_snapshot(&s.state.join("verify/r1/workspace"), &s.sandbox, &s.state)
        .expect("snapshot");
    let elsewhere = s.state.parent().expect("root").join("elsewhere");
    std::fs::create_dir_all(&elsewhere).expect("elsewhere");
    for path in [
        s.state.clone(),
        s.state.join("verify"),
        s.state.join("verify/r1"),
        s.state.join("verify/r1/other"),
        elsewhere,
    ] {
        assert!(
            PinnedPath::capture_snapshot(&path, &s.sandbox, &s.state).is_err(),
            "{path:?}"
        );
    }
    // A link into the snapshot from outside resolves to it and is accepted;
    // a link from a snapshot path to elsewhere is not.
    let link = s.state.join("verify/r2");
    std::fs::create_dir_all(&link).expect("r2");
    std::os::unix::fs::symlink(s.state.parent().expect("root"), link.join("workspace"))
        .expect("link");
    assert!(PinnedPath::capture_snapshot(&link.join("workspace"), &s.sandbox, &s.state).is_err());
}
