# agentd per-agent isolation

Status: implemented on `feat/agentd-isolation`, based on `main` at `c437918`.
The original design was prototyped on Linux with bubblewrap 0.13.1. The production
implementation is verified by the reproducible gates linked below.

## Decision

agentd launches each ACP agent incarnation in one bubblewrap sandbox. It uses
user, PID, mount, network, IPC, UTS and cgroup namespaces. It needs neither root
nor Landlock. Missing bubblewrap, denied namespaces or a missing kind command
fail preflight. agentd never silently falls back to an unsandboxed launch.

The agent sees read-only system and toolchain paths, its writable workspace,
a private persistent CLI home, a copy of the configured credential file, and one broker
directory at `/run/agentd/io`. That directory contains only its `mcp.sock` and,
in proxy mode, its `egress.sock`.

The MCP listener identifies the agent and incarnation. Sandboxed MCP entries
contain no bearer token, no MCP environment and no host socket path. Each frame
contains only a shim session namespace and the MCP request. A token field,
including an empty or null field, is rejected on a per-agent listener.

The host control socket, shared MCP socket, state directory, SQLite file, runtime
sockets and sibling processes are absent. The agent's `/proc` belongs to its PID
namespace. A private network namespace also removes host loopback services and
abstract UNIX sockets. These matter because a local control service can launch
an unsandboxed process on the agent's behalf.

Infinity is currently unsandboxed. Its adapter attaches to a shared host daemon
rather than launching a child. Confining that daemon and its existing sessions is
outside this change. Infinity counts as `none` in mixed-mode warnings and listings.

## Launch and lifetime

```text
agentd
  state/agents/<id>/io/mcp.sock       identity = agent + incarnation
  state/agents/<id>/io/egress.sock    per-agent CONNECT policy
  bwrap
    PID 1 reaper
    agentd sandbox-init              127.0.0.1:3128 -> egress.sock
      claude-agent-acp or codex-acp   ACP over inherited stdio
        agentd mcp --socket /run/agentd/io/mcp.sock
```

`KindConfig.isolation` describes confinement. The supervisor constructs a
`Launch` containing the program, arguments, environment and MCP entry. The ACP
adapter runs it without knowing the sandbox profile. Existing mode, model,
permission, resume, cancellation and mail-starvation timers remain in place.
CLI permission modes still apply inside bubblewrap.

`AgentPaths` derives private paths from state and agent ID. `seed` creates minimal
onboarding and settings only when missing. It never copies the human's MCP,
hooks, configuration or session history. Filesystem facts are collected before
`plan` builds the pure argument vector.

Each listener is bound before the ACP child starts. Broker-side UID checks run
on accepted connections. Revocation invalidates identity, cancels listeners and
connections, stops egress, and removes sockets only when their inodes still match.
The next incarnation receives fresh listeners. Shared MCP tokens remain available
only for `none` agents.

Bubblewrap uses `--die-with-parent` and `--new-session`. agentd's runtime remains
current-thread because parent-death notification is tied to the spawning thread.
Process-group termination remains in the ACP adapter. PID-namespace teardown
kills descendants even when they create a new session. Both graceful exit and
SIGKILL are covered by the real-sandbox tests.

## Mounts and environment

| Path inside | Source and access |
| --- | --- |
| `/usr`, `/etc` | Host, read-only |
| `/bin`, `/sbin`, `/lib`, `/lib64` | Mirror host symlinks, or bind real directories read-only |
| `/proc`, `/dev` | Private procfs and minimal devices |
| `/tmp`, `/run` | Fresh tmpfs |
| `$HOME` | Private persistent home, writable |
| `/run/agentd/bin/agentd` | Daemon executable, read-only |
| `/run/agentd/io` | Per-agent socket directory, writable |
| Kind `home` entries | Minimal initial files and directories inside the private home |
| Credential file | Copied into the private home, with JSON refresh synchronized by CAS |
| `ro_binds`, `rw_binds` | Operator-selected paths at the same absolute paths |
| Agent workspace | Writable, also the working directory |
| `/run/systemd/resolve` | Read-only when present, host-network mode only |

The host `Command` clears its environment before invoking bubblewrap. It supplies
`HOME`, `USER`, `LOGNAME`, `PATH`, `SHELL=/bin/bash`, `TMPDIR=/tmp`, `TERM=dumb`,
allowed `[bwrap].env` names and `kind.env`. Proxy mode sets both uppercase and
lowercase HTTP and HTTPS proxy variables, and clears both `NO_PROXY` spellings.
Values travel in the OS environment instead of bubblewrap `--setenv` arguments,
so an optional provider env token does not enter argv.

Configuration rejects `AGENTD_*`, display, runtime bus, SSH agent and loader
injection variables. Inherited host variables outside the allowlist disappear.
Unsandboxed launch still strips inherited `AGENTD_*` settings.

## Credential refresh and exposure

Defaults are `~/.claude/.credentials.json` and `~/.codex/auth.json`. Each kind
accepts at most one credential file under HOME, or `credentials = []`. At launch,
agentd opens every source path component without following symlinks. The source
must be a regular file no larger than 1 MiB. A missing file permits launch without
credentials. The copy goes into the private home at the same relative path.
The human's CLI directory is never mounted. Settings, instructions, hooks, agents,
plugins, new entries and late-created host files or sockets stay private.

Every two seconds while supervising an agent, and once after its session exits,
agentd reads the private copy through no-follow directory descriptors. Only regular
files within the size cap containing valid JSON can return to the host. It writes
a mode-0600 exclusive temporary file and renames it over the configured host file
only when the host still has the bytes last copied in or out for that agent.
Conflicts are logged and skipped. Broker synchronization is serialized. Successful
refreshes also reach live siblings of the same kind when their private copy and
baseline still match the old bytes. Superseded JSON values are remembered for the
broker's lifetime, including across agent restarts, and cannot be copied back
even with different whitespace or key order.

An agent can write arbitrary valid JSON into the human's credential file. The
broker cannot validate provider token freshness or intent. A CLI that caches
tokens in memory may need a restart after a sibling's refresh. Dummy-credential
tests prove copy, rename and conflict handling, not provider rotation semantics.
The compare and rename sequence cannot provide filesystem-level atomic CAS
against an unrelated host writer that ignores the broker's synchronization.

The recommended R1 route is a revocable provider token in `kind.env`, with
`credentials = []`. Use `CLAUDE_CODE_OAUTH_TOKEN` for Claude or `OPENAI_API_KEY`
for Codex. This leaves all CLI state private and avoids sharing a refresh token
and credential file. The token is intentionally readable inside that agent;
it is distinct from the removed broker MCP token. Do not commit provider tokens
in a repository.

The live gates use existing logins for one minimal turn each. They do not prove
OAuth refresh endpoints or provider token rotation. Those remain unverified.

## Network policy

Proxy mode is the default. `sandbox-init` relays TCP on `127.0.0.1:3128` to the
agent's UNIX egress socket. The host proxy accepts HTTPS `CONNECT host:port` only.
It checks an exact hostname or `*.suffix` and an allowed port, resolves the host
itself, and rejects the entire answer if any address is private, loopback,
link-local, CGNAT, ULA, unspecified, multicast or another blocked address range.
It connects to the checked address without another DNS lookup.

A denied request returns HTTP 403 with `blocked by agentd egress policy for @name`
and emits `Event::EgressDenied`. Each listener and relay limits active connections
to 64. Headers and DNS/connect waits are bounded. Egress events are display-only.

Default hosts are:

| Kind | Observed model/account host | Unverified OAuth or API hosts |
| --- | --- | --- |
| Claude | `api.anthropic.com` | `console.anthropic.com`, `platform.claude.com` |
| Codex | `chatgpt.com` | `auth.openai.com`, `api.openai.com` |

`mcp-proxy.anthropic.com` is deliberately excluded because it permits actions
through the human's account connectors. Telemetry and asset hosts are excluded.
The live turns completed with those denied. Proxy-unaware tools fail closed
because the namespace has no external route or DNS. The proxy validates every DNS
answer and tries each validated address within its connection timeout. After
CONNECT succeeds, it reads one bounded TLS ClientHello record within ten seconds.
SNI must match the CONNECT host, ignoring case. Missing SNI, ECH, non-TLS data,
malformed hellos and oversized records close the tunnel before forwarding bytes.

## Configuration reference

```toml
sandbox = "bwrap" # Linux default; non-Linux defaults to "none". "none" preserves unsandboxed launches.

[bwrap]
path = "/usr/bin/bwrap"
network = "proxy" # "host" is degraded debugging mode.
egress_ports = [443]
# Defaults include existing ~/.cargo/bin, ~/.rustup/toolchains and
# ~/.local/share/mise/installs. An explicit array replaces these defaults.
ro_binds = ["~/.cargo/bin", "/opt/acp/node_modules", "/opt/node"]
rw_binds = []
env = ["LANG", "LC_ALL", "TZ"]

[kind.claude]
command = ["claude-agent-acp"]
sandbox = "bwrap"
home = [".claude/", ".claude.json"]
credentials = ["~/.claude/.credentials.json"]
egress = ["api.anthropic.com", "console.anthropic.com", "platform.claude.com"]

[kind.codex]
command = ["codex-acp"]
credentials = ["~/.codex/auth.json"]
egress = ["chatgpt.com", "auth.openai.com", "api.openai.com"]

[kind.infinity]
wire = "infinity"
socket = "/absolute/path/to/infinity/daemon.sock"
# Always unsandboxed. sandbox = "bwrap" is rejected for this wire.
```

`home` entries are relative and cannot traverse upward. Paths expand `~`, must be
absolute, and cannot expose the state directory, its contents, or ancestors of
HOME, `/run`, `/run/user`, the actual control socket and its directory, or the
shared MCP socket. Read-only grants receive the same checks because they permit
UNIX socket connections. Workspace paths receive the same checks at spawn.
Workspaces under HOME, such as `~/code/x`, remain allowed. Startup rejects state
directories whose per-agent socket paths would exceed `sun_path`.
Egress patterns cannot be IP literals. Wildcards need at least two suffix labels,
as in `*.example.com`, and validation lowercases patterns. The kind `egress` list replaces its
defaults. `none` ignores home, credentials and egress configuration.

`agentctl ls` includes an `isolation` field per row with `bwrap+proxy`, `bwrap+host`
or `none`. On mixed configurations, startup warns by kind that unsandboxed agents
can act as the human and as any agent. The current CLI uses JSON rows, so this is
a new field rather than a text-table column. Labels are recorded only after a
successful session start. Host-network kinds also warn at startup that host
loopback, including local servers, is reachable.

## Reproducible gates

Run `cargo test -p agentd --test isolation -- --nocapture`. The Linux-only test
starts real agentd with fake ACP children under real bubblewrap. It covers blocked
control/state/SQLite and sibling proc paths, sibling MCP isolation, socket sender
attribution, token rejection, host-wide readable proc token scans, credential
rename, allowed public CONNECT, denied private/unknown CONNECT, and descendant
cleanup after SIGTERM and SIGKILL. The public CONNECT check uses `example.com:443`
and needs outbound network access. A real bwrap capability probe skips the test
when bubblewrap or namespaces are unavailable locally. With
`AGENTD_REQUIRE_SANDBOX=1`, unavailable confinement fails the test. Ubuntu CI sets
that variable; macOS remains excluded. A skip is not qualification.

The [driver](../../crates/agentd/tests/support/isolation_driver.py) accepts an
exported main binary, the current fake ACP binary and `--baseline`. This reproduces
control-socket human impersonation and sibling-token theft and impersonation on
`c437918`, with the same probes blocked on this branch. It removes its temporary workspace on success unless `--keep` is set. Failures
retain the workspace for inspection.

The [live driver](../../crates/agentd/tests/support/live_isolation.py) accepts an
agentd binary and a tool-install directory containing `node_modules/.bin`. Supply
a PATH that reaches node and both CLIs. It runs one `pong` turn per CLI in proxy
mode, records turn events and checks whether credential content changed. Optional
trailing kind names run only those gates. This is opt-in account use.

[Profile golden tests](../../crates/agentd/src/sandbox.rs) cover both kinds and
network modes, minimal idempotent seeding, config rejection and env-token argv
exclusion. [Proxy tests](../../crates/agentd/src/egress.rs) cover policy boundaries
and mixed public/private DNS answers.

Ubuntu 24.04 restricts unprivileged user namespaces through AppArmor, as described
in the [Ubuntu release notes](https://documentation.ubuntu.com/release-notes/24.04/).
The repository's Ubuntu 24.04 CI job already disables that restriction and now
installs bubblewrap. A hosted CI run has not been performed for this local branch.

Prototype evidence was collected outside the repository. Those archived prototype
runs are design evidence, not proof of this implementation. The committed tests
and drivers replace the prototype's file links and can regenerate implementation
evidence. The implementation handoff report records local run results separately.

## Remaining risks and swarm hooks

- R1. Agents can read configured provider credentials. A broker-side API forwarder
  that injects auth is the later fix. Env tokens limit the current grant.
- R2. An agent can write arbitrary valid JSON into the human's credential file.
  The broker cannot prove provider freshness. A CLI caching tokens in memory may
  need a restart after a sibling refresh. External host writers do not participate
  in broker synchronization, so the final comparison and rename have a race.
- R3. A writable workspace permits planting hooks or scripts that the human later runs.
  Per-agent clones and reviewed artifacts are future work.
- R4. Host-network mode restores loopback, abstract UNIX sockets and unrestricted
  network access. It is a debugging escape hatch, not equivalent confinement.
- R5. Nested user namespaces expose kernel code. A tested seccomp policy is later work.
- R6. Infinity and `none` agents can use host control and per-agent sockets. Isolation
  holds only when no untrusted host process or unsandboxed agent can do that.
- R7. Toolchain binds expose their full contents. Defaults bind `~/.cargo/bin`, not
  `~/.cargo`, which can contain registry credentials. Review custom binds.
- R8. Proxy-unaware tools fail closed. Agents may describe that as an environment error.
- R9. Read-only `/etc` exposes whatever the host user can read there.
- R10. Future CLIs may require currently denied telemetry or asset endpoints.

There are no resource budgets or mail quotas in this change. The swarm can extend
a broker-controlled task egress policy, use egress-denial events for scope escalation, substitute an
API-auth forwarder for the proxy, create per-agent clones, and launch bubblewrap
inside broker-owned resource scopes. No sandboxed process receives the host user bus.
