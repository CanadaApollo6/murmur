# agentd per-agent isolation

Status: implemented on `feat/agentd-isolation`, based on `main` at `c437918`.
The original design was prototyped on Linux with bubblewrap 0.13.1. The production
implementation is verified by the reproducible gates linked below.

## Decision

agentd launches each ACP agent incarnation in one bubblewrap sandbox. It uses
user, PID, mount, network, IPC, UTS and cgroup namespaces. It needs neither root
nor Landlock. Missing bubblewrap, denied namespaces or a missing kind command
fail preflight. FD binds require [bubblewrap 0.10.0 or newer](https://github.com/containers/bubblewrap/releases/tag/v0.10.0). Ubuntu 24.04's
0.9.0 lacks them, so Linux CI installs a pinned 0.13.1 build. agentd never silently falls back to an unsandboxed launch.

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

Every sandboxed launch also installs one seccomp-BPF filter through bubblewrap's
`--add-seccomp-fd`. It returns `EAFNOSUPPORT` for `socket` and `socketpair` with
`AF_VSOCK`. Vsock can cross network namespaces when `net.vsock.ns_mode` is
`global`, bypassing the CONNECT proxy. The filter returns `ENOSYS` for `keyctl`,
`add_key` and `request_key`. Session keyrings survive fork and exec, including
bubblewrap's `--new-session`. It also returns `ENOSYS` for `io_uring_setup`,
`io_uring_enter` and `io_uring_register`. io_uring can create sockets without
calling `socket`.

Other native syscalls remain allowed. Foreign audit architectures are killed.
On x86_64, syscalls with the x32 bit set return `ENOSYS`. The filter supports
little-endian Linux x86_64 and aarch64. Other architectures fail preflight.
Filter construction or installation failures prevent launch. This also applies
to host-network mode.

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
accepts at most one credential file under HOME, or `credentials = []`. At startup,
agentd resolves existing credential parents, allowing symlinked CLI directories
such as stow layouts. Missing parents remain protected by their lexical path and
resolved ancestors, including targets of dangling symlinks, and are resolved again
at launch. Host I/O uses the resolved directory with no-follow traversal and a no-follow leaf. The source must be a regular file no
larger than 256 KiB. These filesystem errors fail the affected launch with a named
reason in the log and `agentctl ls`, while the daemon remains available. A missing
file or parent permits launch without credentials. The copy goes into the private home at the same relative path.
The human's CLI directory is never mounted. Settings, instructions, hooks, agents,
plugins, new entries and late-created host files or sockets stay private.

Every two seconds while supervising an agent, and once after its session exits,
agentd reads the private copy through no-follow directory descriptors. Only regular
files within the size cap containing JSON objects with exactly the baseline's
top-level key set can return to the host. It writes
a mode-0600 exclusive temporary file and renames it over the configured host file
only when the host still has the bytes last copied in or out for that agent.
Conflicts are logged and skipped. Broker synchronization is serialized. Successful
refreshes also reach live siblings of the same kind when their private copy and
baseline still match the old bytes. The broker remembers SHA-256 digests of normalized superseded JSON for its
lifetime, including across agent restarts. Whitespace and key order cannot
re-admit a retired value. Each host path has at most 1024 digests. Once full,
copy-back stops for that path and warns once; history is never evicted. Each
agent can copy back at most once per 30 seconds, across incarnations, and the
next tick after that interval reads the latest private value. Unchanged rejected
private bytes are remembered and are not parsed or logged again.

An agent can replace values within the baseline's top-level key set in the
human's credential file. Same-kind siblings can therefore pass small JSON values
through credential propagation outside mail attribution. Shared workspaces already
allow a sibling channel; credential propagation is intentionally retained. The
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
CONNECT succeeds, it reassembles one TLS ClientHello across consecutive handshake
records within ten seconds and a 64 KiB total record budget.
SNI must match the CONNECT host, ignoring case. Missing SNI, ECH, non-TLS data,
malformed hellos, interleaved non-handshake records and oversized hellos close
the tunnel and emit `Event::EgressDenied`. The original record bytes are forwarded
unchanged after validation. All configured egress ports are TLS-only.

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
HOME, the actual control socket and its directory, the shared MCP socket, or
any configured Bwrap credential file or its resolved parent. All paths under
`/run` are rejected. Read-only grants receive the same checks because they permit
UNIX socket connections. Workspace paths receive the same checks at spawn.
Workspaces under HOME, such as `~/code/x`, remain allowed. At spawn the broker
persists the canonical workspace path with its filesystem ID from `fstatfs` and
inode. An all-zero filesystem ID falls back to device and inode. Legacy rows
upgrade at their next validated start only if the canonical path and inode still
match the recorded pin. Configured file and directory bind identities are recorded
at startup in `state/sandbox-binds.json` and retained
across restarts. Spawn and re-spawn reject symlinks inside any agent's pinned
workspace or a writable bind, naming the link and its canonical target. A tree
is matched by path prefix and by filesystem ID and inode, so a bind-mount alias
of a pinned tree is caught too. Symlinks
outside those trees remain allowed. A human `spawn` of a Failed agent's name
re-pins its workspace only if the cwd still resolves to the pinned canonical path,
retaining its identity and mailbox. A different canonical cwd requires a new name.
Spawn replies include the canonical cwd. To authorize configured bind
replacements, restart with `--repin-binds`.
The flag validates all sources before capturing them. A failed re-pin retains any
existing pin. Binds accept regular files and directories; workspaces require
directories. A file bind stops matching after a tool replaces the file by rename,
such as `git config --global`. Restart with `--repin-binds` to authorize the
replacement, or bind a directory that contains the file. Every launch opens each
source with `O_PATH|O_CLOEXEC`, plus `O_DIRECTORY` for workspaces, compares identity
and rechecks the fd's resolved
path. Bubblewrap receives `--bind-fd` or `--ro-bind-fd`, verifies the mounted
identity, and closes each fd before executing the child. Broker fds retain
CLOEXEC; only the fork child clears it for these explicit bwrap fds.
The seccomp filter uses the same FD inheritance path. Bubblewrap closes its FD
before executing the child.
A mismatch fails the launch with a reason in the log and `agentctl ls`'s `error`
field. An invalid restored workspace or bind does not abort the entire daemon.
First-use and previously pinned bind failures disable sandboxed launches while
unsandboxed kinds remain available. Bind capture is skipped when every kind uses
`sandbox = "none"`.
Private sockets bind via `/proc/self/fd`, so state-path length does not constrain
their `sun_path`.
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
rename, a real TLS handshake to example.com with matching SNI, refusal of mismatched
SNI, denied private/unknown CONNECT, and descendant
cleanup after SIGTERM and SIGKILL. Separate real-launch tests cover a host vsock
listener, inherited session keyring reads and io_uring denial. Only the vsock test
skips when the host cannot create a vsock listener. The public CONNECT check uses
`example.com:443` and needs outbound network access. A real bwrap capability probe skips the test
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
- R2. An agent can change values within the baseline credential's top-level key set
  and propagate small JSON values to same-kind siblings outside mail attribution.
  Shared workspaces already allow a sibling channel; propagation is retained.
  The broker cannot prove provider freshness. A CLI caching tokens in memory may
  need a restart after a sibling refresh. External host writers do not participate
  in broker synchronization, so the final comparison and rename have a race.
- R3. A writable workspace permits planting hooks or scripts that the human later runs.
  Per-agent clones and reviewed artifacts are future work.
- R4. Host-network mode restores loopback, abstract UNIX sockets and unrestricted
  network access. It is a debugging escape hatch, not equivalent confinement.
- R5. Nested user namespaces expose kernel code. The seccomp filter blocks vsock,
  keyring operations and io_uring. Broader seccomp hardening is still later work.
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
