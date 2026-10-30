# agentd

Local ACP broker with `agentd`, `agentctl`, and a stdio MCP server for Claude and
Codex agents. The SQLite mailroom remains the delivery authority.

## Run

```sh
cargo build -p agentd --bins
agentd --config /path/to/kinds.toml
agentctl spawn reviewer --kind claude --cwd /path/to/project
agentctl spawn builder --kind codex --cwd /path/to/project --sandbox workspace-write "Ask @reviewer to check the plan."
agentctl send @reviewer "Focus on session expiry."
agentctl ls
agentctl approvals
agentctl approve <permission-id> <option-key>
agentctl cancel @builder
agentctl held [@builder]
agentctl release <message-id>
agentctl drop <message-id>
agentctl watch
```

The default state directory is `~/.local/state/agentd/`. `--state-dir` overrides it.
The daemon reads `kinds.toml` there when present. An explicit `--config` must exist.
Without a config, commands default to `claude-agent-acp` and `codex-acp` on PATH.

```toml
# Optional. Paths are relative to the daemon's working directory.
state_dir = "/home/me/.local/state/agentd"

[listen]
# Socket paths must be absolute; defaults are inside state_dir.
control_socket = "/home/me/.local/state/agentd/agentd.sock"
mcp_socket = "/home/me/.local/state/agentd/mcp.sock"

[kind.claude]
wire = "acp"
command = ["claude-agent-acp"]
mode = "default"
workspace_write_mode = "acceptEdits"
full_access_mode = "bypassPermissions"
idle_after_secs = 600 # 0 disables suspension
[kind.claude.env]
# Environment overrides for the shim.

[kind.codex]
wire = "acp"
command = ["codex-acp"]
mode = "read-only"
workspace_write_mode = "workspace-write"
full_access_mode = "agent-full-access"
idle_after_secs = 600
[kind.codex.env]
INITIAL_AGENT_MODE = "read-only"
```

Each kind can also provide `[kind.<name>.sandbox_args]` with `read_only`,
`workspace_write`, and `full_access` argument arrays. `--sandbox` selects the
launch arguments and ACP mode. Claude's `default` mode asks permission for edits;
it is not an OS read-only sandbox. The broker does not add OS isolation. Shims
must support the selected mode; a mode-setting failure prevents prompting.

`agentctl --socket /absolute/path` or `AGENTD_SOCKET` selects a custom control
socket. Responses are JSON except for the short send status. Watch emits JSON
lines, beginning with a snapshot. A `lagged` event signals dropped display events;
the stream is not a durable audit log.

State and socket directories must be owned by the current user with mode 0700.
Both sockets have mode 0600 and check peer UIDs. MCP uses a separate socket that
accepts only authenticated MCP relay requests. These boundaries exclude other
OS users, not processes already running as the broker's user.

Every ACP new/load receives an `agentd mcp --socket ...` stdio server entry.
`AGENTD_MCP_TOKEN` is injected into that entry's environment, never argv or control
output. The token contains 256 bits from the OS random source and binds one agent
and incarnation. Session cleanup revokes it; restart mints a new one. The two tools
are `send_message { to, body, reply_to? }` and `list_agents {}`. Send keys derive
from the MCP request ID and incarnation. Retries within that incarnation recover
the original mail, not a second envelope. This does not promise exactly-once
execution by the recipient.

Agent specs, resume handles and mail survive daemon restarts. SIGINT and SIGTERM
stop connections, shut down broker actors, then drain and close the mailroom.
Stale sockets are reclaimed; live sockets and regular files are preserved.

## Broker API

- `Broker::new(mailroom, kinds, config)` configures ACP kinds and crash policy.
  `KindConfig::claude(path)` selects `default`; `KindConfig::codex(path)` selects
  `read-only`. Both set the mode after new and loaded sessions, before prompting.
  Arguments, environment, MCP server entries, sandbox modes and arguments are configurable.
  `KindConfig::cancel_timeout` bounds prompt completion after cancel (default two
  seconds). Expiry crashes and reaps the session, settling mail by start evidence.
- `spawn(AgentSpec)` registers an agent and returns an `AgentHandle`. A second
  actor for that ID is rejected, including through another broker sharing storage.
  Specs persist in SQLite; the daemon restores actors on restart.
- `send(NewMessage)` accepts durable mail, then nudges the recipient. Sender
  identity is trusted in this in-process API; surfaces must authenticate it later.
  Human mail is user input. Peer mail carries the labelled `<agent-message>`
  framing. Idle delivery batches all queued envelopes in arrival order.
- `AgentHandle::{cancel,pending_permissions,answer_permission,phase,phases,stop}`
  control or observe an actor. Answers include the request's incarnation.
  Rules match an exact normalized tool name, or `*`; the first match wins.
  No rule means escalate to the caller. An optional timeout rejects once, or
  cancels if the agent provides no single-use rejection option.
- `events()` returns a bounded display-only broadcast receiver. Subscribers can
  lag; SQLite remains delivery authority. Activity never becomes agent input.
- `shutdown()` stops and reaps sessions, settles outstanding delivery, and waits
  for the actors to release storage. Call it before closing the mailroom.

`Phase` covers Dormant, Starting, Idle, Busy, Suspending, Ended, Crashed, Failed,
and NeedsRecovery. Busy agents never receive another turn. Five crashes in ten
minutes stop retries; backoff doubles from one second to a 60-second cap.
A failed resume enters NeedsRecovery without creating a fresh conversation.
Only resumable agents suspend on idle timeout. `None` on the spec inherits the
kind's timeout; `None` on the kind disables idle suspension.

The private adapter contract opens or resumes a session, submits turns, cancels,
answers permissions, receives normalized events and suspends. Resume tokens are
opaque persisted data. Prompt completion produces Committed, the only event
that consumes mail. A first execution update, permission request or prompt completion
records turn-start evidence. Only agent message/thought chunks, plans and tool
calls/updates count as execution updates; metadata and user echoes do not.
Protocol errors produce no receipt.

## Mailroom API

`Mailroom::open(path, Config::default()).await` opens a real SQLite file in WAL
mode with full synchronous commits. One dedicated thread owns the connection.
An exclusive SQLite lock rejects a second owner, including another process.
Methods enqueue a transaction and await its result without blocking Tokio.
Cancelling the caller after submission does not cancel the transaction. Retry
`accept` with the same sender and client key to recover its original result.
Call `close().await` to drain submissions, join the thread, and release the file.
Dropping the handle also drains submissions, but does not wait for thread exit.

- `model` exports ULID identities, validated names and bodies, recipients,
  senders, incarnations, immutable envelopes, delivery states and delivery class.
- `swarm` exports the five acts, outcomes with evidence and artifacts, tasks,
  states, stakes, budgets, epochs and exposure types.
- `Mailroom` registers and resolves minimal agents, accepts and reads messages,
  and reads conversation views.
- Delivery operations are `pending_for`, `delivery`, `mark_offered`,
  `mark_started`, `mark_consumed`, `session_ended`, `held`, `release`,
  `drop_held` and `retire`.
- Ledger operations are `task`, `record_spend`, `new_epoch` and `exposures`.
  A Request creates its task inside `accept`; there is no separate creation path.
- `Interceptor`, `Verdict`, `ConversationView`, `HopLimit`, `Config` and typed
  outcomes and errors describe the acceptance and storage boundary.

Only IDs returned in `OfferOutcome::offered` may be submitted to a session.
`mark_consumed` and `mark_started` require the offered incarnation. Unknown IDs,
wrong incarnations, duplicate IDs in a batch and illegal source states fail the
whole batch. Repeating an offer under the same incarnation, or a receipt for
mail already consumed, succeeds without changing anything. Session cleanup and
retirement select only eligible rows, so repeating cleanup changes nothing.

The first five offers are allowed by default. A sixth offer attempt moves Queued
to Held without submitting it or incrementing the counter. An operator release
resets attempts to zero. This grants another bounded set of attempts.

Private actor storage persists kind, incarnation and resume token. Startup
settles offers from the previous incarnation before opening a process. Offers
with durable turn-start evidence become Uncertain; others return to Queued.
The live actor lease remains held across idle suspension and crash backoff.

## Verification

`cargo test -p agentd` runs SQLite tests and subprocess integration tests.
`agentd-fake-acp` is a scriptable test fixture binary, not a broker executable.
`tests/surfaces.rs` runs both executables and fake ACP agents through the injected
MCP entry, including retry, token revocation, human commands and restart.

The ignored `live_claude_sends_through_broker_mcp` test uses `AGENTD_CLAUDE_ACP`
and approves permission prompts through agentctl. It delivers to a fake peer.
Run it with `cargo test -p agentd --test surfaces live_ -- --ignored --nocapture`.

The ignored `live_claude_acp` and `live_codex_acp` tests use logged-in shims at
`AGENTD_CLAUDE_ACP` and `AGENTD_CODEX_ACP`. Run them with
`cargo test -p agentd --test supervisor live_ -- --ignored --nocapture`.
They verify a first turn, idle suspension, session load and remembered context.
