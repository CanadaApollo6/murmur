# agentd

Local broker with `agentd`, `agentctl`, Infinity model agents, and a stdio MCP
server for Claude and Codex agents. The SQLite mailroom remains the delivery authority.

## Run

```sh
cargo build -p agentd --bins
agentd --config /path/to/kinds.toml
agentctl spawn planner --kind infinity --cwd /path/to/project
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

Infinity connects to an already running `infinity daemon`. Add a kind with its
absolute Unix socket path. The model uses `provider/model` syntax. `spawn --model`
overrides the configured model for a new thread.

```toml
[kind.infinity]
wire = "infinity"
socket = "/home/me/.infinity/daemon.sock"
model = "local/qwen3-coder"
```

Each Infinity agent owns one root thread, saved in its opaque resume token.
A daemon-reported missing or archived root enters `NeedsRecovery`. Open timeouts
and transient resume errors use normal crash backoff without creating a
replacement thread.
EOF reconnects with a new incarnation. Unconsumed mail is re-offered. Human and
peer mail both use `DeliverPeerMessage`, with stable dedup ID `peer-{MessageId}`.
Human mail sets `from` to `human` and identifies the human operator in its body.
Mail waits behind an active tool and wakes passive sleep. Only exact root
`InputCommitted` receipts consume mail. Output and replay are not receipts.
Separate history and dedup-ID writes retain a window that can duplicate mail.

The daemon includes a random process boot ID in `Welcome`. The adapter saves it
in the resume token. When a reopened root has a different boot ID and
`Replay.idle` is false, agentd sends one `Interrupt`. It uses the same rule after
a driver-failure protocol error from the root or any child on the connection.
A stopped root ends the attachment without spending the crash budget and records
that it needs an interrupt. Connect clears and persists shutdown even when the
driver is still live. Recovery interrupts busy replay unless `in_progress` shows
a surviving model completion. CLI quit kills RAP servers but lets that completion
finish. Pending non-passive tool calls still require interruption.
An archived root requires recovery instead of reconnecting. Core settles the call
durably and removes pending choices. Recovery never invokes the tool again.
Only agentd-attached threads auto-recover after a daemon restart. An executor
lost inside a running daemon needs `agentctl cancel` unless a driver failure or
session shutdown signals the loss. Detached sleep tasks catch panics and return
a terminal tool error. Invalid or unrepresentable sleep durations return an error
before a task starts. The adapter persists its recovery marker when it changes,
including when idle or a new completion clears it, so an unclean agentd exit
does not restore a cleared marker.

An Interrupt starts a driver even for an orphaned pending choice. Cancellation
has a deadline. Reconnects reissue an outstanding cancellation when
replay is busy. A later `StartOutput` or `ThinkingStart` acknowledges cancellation
and clears its deadline while the new completion continues. Core preserves
accepted real results during settlement. It also persists leading results that
answer pending calls before driver idle or after a step error, including a
provider failure. Unrelated unvalidated mail stays unpersisted.

Failed settlement writes retry every 100 ms for three seconds, then enter Held.
Held runs no inference and retains pending results. It retries on new input,
interrupt, and a five-second timer. Closed channels release a Held driver during
shutdown. A failed choice write enters this same bounded settlement path, so a
call cannot wait forever for a prompt that was never saved. RAP cancellation
notifications have a two-second bound. Retaining accepted results during a
storage outage requires the daemon to remain alive. Receipt deadlines and
exhausted or held settlement errors reconnect without spending the crash budget.
Interrupt deadlines also retry, but `crash_limit` expiries without a committed
input, completed turn, or cancellation acknowledgment enter `NeedsRecovery`.
Successful attachment alone does not reset retry backoff. Mailroom `max_attempts`
still bounds unreceipted delivery and moves exhausted mail to Held. Repairing
storage permits automatic recovery until the interrupt retry limit is reached.

The receipt deadline starts when submission begins on an idle root. A later
completion start clears it, so inference may outlast the receipt timeout. A new
idle event rearms it for outstanding mail. A silent daemon cannot leave offered
mail waiting forever. Model agents never idle-suspend. An unreachable daemon
uses backoff capped at 60 seconds without consuming the model agent crash limit.
Other connected-session failures count as crashes.
ACP spawn failures keep their existing crash policy.

Infinity's daemon owns sandbox and RAP tool configuration. The socket API cannot
apply agentd's per-agent sandbox launch flags or MCP entries. The adapter rejects
launch settings in Infinity kind config. `UserChoiceRequired` becomes an
incarnation-tagged approval under `infinity.user_choice`. Options use zero-based
index keys and preserve the daemon's labels. Infinity does not supply tool names
or authorization effects, so allow/deny rules cannot auto-select these choices.
A timeout interrupts the turn instead of guessing an answer. The inbound adapter
does not install `send_message`. That requires a separate authenticated RAP toolset.

Each kind can also provide `[kind.<name>.sandbox_args]` with `read_only`,
`workspace_write`, and `full_access` argument arrays. `--sandbox` selects the
launch arguments and ACP mode. Claude's `default` mode asks permission for edits;
it is not an OS read-only sandbox. The broker does not add OS isolation. Shims
must support the selected mode; a mode-setting failure prevents prompting.

`agentctl --socket /absolute/path` or `AGENTD_SOCKET` selects a custom control
socket. Responses are JSON except for the short send status. Watch emits JSON
lines, beginning with a snapshot. A `lagged` event signals dropped display events;
the stream is not a durable audit log. Held mail streams one envelope per socket
frame, followed by a completion frame; `agentctl held` collects these into one
JSON array. Each socket permits up to 64 concurrent connections. Excess
connections are closed; accept errors are logged and retried after 100 ms.
Agent children do not inherit daemon variables whose names start with `AGENTD_`.
The MCP entry supplies its binding token separately.

State and socket directories must be owned by the current user with mode 0700.
Both sockets have mode 0600 and check peer UIDs. MCP uses a separate socket that
accepts only authenticated MCP relay requests. These boundaries exclude other
OS users, not processes already running as the broker's user.

Every ACP new/load receives an `agentd mcp --socket ...` stdio server entry.
`AGENTD_MCP_TOKEN` is injected into that entry's environment. For Claude,
claude-agent-acp passes the entry to the Claude SDK, which includes its environment
in `claude --mcp-config <json>`. The token is therefore visible in
`/proc/<pid>/cmdline`. The broker does not include it in control responses.
The token contains 256 bits from the OS random source and binds one agent and
incarnation. Session cleanup revokes it; restart mints a new one. The two tools
are `send_message { to, body, reply_to? }` and `list_agents {}`. Send keys derive
from the incarnation, a random namespace created at each MCP shim startup, and
the MCP request ID. Retries within one shim session recover the original mail.
A replacement shim can reuse request IDs without losing new mail. A retry across
shim restarts can produce a duplicate. Recipient execution remains at least once.

Agent specs, resume handles and mail survive daemon restarts. SIGINT and SIGTERM
stop connections, shut down broker actors, then drain and close the mailroom.
Stale sockets are reclaimed; live sockets and regular files are preserved.

## V1 limitations

Any process running as the same OS user, including an agent with shell access,
can use the control socket as the human. It can also read sibling shims' tokens
from `/proc` and impersonate other agents. Agent isolation is best-effort until
agents run under separate UIDs or a sandbox with a private `/proc`.

There is no per-sender mail quota. An agent can flood another agent's queue
because leaving out `reply_to` restarts the hop count. A per-sender cap and a
per-turn batch limit are follow-up work.

Token redaction in activity output works per frame and is best-effort. A token
split across frames may appear without redaction.

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
  CLI agents receive human mail as user input. Infinity receives deferrable
  operator mail. Peer mail carries the labelled `<agent-message>` framing.
  Idle delivery batches all queued envelopes in arrival order.
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
and NeedsRecovery. Busy CLI agents wait for a turn boundary. Infinity accepts
deferrable mail while busy. Five crashes in ten
minutes stop retries; backoff doubles from one second to a 60-second cap.
A failed ACP resume or a missing Infinity root enters NeedsRecovery without
creating a fresh conversation.
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
