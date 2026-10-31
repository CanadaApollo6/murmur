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
Missing or archived roots and refused resumes enter `NeedsRecovery` without
creating a replacement thread. EOF reconnects with a new incarnation. Unconsumed
mail is re-offered. Human and peer mail both use `DeliverPeerMessage`, with
stable dedup ID `peer-{MessageId}`. Human mail sets `from` to `human` and identifies
the human operator in its rendered body. Mail waits behind an active tool and
wakes passive sleep without interrupting inference. Delivery does not require
`ThreadIdle` or `Replay.idle`. Those signals still drive phase display and
cancellation acknowledgment. Use `agentctl cancel` to stop a runaway agent.
Only matching root `InputCommitted` receipts consume mail. Output, echoes, and
replay history are not receipts. The separate Infinity history and dedup-ID
writes retain a crash window that can duplicate either sender's mail on re-offer.

If an Infinity cancellation deadline expires, the actor reconnects and reissues
`Interrupt` when replay is busy, with a fresh cancellation deadline. Repeated
failures reach the existing crash limit. Core retains accepted tool results and
retries failed interrupt writes every 100 ms for three seconds. At exhaustion it
holds the pending result, emits an error, and stops inference and automatic
write retries. Repair storage and explicitly interrupt to retry. Retaining an
accepted result through a storage outage requires the daemon to remain alive.
RAP cancellation notifications have a two-second timeout and cannot block
settlement forever.

Each resident runtime has a fresh epoch and an in-memory registry of dispatched
calls. Before persisting a tool call or invoking its executor, core writes a
dispatch record with the call ID, epoch, and absolute history position. The
registry marks that exact occurrence as awaiting a result when execution starts.
Async RAP calls remain live after dispatch returns, until their callback arrives
or the call is interrupted. Dropping an executor invocation future removes its
live claim. Accepting a result replaces the claim with the retained result.

Before a driver exits, accepted results of recorded calls enter bounded
settlement, even if the follow-up provider request failed before any output.
Settlement preserves the real result without validating unrelated mail. On load
or attachment, a recorded pending call is orphaned if its exact occurrence is
absent from the runtime's awaiting set. Recovery persists its retained result
when available. Otherwise it uses `interrupted: result lost` for a same-epoch
orphan and `interrupted: daemon restarted` for an older epoch. Epochs remain an
explicit runtime boundary and retain the existing store format. An epoch match
alone never proves a call is live. Late callbacks cannot add a second result.

A record without a matching pending call is cleared without another result.
Recovery never re-executes tools or starts inference.

Seeds and legacy calls without dispatch records remain untouched. Existing
stores cannot recover orphaned calls from before this policy. Those calls need
a real result or an explicit interrupt. A failed dispatch-record write prevents
call persistence and execution. Core retries through bounded settlement. If a
retry succeeds, it persists the call with a synthetic failure result. If retries
expire, it reports an error and holds the thread without persisting the call.
Repair storage and explicitly interrupt to retry held settlement.

An idle-derived receipt deadline is cleared when a later root completion starts,
including the first turn after attachment. Inference may outlast the receipt
timeout; a new core idle event arms receipt recovery if a receipt is missing.
Model agents never idle-suspend. Both human and peer mail can enter a busy
Infinity session unless an approval or cancellation is pending.

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
