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
On Linux, both kinds are preflighted inside bubblewrap. Non-Linux targets default
to `sandbox = "none"`. Install both commands or explicitly
set `sandbox = "none"` for kinds you do not intend to sandbox. An Infinity-only
configuration can set top-level `sandbox = "none"` because Infinity remains on the host.

```toml
# Optional. Paths are relative to the daemon's working directory.
state_dir = "/home/me/.local/state/agentd"

[listen]
# Socket paths must be absolute; defaults are inside state_dir.
control_socket = "/home/me/.local/state/agentd/agentd.sock"
mcp_socket = "/home/me/.local/state/agentd/mcp.sock"

[mail]
# Omitted values keep these defaults. 0 disables a limit.
per_sender = 64 # Undelivered mail from one agent across all recipients.
per_pair = 16   # Undelivered mail from one agent to one recipient.
per_turn = 16   # Envelopes offered to a session in one pump.

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

Queued, offered, uncertain, and held mail count toward the sender and pair limits.
Consumed and dead mail free a slot. Human sends are exempt from both limits.
A send at the limit returns an error asking the agent to wait for delivery.
Mail beyond a batch stays queued for a later pump.

Infinity connects to an already running `infinity daemon`. Add a kind with its
absolute Unix socket path. The model uses `provider/model` syntax. `spawn --model`
overrides the configured model for a new thread.

```toml
[kind.infinity]
wire = "infinity"
socket = "/home/me/.infinity/daemon.sock"
model = "local/qwen3-coder"
stall_timeout_secs = 600 # 0 disables mail-starvation cancellation
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
The adapter opts in to `InputAdmitted` events on create and reconnect. An exact
admission retires the mail's Waiting deadline without consuming it. Older daemons
ignore the opt-in field and never send admission events. Waiting then stays armed
until `InputCommitted`, the conservative behavior used before admission support.
Separate history and dedup-ID writes retain a window that can duplicate mail.

The daemon includes a random process boot ID in `Welcome`. The adapter saves it
in the resume token. When a reopened root has a different boot ID and
`Replay.idle` is false, agentd sends one `Interrupt`. It uses the same rule after
a driver-failure protocol error from the root or any child on the connection.
The daemon settles outstanding calls from command-backed RAP servers when
ShutdownSession or ArchiveSession kills those servers. Each call receives a
terminal tool error. A stopped live driver can continue without agentd detecting
the loss. An archived driver records terminal results but starts no new model
completion or tool dispatch. Archive closes admission before stopping servers. Calls to external servers and MCP proxies survive because those servers
stay running. Their outstanding choices remain answerable after quit. Unknown
or stale choice answers receive a separate `UserChoiceError` without crashing agentd. Model streams also continue
after a CLI quit.
A stopped root reconnects without spending the crash budget or requesting an
Interrupt. Connect clears and persists shutdown so the router admits mail again.
An archived root requires human recovery and remains closed to event input,
including after Interrupt or Connect starts a driver.
Daemon boot changes and driver failures still mark busy replay for Interrupt.
Only agentd-attached threads auto-recover after a daemon restart. Other executor
loss is bounded by the mail-starvation timer while mail waits, or can be
interrupted with `agentctl cancel`. Detached sleep tasks catch panics and return
a terminal tool error. Invalid or unrepresentable sleep durations return an error
before a task starts. The adapter persists its recovery marker when it changes,
including when idle or a new completion clears it, so an unclean agentd exit
does not restore a cleared marker.

Every kind has a mail-starvation stall timer in the supervisor. It bounds mail
queued or deferred behind running work, including mail offered to a busy Infinity
thread. Mail included in the running completion does not arm it. Batch-mates
core has not picked up yet count as other waiting mail, even from one submit.
An initial uncommitted Infinity delivery uses the adapter's receipt deadline
instead. If a completion
finishes without committing an offer, that mail is waiting behind other work
and arms the stall timer until core admits it into a completion. Admission is
not a durable receipt. Cancelling a `Broker::send` caller cannot separate
durable acceptance from the actor wakeup.
`stall_timeout_secs` seconds without real session activity invokes the existing
cancel path. Stream chunks, tool starts and results, completion boundaries,
new choices, receipts, and turn ends reset the clock. Reconnects, restored or
replayed choices, and new mail never reset it. Pending permissions and choices
keep the timer running, even with `permissions.timeout = None`. If other mail
waits and the human takes longer than `stall_timeout_secs`, the turn is cancelled.
Raise the limit or set it to `0` for long human waits with mail flowing.
A pending choice with no other mail waiting still waits indefinitely when
`permissions.timeout = None`. A finite permission deadline stays tied to the
original wire choice ID across reconnects; replay does not renew it, even after
a local answer. Daemon dismissal or authoritative idle retires that deadline. A new live question
replacing an answered choice also gets a fresh deadline, including on the same
tool call.
With no waiting mail, there is no stall deadline. Each `[kind.NAME]` can set
`stall_timeout_secs`; `0` disables it. `KindConfig::stall_timeout` uses a Duration.
The default is 600 seconds for every kind. A tool that runs silently longer than
this limit while other mail waits gets cancelled. Raise the value or disable it
for such tools. Cancellation uses the existing acknowledgment policy, including
Infinity's ten-minute no-progress bound to `NeedsRecovery`.

Migrating agentd-hosted sessions is unsupported. agentd never migrates the
sessions it hosts. Daemon migration retains main's behavior without a migration
freeze or rollback guarantee. Archive admission gates apply independently.

Follow-up: migration handoff protocol: freeze, settle or transfer in-flight calls,
transfer processed-ID dedup state, commit/reconcile, crash/cancel-safe rollback,
notify attached brokers. This covers Astra r16 P2 #2 through #4, Opus r16 P2-1, and the
r14/r15 migration findings about admission races, stranded results, and failed
migration archiving the source. Same-host migration requests are rejected before
source cleanup. Answered-choice dismissal is also deferred:
the daemon retains a choice until its tool returns, so removing it on answer
must update the live driver and notify attached clients as well as persistence.
The stall timer bounds a hung tool after an answer while other mail waits.

Command-backed RAP calls use an opaque invocation scope in their callback URL.
A real callback and shutdown error share one dedup ID; a later call with a reused
model ID has a different scope. Core settlement and cancellation retire registry
entries. External servers and MCP proxies need no shutdown registry entry.
Shutdown records the first terminal result received locally. A shutdown error
means the tool's outcome may be unknown; check side effects before retrying a
non-idempotent operation. A real result received during server termination wins
if it retires the entry before shutdown drains it.

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
Interrupt deadlines also retry. Ten minutes of repeated expiry without a
committed input, completed turn, or cancellation acknowledgment enters
`NeedsRecovery`.
Successful attachment alone does not reset retry backoff. Mailroom `max_attempts`
still bounds unreceipted delivery and moves exhausted mail to Held. Repairing
storage permits automatic recovery within that ten-minute window.

The receipt deadline starts when submission begins on an idle root. A later
completion start clears it, so inference may outlast the receipt timeout. A new
idle event rearms it for outstanding mail. A completion that commits only CLI
or subscription input leaves broker mail subject to the stall timer. A silent
daemon cannot leave offered
mail waiting forever. Model agents never idle-suspend. An unreachable daemon
uses backoff capped at 60 seconds without consuming the model agent crash limit.
Other connected-session failures count as crashes. A model stream that stalls
without finishing is bounded by the mail-starvation timer when mail waits.
ACP spawn failures keep their existing crash policy.

Infinity's daemon owns sandbox and RAP tool configuration. The socket API cannot
apply agentd's per-agent sandbox launch flags or MCP entries. The adapter rejects
launch settings in Infinity kind config. `UserChoiceRequired` becomes an
incarnation-tagged approval under `infinity.user_choice`. Options use zero-based
index keys and preserve the daemon's labels. Infinity does not supply tool names
or authorization effects, so allow/deny rules cannot auto-select these choices.
A timeout interrupts the turn instead of guessing an answer.

### Give Infinity agents `send_message`

When an Infinity kind is configured, agentd starts a RAP server. Set its fixed
loopback address in `kinds.toml`. The default is `127.0.0.1:7433`. A bind failure
stops startup. Port 0 and non-loopback addresses are rejected.

```toml
[rap]
listen = "127.0.0.1:7433"
```

Add this server to `~/.infinity/rap.json` by hand. Keep any existing entries.
No other toolset may offer `send_message`.

```json
{
  "tool_sets": [
    { "type": "toolset_server", "server_url": "http://127.0.0.1:7433" }
  ]
}
```

The toolset advertises only `send_message`. agentd claims each invocation over a
separate Unix connection to the configured Infinity daemon. The daemon supplies
the arguments, callback URL, and root-first thread stack. agentd binds the root
to the live agent incarnation. Child threads send as that root agent and share
its mail limits. HTTP arguments and ancestry do not grant authority.

Duplicate invocations share one send. Completed results remain cached for ten
minutes, including across binding revocation or replacement. A retry after agentd restarts recovers
the existing mail receipt through the invocation's mailroom key.

HTTP 200 acknowledges the invocation before the send finishes. The spawned task
owns execution and callback delivery. Verified send errors also use the callback.
HTTP 403 rejects invalid invocation UUIDs or claims. A valid claim without a
restored binding, or a temporarily unreachable daemon, receives HTTP 503.

The Infinity daemon retries transport errors and HTTP 503 for up to 60 seconds.
Every attempt uses the same UUID and body. After an ambiguous attempt, later
HTTP rejections cannot establish failure. Exhaustion produces this result text.

> The outcome is unknown: the operation may have been performed. Check before repeating it.

After HTTP acknowledgment, the daemon waits 30 seconds for a callback, then
re-POSTs the same invocation. Recovery continues with backoff until the callback
settles it or the ten-minute total deadline produces the outcome-unknown result.

Callback POSTs have no redirects. One delivery worker per completed invocation
retries with exponential backoff capped at three seconds until acknowledgment
or ten-minute expiry. At most 32 workers send callbacks concurrently. Revocation
and rebinds preserve the verified callback destination and result.

Start the separate Infinity daemon with `INFINITY_WS=off` to disable its
unauthenticated WebSocket listener. `INFINITY_WS_PORT=0` still selects an
OS-assigned port. agentd warns on each attach when the listener is enabled.
Any local process can drive Infinity sessions through that listener. The RAP
callback listener also remains unauthenticated. These limits are recorded in
[the roadmap](../../docs/murmur/roadmap.md#carried-risks).

Each kind can also provide `[kind.<name>.sandbox_args]` with `read_only`,
`workspace_write`, and `full_access` argument arrays. `--sandbox` selects the
launch arguments and ACP mode. Claude's `default` mode asks permission for edits;
it is not an OS read-only sandbox. Bubblewrap confinement is described below. Shims
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

## Agent isolation

On Linux, ACP agents run in per-incarnation bubblewrap sandboxes by default.
Each has private user, PID, mount, network, IPC, UTS and cgroup namespaces.
The control socket, state database and sibling processes are absent. Identity
comes from a per-agent MCP socket, with no broker token in argv or environment.
A private network namespace routes HTTPS CONNECT through an allowlist proxy.
Missing bubblewrap, unavailable namespaces or missing commands fail startup preflight.

See [the isolation design and configuration reference](../../docs/murmur/isolation.md)
for `[bwrap]`, per-kind `home`, `credentials`, `egress`, env-token examples and gates.
File binds stop matching after tools rewrite the file by rename, such as
`git config --global`. Restart with `--repin-binds` or use a directory bind.
Existing CLI permission modes still apply inside the sandbox. `agentctl ls` shows
`isolation` as `bwrap+proxy`, `bwrap+host` or `none`.

Set top-level or per-kind `sandbox = "none"` for the unsandboxed escape hatch.
`[bwrap] network = "host"` keeps filesystem and PID confinement but restores
host loopback and unrestricted egress. It is degraded debugging mode.
Infinity agents are currently unsandboxed because their shared daemon runs on
the host. Mixed configurations warn at startup. An unsandboxed agent can act
as the human and reach every agent's socket.

The broker copies only `~/.claude/.credentials.json` or `~/.codex/auth.json` into
the private home. It never mounts the human's CLI directory. Credential reads use
no-follow directory descriptors and accept regular files up to 1 MiB. Valid JSON
refreshes return through compare-and-swap and propagate to unchanged live
siblings. Conflicts and superseded values are skipped and logged. Refresh checks
run every two seconds and once after session exit.

An agent can write arbitrary valid JSON into the human's credential file. A CLI
that caches tokens in memory may need a restart after a sibling's refresh.
An unrelated host writer can still race the final comparison and rename.
Use `credentials = []` and `CLAUDE_CODE_OAUTH_TOKEN` or `OPENAI_API_KEY` in
`kind.env` to avoid exposing the human's refresh token.
The OAuth hosts listed in the design are unverified. The default allowlist
excludes account connectors at `mcp-proxy.anthropic.com`.

Writable workspaces can still plant scripts the human runs later. Toolchain
binds and `/etc` expose their readable contents. Kernel hardening, credential
forwarding and resource budgets remain future work. Proxy-unaware tools fail
closed. A denial names the agent in its HTTP 403 response.

## Other V1 limitations

There is no per-sender mail quota. An agent can flood another agent's queue
because leaving out `reply_to` restarts the hop count. A per-sender cap and a
per-turn batch limit are follow-up work.

For unsandboxed agents, token redaction in activity output works per frame and is best-effort. A token
split across frames may appear without redaction.

Infinity recovery through command-backed RAP tool servers is tested on Linux
only. On macOS CI those tools were never invoked, for a reason not yet
diagnosed, so the twelve tests that use them are ignored there.

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
