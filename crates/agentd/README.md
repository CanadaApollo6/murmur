# agentd

Local broker with `agentd`, `agentctl`, Infinity model agents, and a stdio MCP
server for Claude and Codex agents. The SQLite mailroom remains the delivery authority.

## Run

For a first run with one Claude agent and one Codex agent, follow the
[quickstart](../../docs/murmur/quickstart.md). It lists only commands that
were run against a live daemon.

```sh
cargo build -p agentd --bins
agentd --config /path/to/kinds.toml
agentctl spawn planner --kind infinity --cwd /path/to/project
agentctl spawn reviewer --kind claude --cwd /path/to/project
agentctl spawn builder --kind codex --cwd /path/to/project --sandbox workspace-write "Ask @reviewer to check the plan."
agentctl spawn tester --kind claude --cwd /path/to/project --rule send_message=allow
agentctl send @reviewer "Focus on session expiry."
agentctl ls
agentctl approvals
agentctl approve <permission-id> <option-key>
agentctl cancel @builder
agentctl stop @builder
agentctl restart @builder
agentctl held [@builder]
agentctl release <message-id>
agentctl drop <message-id>
agentctl watch
agentctl tasks
```

`agentctl stop @name` ends the agent's session and keeps the agent stopped.
A running turn is cancelled first. If the agent does not finish the turn within
the kind's cancel timeout (two seconds by default), agentd ends the session anyway.
Mail to a stopped agent stays queued under the normal mail limits. `agentctl ls`
shows the phase `stopped`, and the agent stays stopped across an agentd restart.

Ending the session kills an ACP agent's process. An Infinity agent's work runs in
the shared Infinity daemon, and the daemon protocol has no command that ends a
thread's work in bounded time: `Interrupt` is the only cancel, and agentd can only
detach from the thread. If the thread does not acknowledge the interrupt within
the cancel timeout, `stop` still stops the agent but fails with
`agent stopped, but the Infinity turn has not acknowledged the cancel and may
still be running in the daemon`. `restart` reports the same case as
`agent restarted, but ...` and reattaches to the thread. Stop also interrupts a
thread that is busy with work agentd did not submit.

From the moment a stop or restart interrupts an Infinity turn, `agentctl ls` and
the event feed show this warning in the agent's `error`, next to any other error,
in every phase and across an actor respawn. Every reply to `stop` or `restart`
carries it, including a reply that fails for another reason. Only evidence that
the turn is over clears it: the thread reports the turn ended, or a session
attached to the thread reports it idle. A new command does not clear it, and
neither does an interrupt acknowledgement that leaves the thread running a new
completion. The warning does not survive an agentd restart.

If agentd cannot settle the ended session's mail (a storage error, or a failed
session shutdown), the command fails, the agent waits in phase `ended`, and its
mail keeps its state. `agentctl ls` and the feed show the failure as the agent's
`error`. Run the same command again to settle it. The repeated command still
reports Infinity work that may be running, and a restart from `needs-recovery`
or `failed` stays fresh.

`agentctl restart @name` ends any current session the same way and starts a new
one. From `stopped` or a live phase, the new session resumes the saved
conversation. From `needs-recovery` or `failed`, restart discards the saved
conversation and starts a fresh session. A stop from `needs-recovery` or `failed`
discards it too, so the restart that follows is fresh, as restart alone would be.
This is the only way out of `needs-recovery`. If agentd shuts down while a stop
or restart waits for the cancel, the command fails and nothing is started. Mail held from an uncertain turn stays held; use `agentctl held`,
`release` and `drop` for it. Both commands are idempotent and affect only the
named agent.

The default state directory is `~/.local/state/agentd/`. `--state-dir` overrides it.
`agentctl` has no `--state-dir` flag. With a custom state directory, set
`AGENTD_SOCKET` or pass `--socket` to every `agentctl` command.
The daemon reads `kinds.toml` there when present. An explicit `--config` must exist.
Without a config, commands default to `claude-agent-acp` and `codex-acp` on PATH.
On Linux, that is the PATH inside the sandbox. A `command` outside the default
binds also needs its directory in `[bwrap] ro_binds`.
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

### Permission rules

Every permission request goes to the operator unless a rule answers it. Rules
are opt-in and per agent. Give them at spawn as `TOOL=DECISION`, where the
decision is `allow`, `deny` or `ask`:

```sh
agentctl spawn reviewer --kind claude --cwd /path/to/project --rule send_message=allow
```

The first rule that names the request's tool, or `*`, decides. `allow` and
`deny` pick the agent's allow-once or reject-once option. ACP tool kinds give
the names `shell` (kind `execute`), `edit`, `fetch` and the other ACP kinds.
Rules have no effect on Infinity kinds, whose requests carry no tool name.

`send_message` names only agentd's own `send_message` tool, from the MCP
server agentd registered for that session as `agentd`. Only claude-agent-acp
requests get this name. agentd recognizes the tool from fields that adapter
sets, never from the title or arguments: `toolCall._meta.claudeCode.mcpServer`
is `{"name":"agentd","source":"dynamic"}` and
`toolCall._meta.claudeCode.toolName` is `mcp__agentd__send_message`. The
match is confirmed against a live session with claude-agent-acp 0.90.0. In
the CLI source, a same-named server from user, project or local configuration
reports that scope as its `source`, and a plugin's server is renamed and
reports `plugin`. Those cases have not been run in a live session.

Before this name existed, the request carried the name `other`, like every
MCP tool call from Claude. A rule that names `other` no longer answers it.

A `send_message` tool from another MCP server, a shell command that mentions
it, and a request without these fields keep their usual name. The
`send_message` rule does not answer them. Another rule that names them, or
`*`, still does. Older adapters that leave out these fields are treated the
same way.

codex-acp requests never get the name. Codex identifies an MCP call by server
name alone, and it skips the server agentd passes in when its own
configuration defines one named `agentd`. An approval from Codex for an MCP
call keeps the name `shell`. In a live session, codex-acp 2.2.2 called
`send_message` without asking for approval at all.

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
are `send_message { to, body, reply_to?, act? }` and `list_agents {}`. Send keys derive
from the incarnation, a random namespace created at each MCP shim startup, and
the MCP request ID. Retries within one shim session recover the original mail.
A replacement shim can reuse request IDs without losing new mail. A retry across
shim restarts can produce a duplicate. Recipient execution remains at least once.

### Tasks

`send_message` and `agentctl send` take an optional `act`. Without it, mail is an
`Inform`, as before. Both surfaces share one JSON shape:

```json
{"kind":"request","deliverable":"a passing test","budget":{"wall_secs":600,"messages":20,"hops":2,"tokens":null},"stakes":"normal","parent_task":"<task id>"}
{"kind":"report","task":"<task id>","outcome":{"kind":"done","artifacts":["commit:abc123"]}}
```

A request opens a task owned by the recipient. `stakes` defaults to `normal` and
`tokens` to unmetered. `wall_secs` and `messages` must be at least 1, and
`tokens` must be null or at least 1: a zero budget costs the parent nothing, so
it would allow unlimited children. Every text field needs a non-whitespace
character. The body and the act together must fit in 64 KiB. An agent must name its own open task in `parent_task`
(or reply to a message of that task); the child's budget is carved from it. The
operator's request opens a root task and must omit `parent_task`. Outcomes are
`done { artifacts }`, `partial { artifacts, not_possible, evidence }`,
`infeasible { evidence }`, `failed { tried }` and `blocked { on }`, where
`evidence` is `{ tried, obstacle, reproduction? }`. `partial` and `infeasible`
move the task to `Verifying`; the others close it. `propose` and `decide` have no
wire form and are rejected. Unknown fields are rejected for every kind,
`inform` included. An invalid act returns an error and stores nothing. The tool
schema lists each kind as a `oneOf` branch with its own required fields, and a
unit test holds the schema and the parser to one table of cases.

The rendered mail tag carries `act="request"` or `act="report"` and `task="<id>"`.
Everything the sender wrote stays inside the tag, escaped like the body: the
deliverable follows the body as `Deliverable: ...`, and a report's outcome
follows it as `Outcome: {...}`. After the tag, the broker writes only values it
produced. For a request, that is the task id, stakes, budget and the exact report
to send. For a report, it is the outcome kind and whether the task closed or waits
for verification. These notes replace the usual "Reply with send_message" line.

A report must be addressed to the agent that requested the task. Agents cannot
mail the operator or a retired agent, so a task the operator requested, or one
whose requester has retired, is reported to the assignee's own address; the
operator reads the outcome with `agentctl tasks`. The mailroom
accepts reports only from the task's owner, so `agentctl send --act` with a
report is always rejected.

```sh
agentctl send @builder "Fix the flaky test." --act '{"kind":"request","deliverable":"a passing test","budget":{"wall_secs":600,"messages":20,"hops":2}}'
agentctl tasks
```

`agentctl tasks` prints every task with its `id`, `parent`, `state` (`open`,
`verifying` or `closed`), `requester`, `assignee`, `deliverable`, `stakes`,
`budget`, `spent` and `outcome`.

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
no-follow directory descriptors and accept regular files up to 256 KiB. Valid JSON
refreshes return through compare-and-swap and propagate to unchanged live
siblings. Conflicts and superseded values are skipped and logged. Refresh checks
run every two seconds and once after session exit.

An agent can replace the values in the human's credential file. A refresh must
be a JSON object with the same top-level keys as the copy that the agent
started with. A CLI that caches tokens in memory may need a restart after a
sibling's refresh.
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

Agent mail is bounded by the `[mail]` limits described under Run. Leaving out
`reply_to` restarts the hop count, so the hop limit does not stop a long
exchange. The per-sender and per-pair caps still bound how much undelivered
mail one agent can queue. Human sends are exempt.

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
- `AgentHandle::{cancel,pending_permissions,answer_permission,phase,phases,error,stop}`
  control or observe an actor. `error` is what `agentctl ls` shows. Answers include the request's incarnation.
  Rules match an exact normalized tool name, or `*`; the first match wins.
  No rule means escalate to the caller. An optional timeout rejects once, or
  cancels if the agent provides no single-use rejection option.
- `operate(id, Operation::Stop | Operation::Restart)` stops or restarts an agent.
  It cancels a running turn and waits at most the kind's `cancel_timeout` before
  it ends the session. The stop flag persists in `agent_specs.stopped` (schema
  version 3). An actor that exited in `Failed` or panicked is respawned from its
  saved spec. The respawn discards the saved session only when the dead actor's
  last phase was `failed` or `needs-recovery`, so a busy actor that panicked
  resumes. An actor ended by `shutdown()` or a replacement is not, and
  `spawn` after `shutdown()` returns `Error::Closed`.
- `events()` returns a bounded display-only broadcast receiver. Subscribers can
  lag; SQLite remains delivery authority. Activity never becomes agent input.
- `shutdown()` stops and reaps sessions, settles outstanding delivery, and waits
  for the actors to release storage. Call it before closing the mailroom.

`Phase` covers Dormant, Starting, Idle, Busy, Suspending, Ended, Crashed, Failed,
NeedsRecovery and Stopped. Busy CLI agents wait for a turn boundary. Infinity accepts
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
`accept` with the same sender, client key and content to recover its original
result. Reusing a client key for a different recipient, body, reply or act is
rejected.
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
- Ledger operations are `task`, `tasks`, `record_spend`, `new_epoch` and
  `exposures`.
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

## Graph UI

The graph UI in `agentd-web/` answers two questions at a glance: what is stuck,
and what is waiting on you? It shows agents and the mail between them, pending
permissions and held mail with commands to copy, and each agent's activity and
queue. It is read-only. It never changes anything itself, and it only displays
commands that the server built.

A default `cargo build` does not use npm and does not include the UI. To use the
UI, do one of these procedures.

To embed the UI in the binary, build with the `bundled-web` feature. The build
needs npm.

```sh
cargo build -p agentd --features bundled-web
```

To serve a separate build, run npm and set `assets` to the output directory.

```sh
cd agentd-web && npm ci && npm run build
```

```toml
[ui]
listen = "127.0.0.1:7434"
assets = "/absolute/path/to/agentd-web/dist"
```

Either way, set `listen`, restart agentd, run `agentctl ui`, and open the URL that
it prints. If the page says that the token was rejected, run `agentctl ui` again.

For development, run `npm run watch` in `agentd-web` with `assets` set as shown.
The server reloads the files on the next `GET /`. To test the app, run these
commands.

```sh
cd agentd-web && npm ci && npm test                # reducer, client and layout
npx playwright@1.60.0 install chromium
cargo test -p agentd --features e2e-web --test web_e2e  # real agentd and browser
```

Set `AGENTD_UI_SHOTS=<directory>` to write screenshots during the browser tests.

## Read-only event feed and web access

`agentctl watch` sends a cursor snapshot followed by durable mail transitions and
volatile agent, permission, activity, turn-end, and egress events. It accepts
`--cursor <seq> --nonce <nonce>` to resume. Echo the last applied mail frame's
sequence and nonce, or the anchor from a full snapshot. A valid resume sends
`mail: null` and replays every later mail event. A missing, mismatched, or pruned
anchor sends a full mail snapshot. Mail event history keeps the newest 65,536
rows, checked every 1,024 new events and pruned in batches of at most 4,096.

Mail snapshots contain exact edge counts, the newest 1,000 stuck deliveries
(held or uncertain), and the newest 1,000 moving deliveries (queued or offered).
Each list has a truncation flag. Mail frames carry `prev_state` and `state` so
clients can update edge counts without holding message history. Frames contain
metadata, never message bodies. Agents use `agent:<ULID>` throughout the feed.
Activity is display-only, coalesced every 100 ms, and limited to the newest 2 KiB
of JSON-encoded text. State and activity have separate channels. Egress denials
are limited to one per agent per second. The existing in-process
`Broker::events()` channel keeps its original events.

The web listener is disabled by default. To enable it, add this configuration.

```toml
[ui]
listen = "127.0.0.1:7434"
# Optional directory containing index.html and its assets.
# assets = "/absolute/path/to/agentd-web/dist"
```

Run `agentctl ui` to print the authenticated URL. The token is in the URL
fragment, which is not sent to the server. `state_dir/ui.token` contains 256 random
bits, has mode 0600, and survives restarts. Delete it and restart agentd to rotate
it. Unsandboxed agents running as your user can read it and already have full
control through the Unix socket. Startup prints a warning for those kinds.

The listener accepts only loopback addresses with a fixed, nonzero port. Every
request checks `Host`. WebSocket requests also check `Origin` when present.
The listener applies a restrictive CSP and sends no CORS headers.

- `GET /events?cursor=<seq>&nonce=<nonce>` upgrades to the same feed as
  `agentctl watch`. Supply both WebSocket subprotocols, `murmur.events.v1` and
  `murmur.token.<hex>`. The server echoes only the first.
- `GET /mail?from=&to=&states=&before_msg_seq=&limit=` returns metadata newest
  first, with a maximum limit of 1,000. `states` is comma-separated. Each job
  examines at most 10,000 message candidates. A short or empty page may still
  have `next_before_msg_seq`; continue until it is null.
- `GET /messages/<ULID>` returns one envelope, its body, `sent_at`, and `bytes`.
  Both HTTP data routes require `Authorization: Bearer <hex>`.

All browser data reads, feed reads, `ls`, and agent-callable `list_agents` share
one observer permit on the existing exclusive mailroom worker. Delivery jobs do
not acquire that permit. Eight feed subscribers are allowed across both
transports. HTTP allows 64 connections. A blocked feed send closes after ten
seconds. WebSocket input is limited to 4 KiB; pings run every thirty seconds,
and sixty seconds without a pong closes the connection.

Without a bundle or an `assets` directory, `/` displays build instructions. An
`assets` directory replaces the bundle. With `assets`, the server canonicalizes the root at startup and refreshes its manifest
on `GET /`, at most once per second on a blocking worker. It opens the root and
every entry through descriptor-relative no-follow operations, skips and logs
symlinks, and refuses roots inside `state_dir`. A tree may contain at most 4,096
files, 8,192 entries, 64 MiB of content, and 32 directory levels. Oversized trees
are logged and refused. Responses share an immutable manifest without holding
its lock across writes.
Request paths never become filesystem paths. The web API has no write routes.
Permission and held-mail frames include server-built POSIX shell commands for
`agentctl`. Commands quote every argument and use the bound control socket;
values containing control or invisible formatting characters, including bidi
controls, have no copyable command.

The full-scale worker cost gate is an explicit test. It creates a 1M-message
fixture with 64 KiB bodies in the newest 20,000 messages, a 200,000-message dormant
backlog, and concurrent observers on the production current-thread runtime.
It checks every generator result, completed full snapshots from each reconnecting
client inside the sampled window, retained replay, and a multi-thousand-row prune.
Baseline and loaded accepts use the same worker tracing.
Seven reconnecting feeds disconnect only after their complete serialized full
snapshot is consumed, then reconnect immediately without sleeping. Each attempt
has a 10-second hang limit that fails the gate. The eighth feed retains the full
backlog replay. Completed full snapshots must reach at least 12.5/s on the
production filesystem and 28.35/s on RAM disk. These are fixed floors; snapshot
job rate is printed only, since it includes work for clients that disconnected.
Cancellation remains covered by dedicated abandoned-feed tests.

The default v8 production rule requires loaded accept worker-queue p99 at or below
20 ms and whole-job service p99 at or below 20 ms for every observer class,
including full snapshots. Each class needs at least 100 service samples. Both
snapshot medians, including serialization, must remain below 100 ms. Total accept
p99, paired delta, and class maxima are printed but do not decide the result.
Thread CPU p99 is printed for each worker and observer class as a diagnostic;
whole-job wall time still decides the service limit. Thread CPU is unavailable
on non-Linux hosts and is printed as `NA` there.
On Linux the gate also prints RSS and the process high-water mark. Post-load RSS
is sampled one second after all clients, encoders and queued reads drain, with
the mailroom still open. A second sample follows mailroom close and another idle
second. No allocator trim or collection is forced; the peak includes fixture
initialization as well as the sampled load.

The daemon and its library test binary share Microsoft's Rust `mimalloc` 3.6.1
global allocator, built from the same repository's C sources. The git dependency
is pinned to commit `56dc5eb80145f9a3ba7ca23d47c6a248b9d8586c`, the commit behind
the annotated `v3.6.1` tag. Default features and C malloc interposition are disabled,
so SQLite keeps its system allocator.
`agentctl`, the fake ACP helper and other production binaries keep their existing
allocator. The daemon and gate allocator declarations come from the same module.
The license generator verifies the shared repository-root MIT notice by checksum.
The existing build profiles are unchanged; upstream's LTO recommendation is a
performance option rather than a build requirement.

The baseline has 2,048 accepts. The loaded window keeps the same closed-loop
accepts and observer workload for at least 2,048 accepts, extending until all six
recurring observer classes have enough samples. It waits for 101 completions per
class to allow for window boundaries; the final check still requires at least
100 whole jobs per class. A 120-second ceiling reports a shortfall as a failure.
The classes are full snapshot, feed page, list_agents, rare-filter mail, and the
held/uncertain and queued/offered mail views. Admissions that skip cancelled reads
are not observer work; the one-time resume validation occurs during warmup.

Run five fresh sequential attempts on the production filesystem, with an optimized
build and enough disk space for the database and WAL. Retain every result and record
host load; the gate does not retry a miss.

```bash
cargo test --release -p agentd --lib m5_cost_gate -- --ignored --nocapture
```

For a separate ten-minute memory diagnostic, run `m5_memory_soak` instead. It
shares the complete fixture and all 18 workload tasks with the gate, including
closed-loop accepts, reconnects, full replay, mail views and pruning. It prints
RSS and workload counters every ten seconds, checks continued work and a real
multi-thousand-row prune, then reports RSS after draining clients and closing the
mailroom. Worker tracing and per-accept timing retention are off during the soak
so the diagnostic does not accumulate its own timing records. The normal gate's
window, load and pass rules are unchanged.

```bash
cargo test --release -p agentd --lib m5_memory_soak -- --ignored --nocapture
```

For RAM-disk diagnostics, build once with `--no-run`, then execute the printed
test binary by absolute path from a private directory on the RAM disk:

```bash
# Run from a private directory under /dev/shm, using the binary printed by cargo.
M5_GATE_RAM_DISK=1 /absolute/path/to/agentd-TEST_HASH m5_cost_gate --ignored --nocapture
```

`M5_GATE_RAM_DISK=1` reports class service times without asserting their 20 ms
limit. Queue p99, sample floors, snapshots, replay, pruning and workload checks
remain strict. The marker does not move the fixture: it uses the current working
directory. Unset means strict production mode; other marker values are errors.
Allocator-related service tails on RAM disk remain a named follow-up.
