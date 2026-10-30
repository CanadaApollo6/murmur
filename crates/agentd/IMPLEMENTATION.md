# ACP and supervisor implementation

Base is `origin/main` at `a2b4af5`. Work is limited to agentd and its Cargo.lock
dependency edges. No push or pull request is part of this slice.

## Data ownership

One actor owns Phase, Session, incarnation, offered IDs and pending permissions.
The mailroom owns durable delivery and opaque resume state. Its process-local
actor lease excludes another actor even across separate Broker values. The
SQLite writer lock already excludes another process opening the same file. The
lease is an owner-tagged drop guard, so cancellation while claiming it queues
release after the SQLite transaction without releasing another actor's lease.
Incarnations advance durably before each open attempt. Startup settles the
previous incarnation before opening; each later session end settles its own.

The actor calls mark_offered before submit and filters the turn to the returned
offered IDs. TurnStarted persists evidence. Only Committed consumes mail.
Cancellation and any valid ACP stop reason complete the carried turn; ACP
request errors never generate Committed. Failure after start holds mail as
Uncertain. Failure before start requeues it. This preserves the accepted v1
preference for duplicates over lost mail and makes no exactly-once claim.

## Sketch deviations and reasons

- The wire is plain ndjson JSON-RPC rather than agent-client-protocol 0.11.1.
  The installed crate's one-shot client uses builder callbacks and a connection
  scope. This slice needs only initialize, new/load, mode/model, prompt, cancel,
  permission responses and updates. The spike already proves their JSON shapes.
  One bounded reader and one session-owned writer keep prompt ordering and
  process lifetime visible without adding connection or compatibility layers.
- Only ACP is implemented. There is no Wire enum, AdapterSet, AdapterDeps,
  DeliveryGate::Anytime, or MCP endpoint factory. Broker maps configured kinds
  directly to ACP adapters. Every delivery is at a turn boundary.
- KindConfig has separate command and args, an explicit mode, raw JSON MCP
  configuration entries, and a handshake/write timeout. It has constructors for
  the two proven kinds. It does not launch npm or pin shim installations itself.
  There is no config-file parser or validated KindName type in this slice.
- AgentSpec omits task. Callers send the initial instruction as ordinary human
  mail, using the same tested path. Model selection uses session/set_model and
  fails explicitly when unsupported. Sandbox arguments are supplied per kind;
  restrictive ACP modes are always applied independently of those arguments.
- The persisted registry subset lives in mailroom's additive actor_state table.
  It stores kind, incarnation and opaque token, with a separate in-memory actor
  lease. There is no Registry module or persisted full AgentSpec. Callers must
  supply the spec and kind configuration again after broker restart.
- ResumeToken remains private to the crate and its shape is decoded only by ACP.
  There are no ResumeToken events because ACP's session ID is stable. Load
  replay is swallowed until the load response. No fallback to new is allowed.
- The public facade exposes only Broker and AgentHandle operations. Adapter,
  Session and actor internals are private as required by design.md. Public phase
  snapshots omit Idle timestamps and embedded pending requests; deadlines and
  a map of pending permissions remain actor-owned. Multiple concurrent requests
  can be represented rather than only the sketch's single optional request.
- Activity is normalized display text and tool titles rather than a structured
  text/thinking/tool-result/usage enum. Unsupported display metadata is ignored.
  Permission detail is an opaque display string; no ACP schema type is exposed.
  End reasons are diagnostic strings rather than an enum with daemon cases.
- A permission request is also turn-start evidence. It proves execution began,
  even if a shim omitted an earlier update. Malformed frames, wrong session or
  request IDs, and prompt errors end the session without a receipt. Frames are
  limited to 8 MiB, the receive queue to 64 frames and stderr to 8 KiB.
- Rules match an exact normalized tool name or `*`, not general globs. This keeps
  the policy small without a new matcher dependency. First-match Ask and no rule
  escalate to the trusted API caller. Named-agent escalation and surface sender
  authentication remain deferred with MCP/RAP. The API requires incarnation on
  every answer and validates the offered option key. Timeout cancels when the
  wire offers no RejectOnce option, avoiding a persistent rejection grant.
- PermissionAnswer::CancelTurn withdraws all pending requests via real cancel.
  The actor remains Busy until TurnEnded. Unknown, answered and dead-incarnation
  requests return StalePermission without changing the current session.
- Spawn opens immediately rather than waiting for first mail. This makes launch
  and resume failure observable immediately. Idle process death goes Dormant,
  following the supervisor sketch; Busy death goes Ended then crash backoff.
  Crash timers retry even when the failed turn's mail is now Uncertain.
- Stop ends the actor without retiring the durable agent. Spawn can reattach
  after stop. There is no Retired phase, durable retirement facade, or Restart
  command for a still-running Failed or NeedsRecovery actor. The minimal facade
  can stop and spawn it again; automatic recovery never creates a fresh session.
- Backoff and failure-window durations are configurable for process tests. The
  defaults remain 1, 2, 4 seconds through a 60-second cap, five crashes in 600
  seconds. Idle suspension is implemented and only enabled for resumable wires.
- The event receiver is a bounded display stream without a snapshot cursor.
  Durable message decisions remain in SQLite. Graph completeness and production
  event serving are outside this slice.
- The fake ACP peer is a Cargo test fixture binary under tests/support, enabling
  integration tests to launch a real process through CARGO_BIN_EXE. It is not
  the deferred agentd executable.

## Verification evidence

The fake-process tests cover both restrictive modes, response-only start
fallback, turn receipts, Busy batching with human and peer input, idle resume,
failed and unsupported loads, nonresumable idle behavior, SQLite reopen,
previous-incarnation cleanup, death on both sides of start, prompt errors,
backoff and the fifth-crash threshold, policy and escalation, option validation,
timeouts, stale answers across a real process replacement, cancel and duplicate
actor rejection across brokers. There are no adapter or actor mocks. A separate mailroom unit test blocks the
real SQLite worker to prove cancellation releases a committed actor claim.

The mutation script records 14 runtime test failures and restored green for every
case; compilation failure alone does not count. A first stale-lookup-only
mutation survived because the independent incarnation guard still rejected the
answer. The final mutation removes both request binding checks. Live tests use
the same receipt path and are not separately mutated against paid services.

Live Claude 0.90.0 and Codex 2.2.2 shims were logged in. Both passed new-session
turn, restrictive mode setup, idle suspension, session/load and remembered
context. Codex initially exposed a test assertion that concatenated JSON frames
instead of text chunks. Normalizing display text fixed the chunk boundary case;
both tests then passed. Live permission and cancel behavior in this new Rust
client remains partial; those paths are covered with real fake-agent processes.

cargo-about was absent and was installed with the requested locked CLI command.
The full third-party generator ran. THIRD-PARTY was byte-identical afterward,
including its npm sections, so it needs no commit.

The repository's additional ./check.bash stopped at its prerequisite check:
`Cannot find jujutsu jj`. Its remaining checks are partial. The four requested
commands are run independently and recorded below.

## Remaining questions and limits

- Named-agent approval routing, authentication, general permission globs, config
  persistence and a first-class restart operation belong to a later facade slice.
- Unix sessions get their own process group, kill on drop, bounded stderr and
  explicit kill/reap before another session opens. Windows descendant cleanup
  is partial. Hard broker SIGKILL recovery of orphaned descendants is also
  partial; there is no persisted PID or external process supervisor here.
- Mailroom release/drop/retire APIs remain available directly. A release needs
  a later send or actor wake to pump; there is no release facade in this slice.
- No CLI, MCP/RAP, Infinity, services, claims or UI behavior is qualified here.

Raw final summaries, mutation pairs and source hashes are recorded in
[VERIFICATION.txt](VERIFICATION.txt).

## Changed files and size

The Rust diff adds 1,566 non-test physical lines and removes two. It adds 1,056
test lines, including the subprocess fixture and mailroom cancellation unit test.
Counts include blank lines and comments. The mutation driver adds 54 Python lines.

New files are src/adapter.rs, src/adapters/mod.rs, src/adapters/acp.rs,
src/supervisor.rs, tests/supervisor.rs, tests/support/fake_acp.rs,
scripts/check-mutations.py, IMPLEMENTATION.md and VERIFICATION.txt, all under
crates/agentd. Existing changes are agentd's Cargo.toml, README.md, src/lib.rs,
src/model.rs, src/mailroom.rs and src/schema.sql, plus root Cargo.lock.

The public additions are KindConfig and SandboxArgs; AgentSpec, Broker,
AgentHandle, Config, Phase, Event, TurnOutcome and Error in supervisor; and the
permission policy, rule, request, answer, option, effect and ID types in model.
The adapter and live session API remain private. The broker operations are new,
spawn, send, events and shutdown; agent operations are phase, phases, cancel,
pending_permissions, answer_permission and stop.
