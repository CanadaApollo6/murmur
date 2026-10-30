# agentd mailroom

Library-only first slice of murmur's broker. No sessions, adapters, surfaces, or
supervisor are included.

`Mailroom::open(path, Config::default()).await` opens a real SQLite file in WAL
mode with full synchronous commits. One dedicated thread owns the connection.
An exclusive SQLite lock rejects a second owner, including another process.
Methods enqueue a transaction and await its result without blocking Tokio.
Cancelling the caller after submission does not cancel the transaction. Retry
`accept` with the same sender and client key to recover its original result.
Call `close().await` to drain submissions, join the thread, and release the file.
Dropping the handle also drains submissions, but does not wait for thread exit.

## Public API

- `model` exports ULID identities, validated names and bodies, recipients,
  senders, incarnations, immutable envelopes, delivery states and delivery class.
- `swarm` exports the five acts, outcomes with evidence and artifacts, tasks,
  states, stakes, budgets, epochs and exposure types.
- `Mailroom` opens and closes storage, registers and resolves minimal agents,
  and accepts and reads messages and conversation views.
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
whole batch. Session cleanup and retirement select only eligible rows, so
repeating cleanup with no matching rows changes nothing.

The first five offers are allowed by default. A sixth offer attempt moves Queued
to Held without submitting it or incrementing the counter. An operator release
resets attempts to zero. This grants another bounded set of attempts.

## Choices and deviations from the sketches

- The crate is a member of murmur's workspace, as requested. The `lib.rs` sketch
  placed a complete broker in a separate workspace. Only `model`, `swarm` and
  `mailroom` are exported here. The mailroom is public to make this slice usable
  and testable until the facade exists. It accepts a trusted `Sender`; it is not
  an authentication boundary.
- Agent specs, placements, resume tokens, permissions, kinds and peer rendering
  are omitted. They require the registry, adapters or surfaces excluded here.
  Minimal agent registration and name resolution live in the single DB owner.
- `Envelope` has private fields, read accessors and serialization only. This
  strengthens the sketch's immutability and accept-only construction contract.
- Human envelopes store `exposure: None`. Agent envelopes snapshot a broker-owned
  reference, including sequence zero before any exposure. The sketch required an
  agent ID even for a human sender. No synthetic agent identity is invented.
- Agent epochs start at zero. `new_epoch` explicitly starts a fresh context and
  retains old exposures. Incarnation and epoch remain separate concepts.
- `Body` and `AgentName` also validate deserialization. The body limit is bytes,
  including for Unicode. Names follow the sketch's lowercase ASCII grammar.
- The five acts remain. `Inform` has no claims. `Propose` uses a scope string;
  `Decide` keeps its proposal and accepted flag. Claim IDs, `relies_on`, claim
  support and decision-right types are omitted because that machinery is out of
  scope. Propose and Decide are rejected until decision rights exist.
- `Evidence` and `ArtifactRef` are retained as necessary Outcome payloads.
  `ExposureSource` retains Message and Artifact; BoardEntry and its ID type are
  omitted with the board. Only message exposures can currently be written.
- `Task` adds its deliverable and requester, matching the design and SQL sketch
  where the Rust sketch omitted them. The Request envelope points at the new
  child task. `NewMessage::task` identifies the current or parent task; replies
  inherit it if omitted. Human Requests create roots; agent Requests require an
  open parent owned by the sender. Reports require an open task owned by the
  sender and cannot name a conflicting current task.
- Child reservations are included in the parent's `spent` and never refunded.
  This avoids a separate reservation ledger while bounding the whole tree.
  `tokens: None` means unmetered. A finite token parent cannot grant an unmetered
  child. For finite token tasks, spend must specify a token amount, including
  `Some(0)` for non-token spend. Checked arithmetic rejects overflows.
- Spend is explicitly recorded with `record_spend`, rather than automatically
  debiting message or hop budgets at acceptance. This slice has no usage or
  turn-time source. The future supervisor must supply accounting; exhausted
  budgets do not generate autonomous Blocked messages here.
- Partial and Infeasible reports persist in the immutable message and leave the
  task Verifying. Done closes with success; Failed and Blocked close without
  success, following the available TaskState shape. Verification workers,
  calibration, reopening and automatic retries are omitted by this brief.
- The sketch mutex becomes a dedicated owner thread and typed async requests.
  There is no event bus or `recipient_busy` flag because no actor exists.
  `rusqlite` uses bundled SQLite. Version 0.32.1 shares `libsqlite3-sys 0.30.1`
  with the workspace's existing SQLx lockfile and avoids a native links conflict.
- The schema adds stable acceptance sequence ordering, act, task, exposure,
  agent epoch, foreign keys, indexes and state-shape checks. Typed compound
  values and IDs use JSON text. Reports carry their Outcome in `act`; Verifying
  refers to that immutable report. Schema version 1 is checked on open; future
  versions are rejected. No general migration framework is introduced.
- Interceptors run inside the acceptance transaction, after duplicate detection
  and before writes. Reject wins over any earlier Hold; otherwise the first Hold
  reason wins. All configured interceptors run unless one rejects. The view has
  the last 64 envelopes and directed pair counts over the full conversation.
- `pending_for` is the requested name for sketch `queued_for`. `mark_started`
  persists turn-start evidence. `session_ended` adds the requested boolean, ORs
  it with stored evidence, and covers offered incarnations up to the ended one.
- Strict transition errors replace the sketch's idempotent offer/consume and
  ignored unknown, dead or consumed receipts. This follows the explicit brief.
  `release` returns a recipient or an error instead of silently returning None.
- The attempts cap adds the requested Queued-to-Held transition beyond the
  sketch diagram. `mark_offered` returns offered and held IDs separately so the
  caller cannot mistake a capped message for an offered one. `retire` combines
  setting the retired flag and dead-lettering live mail in one transaction.
- Human replies may resume any conversation and reset its hop. Agent replies
  must involve the replying sender. This permits the operator to intervene in
  an agent conversation without treating the human as an agent participant.

## Remaining questions and partial behavior

- The supervisor must choose epoch changes, persist its live incarnation and
  invoke session cleanup at startup. Automatic restart recovery is not built.
- Usage accounting is caller-supplied. Reservation reclamation and the meaning
  of Blocked beyond a recorded outcome need the next lifecycle slice.
- Partial and Infeasible verification remains partial. There is no operation to
  resolve Verifying until verification and calibration are implemented.
- There is no human recipient address in the sketch. A human-root task's Report
  can target a selected agent while recording its outcome in the ledger. Routing
  reports to a human control client belongs to the future facade.
- Exclusive ownership also excludes external readers while the broker is open.
  Live inspection must use the library's read methods. An eventual UI may need
  a different ownership lock if independent read connections are required.
- The SQL schema can represent only message exposure rows in this slice. Board
  and artifact exposure writes will require a schema extension.

## Implementation checkpoint

The blocking work was reading the design, choosing immutable message data and
creating the requested branch. Domain definitions, database operations and
real-file tests formed one implementation stream. The SQLite connection and
acceptance transaction serialize the invariants shared by deliveries, tasks and
exposures. One implementation owner kept that contract in one checkout; no
nested agents or PR workflow were used.

Model the Domain shaped the implementation by expressing delivery states as an
enum and testing the legal source states for each operation.
