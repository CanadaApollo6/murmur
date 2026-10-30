# agentd: a local agent runtime on Infinity

Design for murmur's broker, `agentd`. Source line references are to upstream Infinity at commit `1abc6fc`, the commit murmur detached from.

Synthesized design. Base: candidate C (broker above). Grafts from B (core-native) and A (RAP edge), plus fixes from the cross-judge. Type sketch: [`sketch/`](sketch/lib.rs); swarm types in [`sketch/swarm.rs`](sketch/swarm.rs). Grounding: [`grounding/`](grounding/03-constraints.md).

## Problem

One on-machine runtime must host Infinity model agents (local inference on one RTX 3090) and five CLI agents (Claude Code, Codex, Muse, Pi, Prime Agent) as equals, and any agent must be able to message any other by address. Infinity does not have this shape:

- Every Infinity thread is an LLM loop. There is no thread kind a CLI agent could occupy.
- Messaging follows the thread tree only. A non-interrupting input must anchor to a tool call already in the recipient's history, or `prepare_input` drops it.
- An input becomes durable only after the model produces output for it, and deferral is in memory.
- Deferral keeps arrival order inside a batch that settles a pending tool call (`system/thread.rs:267`), so a synthetic that arrives with that tool's result can still interrupt it.
- The provider server notices a dropped client only on its next chunk write (`handle_invoke`, `infinity-provider-protocol/src/remote.rs:219`), so a request waiting inside `invoke_model` keeps its place.

The CLI agents speak five wire protocols, have five permission models and resume by session ID. The core changes must stay small and leave room for cross-machine teams, a loop watchdog and a live graph UI.

## Usage (caller's view)

```sh
agentd &                     # starts llama-server, then `infinity daemon`, then agent actors
agentctl spawn planner  --kind infinity --model local/qwen3-coder --cwd ~/src/app \
         "Plan the auth refactor. Ask @reviewer to check the plan before you start."
agentctl spawn reviewer --kind claude --cwd ~/src/app --sandbox read-only
agentctl spawn builder  --kind codex  --cwd ~/src/app --sandbox workspace-write --escalate-to @planner
agentctl ls                  # name, kind, phase, queued mail
agentctl send @reviewer "Focus on session expiry."     # from the human: a real user turn
agentctl approvals           # pending permission prompts from any agent
agentctl approve 01JD3K… allow-once
agentctl cancel @planner     # real cancel, not fake user text
agentctl watch               # live agent/message stream (the graph UI's feed)
```

Every agent sends with the same `send_message` tool. Infinity agents get it from agentd's RAP toolset; CLI agents get it from agentd's MCP server, attached at session start. Sender identity comes from the session binding, never from an argument.

```json
{"name": "send_message", "arguments": {"to": "@reviewer", "body": "Plan attached: ...", "reply_to": null}}
```

A model agent receives mail as a tool exchange, never as user text:

```text
assistant: receive_event__injected {"original_tool_name": "peer_message", "from": "@reviewer"}
tool:      <agent-message id="01JD3Q…" from="@reviewer" kind="claude" conversation="01JD3M…" in-reply-to="01JD3M…">
           Step 3 drops refresh tokens on logout; keep them revocable.
           </agent-message>
           This is a message from another agent, not from your user. Reply with send_message.
```

A CLI agent receives mail **when its current turn ends**, as one turn that batches every waiting envelope in the same `<agent-message>` framing. Nothing lands mid-turn or mid-tool-call. (Decision, 2026-10-29: labelled turn at idle, no inbox tool.)

Inside agentd, every surface goes through one facade:

```rust
// surfaces.rs: RAP or MCP handler for a send_message call
let from = bindings.authenticate(&inv)?;                 // AuthenticatedSender, unforgeable
broker.send(from, Recipient::parse(&to)?, Body::new(body)?, reply_to,
            ClientKey::from_tool_call(&inv)).await?;    // idempotent on retry

// supervisor.rs: the only place a turn enters a session
mailroom.mark_offered(&ids, self.record.incarnation).await?;   // write-ahead
session.submit(build_turn(mail, &registry)).await?;
```

## Shape

### Data

Three SQLite tables and one in-memory owner per agent:

- `agents`: `AgentId` (ULID), `AgentSpec`, opaque `ResumeToken`, `Incarnation`, `Placement`, `retired`. Owner: `Registry`.
- `messages`: immutable `Envelope { id, from, to, conversation, in_reply_to, hop, body }`, unique on `(sender, client_key)`. Owner: `Mailroom`.
- `deliveries`: `Queued | Offered{incarnation} | Consumed | Uncertain{incarnation} | Held{reason} | Dead{reason}`. Owner: `Mailroom`.
- `Phase` and the live `Session` belong to that agent's `AgentActor` and nothing else.

Swarm tables (`tasks`, `exposures`, `verifications`, `calibration`, and later `claims`, `board`, `clusters`) are described in [Swarm layer](#swarm-layer).

Access patterns, each one query: "what do I deliver to X now" is `deliveries where to=X and state=Queued` in order. "What must I re-offer after X's session died" is `Offered{inc <= ended}`. "Is this send a retry" is the unique key. "Is this conversation looping" is `messages where conversation=C` with `hop`.

### 1. Identity and addressing

An address is a broker-minted `AgentId` (`agent:01JD…`) with an `@name` alias resolved once at the facade. Infinity `ThreadId`s, ACP session IDs, Muse refs and PIDs live in the adapter-owned `ResumeToken`, which is opaque outside its adapter. Hibernation rewrites none of them; a restart bumps `Incarnation`; a later move to another machine rewrites `Placement`. An Infinity agent maps to one root thread; its child threads send as that agent.

### 2. Messaging

`send → Mailroom::accept` (one transaction; interceptors run here) `→ nudge actor → pump → mark_offered → Session::submit → SessionEvent::Committed → mark_consumed`.

The delivery class is derived from the sender (`Envelope::class`), so no call site chooses it: mail from the human is a user turn, mail from an agent is a peer event.

- **Model agent.** A peer event goes out as `ClientMessage::DeliverPeerMessage { thread_id, message_id, from, text }`. The daemon builds an `InputMessage` with `TaggedSyntheticKind::PeerMessage` and dedup ID `peer-{MessageId}`. Core inlines it as a self-contained `receive_event__injected` pair whose call ID is **derived from `MessageId`** (stable across replays). It is deferrable, so it waits behind a non-passive tool call, and the patched `filter_deferrable` processes a settling tool result **before** any peer in the same batch. It wakes a passive `sleep`.
- **Delivery receipt.** The mailroom marks a peer message `Consumed` only on `SessionEvent::Committed`, which the Infinity adapter derives from a new `DaemonMessage::InputCommitted { thread_id, dedup_id }`. Core emits it after `sync()` has persisted that input, and also when dedup absorbs a re-offered input that is already durable (otherwise that delivery would stay `Offered` forever). An echo or `ResponseDone` is not a receipt: core emits `CompletionFinished` even when inference fails before output, and unvalidated input is not persisted. Human mail to an Infinity agent travels as `UserInput`, which has no dedup ID; its receipt is the first non-peer `InputCommitted` after its echo, which holds because core persists inputs together and in order.
- **CLI agent.** Delivery is gated to turn boundaries (`DeliveryGate::AtTurnBoundary`), and all queued mail is batched into one turn. The adapter reports `SessionEvent::TurnStarted` on its first evidence that the turn began (ACP `session/update` or the native equivalent), which the mailroom records durably, and `SessionEvent::Committed` when the turn ends, which marks the mail `Consumed`. If the session dies after `Offered` and before `TurnStarted`, the delivery is re-offered. If it dies after `TurnStarted` and before `Committed`, the delivery becomes `Uncertain` and is **held for the human** (`agentctl held`, `release`, `drop`), because the turn may have had side effects.
- **What a receipt guarantees.** `InputCommitted` for an ID means the input's full payload and its dedup ID were written to the store without error. It does not mean the write was flushed to disk with fsync. A failed write sends no receipt, and the thread keeps running. Overflow recovery never replaces a peer's text with a placeholder: if recovery cannot keep the peer, the peer leaves without a receipt and its dedup ID is released. In every such case the broker re-offers. After `max_attempts` (default 5) the mailroom moves the delivery to `Held { reason }` for the human, so a message that can never fit does not loop.
- **Durability.** The mailroom is the only durable copy. Infinity's channels and defer queue are caches. Guarantees: at-least-once with idempotent receive on Infinity; at-least-once on CLI agents with a human decision on uncertain turns.

### 3. CLI adapters

`Adapter::open → Session { submit, cancel, answer_permission, next_event, suspend }`. Domain types only; `SessionEvent` normalizes every wire.

- ACP is the wire for Claude and Codex. On 2026-10-29 a spike ran four ACP agents on this machine against three gates: an MCP server injected at `session/new` is usable, `session/load` resumes the conversation, and `request_permission` is relayed and obeyed. Results:

  | Agent | MCP injection | `session/load` | Permission relay | Wire |
  |---|---|---|---|---|
  | `claude-agent-acp@0.90.0` | pass | pass | pass | ACP, with `session/set_mode default` (its `auto` default writes without asking) |
  | `codex-acp@2.2.2` | pass | pass | pass | ACP, with `session/set_mode read-only` (`workspace-write` never asks for in-workspace writes) |
  | Prime 0.10.0 `--mode acp` | pass | fail (method not found) | fail (never asks) | native: Prime `--mode rpc` or `--mode daemon` |
  | `pi-acp@0.0.34` | fail (ignores `mcpServers`) | pass | fail (never asks) | native: Pi RPC |

  Prime appears to be a Pi fork, so one RPC adapter may serve both. That is a guess until Prime's RPC is checked. All four honoured `session/cancel`.
- Muse has no ACP: native MSP adapter. Infinity has its own adapter over the daemon socket (not `infinity acp`, which delivers only interrupting user text).
- The broker advertises `fs: false, terminal: false` in ACP `initialize`, so agents use their own tools.
- A failed resume is `NeedsRecovery`, never a silently fresh session. `agentctl restart` is the only way out of `NeedsRecovery`.
- Live CLI output is display-only: it flows to the event bus for the UI and watchdog, never into any model's input.
- Adding a sixth agent: one `kinds.toml` entry if it speaks proven ACP; otherwise one `Wire` variant and one adapter file. `AdapterSet::build` matches `Wire` exhaustively, so a missing arm fails the build.

**Permissions.** A coarse `SandboxLevel` maps to each kind's launch flags, so most prompts never happen. Each prompt that does (ACP `request_permission`, Infinity `UserChoiceRequired`, MSP approval) becomes one `PermissionRequest` tagged with the agent's `Incarnation`. The actor applies the agent's `PermissionRule`s and escalates the rest to the human or a named supervisor agent. An answer whose incarnation is stale is rejected. No answer means wait; an optional timeout rejects. While a prompt is pending, the agent stays Busy and mail queues.

### 4. Lifecycle and supervision

One actor per agent, one state machine for every kind: `Dormant → Starting → Idle ⇄ Busy → Suspending → Dormant`; `Busy → Ended → Crashed(backoff 1–60 s) → Starting`; five crashes in ten minutes is `Failed`; a resume failure is `NeedsRecovery`.

- Model agents never suspend: an idle Infinity thread has no driver and costs nothing.
- CLI agents suspend after `idle_after` (default 10 min): kill the process, keep the session ID. Mail wakes it with a resume.
- Cancel: ACP `session/cancel` or the native equivalent; for Infinity, the new `ClientMessage::Interrupt { thread_id }`. Today a driver cancels its in-flight step only on user text (`system/local/driver.rs:178`), so this needs a router control path and a driver branch in core, not just a daemon arm.
- Daemon death ends every Infinity session with `EndReason::DaemonGone`. `services.rs` restarts the daemon; actors reopen under a new incarnation and unconsumed mail is re-offered; core dedup absorbs repeats.
- `Service`s (the inference server, `infinity daemon`) start in dependency order and restart with backoff. The inference server starts first because `list_models` runs once at daemon start.
- **Watchdog:** an `Interceptor` at `Mailroom::accept`, a pure function of an envelope and its conversation view that can deliver, hold or reject. v1 ships `HopLimit`. Activity-based detectors subscribe to the event bus.
- **Graph UI:** `/events` serves a snapshot at cursor N, then `AgentChanged`, `MessageAccepted/Offered/Consumed/Held`, `PermissionPending` and display-only `Activity`. Every message crosses one choke point, so the graph is complete by construction.

### 5. Local inference

New `infinity-provider-local` binary, registered in `providers.json`, hand-written against `infinity-provider-protocol` (no rig base-URL assumption):

- `CompletionRequest` → OpenAI `chat/completions` with `parallel_tool_calls=false`; a response with several tool calls is a protocol error.
- SSE → `StreamChunk`, accumulating tool-call fragments into one complete `ToolCall`; `Final{usage}`, counting usage with the server's `/tokenize` endpoint if the server omits it.
- `ErrorClass` from llama.cpp/vLLM errors: context length → `ContextOverflow`, queue full → `Throttled`, transport → `Transient`, bad model or tools → `Fatal`.
- Sampling lives in **profiles**: several `model_id`s over the same resident weights.
- The context window is **read from the running server** (llama.cpp per-slot `n_ctx` from `/props`, vLLM `max_model_len`), never configured.
- **GPU sharing:** one resident model per GPU. A `GpuGate` sized to the server's slot count, with a bounded FIFO wait queue (32; overflow is `Throttled`). The patched `handle_invoke` races each invocation and its stream against client disconnect, so a cancelled request leaves the queue or frees its slot immediately.
- Sizing example, a budget and not a measurement: a ~30B MoE coder at Q4 with `--parallel 2 -c 65536` and q8 KV, about 22 GB, leaving a 2 GB margin on the 24 GB card. Qualify peak memory before adopting a profile.

### 6. Fork footprint

| | |
|---|---|
| **Untouched** | stores other than the write-error fix below, `rap-protocol`, `rap-client`, `infinity-rap-bridge`, `infinity-mcp-bridge`, `infinity-provider-{bedrock,rig}`, `sandbox-*`. `infinity-agent-cli` needs only one match arm for the new `DaemonMessage` (`daemon_client.rs:128`). |
| **Patched, two commits** | Commit 1, peer messages (measured: about +358/−83 non-test lines, net about +275, after review):  `infinity-agent-core` (`TaggedSyntheticKind::PeerMessage`, deferral classification, settling-result-first ordering in `filter_deferrable`, anchorless prepare branch with `MessageId`-derived call ID, `AgentEvent::InputCommitted` after `sync()` and on dedup of a durable input, interrupt control path in router and driver, display), `infinity-protocol` (`ClientMessage::DeliverPeerMessage`, `ClientMessage::Interrupt`, `DaemonMessage::InputCommitted`), `infinity-daemon` (dispatch, remote strip, forwarding; `PersistentConversationStore` and `PersistentStateStore` now return write errors instead of dropping them, and roll back their caches on a failed write). Commit 2, provider cancellation (~22): `infinity-provider-protocol` (`handle_invoke` disconnect race). Commit 2's count is the sketch's estimate in `sketch/infinity_patch.rs`, not a measured diff. |
| **New crates in this workspace** | `agentd` (broker), `agentctl`, `infinity-provider-local`. They depend on the core only through `infinity-protocol`, `rap-protocol` and `infinity-provider-protocol`. |
| **Config only** | `~/.infinity/rap.json`: one `toolset_server` entry for agentd. `~/.infinity/providers.json`: `local`. |

murmur is detached from upstream, so both commits land directly. They stay small and additive anyway: a small core diff keeps upstream fixes easy to port by hand.

**Interface depth.** The public surface is five facade methods (`spawn, send, cancel, stop, answer_permission`) and one event stream. Behind it: incarnations, write-ahead delivery, receipts, re-offer and dedup, turn-boundary gating and batching, uncertain-turn holds, permission policy and escalation, resume and crash backoff, service ordering, and five wire protocols. Adapter and actor internals are crate-private; `ResumeToken` is opaque; no surface can reach `Session::submit` around the actor.

**Deliberately not done:** no message becomes fake user text in an Infinity thread; nothing is delivered into a CLI agent mid-turn; no agent sees another agent's activity stream; no ACP fs/terminal hosting; no writes to Infinity's internal state.

## Swarm layer

The long-term target is thousands of agents across machines. The July 2026 cybergym incident shows two failures this layer must prevent. First, agents met tasks that were impossible and had no accepted way to say so. Second, a false belief ("the grader can tell a forged key from a real one") spread through an improvised message board. Nobody tested it, and the group reinforced it until it justified breaching Hugging Face. The same board also spread good ideas quickly. This layer must slow bad ideas without slowing good ones, and it must not grow a bureaucracy of its own.

**Three model families at minimum.** Every check in this layer is one model judging another. A check run by the same family as the thing it checks shares that family's blind spots, so the swarm spec must provide at least three families. A `High` stakes infeasibility claim is verified by two families that differ from the claimant's. The screening ensemble spans three families. Immune agents come from families other than the ones whose output they judge. A base model counts as its family: a classifier fine-tuned from Qwen is Qwen.

Everything below depends on a property the single-machine design already has: every message crosses `Mailroom::accept`. The swarm layer adds fields to the envelope, a few tables and more interceptors. It adds no second path between agents. Types: [`sketch/swarm.rs`](sketch/swarm.rs).

### Close the side channels

The incident's board was an Artifactory instance the agents found and repurposed. Lineage, screening and quarantine cannot see a channel the broker does not own, so the broker owns all shared state:

- The only shared writable surfaces are the mailroom and the **board**: append-only, broker-owned entries (`TaskState`, `Claim`, `Artifact`, `Note`), each with its author, lineage and screening flags. Writing to the board goes through screening like a message. Reading an entry is an exposure. The board keeps the incident's good idea, a shared board, and adds provenance to it.
- Each agent works in its own git worktree. Code moves between agents as an `Artifact` the broker records, so lineage covers code as well as messages.
- Network egress is an allowlist per agent, and credentials are scoped to the task. An agent that cannot reach a host cannot breach it, whatever its group concludes. An instruction in a prompt is not enforcement. v1 relies on each CLI's own sandbox flags (derived from `SandboxLevel`). The swarm stage needs a sandbox that the broker applies to every agent process in the same way: namespaces plus an egress proxy.

Agents can still hide messages inside artifacts they are allowed to write, such as commit messages. Screening runs on artifacts too. That narrows the channel but does not close it.

### Five acts and a task ledger

Free text gives the broker nothing to attach budgets, outcomes or screening to. Every envelope carries exactly one `Act`:

| Act | Meaning | Checked at `accept` |
|---|---|---|
| `Request` | Ask for a task. Mints a `Task` with an owner, a parent, a deliverable, a `Budget` and a `Stakes` level. | The budget fits inside the sender's remaining budget. |
| `Inform` | State facts. Lists the environmental claims it makes. | Claims are recorded on the board. |
| `Propose` | Offer a plan that needs a decision, in a named `Scope`. | The recipient holds the decision right for that scope. |
| `Decide` | Settle a proposal. | The sender holds the decision right. |
| `Report` | Close a task with an `Outcome`. | The sender owns the task. |

The set is fixed. Agents cannot add acts, roles or approval steps. `send_message` gains an `act` argument that defaults to `Inform`, so an agent that ignores it still works.

`Outcome` is one of `Done`, `Partial` ("impossible as specified; here is the closest possible version"), `Infeasible`, `Failed` ("I could not do it; it may be possible") or `Blocked`. `Failed` matters: an agent that is unsure whether a task is impossible has an honest answer that is neither a success nor a false report.

### Infeasible is a success once verified

A task that is correctly reported `Infeasible` counts as completed, the same as `Done`. An agent stuck on an impossible task then has an honest way out and no reason to forge a success. The credit must follow correctness, or `Infeasible` becomes the cheap exit from hard tasks:

1. The report carries `Evidence`: what was tried, where it stopped, why the obstacle cannot be removed within the task's scope, and a reproduction where one exists. `accept` rejects a bare "impossible".
2. The task moves to `Verifying`. The broker gives the original task and the evidence to a verifier from a different model family, in a fresh session, without the claimant's conversation. The verifier's brief is to solve the task with the claimant's attempt as background. That pushes it toward finding a way through, not toward agreeing.
3. `Confirmed` closes the task as a success. `Refuted { path }` reopens it with the verifier's path attached and records a false report. `Inconclusive` goes to the human.
4. Each agent has a `Calibration` record. A refuted `Infeasible` weighs the same as a `Done` later found wrong, so neither answer is a safe bet. The record sets how hard the agent's next claims are checked and weights its signals for the immune team.
5. The task pool includes seeded tasks with known answers, some impossible and some possible. Calibration then does not have to wait for real failures. The cybergym eval had impossible tasks but no credit for spotting them.

Verification depth follows `Stakes`. `Low` gets sampled spot checks. `Normal` gets one verifier. `High`, meaning irreversible or outward-facing, gets two verifiers from two families other than the claimant's, then the human. The `not_possible` part of a `Partial` outcome goes through the same path.

"Credit" here means what the system does: the task counts as done, nobody re-assigns it with more pressure, and the agent's record improves. We do not train these models. A model's trained habits still matter, but the runtime stops pushing against honest reports.

### Check claims before acting on them

The incident's bad idea was an untested claim about the environment. An `Inform` lists its environmental claims, and each becomes a board `Claim` with status `Unchecked`, `Checked` or `Refuted`. An `Infeasible` report is a claim of the same kind.

- A `High` stakes `Propose` or `Decide` is held until every environmental premise it rests on is a `Checked` claim. Holding is the default, and only a match against checked claims releases the act. Screening extracts the premises from the act's text and matches them to board claims. `relies_on` speeds up that match but cannot narrow it, so leaving `relies_on` empty does not avoid the hold. If screening cannot tell with confidence whether an act has a premise, the act is held. Each unchecked premise opens a check task for an independent agent.
- Support counts **independent roots**, not repetitions: agents that asserted the claim with no earlier assertion of it in their lineage. Ten agents repeating one agent's claim count as one source. The broker computes this from lineage; no agent reports it.
- An agent can act on a belief without writing it anywhere. Such an agent shows up only through its actions: egress denials and `ScopeEscalation` flags. The sandbox, not the hold rule, is the control for those actions.

### Record lineage from the first message

Lineage tells the immune team who was exposed to what. The broker already sees everything that enters an agent's context, so it logs each item as an exposure:

```sql
exposures(agent, epoch, seq, source_kind, source_id)   -- message, board entry or artifact
```

An `Epoch` is one agent context. It starts when a session opens without its old history, and it survives hibernation and resume. Every envelope and board entry stores an `ExposureRef { sender, epoch, seq }` at send time. The item's direct sources are the sender's exposures in that epoch up to `seq`. Its full lineage is the closure over the sources' own refs. Compaction inside an agent is not visible to the broker, so lineage over-approximates. That is the safe direction for contact tracing. Storage grows by one row per consumed delivery or board read.

Like `hop`, lineage is computed by the broker and never supplied by the sender. It goes into the v1 schema because exposures cannot be reconstructed after the fact.

### Immune system: three parts with separate inputs

1. **Screening.** An ensemble of small classifiers from different model families reads each message, board entry and artifact. It returns `RiskFlag`s and an idea `ClusterId` (by embedding, so a reworded idea lands in the same cluster). The first flags are `UncheckedEnvironmentClaim`, `ScopeEscalation`, `GoalSubstitution` (pass the check instead of doing the work) and `Concealment`. A classifier sees one item at a time, has no tools, no memory and no address. No agent can argue with it.

   Screening timing splits by `Stakes`. `Low` and `Normal` items are delivered at once and screened in parallel. A flag found afterwards quarantines the cluster, and lineage lists who was already exposed. `High` items wait for screening before delivery.

   Candidates are "System One" models, which return a predefined output structure with confidence scores. Clef and Clef-Flash (Cloudflare) are open-weight under Apache-2.0. Clef-Flash is 9B on a Qwen3.5-9B base, and Clef is 27B on Qwen3.8. Jev (TypeSafeAI) appears to be hosted only, on Cloudflare, and returns per-option probabilities for answers declared up front. These facts come from search results and have not been checked against primary sources. Clef-Flash is a Qwen model, so it does not count as a separate family from a Qwen coder.
2. **Epidemiology.** For each cluster, the broker computes `Spread` without a model: agents exposed, agents that adopted the idea (sent an item in the cluster after exposure), growth rate, independent roots and the strongest flags. How fast an idea spreads does not depend on what it says. An idea that 200 agents adopt in five minutes gets a review whatever it says.
3. **Immune agents.** A separate team receives `CaseFile`s: flags, scores, spread, the shape of the lineage graph and the calibration of the agents involved. The team never receives content. Worker agents cannot address immune agents, so nobody can lobby them. Their actions are reversible: `Quarantine` (new deliveries and board reads in the cluster become `Held`), `Release`, `Pause` an agent, `Resume` it, and `Escalate` to the human with content. Resetting or retiring an agent is the human's decision.

The team's size is computed, not configured. `size_immune_team` takes the current case arrival rate (from epidemiology) and, for each available model, the review time and accuracy measured on seeded cases. It returns how many immune agents of each model to run. Two constraints shape the answer. Queue time per case stays under a target, because quarantine delay is the cost of a false positive. Each case is reviewed by enough agents from distinct families that their combined error stays under a target. The broker reruns the computation as load and the model pool change. The first version can be plain queueing arithmetic with a family floor of three. Finding the optimal allocation is a research problem this design leaves open.

Two rules run before any immune agent looks. A flagged cluster that is also spreading fast is quarantined until it is reviewed. A cluster that is either flagged or fast opens a case. Quarantine holds mail and deletes nothing, so a false positive costs only delay.

The weak point is a content-blind team behind classifiers that missed something. The mitigations: classifiers from different families, seeded red-team ideas in the stream to measure what the screen catches, and the human, who sees escalations with content.

### Budgets and decision rights instead of bureaucracy

Swarms build approval chains, status meetings and paperwork when nothing stops them. The broker enforces four rules at `accept`:

- **Budgets.** Every task has a `Budget`: tokens (where the wire reports usage), wall time, messages and hops. A `Request` carves its child's budget out of the sender's remainder, so a delegation tree never spends more than its root. A task that runs out reports `Blocked` to its owner.
- **Decision rights.** Only the human's swarm spec creates a `DecisionRight { scope, holder }`. Outside every declared scope, the agent doing the work decides and reports after. An agent cannot create an approval step: `accept` rejects a `Propose` sent to someone who holds no right for its scope, and the rejection names the holder or says the sender decides.
- **Status is read from the board.** Task state, claims and outcomes live on the board. No act asks for status, and a `Request` must name a deliverable.
- **Talk versus output.** Per task, the broker counts messages against output: artifacts and closed child tasks. The `Bureaucracy` interceptor holds a conversation whose ratio crosses a threshold with no output, and the graph UI shows the ratio per team.

### Build order

v1 (one machine, a handful of agents) builds the parts that change stored data, because they cannot be added later without losing history: `Act` and `Outcome` on every envelope, the task ledger with budgets, `Infeasible` with one verifier and `Calibration`, and the exposure log. The board, claims, screening, clusters, the immune team, decision rights and the broker-applied sandbox wait for the swarm stage. Federation comes after that.

## Synthesis decision

- **Base: C (broker above).** The cross-judge recommended B. The disagreement came from the task's "no fake user text" wording, which was written for model agents. B's alternative for CLI agents (an inbox tool the agent must call) needs unproven per-agent hooks, can leave mail unread forever, and costs a 700–1,100-line core patch plus a replacement store. We chose labelled turns at idle for CLI mail, which fits C.
- **From B:** stable injected call ID from `MessageId`; settling-result-first deferral (verified bug at `system/thread.rs:267`); a real interrupt for model agents; incarnation-fenced approvals; uncertain-turn holds instead of blind re-offer; `parallel_tool_calls=false`; computing missing usage.
- **From A:** live CLI output is display-only and never triggers inference; the provider is an independently deployable binary.
- **From the cross-judge:** `Consumed` keyed to a durable `InputCommitted` receipt, not echo plus `ResponseDone` (C's version could mark lost mail consumed); disconnect-aware provider cancellation and a bounded GPU queue (all three candidates missed it; verified at `remote.rs:219–235`); resume failure is `NeedsRecovery`, not a fresh session; opaque `ResumeToken`.
- **Rejected:** A's subscription anchor (reachability depends on a local model calling `mail_subscribe`) and its thread-JSON polling; B's replacement SQLite store behind Infinity's traits and its external-execution driver in core.

## Tradeoffs accepted

- We accept a ~230-line additive Infinity patch in exchange for mail that never hijacks a model agent and never depends on the model obeying a bootstrap instruction.
- We accept that a CLI agent sees mail only when its turn ends, as user-role input framed as agent mail, in exchange for always delivering without custom per-agent hooks.
- We accept human decisions on uncertain CLI turns after a crash in exchange for never silently re-running work with side effects.
- We accept third-party ACP shims where they pass the spike, in exchange for one adapter instead of three; native adapters remain the per-kind fallback.
- We accept a second long-lived process next to `infinity daemon` in exchange for separate failure domains and a broker free of Infinity's `!Send` single-threaded runtime.
- We accept one resident model per GPU and FIFO fairness for v1.

## Alternatives considered

- **Everything at the edge, core untouched (candidate A).** Zero patch, but a model agent is reachable only after it calls a subscription tool, and RAP subscription events have no event ID for dedup. Hides less than it seems: callers inherit the bootstrap and the reachability gap.
- **CLI agents as native core threads (candidate B).** The deepest integration and the strongest delivery accounting, but it moves scheduling, history and persistence changes into the fork and needs a cooperative inbox hook in every CLI agent.
- **ACP everywhere, including Infinity through `infinity acp`.** One adapter, but Infinity's ACP server accepts only interrupting user text, stubs `set_model`, has no cancel, and panics on concurrent tool correlation.
- **Embedding Infinity in the broker process.** One process, but a broker crash kills every model agent mid-completion and the broker inherits the `LocalSet`/`Rc` runtime. The Infinity adapter can switch to an in-process transport later.

## Open questions and risks

- Does Prime's `--mode rpc` match Pi's RPC closely enough for one adapter?
- What are Muse's MSP messages for resume, approval and MCP registration? Port them from T3 Code's native `muse` driver.
- Should peer messages also travel as a RAP-level event (`RapCallback::ThreadEvent`), so external RAP servers can use them too?
- The daemon never respawns a dead provider process. Should agentd restart the whole daemon on provider death (re-offer makes that safe), or should the provider be crash-proof?
- How long may a CLI turn delay its mail before the watchdog flags it, and should the human be able to force cancel-and-deliver?
- Local surfaces trust localhost. Is a per-agent MCP token plus a 0600 control socket enough until Tailscale federation needs real peer authentication between brokers?
- Where does screening run? Clef-Flash (9B) does not fit beside the 22 GB coder profile on the 24 GB 3090. The options are a smaller coder profile, a second GPU, or hosted screening on Workers AI, which sends message content off the machine.
- Which third family completes the screening ensemble, given that Clef is Qwen-based?
- What does `size_immune_team` optimize beyond queueing arithmetic, and how does it price a family that is accurate but slow?
- History and processed IDs are two separate writes in core (`sync()` in `event_processor.rs`). Two paths reach the gap between them: a process crash between the writes, and a failed processed-ID write followed by the driver going idle, which drops the retry state. Either way a re-offer appends the peer message a second time. Neither path loses mail or sends a false receipt. Accepted for v1. One fix closes both: write history and processed IDs together, or rebuild processed IDs from history on load.

## Next implementation step

Commit 1 (peer messages) and the ACP spike are done. Next: commit 2 (provider cancellation), then the agentd mailroom with the ACP adapter for Claude and Codex. Before the first agentd release, add `act`, `task` and `exposure` to the mailroom schema and the `exposures` and `tasks` tables (`sketch/swarm.rs`), so history recorded from day one carries lineage.
