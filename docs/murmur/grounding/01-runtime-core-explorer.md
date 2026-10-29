# Explorer findings: Infinity runtime core (infinity-agent-core)

Source: Muse Wonder Full explorer, read-only pass over upstream Infinity at commit 1abc6fc. Paths are relative to that repo.

## Components

Builder / system shell
- `AgentSystemBuilder::new_local` / `new` / `build` / `build_local` / `start` / `start_with_observer` — crates/infinity-agent-core/src/system/builder.rs:331. `new_local` creates the in-process queue; `new` takes a platform `InputSender` (SQS on Lambda) and exposes only the step API.
- `SystemInner<C,S,M,H>` — builder.rs:62. Shared `Rc` config: both stores, `Box<dyn ModelSource>`, `Box<dyn ThreadConfigSource>`, loopback `sender`, `callback_url`, builtin tool flags.
- `AgentSystem::step` — builder.rs:452. Step mode (serverless): partitions one batch by `group_id`, loads each thread fresh, runs one slice per thread concurrently, drops all state.
- `LocalAgentSystem` — builder.rs:502. Built-but-not-started local system.

Local driver (resident runtime)
- `ChannelSender` — system/local/sender.rs:30. `InputSender` over `mpsc::UnboundedSender<(InputMessage, String)>`; loopback for tool results, child seeds, parent reports, timers.
- `route_loop` / `RunningSystem` / `SubscribeHandle` — system/local/router.rs:239. Owns all driver futures in one `FuturesUnordered` (not one task per thread), maps `ThreadId -> WorkerChannels`, spawns drivers on demand, admission for non-user-text to unknown/stopped threads.
- `drive_thread` / `InFlightStep` / `check_before_idle` — system/local/driver.rs:87. Per-thread loop: batches inputs, runs steps, cancels in-flight completion on user text, defers synthetics, auto-compaction at >75% context, idles out.
- `LaunchingSystem` / `ThreadBuilder::launch` / `LaunchRegistry` / `UnionConfigSource` / `UnionModelSource` — system/local/launch.rs:164. Per-thread tools/prompt/model unioned onto system config, keyed by root thread, process-lifetime only (not persisted).
- `ThreadHandle::recv/send_user_text/send` / `HandleObserver` / `attach` — system/local/handle.rs:76. Client view: `ReplaySnapshot` then live `AgentEvent`s.

Slice pipeline (shared by local and step mode)
- `Thread::load/resolved_config/filter_deferrable/step_no_defer/handle_completion_event` — system/thread.rs:96. One step: prepare batch -> at most one completion round -> `sync()` commit -> dispatch at most one async tool.
- `HistoryManager` — event_processor.rs:141. In-memory history, three-phase persistence (unvalidated -> pending -> synced), `turn_buffer`; owns dedup, tool-result tail matching, interruption injection, `safe_spawn_point`, `get_thread_stack`.
- `prepare_input` — event_processor.rs:889. Folds one `InputMessage` into history; closed threads, compaction, OAuth/user-choice, synthetics.
- `run_completion` — event_processor.rs:1480. Streams one provider invocation (loops after synchronous tools), retries by `ErrorClass`, context-overflow recovery, cancellation.
- `execute_action` / `execute_action_with_error_result` — event_processor.rs:1933. Post-commit async tool dispatch; failure enqueues an error tool result.

Messages
- `InputMessage { content, group_id: ThreadId, metadata, synthetic, display_as, subscription }`, `TaggedSyntheticKind::{SubscriptionEvent, ThreadReport, ParentMessage, Compaction, CompactionComplete}` — message.rs:140. The only input envelope; `group_id` is the routing key (`rap_protocol::ThreadId`).
- `InfinityMessage::{User, Assistant, ToolCall, ToolResult, SubscriptionEvent}` — message.rs:183. Persisted history form.

Stores
- `ConversationStore` / `StateStore` / `InputSender` / `SpawnContext::{InheritUpTo, Fresh}` — traits.rs:27.
- `InMemoryConversationStore` / `InMemoryStateStore` — stores.rs:68.
- `PersistentConversationStore` / `PersistentStateStore` — crates/infinity-daemon/src/memory_store.rs:50. Delegate to in-memory core, add per-thread JSON files and `ThreadExtras` (title, children, tokens, `selected_model`). Whole-file rewrite per mutation via `std::fs::write(...).ok()` (write errors silently dropped).
- `DsqlConversationStore`, `DynamoDbStateStore` — Lambda impls.

Tools
- `Tool::execute/execute_synchronous/supports_sync/is_passive`, `ToolContext { message_sender, group_id, callback_url, user_id, thread_stack, safe_spawn_point }` — tools/mod.rs:49.
- Builtins (tools/thread.rs:15): `SpawnThreadTool` (sync), `ReportToParentTool`, `CloseThreadTool`, `SendMessageToChildTool`; sleep tools (tools/sleep.rs); `RapTool::invoke_rap_tool` (tools/rap_tool.rs:72).
- `DeferQueue::{InMemoryDeferQueue, NoDeferral}` — system/defer.rs:27.

Model selection
- `ModelSource::resolve` / `StaticModel` — system/model.rs:25. Resolved at the start of every completion round.
- `ThreadConfigSource::resolve` / `ThreadConfig` — system/config.rs:24.
- `ModelProvider::{list_models, invoke_model}` / `ModelEntry` / `CompletionModel` / `SingleModelProvider` — infinity-provider-protocol/src/lib.rs:68; `CompletionRequest/StreamChunk/ErrorClass/Usage` — completion.rs:16; `serve_provider/RemoteModelProvider` (NDJSON over Unix socket, one request per connection) — remote.rs:149.
- `CatalogModelSource` — infinity-daemon/src/models.rs:380. Per-thread persisted `ModelRef {provider_id, model_id}`.

Observation
- `ThreadObserver::{on_event, on_subscribe}` — system/observer.rs:30; `AgentEvent` / `ReplaySnapshot` / `UserChoice` — system/events.rs:24; `ThreadLifecycleEvent::{Live, Idle}` — driver.rs:64.

## Flow

Startup
1. `new_local` -> `ChannelSender::new_pair()` creates one unbounded channel. Static vs dynamic config are mutually exclusive (builder.rs:186).
2. `.start()` spawns `route_loop` with `tokio::task::spawn_local` — the whole system requires a `LocalSet`.
3. `thread_builder().launch()` mints UUID `ThreadId`, registers `LaunchConfig`, `attach()` subscribes; the subscribe spawns the first driver which replies with a `ReplaySnapshot` before ack (race-free).
4. `Thread::load` -> `HistoryManager::new_with_history`: ancestor chain, history with ancestors, metadata keyed by root, processed IDs keyed by this thread.

One user message
5. `ThreadHandle::send_user_text` overwrites `group_id` with the handle's thread, random dedup ID -> channel.
6. `route_loop` keys on `group_id`: live worker gets it; else admission then a new `drive_thread` future.
7. `drive_thread` batches, `filter_deferrable` (synthetics held while a non-passive tool call is pending), runs `step_no_defer`.
8. Prepare: dedup by message ID, tool-result tail matching, interruption of pending call for non-tool-results.
9. Complete: `model.resolve(thread_id)` per round; `run_completion` calls `provider.invoke_model(model_id, CompletionRequest { preamble, chat_history, tools, max_tokens: None, additional_params: None })`. Chunks into `turn_buffer`.
10. Commit/dispatch: `history.sync()`, `AgentEvent::CompletionFinished`, then dispatch the async tool. The tool call is durable before any result can arrive.
Steps 8–10 are the slice. A slice may contain several provider invocations (loops after sync tools).

Async vs sync tools
11. Async `Tool::execute` (default): `RapTool` POSTs `RapInvocation {group_id, thread_ancestors, callback_url}` fire-and-forget; `TokioSleepTool` spawns a timer. Result re-enters as a new `InputMessage` (dedup ID = tool-call ID) via `ToolContext.message_sender`. `match_tool_result` requires the matching unanswered call at the history tail, else dropped.
12. Sync tools (`supports_sync()`): only `SpawnThreadTool` and `CancelSubscriptionTool`. Run inline and loop in the same slice.

Child threads
13. `spawn_thread {instructions, child_of, fresh_context?}` validates `child_of == thread_stack`, `conversation_store.spawn_thread(parent, tool_call_id, InheritUpTo(safe_spawn_point) | Fresh)`.
14. Child seeded with a copy of the spawn call plus a `ToolResult` carrying instructions.
15. `ReportToParentTool` / `CloseThreadTool` send `InputMessage` to parent with `synthetic: ThreadReport {tool_call_id, child_thread_id}`; parent mints a synthetic `receive_event__injected` tool call and stores `InfinityMessage::SubscriptionEvent`. `SendMessageToChildTool` mirrors it (`ParentMessage`) and validates `parent_id == context.group_id`.
16. Non-associative subscription events spawn an ephemeral child thread; the subscribing thread is not interrupted.

Hibernation
17. `ThreadHandle::recv` fed by `HandleObserver::on_event`. `CompletionFinished` ends a round, not the conversation.
18. Locally, hibernation = driver exit. `check_before_idle`: pending tool call -> park; else exit even with active subscriptions. Later input respawns a driver that reloads from stores.

Router admission
19. With no live driver and non-user-text input: `thread_exists` must be true and `is_thread_stopped` false (both fail open on store error). User text bypasses both and implicitly creates threads.
20. User text arriving mid-step cancels the step, prefixes `<interrupt>`, injects "Tool call interrupted by user" for a pending call. Non-user inputs queue.

Persistence
- ConversationStore: per-thread own messages `(InfinityMessage, message_id)`, hierarchy row, compaction summaries; history reconstructed from ancestors.
- StateStore: processed IDs (per thread), metadata (per root), subscriptions, pending user choices.
- Inputs are not persisted until the model produces output for them (poison-message safety).

## Seams

(1) Local-model provider: clean. Implement `ModelProvider` (or `CompletionModel` + `SingleModelProvider`), use in-process or out-of-process via `serve_provider`/`RemoteModelProvider` (how daemon provider binaries work). Obligations: classify errors via `ErrorClass`; emit complete `StreamChunk::ToolCall` — only the first tool call per stream executes, the rest is dropped until `Final`; unsigned `StreamChunk::Reasoning` blocks are skipped (use `ReasoningDelta` or text); honest `Usage.total_tokens` and nonzero `context_window` or auto-compaction never fires. `run_completion` sends `max_tokens: None, additional_params: None`, so sampling config must live inside the provider. `infinity-provider-rig::RigCompletionModel` offers an OpenAI-compatible route (client construction not verified).

(2) External CLI agents: no first-class shape. (a) async `Tool::execute` that supervises the CLI and delivers one `ToolResult` — but `ToolContext` has no cancellation token, so interruption orphans the subprocess; (b) a RAP tool server wrapping the CLI (progress via subscription events, each spawning an ephemeral child thread); (c) a `ModelProvider` facade where the CLI is the model behind a thread — the CLI's own tool calls either stay opaque in text or get re-executed by core if emitted as `ToolCall` chunks. There is no non-LLM thread kind: every `Ready` input triggers a completion. A supervised-peer design needs (c) with careful chunk translation, or a new driver variant.

(3) Peer messaging: mechanically possible, semantically unguarded. `ToolContext.message_sender` is `pub`; `RunningSystem::sender()` etc. are documented for "external message injectors"; no auth/capability check in core. Plain user text to a peer works but cancels in-flight completions and interrupts pending tools. A forged `ThreadReport`/`ParentMessage` lands iff `find_tool_call(tool_call_id)` resolves any call in the recipient — relationship unverified. A proper `PeerMessage` needs: new `TaggedSyntheticKind` variant, deferrable classification in `is_deferrable_synthetic_event`, a `prepare_input` branch that mints a `receive_event__injected`-style call, a history variant (or reuse `SubscriptionEvent`), inclusion in `sync()` durable dedup, unique dedup IDs, `input_event` naming. External processes need a bridge: `ChannelSender` can't cross a process boundary; today the bridge is the RAP callback server.

## Non-obvious

- The local runtime is single-threaded by construction: `spawn_local` router, `Rc<SystemInner>`, `Rc<dyn Tool>`, `#[async_trait(?Send)]` on ModelSource/ThreadConfigSource/ThreadObserver. Drivers are futures in one `FuturesUnordered`, so idle threads cost zero memory.
- One async tool at a time; only the first streamed tool call counts.
- Inputs persist only after the model produces output for them; redelivery is the recovery path, which the in-process channel does not provide.
- Metadata keyed by root, dedup/subscriptions/choices by exact thread.
- Model inheritance: core resolves the root's launch model live; the daemon copies `selected_model` at spawn. Launch configs are lost on restart.
- `RunningSystem::send_user_text` to any string ID creates a thread; `LaunchingSystem::thread_handle` on unknown ID returns `Ok(None)`.
- Closed (`close_thread`) != stopped (daemon session shutdown).
- No pause/inspect/stop-thread API in local mode: supervision = subscribe + interruptive user text + whole-system `shutdown()`.
- Step mode has no router admission and typically uses `NoDeferral`.

## Open questions

- How the daemon's RAP callback listener maps HTTP callbacks into `InputMessage`s (explorer B's slice).
- Whether `infinity-provider-rig` exposes an OpenAI-compatible client with configurable `base_url`.
- How daemon session stop interacts with a live driver mid-completion.
- A panicking tool/observer kills only its driver future, but unvalidated inputs are lost with it.
