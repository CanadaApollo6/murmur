# Grounding synthesis: constraints any design must honor

Distilled from 00/01/02 in this directory. Those files have the citations; read them for detail.

## Infinity facts that shape the design

1. **One input envelope, one routing key.** Everything that reaches a thread is an `InputMessage` with `group_id` = thread ID, pushed through an `InputSender` (`ChannelSender` locally). The router spawns or wakes a per-thread driver on demand. External processes already inject into threads this way through the RAP callback HTTP server (`rap_callback.rs` -> `ChannelSender`).
2. **Every thread is an LLM loop.** There is no non-LLM thread kind: every ready input triggers a completion through the resolved `ModelProvider`. Only the first tool call per completion stream executes. One async tool call in flight per thread.
3. **Hibernation is driver exit.** An idle thread is pure store data (`~/.infinity/threads/*.json` via `PersistentConversationStore`), woken by any new input. Inputs persist only after the model produces output for them.
4. **Tree-only inter-thread messaging.** `spawn_thread`, `report_to_parent`, `send_message_to_child`, `close_thread`. No peer messaging. Synthetic messages (`ThreadReport`, `ParentMessage`) must anchor to a tool call ID in the recipient's history, else `prepare_input` drops them. Plain user text to another thread works today but cancels its in-flight completion and interrupts pending tools. A real `PeerMessage` needs a new `TaggedSyntheticKind`, a prepare branch that mints a `receive_event__injected`-style call, deferral classification, and durable dedup.
5. **Deferral exists.** Synthetic events are held in `InMemoryDeferQueue` while a non-passive tool call is pending, so they don't interrupt it. The queue is in memory only.
6. **Model providers are clean.** `ModelProvider { list_models, invoke_model }`, run out of process via `serve_provider` (NDJSON over a Unix socket) and listed in `~/.infinity/providers.json`. The runtime sends `max_tokens: None, additional_params: None`, so sampling config lives in the provider. `context_window` must be honest for compaction. `ErrorClass::ContextOverflow` is load-bearing. `infinity-provider-rig` bridges rig backends; whether rig 0.31's OpenAI client takes a custom base URL is unverified. A hand-written OpenAI-compatible provider is a few hundred lines (request converter plus SSE-to-`StreamChunk` with tool-call fragment accumulation).
7. **RAP servers are the supervised-process mechanism.** The daemon spawns, process-groups, restarts and SIGINTs command RAP servers (`ManagedRapServer`). Long-running work is a subscription: an immediate `tool_result` with `subscription=true`, then `subscription_event`s (`associative=true` inlines into the owning thread; `false` spawns an ephemeral handler child thread), `user_choice` for prompts, `view_update` for UI-only state, `final=true` to end. Events anchor to one `(group_id, tool_call_id)` owned by one thread. Command servers are killed when a session idles unless an active subscription keeps it alive.
8. **ACP both ways is available.** Infinity is an ACP *server* (`infinity acp`; partial: no cancel, no set_model, text-only, single in-flight tool correlation). The `agent-client-protocol 0.11.1` crate it already depends on also has the client role (`role::acp::Client`), unused. A host using it must answer agent->client requests: `session/request_permission`, `fs/*`, `terminal/*`.
9. **Single-threaded by construction.** Daemon, router and drivers run on one `current_thread` runtime + `LocalSet` (`Rc`, `?Send` traits). Providers and RAP servers are separate OS processes so blocking work does not stall the loop.
10. **No auth on local surfaces.** Callback listener, RAP endpoints, WebSocket and Unix socket trust localhost. sandbox-local binds 0.0.0.0.
11. **Remote daemons already exist**: SSH-tunnelled `remotes.json`, with `Emigrate`/`ImportSession` session migration in the daemon protocol.

## CLI agents on this machine (from 00-cli-agents-scout.md)

| agent | best programmatic channel | ACP | resume |
|---|---|---|---|
| Claude Code 2.1.312 | `claude -p --input-format stream-json --output-format stream-json` | adapter `claude-acp` (not installed) | `--resume <id>`, `--session-id` |
| Codex 0.165.0 | `codex app-server` (JSON-RPC, stdio/ws) | adapter `codex-acp` (not installed) | `codex exec resume <id>` |
| Muse Code 1.4.14 | `muse serve` (MSP over stdio, `--listen` socket) | none | `muse resume <ref>` |
| Pi 1.4.0 | `pi --mode rpc` | adapter `pi-acp` (not installed) | `--session <id>`, `-r` |
| Prime Agent 0.10.0 | `prime-agent --mode acp` (native), also `rpc`, `daemon` | native | `-r`, `--fork` |

All five can resume a prior session by ID, so a CLI agent can also "hibernate" by exiting its process and resuming later, at a cold-start cost. Each has its own permission/approval flags. T3 Code on this machine already drives Claude, Codex, Muse and Pi through per-provider drivers.

## Product requirements (from the user)

- One on-machine runtime on the user's Linux box (RTX 3090, 24 GB) that hosts **both**:
  - (A) Infinity-loop agents whose inference runs on local models (llama.cpp or vLLM, OpenAI-compatible HTTP), and
  - (B) external CLI agents: Claude Code, Codex, Muse, Pi, Prime Agent, as supervised peers.
- Agents of either kind can message each other (peer, not only parent/child).
- Build on Infinity's baseline; stay close enough to upstream to keep pulling its changes.
- Longer-term direction, not v1 scope but the design must not block it: a chatty agent team across the user's machines over Tailscale; a watchdog that detects agents looping or bureaucratic back-and-forth; a live graph GUI of agents and messages.
