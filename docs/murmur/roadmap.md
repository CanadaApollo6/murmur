# murmur roadmap

Status as of 2026-11-03. The [design](design.md) explains each part. This page
orders the remaining work.

## Where murmur is

`agentd` runs on one machine. It hosts Claude and Codex agents over ACP and
Infinity model agents over the daemon socket, and the SQLite mailroom is the
only delivery authority.

| Area | State |
|---|---|
| Core peer messages, provider cancellation | Merged (#1, #2) |
| Mailroom, ACP supervisor, `agentctl`, bound MCP tools, Infinity adapter | Merged (#3 to #6) |
| [ACP per-agent isolation](isolation.md): bubblewrap, egress proxy, credential sync | Done on `feat/agentd-isolation`, not merged |
| v1 swarm schema: `Act` on every envelope, task ledger with budgets, `Report` outcomes, exposure log | Built in the mailroom |
| Mail limits | Done on `feat/agentd-mail-limits`, not merged |
| Infinity model agents sending mail | Done on `feat/agentd-rap-send`, not merged |
| `infinity-provider-local` | Not built |
| Event bus and graph UI | Not built. `agentctl watch` streams events |
| Verifier and calibration | Not built. `Partial` and `Infeasible` reports reach `Verifying` and stay there |
| Pi, Prime and Muse adapters | Not built |
| Infinity sandbox | Not built. Infinity agents run on the host (isolation risk R6) |

## Milestones

Each milestone ends in a merged PR with its tests in `./check.bash`.

1. **Land isolation.** Merge `feat/agentd-isolation`.
2. **Mail limits.** A per-sender cap and a per-pair cap on undelivered mail,
   checked at `Mailroom::accept`, and a per-turn batch limit at the pump. This
   bounds what one agent can queue before more agents can send.
3. **Infinity agents send mail.** Done on `feat/agentd-rap-send`. A RAP toolset
   provides `send_message` through daemon-owned invocation claims and live root
   bindings. Child threads share their root agent's sender and mail limits.
4. **Local inference.** Implemented on `feat/local-inference` for TensorFold,
   llama.cpp, vLLM, SGLang, and generic loopback servers. Configured profiles,
   per-server gates, streaming tools and reasoning, bounded retries, and overflow
   guards have deterministic and mock-engine coverage. Live engine probes and
   the RTX 3090 agent qualification remain pending. See
   [Run local inference](local-inference.md).
5. **Event bus and graph UI.** `/events` over WebSocket: a snapshot at a cursor,
   then agent, message and permission events.
6. **Close the v1 swarm loop.** A verifier from another model family checks each
   `Verifying` task in a fresh session. `Calibration` records feed later checks.
   Seeded tasks with known answers measure both.
7. **Native adapters.** Pi and Prime over RPC (one adapter if Prime's RPC matches
   Pi's), and Muse over MSP.
8. **Swarm stage.** The board and claims, screening, epidemiology, the immune
   team, decision rights (`Propose` and `Decide` are rejected today), a
   broker-applied sandbox for every agent including Infinity, per-agent clones,
   and an API-auth forwarder in place of copied credentials.
9. **Federation** across machines.

Milestone 3 comes before milestone 4, which reverses the design's earlier order.
A local model agent that cannot send mail is not yet a peer, and the RAP toolset
works with the existing providers.

## Carried risks

These stay open until a milestone closes them. The design's
[open questions](design.md#open-questions-and-risks) has the details.

- History and processed IDs are two writes in core, so a crash between them can
  append a re-offered peer message twice.
- The Infinity daemon's RAP callback listener is unauthenticated. Disabling the
  WebSocket listener does not close this ingress. Callback authentication needs
  separate work.
- Recovery across an Infinity daemon restart is outside the RAP send guarantee.
- The Infinity daemon never respawns a dead provider process.
- Migration of agentd-hosted sessions is unsupported until the handoff protocol
  exists.
- Replay can re-offer an answered choice until dismissal clears persistence.
- `r12o_repeated_cli_quit_crash_budget` in `tests/infinity_recovery.rs` fails
  about once in 15 isolated runs. It waits 3 seconds for a killed command RAP
  server's port to close.
