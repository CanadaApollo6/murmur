# murmur roadmap

Status as of 2026-11-11. The [design](design.md) explains each part. This page
orders the remaining work.

## Where murmur is

`agentd` runs on one machine. It hosts Claude and Codex agents over ACP and
Infinity model agents over the daemon socket, and the SQLite mailroom is the
only delivery authority.

| Area | State |
|---|---|
| Core peer messages, provider cancellation | Merged (#1, #2) |
| Mailroom, ACP supervisor, `agentctl`, bound MCP tools, Infinity adapter | Merged (#3 to #6) |
| [ACP per-agent isolation](isolation.md): bubblewrap, egress proxy, credential sync | Merged (#7) |
| v1 swarm schema: `Act` on every envelope, task ledger with budgets, `Report` outcomes, exposure log | Built. Agents open and report tasks with `send_message`. The operator opens them with `agentctl send --act` and lists them with `agentctl tasks` |
| Mail limits | Merged (#8) |
| Infinity model agents sending mail | Merged (#9) |
| `infinity-provider-local` | Merged (#10). Live engine probes and agent qualification are pending. See [Run local inference](local-inference.md) |
| Event bus and graph UI | Merged (#13, #14). See the [agentd README](../../crates/agentd/README.md#graph-ui) |
| Verifier and calibration | Built. With a `[verify]` table, a verifier of another family checks each `Partial` and `Infeasible` claim in a sandboxed snapshot of the claimant's workspace. Without one, those claims go to `review` for the operator. Seeded tasks (`agentctl send --expect`) settle by their known answer. Calibration events score claimants and verifier families against seeds and operator decisions, and `agentctl calibration` prints them. Nothing acts on the scores yet. Checking `Done` claims and two verifiers for high stakes are not built |
| Pi, Prime and Muse adapters | Not built |
| Infinity sandbox | Not built. Infinity agents run on the host (isolation risk R6) |

## Milestones

Each milestone ends in a merged PR with its tests in `./check.bash`.

1. **Land isolation.** Done (#7).
2. **Mail limits.** Done (#8). A per-sender cap and a per-pair cap on undelivered mail,
   checked at `Mailroom::accept`, and a per-turn batch limit at the pump. This
   bounds what one agent can queue before more agents can send.
3. **Infinity agents send mail.** Done (#9). A RAP toolset
   provides `send_message` through daemon-owned invocation claims and live root
   bindings. Child threads share their root agent's sender and mail limits.
4. **Local inference.** Implemented (#10) for TensorFold,
   llama.cpp, vLLM, SGLang, and generic loopback servers. Configured profiles,
   per-server gates, streaming tools and reasoning, bounded retries, and overflow
   guards have deterministic and mock-engine coverage. Live engine probes and
   the RTX 3090 agent qualification remain pending. See
   [Run local inference](local-inference.md).
5. **Event bus and graph UI.** Done (#13, #14). `/events` over WebSocket: a
   snapshot at a cursor, then agent, message and permission events. The graph UI
   is read-only and the web listener is off by default.
6. **Close the v1 swarm loop.** Built (#30 to #33, and the verifier runner). A
   verifier from another model family checks each `Verifying` task in a fresh
   session. Calibration events record who was right. Seeded tasks with known
   answers measure both. Nothing acts on the calibration scores yet.
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

## Proposed after milestone 6

These items are proposed and not yet agreed. They would come before the native
adapters, in this order.

1. **Trust by provenance.** Every sender gets a trust label, set by how its mail
   arrived: the operator, an agent, or an outside source. The broker derives
   taint from the exposure log. An agent that has read untrusted content is
   tainted, and its outbound acts are then checked. No agent supplies the label.
   Today everything sent with `agentctl send` counts as the operator.
2. **Delegation to a kind, and parked `blocked` tasks.** A `Request` addressed to
   a kind makes the broker start an ephemeral agent, carve its budget and retire
   it on its report. This reuses the verifier runner. A `blocked` report parks
   its task, and an answer from the requester reopens it. `list_agents` lists
   the kinds an agent may delegate to, with their families.
3. **A timer and webhook source.** One generic source that sends mail through
   `accept` as a non-human sender, labelled untrusted.

The order matters. A source without a trust label would arrive with the
operator's rights.

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
