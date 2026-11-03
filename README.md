# murmur

murmur is an agent runtime for one machine first and many machines later. It hosts local-model agents and CLI agents (Claude Code, Codex, Muse, Pi and Prime Agent) as peers. Any agent can message any other by address, and every message passes through one broker. The broker keeps the mail durable, records who saw what, and enforces budgets.

The long-term target is large agent swarms that work across machines without spreading bad ideas or growing their own bureaucracy. The design treats a verified "this task is impossible" as a success. It checks claims about the environment before anyone acts on them, and it gives an immune team spread statistics and risk flags instead of message content.

## Status

`agentd` runs on one machine. It hosts Claude and Codex agents over ACP and Infinity model agents, and every message goes through its SQLite mailroom. The [roadmap](docs/murmur/roadmap.md) lists what is built and what comes next.

- [Design](docs/murmur/design.md): the `agentd` broker, CLI adapters, local inference on one GPU, and the swarm layer.
- [Isolation](docs/murmur/isolation.md): the bubblewrap sandbox for ACP agents and its remaining risks.
- [agentd usage](crates/agentd/README.md): running `agentd` and `agentctl`.

## Agent credential sharing

The [agentd isolation design](docs/murmur/isolation.md) describes the sandbox and
its remaining risks. Credential copy-back preserves the baseline JSON object's
top-level keys, limits files to 64 KiB, and accepts at most one update per agent
per 30 seconds. Same-kind siblings can pass small JSON values through credential
propagation outside mail attribution. Shared workspaces also allow a sibling
channel. The broker cannot verify token freshness or intent, and an unrelated
host writer can race the final comparison and rename. Configure provider env
tokens with `credentials = []` to avoid credential copy-back and propagation.

## Based on Infinity

murmur is a fork of [Infinity](https://github.com/hydro-project/infinity) by the Hydro contributors, detached at commit `1abc6fc`. It does not track upstream. The crates keep their `infinity-*` names for now, and the runtime, RAP and provider protocol are Infinity's. Infinity's own README is in [docs/infinity-README.md](docs/infinity-README.md). murmur is not affiliated with or endorsed by the Infinity project.

## License

Apache-2.0, the same as Infinity. See [LICENSE.txt](LICENSE.txt) and [NOTICE](NOTICE).
