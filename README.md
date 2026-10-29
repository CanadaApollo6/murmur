# murmur

murmur is an agent runtime for one machine first and many machines later. It hosts local-model agents and CLI agents (Claude Code, Codex, Muse, Pi and Prime Agent) as peers. Any agent can message any other by address, and every message passes through one broker. The broker keeps the mail durable, records who saw what, and enforces budgets.

The long-term target is large agent swarms that work across machines without spreading bad ideas or growing their own bureaucracy. The design treats a verified "this task is impossible" as a success. It checks claims about the environment before anyone acts on them, and it gives an immune team spread statistics and risk flags instead of message content.

## Status

Design stage. Nothing in this repository runs as murmur yet.

- [Design](docs/murmur/design.md): the `agentd` broker, CLI adapters, local inference on one GPU, and the swarm layer.
- [Type sketch](docs/murmur/sketch/lib.rs): Rust signatures with `todo!()` bodies.
- [Grounding notes](docs/murmur/grounding/03-constraints.md): what the Infinity code does today, with file and line references.

The first implementation step is the peer-message change to the Infinity core described in the design's "Next implementation step".

## Based on Infinity

murmur is a fork of [Infinity](https://github.com/hydro-project/infinity) by the Hydro contributors, detached at commit `1abc6fc`. It does not track upstream. The crates keep their `infinity-*` names for now, and the runtime, RAP and provider protocol are Infinity's. Infinity's own README is in [docs/infinity-README.md](docs/infinity-README.md). murmur is not affiliated with or endorsed by the Infinity project.

## License

Apache-2.0, the same as Infinity. See [LICENSE.txt](LICENSE.txt) and [NOTICE](NOTICE).
