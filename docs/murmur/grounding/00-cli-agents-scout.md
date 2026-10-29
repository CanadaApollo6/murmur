# Scout: headless interfaces of local CLI agents (from --help, no sessions run)

| agent | binary | version | headless | multi-turn programmatic channel | ACP | resume | local endpoint |
|---|---|---|---|---|---|---|---|
| Claude Code | ~/.local/share/mise/command-wrappers/bin/claude | 2.1.312 | `-p --output-format text|json|stream-json` | `--input-format stream-json` (+ `--replay-user-messages`, `--include-partial-messages`, `--include-hook-events`) | no native; `claude-acp` adapter in T3 ACP registry (npx), not installed | `-c`, `-r [id|name]`, `--fork-session`, `--session-id` | not in help (inferred `ANTHROPIC_BASE_URL`) |
| Codex | ~/.local/share/mise/command-wrappers/bin/codex | 0.165.0 | `codex exec --json` (JSONL), `-o FILE`, `--output-schema FILE` | `codex app-server` (experimental, stdio/ws) | no native; `codex-acp` in registry, not installed | `codex exec resume --last|ID` | `--oss --local-provider lmstudio|ollama` |
| Muse Code | ~/.local/share/mise/installs/muse/latest/muse | 1.4.14 | `muse exec --json`, `--prompt-file` | `muse serve` (MSP over stdio; `--listen` socket daemon) | no | `muse resume --last|ref` | `--base-url URL`, `--provider echo|meta|local` |
| Pi | ~/.local/share/mise/installs/pi/latest/pi/pi | 1.4.0 | `pi -p`, `--mode json` | `--mode rpc` | no native; `pi-acp` in registry, not installed | `-c`, `-r`, `--session`, `--fork`, `--no-session` | named-provider env vars only |
| Prime Agent | ~/.local/share/prime-agent/bin/prime-agent (Rust ELF) | 0.10.0 | `-p`, `--mode text|json` | `--mode rpc`, `--mode daemon` + `--daemon-socket` | YES native `--mode acp` | `-c`, `-r`, `--fork`, `--no-session` | `--provider` only |

Notes
- Permission flags: Claude `--permission-mode`; Codex `-s read-only|workspace-write|danger-full-access`, `--approve-for-me`; Muse `--approval-mode untrusted|on-request|never`; Pi `--tools/--exclude-tools`; Prime Agent `--autonomous` with gate/limit flags. Prime Agent's help closely mirrors Pi's (likely a Pi-derived fork, inferred).
- T3 Code on this machine already drives Claude (claudeAgent driver), Codex, Muse (native `muse` driver), Pi, Cursor, OpenCode, and ACP-registry agents. ACP registry cache: ~/.t3/caches/acp-registry/registry.json (claude-acp, codex-acp, pi-acp, ...; no Muse or Prime).
- The 3090 has no local LLM server running right now (no listener on 11434/8080/8000/1234).
