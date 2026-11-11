# agentd quickstart

This guide runs `agentd` with one Claude agent and one Codex agent on Linux.
Every command here ran in a live session on 2026-11-10 at commit `294d983`.
The outputs are copied from that run, with IDs and paths shortened.
Timings are single measurements on one machine. They are not benchmarks.

The run kept all state in one scratch directory. Nothing was installed on PATH,
and `~/.local/state/agentd` was never created.

## Prerequisites

- Linux with unprivileged user namespaces.
- Bubblewrap 0.10.0 or newer at `/usr/bin/bwrap`. The run used 0.13.1.
- Node.js at `/usr/bin/node`.
- The ACP shims from npm: `@agentclientprotocol/claude-agent-acp` and
  `@agentclientprotocol/codex-acp`. Each package bundles its own CLI.
  The run used versions 0.90.0 and 2.2.2 from an existing `npm install` in a
  separate directory, here called `$ACP`. The shims do not need to be on PATH.
- Existing logins in `~/.claude/.credentials.json` and `~/.codex/auth.json`.
  agentd copies these into each sandbox. See [isolation](isolation.md).

## 1. Build

```sh
cargo build -p agentd --bins
B=$PWD/target/debug
```

The binaries are `$B/agentd` and `$B/agentctl`.

## 2. Make a scratch directory and a config

The state directory must be owned by you with mode 0700.

```sh
RUN=$PWD/.live-run          # any private directory
mkdir -p -m 700 "$RUN/state"
git clone -q . "$RUN/clone" # the agents' working directory
```

Write `$RUN/kinds.toml`. Replace `$ACP` with the absolute path of the npm install.

```toml
[bwrap]
# An explicit list replaces the default binds. The shims live under $ACP,
# so that directory must be visible inside the sandbox.
# Every path in the list must exist. Remove the ones that your machine lacks.
ro_binds = ["~/.local/share/mise/installs", "~/.cargo/bin", "$ACP"]

[ui]
listen = "127.0.0.1:7434"

[kind.claude]
command = ["$ACP/node_modules/.bin/claude-agent-acp"]

[kind.codex]
command = ["$ACP/node_modules/.bin/codex-acp"]
```

The default binds are `~/.cargo/bin`, `~/.rustup/toolchains` and
`~/.local/share/mise/installs`, and agentd drops the ones that do not exist.
It does not drop a missing path from an explicit list. A missing path there
stops sandboxed agents from starting.

Without `[kind.*] command`, agentd looks for `claude-agent-acp` and `codex-acp`
on PATH inside the sandbox. If they are missing, startup fails with an empty
reason after the final colon:

```text
agentd: sandbox preflight failed for claude (command claude-agent-acp); check bwrap, user namespaces and ro_binds, or set sandbox = "none":
```

## 3. Start agentd

```sh
RUST_LOG=info,agentd=debug "$B/agentd" --config "$RUN/kinds.toml" --state-dir "$RUN/state" 2>> "$RUN/agentd.log" &
```

The control socket appeared in about 150 ms. agentd prints nothing on a clean
start. Warnings and crash reasons go to stderr.

Set `RUST_LOG`. Without it agentd logs errors only, and the reason for a
crashed session is a warning.

`agentctl` has no `--state-dir` flag. It looks for the socket in
`~/.local/state/agentd`. Point it at your socket for every command:

```sh
export AGENTD_SOCKET="$RUN/state/agentd.sock"
```

Without this, every command fails with `agentctl: No such file or directory (os error 2)`.

## 4. Spawn the agents

```sh
"$B/agentctl" spawn reviewer --kind claude --cwd "$RUN/clone"
"$B/agentctl" spawn builder --kind codex --cwd "$RUN/clone"
```

```json
{
  "cwd": "/…/.live-run/clone",
  "id": "agent:01M73DZGKT20WV1JMK31K2XDSN",
  "name": "@reviewer"
}
```

`--sandbox` defaults to `read-only`. Both agents reached `idle` within a few
seconds.

```sh
"$B/agentctl" ls
```

```json
[
  {
    "error": null,
    "id": "agent:01M73DZGKT20WV1JMK31K2XDSN",
    "isolation": "bwrap+proxy",
    "kind": "claude",
    "name": "@reviewer",
    "phase": "idle",
    "queued": 0
  }
]
```

While an agent is `starting`, `isolation` reads `none`. It changes after the
session starts.

## 5. Watch events

Start the feed in a second terminal, from the repository root. A new terminal
has neither `B` nor `AGENTD_SOCKET`, so set both again. The feed prints one
JSON object per line, starting with a snapshot.

```sh
B=$PWD/target/debug
export AGENTD_SOCKET="$PWD/.live-run/state/agentd.sock"
"$B/agentctl" watch
```

Agent replies to you are not mail. They appear only here, as `activity` events.

```json
{"agent":"agent:01M73DZGKT20WV1JMK31K2XDSN","event":"activity","text":"ready"}
```

## 6. Send a message

```sh
"$B/agentctl" send @reviewer "Reply with the single word ready. Do not use tools."
```

```text
Queued 01M73DZS1966PJT4ZA5SA6QVCB for @reviewer.
```

The feed shows the mail move `queued`, `offered`, then `consumed`, and a
`turn_ended` event with outcome `completed`. The round trip took about 1.3 s.
A send to a busy agent prints `(busy; it sees this when its turn ends)`.

## 7. Answer a permission request

Claude's default mode asks before it calls agentd's own `send_message` tool.
Each peer send from Claude waits for you.

```sh
"$B/agentctl" approvals
```

Each option includes a ready-made command:

```json
{
  "command": "agentctl --socket '/…/agentd.sock' approve -- '01M73E04Z0MEFVRBGNQ3QDRF7B' 'allow-once'",
  "effect": "AllowOnce",
  "key": "allow-once",
  "label": "Yes"
}
```

Use the ID and the option key from your own `approvals` output. Each request
gets a new ID, and agentd rejects an old one as stale.

```sh
"$B/agentctl" approve <permission-id> allow-once
```

The ready-made command calls `agentctl` by name. Replace that name with
`"$B/agentctl"` when the binary is not on PATH.

`allow-with-updates` approves the rest of that session's `send_message` calls.

To let an agent send to its peers without asking, give it a rule when you
spawn it. This is opt-in. Without a rule, every `send_message` call still waits
for you.

```sh
"$B/agentctl" spawn reviewer --kind claude --cwd "$RUN/clone" --rule send_message=allow
```

The rule matches only agentd's own `send_message` tool. A tool with the same
name from another MCP server, or a shell command that mentions it, still asks.
The rule applies to that agent only.

## 8. Cancel a turn

```sh
"$B/agentctl" cancel @reviewer
```

```json
{
  "cancelled": "@reviewer"
}
```

The agent was `idle` about 60 ms later. The cancelled message is `consumed`.

## 9. Held and uncertain mail

```sh
"$B/agentctl" held
"$B/agentctl" release <message-id>   # deliver it again
"$B/agentctl" drop <message-id>      # give up on it
```

Mail whose turn had started when its session died is `Uncertain`. agentd does
not redeliver it. Release it only if a second execution is safe. Mail that was
offered before a turn started returns to the queue. After the maximum number
of attempts it is held.

## 10. Graph UI

The `[ui]` listener above serves a page that explains how to build the UI.
To load the graph, build it and add its path:

```sh
(cd agentd-web && npm ci && npm run build)
```

```toml
[ui]
listen = "127.0.0.1:7434"
assets = "/absolute/path/to/agentd-web/dist"
```

Restart agentd, then print the URL:

```sh
"$B/agentctl" ui
```

```text
http://127.0.0.1:7434/#token=<64 hex characters>
```

The page shows agents, mail edges, and an attention list.

## 11. Stop and restart

```sh
kill -TERM <agentd pid>
```

agentd exited in about 40 ms and took its sandboxes with it. While it is down,
`agentctl send` fails. Nothing queues outside the daemon.

Start it again with the same command. Agents return to `idle` with their
conversations. Claude recalled a message from before the restart.

After `SIGKILL` in the middle of a turn, the sandboxes also exited. On restart
the agent was `idle`. The in-flight message was `Uncertain`, and mail queued
behind it was delivered once.

## Mail limits

Agents get an error when they reach a limit. With `per_pair = 2` set under
`[mail]`, Claude's third quick send to `@builder` returned:

```text
mail limit: per_pair cap 2 reached with 2 undelivered messages to 01M73DZGN0EGNR48PG3D4M6CD3; wait for delivery before sending more
```

The defaults are `per_sender = 64` and `per_pair = 16`. Sends from `agentctl`
are exempt.

## Known problems in this run

- `agentctl ls` shows `"error": null` after a crash. The reason is only in the
  daemon log.
