# Run local inference

Build and install the provider on the machine that runs Infinity.

```sh
cargo install --path crates/infinity-provider-local
```

Register it in `~/.infinity/providers.json`. Use an absolute config path or a
leading `~/`, which the provider expands itself.

```json
{
  "local": {
    "command": ["infinity-provider-local", "--config", "~/.infinity/local.toml"]
  }
}
```

Create `~/.infinity/local.toml` from one of the examples below. You may combine
servers in one file. Set each `context_window` to the prompt-plus-reply capacity
that your running server actually admits. Restart the daemon after changing the
context or model because its catalog is built once. Failed discovery is retried
on the next invocation, so restarting an engine to correct a mismatch does not
require restarting the provider.

The provider requires an HTTP loopback origin, such as `http://127.0.0.1:8091`,
`http://localhost:8091`, or `http://[::1]:8091`. A `/v1` path suffix is invalid.
Set `api_key_env` to an environment variable name if the engine needs a bearer
key. The variable must exist in the provider process environment.

If `model` is omitted, discovery requires exactly one model ID from `/v1/models`.
Set it explicitly for engines that advertise aliases or loaded adapters. A
configured model absent from a nonempty model list is logged as an error.
With a configured model, llama.cpp and generic servers do not need
`/v1/models`; the provider logs a warning and uses the configured model.

Request conversion drops empty text parts and any message left empty, with one
exception: a tool result is always sent, even with empty content. Dropping it
would leave its tool call without an answer, which the chat templates reject.

## TensorFold

Run native TensorFold 1.2.x or Python 0.3.6.3 yourself. Keep server-side compaction
off. For Python, set `TF_PASS_UNKNOWN_TOOLS=1` before starting the server so
unknown calls reach core for recovery.

```toml
[[server]]
name = "qwen27"
engine = "tensorfold"
base_url = "http://127.0.0.1:8091"
model = "qwen27"
context_window = 196608
slots = 1
supports_image_input = false
include_usage = true
connect_timeout_secs = 5
first_token_timeout_secs = 600
progress_timeout_secs = 600
discovery_timeout_secs = 10

[[server.profile]]
id = "think"
display_name = "Qwen3.8 27B think (TensorFold)"
temperature = 0.6
top_p = 0.95
top_k = 20
thinking = true
max_output_tokens = 16384
extra = { min_p = 0.0 }
```

All three TensorFold servers default to 4096 reply tokens when the cap is omitted.
Thinking profiles usually need a larger `max_output_tokens`. Requests get a fresh
random integer seed on every dispatch, including a retry. To reproduce a fixed
sample, set an integer `extra.seed`. Greedy sampling and pinned seeds can repeat
malformed tool arguments until core exhausts its retry budget.

Python's tool parser can silently drop malformed or truncated tool envelopes.
A successful reply with no call, or with only preceding prose, does not reveal
that loss. The provider warns about empty replies and visible tool markup but
does not parse markup or retry legitimate empty replies. Tool-using agent
qualification must measure missing calls before adopting a Python profile.

Python reports no context capacity through discovery. The provider instead
parses the engine's explicit refusal limit. A fitted capacity below
`context_window` is a configuration error, even for an oversized tool result.
Image-memory and visual-token budget refusals are fatal because shrinking text
cannot shrink an image.

## llama.cpp

Use the tested source target `a0e6dc7`. Start `llama-server` with `--jinja`,
`--reasoning-format deepseek`, and `--parallel N`. Choose `N` and the server's
context settings so each slot has the configured context. Discovery reads the
per-slot `default_generation_settings.n_ctx` and `total_slots` from `/props`.

```toml
[[server]]
name = "llama"
engine = "llama.cpp"
base_url = "http://127.0.0.1:8080"
context_window = 32768
slots = 1

[[server.profile]]
id = "think"
display_name = "Local llama.cpp"
temperature = 0.6
thinking = true
max_output_tokens = 16384
```

## vLLM

Use vLLM 0.30.0. Select parsers that match the weights and template. Start with
`--enable-auto-tool-choice --tool-call-parser X --reasoning-parser Y`, replacing
`X` and `Y` with the engine's parser names for your model.

```toml
[[server]]
name = "vllm"
engine = "vllm"
base_url = "http://127.0.0.1:8000"
context_window = 32768
slots = 1

[[server.profile]]
id = "think"
display_name = "Local vLLM"
temperature = 0.6
thinking = true
max_output_tokens = 16384
```

Discovery reads `max_model_len` from `/v1/models`. Reasoning accepts both
`reasoning_content` and `reasoning`. The ignored live probe records which field
the running build streams.

## SGLang

Use SGLang 0.5.23. Start with `--tool-call-parser X --reasoning-parser Y`, selecting
parsers for the model. Discovery reads `/v1/models` and `/server_info`. If
`/server_info` returns 404, the gate uses `slots` from the config, or one slot.

```toml
[[server]]
name = "sglang"
engine = "sglang"
base_url = "http://127.0.0.1:30000"
context_window = 32768
slots = 1

[[server.profile]]
id = "think"
display_name = "Local SGLang"
temperature = 0.6
thinking = true
max_output_tokens = 16384
```

An omitted reply cap is assumed to clamp to the remaining context room. This
assumption remains unverified until the SGLang live probe runs.

## Generic engines

Supply all capacity settings by hand. Generic engines have no discovery of
context or slots, but still use `/v1/models` to resolve an omitted model.

```toml
[[server]]
name = "generic"
engine = "generic"
base_url = "http://127.0.0.1:9000"
model = "served-model"
context_window = 32768
slots = 1
include_usage = false

[[server.profile]]
id = "default"
display_name = "Generic local model"
temperature = 0.6
max_output_tokens = 16384
```

## Configure dispatch limits

The server's slot limit is the smaller of the configured and discovered counts.
If only one is known, that count applies. Otherwise, the gate uses one slot.
The gate is sized at the first successful discovery; later slot drift is logged.
Each server has its own gate, with at most 32 waiting requests. There is no gate
shared across engines or GPUs. Size concurrent resident servers yourself.

Connect, first-token, progress, and discovery defaults are 5, 600, 600, and 10
seconds. First-token and progress clocks start after queue admission. Comments,
keepalives, role-only deltas, and usage do not reset them. There is no total
generation timeout. Dropping a stream closes its response and releases its slot.

A request's `max_tokens` wins over the profile's `max_output_tokens`. With neither,
the provider omits the cap. If the capped request is refused for overflow before
any chunk reaches core, it retries once without the cap through the same slot.
**That retry drops your reply bound for that turn.** The engine's own default may
still cap the reply. `length` finishes are fatal and distinguish a full window
from an output cap when usage permits; otherwise the message names both limits.

Use `profile.extra` for engine sampling options. Unknown config fields and
provider-owned request overrides are rejected. Request `additional_params` is
ignored with one warning. Set `include_usage = false` for servers that reject
`stream_options.include_usage`. Missing usage stays missing; the provider never
calls a tokenize endpoint to reconstruct it.

## Run probes against existing servers

The provider does not start or stop an engine. The ignored probes require an
already-running server and check a tool call, reasoning, and a true overflow.

```sh
LOCAL_PROBE_TENSORFOLD_NATIVE_URL=http://127.0.0.1:8091 \
  cargo test -p infinity-provider-local --test live tensorfold_native -- --ignored --nocapture
```

The other test names and environment prefixes are `tensorfold_cuda` with
`TENSORFOLD_CUDA`, `llama_cpp` with `LLAMACPP`, `vllm` with `VLLM`, and `sglang`
with `SGLANG`. Set `LOCAL_PROBE_<PREFIX>_MODEL` when discovery returns multiple
IDs, `LOCAL_PROBE_<PREFIX>_CONTEXT` for the provider context (default 1024), and
`LOCAL_PROBE_<PREFIX>_API_KEY` when authentication is enabled. The tool and
reasoning checks require working engine parsers. These probes are not GPU or
agent qualification.

## Qualify an agent profile

Qualification is pending. No engine, model, quantization, or RTX 3090 profile is
qualified by CPU tests or mock HTTP tests. Run Qwen3.8-27B as an agentd-hosted
agent on TensorFold native if it supports sm86, otherwise Python 0.3.6.3, then
record these gates before adopting it.

1. Complete a `send_message` round trip and recover from an unknown tool.
2. Freeze a 50-turn corpus requiring calls across at least four tools. At least
   ten turns need arguments over 2 KB; at least ten need nested objects or
   arrays. Use the qualification sampling profile. At most two missing calls
   pass; report the rate either way. More than two marks that engine
   **unqualified for tool-using agents**. Move to native or another engine.
3. Recover an oversized tool result through a placeholder, and verify that a
   configured context above the fitted capacity gives an actionable fatal.
4. Exercise 75% compaction with one slot, three cancellations, and two agents
   queueing.
5. Run until at least two compactions fire, with a limit of 150 turns. Fewer than
   two compactions fails qualification. Count window-floor `length` fatals by
   usage and record their turns relative to compaction. More than one per
   compaction cycle fails. Count output-cap fatals separately. Any output-cap
   fatal requires raising the profile cap and repeating the run; report that
   change.

Record the engine build, weights, quantization, flags, environment, fitted
context from the server log, peak VRAM from `nvidia-smi`, and measured prefill and
decode speeds. Track earlier compaction as a core follow-up if window-floor
fatals exceed the gate. Request an explicit parser-failure signal from TensorFold
so future versions can retry malformed calls without guessing from empty replies.
