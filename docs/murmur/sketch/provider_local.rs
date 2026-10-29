//! Crate `infinity-provider-local`, binary of the same name. A new Infinity model
//! provider for OpenAI-compatible local servers (llama.cpp `llama-server`, vLLM).
//! Depends only on `infinity-provider-protocol` (features `remote`). It relies on the
//! fork's second commit (`serve_provider` races each invocation against client
//! disconnect, see `infinity_patch.rs`) so a cancelled request leaves the GPU queue or
//! frees its slot at once.
//!
//! Registered in `~/.infinity/providers.json`:
//!
//! ```json
//! { "local": { "command": ["infinity-provider-local", "--config", "~/.config/agentd/local-models.toml"] } }
//! ```
//!
//! `main` is the Bedrock pattern (`crates/infinity-provider-bedrock/src/main.rs`):
//! build the provider, `serve_provider(Arc::new(p))`, print the socket path as the only
//! stdout line, log to stderr, await forever.
//!
//! Hand-written, not `infinity-provider-rig`: whether rig 0.31's OpenAI client takes a
//! custom base URL is unverified, and this provider must own four things rig would
//! hide: tool-call fragment accumulation, llama.cpp/vLLM error-string classification,
//! per-profile sampling, and the GPU gate.

use std::sync::Arc;

use async_trait::async_trait;
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, ErrorClass, ModelEntry, ModelProvider, ModelStream,
    StreamChunk, Usage,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// `local-models.toml`.
///
/// ```toml
/// [backend]
/// kind = "llama-cpp"                 # or "vllm"
/// base_url = "http://127.0.0.1:8091/v1"
///
/// [[profile]]                        # one ModelEntry per profile
/// model_id = "qwen3-coder"           # what Infinity's ModelRef names: local/qwen3-coder
/// served_model = "qwen3-coder-30b-a3b"
/// temperature = 0.7
/// top_p = 0.8
/// max_output_tokens = 8192
///
/// [[profile]]
/// model_id = "qwen3-coder-precise"   # same weights, different sampling
/// served_model = "qwen3-coder-30b-a3b"
/// temperature = 0.2
/// ```
///
/// Profiles exist because core always sends `max_tokens: None, additional_params: None`:
/// sampling can only live in the provider, so "the same model, sampled differently" is
/// a different model id. Switching profile is ordinary `SwitchModel`.
#[derive(serde::Deserialize)]
pub struct LocalConfig {
    pub backend: Backend,
    pub profile: Vec<Profile>,
}

#[derive(serde::Deserialize)]
pub struct Backend {
    pub kind: BackendKind,
    pub base_url: String,
}

#[derive(serde::Deserialize, Clone, Copy)]
#[serde(rename_all = "kebab-case")]
pub enum BackendKind {
    LlamaCpp,
    Vllm,
}

#[derive(serde::Deserialize, Clone)]
pub struct Profile {
    pub model_id: String,
    pub served_model: String,
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub max_output_tokens: Option<u64>,
    #[serde(default)]
    pub supports_image_input: bool,
}

/// Facts read from the running server at `connect`, never configured (single source of
/// truth; there is deliberately no `context_window` key in `local-models.toml`):
/// - `context_window`: llama.cpp `GET /props` -> per-slot `n_ctx` (the server splits
///   `-c` across `--parallel` slots, so 65536 with 2 slots is 32768 per request);
///   vLLM `GET /v1/models` -> `max_model_len`.
/// - `slots`: llama.cpp `total_slots` from `/props`; vLLM `max_num_seqs` from config.
///
/// An honest `context_window` is load-bearing: core auto-compacts at >75% of it, and a
/// window larger than the slot turns every long thread into a ContextOverflow loop.
pub struct ServerFacts {
    pub context_window: usize,
    pub slots: usize,
}

pub struct LocalProvider {
    http: reqwest::Client,
    cfg: LocalConfig,
    facts: ServerFacts,
    gate: GpuGate,
    /// Computes `Usage` when the server's final chunk omits it.
    tokenizer: Arc<Tokenizer>,
}

/// The served model's own tokenizer, reached through the server (`POST /tokenize` on
/// llama.cpp and vLLM), so counts match what the server actually ran.
pub struct Tokenizer {
    http: reqwest::Client,
    base_url: String,
}

impl Tokenizer {
    pub async fn count(&self, served_model: &str, text: &str) -> Result<u64, BoxError> {
        todo!()
    }
}

impl LocalProvider {
    /// Retries `/props` (or `/v1/models`) for up to 120 s: the broker starts the
    /// inference service before the Infinity daemon, but a model load can take a while.
    pub async fn connect(cfg: LocalConfig) -> Result<Self, BoxError> {
        todo!()
    }
}

#[async_trait]
impl ModelProvider for LocalProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        // profile -> ModelEntry { model_id, display_name: "{model_id} (local)",
        //   context_window: facts.context_window, max_output_tokens, supports_image_input }
        todo!()
    }

    /// ```text
    /// let profile = find(model_id) else Err(Fatal "unknown model")
    /// let permit  = gate.acquire().await?        // FIFO; Err(Throttled) if 32 already wait;
    ///                                            // dropped with the stream, or with this
    ///                                            // future when the client disconnects
    /// let body    = to_chat_completions(&request, profile)
    ///     // preamble -> system; Message::User/Assistant/ToolCall/ToolResult -> OpenAI
    ///     // roles; ToolDefinition -> tools[]; parallel_tool_calls: false; stream: true,
    ///     // stream_options.include_usage: true; llama.cpp: cache_prompt: true
    /// let resp = http.post(base_url + "/chat/completions").json(body).send()
    ///     // connect refused / 5xx / reset -> Transient (inference service restarting)
    ///     // 429 / slot unavailable / queue full -> Throttled
    ///     // 400 + "context"/"n_ctx"/"maximum context length" -> ContextOverflow
    ///     // unknown model, bad tools schema, other 4xx -> Fatal
    /// Ok(sse_to_chunks(resp, permit, tokenizer.clone(), prompt_text))
    /// ```
    async fn invoke_model(
        &self,
        model_id: &str,
        request: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        todo!()
    }
}

/// SSE -> `StreamChunk`.
///
/// - `delta.content` -> `Text`; `delta.reasoning_content` (llama.cpp with reasoning
///   models) -> `ReasoningDelta` (unsigned `Reasoning` blocks would be skipped by core).
/// - `delta.tool_calls[i]`: accumulate `function.name` and `arguments` fragments; emit
///   one complete `StreamChunk::ToolCall` when `finish_reason` arrives. The request set
///   `parallel_tool_calls: false`, so a second index (or a second call id) is a protocol
///   error: `CompletionError::ResponseError("protocol: N tool calls with
///   parallel_tool_calls=false")`, which core classes as `Transient` and retries.
/// - final `usage` -> `Final(FinalResponse { usage: Some(Usage { .. }) })`. Missing usage
///   would starve compaction, so if the server omits it, count prompt and output with
///   `Tokenizer::count` (the served model's tokenizer) and report that.
/// - Dropping the returned stream (core cancelled the completion, or the patched
///   `serve_provider` saw the client disconnect) drops the HTTP response, which aborts
///   the request server-side and frees the slot, and releases the permit.
fn sse_to_chunks(
    resp: reqwest::Response,
    permit: GpuPermit,
    tokenizer: Arc<Tokenizer>,
    prompt_text: String,
) -> ModelStream {
    todo!()
}

/// Admission control for one GPU. Several model agents share one resident model; the
/// server batches up to `slots` requests concurrently (llama.cpp slots, vLLM continuous
/// batching). The gate keeps at most `slots` requests in flight and queues at most
/// `QUEUE_LIMIT` more FIFO here, where waiting is visible and cancellable, instead of
/// inside the server where a queued request still holds an HTTP connection and can time
/// out mid-queue. Beyond that the request fails fast with `ErrorClass::Throttled`, and
/// core backs off.
///
/// A waiter whose client disconnects is dropped by the patched `serve_provider` race,
/// which drops this future and leaves the queue at once.
///
/// Fairness across agents is FIFO: the provider sees no agent identity (the request
/// carries none besides thread ids inside the preamble), and per-agent priority is not
/// a v1 need.
pub struct GpuGate {
    /// `slots + QUEUE_LIMIT` permits: holding one means "admitted" (running or waiting).
    admitted: Arc<tokio::sync::Semaphore>,
    /// `slots` permits: holding one means "running". tokio's semaphore is FIFO-fair.
    running: Arc<tokio::sync::Semaphore>,
}
pub const QUEUE_LIMIT: usize = 32;

pub struct GpuPermit {
    _admitted: tokio::sync::OwnedSemaphorePermit,
    _running: tokio::sync::OwnedSemaphorePermit,
}

impl GpuGate {
    pub fn new(slots: usize) -> Self {
        Self {
            admitted: Arc::new(tokio::sync::Semaphore::new(slots + QUEUE_LIMIT)),
            running: Arc::new(tokio::sync::Semaphore::new(slots)),
        }
    }

    pub async fn acquire(&self) -> Result<GpuPermit, CompletionError> {
        let admitted = self.admitted.clone().try_acquire_owned().map_err(|_| {
            CompletionError::provider(ErrorClass::Throttled, "GPU queue full (32 waiting)")
        })?;
        let running = self.running.clone().acquire_owned().await.expect("gate never closed");
        Ok(GpuPermit { _admitted: admitted, _running: running })
    }
}
