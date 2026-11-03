use crate::{
    BoxError,
    config::{Config, ServerConfig},
    convert,
    engines::{Engine, tensorfold_context_limit},
    errors::EngineError,
    gate::Gate,
    stream::{self, Accumulator},
};
use async_trait::async_trait;
use futures_util::StreamExt;
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, ErrorClass, ModelEntry, ModelProvider, ModelStream,
};
use serde_json::Value;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::Mutex,
    time::{Instant, timeout, timeout_at},
};

#[derive(Default)]
struct DiscoveryState {
    passed: bool,
    model: Option<String>,
    actual_context: Option<u64>,
    slots: Option<usize>,
}
struct Server {
    config: ServerConfig,
    client: reqwest::Client,
    state: Mutex<DiscoveryState>,
    gate: Gate,
    warned_usage: AtomicBool,
}
pub struct LocalProvider {
    servers: Vec<Arc<Server>>,
    warned_params: AtomicBool,
}
fn fatal(message: impl Into<String>) -> CompletionError {
    CompletionError::provider(ErrorClass::Fatal, message)
}
fn transient(message: impl Into<String>) -> CompletionError {
    CompletionError::provider(ErrorClass::Transient, message)
}
fn mismatch(config: &ServerConfig, actual: u64) -> CompletionError {
    fatal(format!(
        "server {} context {actual} is smaller than configured context_window {}; lower context_window and restart the daemon, or restart the server with more context",
        config.name, config.context_window
    ))
}
impl LocalProvider {
    pub fn new(config: Config) -> Result<Self, BoxError> {
        config.validate()?;
        let mut servers = Vec::new();
        for config in config.server {
            let mut headers = reqwest::header::HeaderMap::new();
            if let Some(env) = &config.api_key_env {
                let key = std::env::var(env).map_err(|_| {
                    format!(
                        "{}: API key environment variable {env} is missing",
                        config.name
                    )
                })?;
                let mut value = reqwest::header::HeaderValue::from_str(&format!("Bearer {key}"))?;
                value.set_sensitive(true);
                headers.insert(reqwest::header::AUTHORIZATION, value);
            }
            let client = reqwest::Client::builder()
                .no_proxy()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(config.connect_timeout_secs))
                .default_headers(headers)
                .build()?;
            servers.push(Arc::new(Server {
                gate: Gate::new(config.slots.unwrap_or(1)),
                config,
                client,
                state: Mutex::new(DiscoveryState::default()),
                warned_usage: AtomicBool::new(false),
            }));
        }
        Ok(Self {
            servers,
            warned_params: AtomicBool::new(false),
        })
    }
}
impl Server {
    fn url(&self, path: &str) -> String {
        format!(
            "{}{}",
            self.config.base_url.as_str().trim_end_matches('/'),
            path
        )
    }
    async fn get(&self, path: &str) -> Result<Value, CompletionError> {
        self.fetch(path, false).await
    }
    /// With `absent_ok`, a 404 yields `Value::Null`; any other failure is still
    /// Transient, so discovery retries instead of losing the route's data.
    async fn fetch(&self, path: &str, absent_ok: bool) -> Result<Value, CompletionError> {
        let response = self
            .client
            .get(self.url(path))
            .send()
            .await
            .map_err(|e| transient(format!("{} discovery {path}: {e}", self.config.name)))?;
        if absent_ok && response.status() == reqwest::StatusCode::NOT_FOUND {
            tracing::warn!(server = self.config.name, path, "discovery route is absent");
            return Ok(Value::Null);
        }
        response
            .error_for_status()
            .map_err(|e| transient(format!("{} discovery {path}: {e}", self.config.name)))?
            .json()
            .await
            .map_err(|e| transient(format!("{} discovery {path}: {e}", self.config.name)))
    }
    async fn discover(&self, force: bool) -> Result<String, CompletionError> {
        let mut state = self.state.lock().await;
        if state.passed && !force {
            return Ok(state
                .model
                .clone()
                .expect("bug: successful discovery resolves model"));
        }
        state.passed = false;
        let attempt = async {
            // With a configured model, llama.cpp and generic need nothing from
            // /v1/models (llama.cpp's context comes from /props), so a server
            // without the route still serves chat.
            let models_optional = self.config.model.is_some()
                && matches!(self.config.engine, Engine::LlamaCpp | Engine::Generic);
            let models = match self.get("/v1/models").await {
                Ok(models) => models,
                Err(error) if models_optional => {
                    tracing::warn!(
                        server = self.config.name,
                        %error,
                        "cannot list models; using the configured model"
                    );
                    Value::Null
                }
                Err(error) => return Err(error),
            };
            let empty = Vec::new();
            let data = match models["data"].as_array() {
                Some(data) => data,
                None if models_optional => &empty,
                None => return Err(transient("discovery /v1/models has no data array")),
            };
            let ids: Vec<_> = data.iter().filter_map(|v| v["id"].as_str()).collect();
            let model = match &self.config.model {
                Some(model) => {
                    if !ids.is_empty() && !ids.contains(&model.as_str()) {
                        tracing::error!(
                            server = self.config.name,
                            model,
                            ?ids,
                            "configured model is absent from /v1/models"
                        );
                    }
                    model.clone()
                }
                None if ids.len() == 1 => ids[0].to_owned(),
                None => {
                    return Err(fatal(format!(
                        "server {} advertises model ids {ids:?}; set model in the config",
                        self.config.name
                    )));
                }
            };
            let model_info = data
                .iter()
                .find(|v| v["id"].as_str() == Some(&model))
                .unwrap_or(&Value::Null);
            // llama.cpp's /props carries the context check, so it is required.
            // SGLang's /server_info and TensorFold's /health carry only slots, so
            // only an absent route is tolerated; the gate size is set once.
            let auxiliary = match self.config.engine {
                Engine::LlamaCpp => self.get("/props").await?,
                Engine::Sglang => self.fetch("/server_info", true).await?,
                Engine::TensorFold => self.fetch("/health", true).await?,
                _ => Value::Null,
            };
            Ok((model, self.config.engine.discover(model_info, &auxiliary)))
        };
        let (model, discovered) = timeout(
            Duration::from_secs(self.config.discovery_timeout_secs),
            attempt,
        )
        .await
        .map_err(|_| transient(format!("{} discovery timed out", self.config.name)))??;
        if let Some(context) = discovered.context {
            state.actual_context = Some(context);
            if context < self.config.context_window as u64 {
                return Err(mismatch(&self.config, context));
            }
        }
        let slots = match (self.config.slots, discovered.slots) {
            (Some(a), Some(b)) => a.min(b),
            (Some(a), None) | (None, Some(a)) => a,
            (None, None) => 1,
        };
        if let Some(previous) = state.slots {
            if slots != previous {
                tracing::warn!(
                    server = self.config.name,
                    previous,
                    slots,
                    "server slot count drifted; keeping the original gate size"
                );
            }
        } else {
            if self.config.slots.is_none() && discovered.slots.is_none() {
                tracing::warn!(
                    server = self.config.name,
                    "no slot count known; using one slot"
                );
            }
            self.gate
                .resize_initial(self.config.slots.unwrap_or(1), slots);
            state.slots = Some(slots);
        }
        state.model = Some(model.clone());
        state.passed = true;
        Ok(model)
    }
    async fn invalidate(&self) {
        self.state.lock().await.passed = false;
    }
    async fn refusal(&self, error: EngineError, model: &str) -> CompletionError {
        if error.class(self.config.engine) == ErrorClass::ContextOverflow {
            self.invalidate().await;
            if let Err(error) = self.discover(true).await {
                return error;
            }
            if self.config.engine == Engine::TensorFold
                && let Some(n) = tensorfold_context_limit(&error.message)
            {
                let mut state = self.state.lock().await;
                state.actual_context = Some(n);
                if n < self.config.context_window as u64 {
                    // A mismatch is never cached as passed: the next invocation rechecks.
                    state.passed = false;
                    return mismatch(&self.config, n);
                }
            }
            return CompletionError::provider(ErrorClass::ContextOverflow, error.message);
        }
        if error.model_not_found() {
            return if self.config.model.is_none() {
                self.invalidate().await;
                fatal(format!(
                    "{}; the server's model changed; the next request resolves it again",
                    error.message
                ))
            } else {
                fatal(format!(
                    "model `{model}` is not served at `{}`; fix model in the config; {}",
                    self.config.base_url, error.message
                ))
            };
        }
        CompletionError::provider(error.class(self.config.engine), error.message)
    }
}
#[async_trait]
impl ModelProvider for LocalProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        Ok(self
            .servers
            .iter()
            .flat_map(|s| {
                s.config.profile.iter().map(move |p| ModelEntry {
                    model_id: format!("{}/{}", s.config.name, p.id),
                    display_name: p.display_name.clone(),
                    context_window: s.config.context_window,
                    max_output_tokens: p.max_output_tokens,
                    supports_image_input: s.config.supports_image_input,
                    keeps_unsigned_reasoning: true,
                })
            })
            .collect())
    }
    async fn invoke_model(
        &self,
        model_id: &str,
        request: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        let (name, profile_id) = model_id
            .split_once('/')
            .ok_or_else(|| fatal("local model id must be <server>/<profile>"))?;
        let server = self
            .servers
            .iter()
            .find(|s| s.config.name == name)
            .cloned()
            .ok_or_else(|| fatal(format!("unknown local server {name}")))?;
        let profile = server
            .config
            .profile
            .iter()
            .find(|p| p.id == profile_id)
            .cloned()
            .ok_or_else(|| fatal(format!("unknown local profile {model_id}")))?;
        if request.additional_params.is_some() && !self.warned_params.swap(true, Ordering::Relaxed)
        {
            tracing::warn!(
                "local provider ignores CompletionRequest.additional_params; use profile.extra"
            );
        }
        let model = server.discover(false).await?;
        let permit = server.gate.acquire().await?;
        Ok(Box::pin(async_stream::try_stream! {
            let _permit = permit;
            let mut cap = request.max_tokens.or(profile.max_output_tokens);
            let mut emitted = false;
            'dispatch: loop {
                let body = convert::body(&server.config,&profile,&model,&request,cap);
                let mut deadline = Instant::now()+Duration::from_secs(server.config.first_token_timeout_secs);
                let response = match timeout_at(deadline, server.client.post(server.url("/v1/chat/completions")).json(&body).send()).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => { server.invalidate().await; Err(transient(format!("local transport failure: {error}")))?; unreachable!() },
                    Err(_) => { server.invalidate().await; Err(transient("local first-token timeout before headers"))?; unreachable!() },
                };
                let status = response.status();
                let mut refusal = None;
                let mut accumulator = Accumulator::default();
                if !status.is_success() {
                    let body = match timeout_at(deadline,response.text()).await {
                        Ok(Ok(body)) => body,
                        Ok(Err(error)) => { server.invalidate().await; Err(transient(format!("broken HTTP error body: {error}")))?; unreachable!() },
                        Err(_) => { server.invalidate().await; Err(transient("local first-token timeout reading error body"))?; unreachable!() },
                    };
                    refusal = Some(EngineError::normalize(Some(status.as_u16()),&body));
                } else {
                    let events = stream::events(response); tokio::pin!(events);
                    loop {
                        let event = match timeout_at(deadline,events.next()).await {
                            Ok(Some(Ok(event))) => event,
                            Ok(Some(Err(error))) => { server.invalidate().await; Err(error)?; unreachable!() },
                            Err(_) => { server.invalidate().await; Err(transient("local first-token or progress timeout"))?; unreachable!() },
                            Ok(None) => break,
                        };
                        if event.trim() == "[DONE]" { break; }
                        let chunk: Value = serde_json::from_str(&event).map_err(|e| transient(format!("invalid SSE JSON: {e}")))?;
                        if chunk.get("error").is_some() || chunk["object"] == "error" {
                            refusal = Some(EngineError::normalize(None,&event)); break;
                        }
                        let (generated,chunks) = accumulator.push(server.config.engine,&chunk);
                        if generated { deadline = Instant::now()+Duration::from_secs(server.config.progress_timeout_secs); }
                        for chunk in chunks { emitted=true; yield chunk; }
                    }
                }
                if let Some(error) = refusal {
                    let overflow = error.class(server.config.engine) == ErrorClass::ContextOverflow;
                    if overflow {
                        server.invalidate().await;
                        if server.config.engine == Engine::TensorFold && let Some(n) = tensorfold_context_limit(&error.message) {
                            server.state.lock().await.actual_context=Some(n);
                        }
                    }
                    if overflow && cap.is_some() && !emitted { cap=None; continue 'dispatch; }
                    Err(server.refusal(error,&model).await)?;
                }
                if accumulator.usage.is_none() && !server.warned_usage.swap(true,Ordering::Relaxed) { tracing::warn!(server=server.config.name, "local server supplied no token usage"); }
                let room = server.state.lock().await.actual_context.unwrap_or(server.config.context_window as u64);
                for chunk in accumulator.finish(server.config.engine,&server.config.name,room,cap)? { yield chunk; }
                break;
            }
        }))
    }
}
