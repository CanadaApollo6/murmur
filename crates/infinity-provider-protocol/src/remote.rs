//! Unix-socket transport for [`ModelProvider`]s.
//!
//! This module lets any [`ModelProvider`] implementation run in a separate
//! process and be consumed remotely:
//!
//! * [`serve_provider`] exposes a provider over a Unix domain socket at a
//!   freshly generated temp path. Provider binaries call this, print the
//!   returned path on stdout, and then run the returned server future.
//! * [`RemoteModelProvider`] is the client side: it implements
//!   [`ModelProvider`] by forwarding calls over the socket.
//!
//! ## Protocol
//!
//! Newline-delimited JSON. Each request opens a fresh connection (so
//! concurrent invocations simply use concurrent connections): the client
//! sends one [`ProviderRequest`] line, then reads [`ProviderResponse`] lines.
//!
//! * `ListModels` → exactly one response: `Models` or `Error`.
//! * `InvokeModel` → either a single `Error` (the invocation failed), or
//!   `InvokeStarted` followed by zero or more `Chunk`s and a final
//!   `StreamEnd`. Mid-stream provider errors are forwarded as `Chunk(Err)`
//!   lines without ending the stream, mirroring the in-process behavior
//!   where a stream may yield `Err` items.
//!
//! The client must keep its write half open until it has read the last line.
//! The server treats EOF from the client as a hang-up and drops the provider's
//! work, so a queued invocation frees its place without waiting for a write.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::net::{UnixListener, UnixStream};
use tokio_util::codec::{Framed, LinesCodec};

use crate::{
    CompletionError, CompletionRequest, ErrorClass, ModelEntry, ModelProvider, ModelStream,
    StreamChunk,
};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A connection carrying one JSON value per line, in both directions.
type JsonLines = Framed<UnixStream, LinesCodec>;

// ── Wire types ──

/// A completion error as it travels over the wire: the stringified message
/// plus the provider's [`ErrorClass`], so retry classification survives the
/// process boundary.
#[derive(Debug, Serialize, Deserialize)]
pub struct WireError {
    pub message: String,
    /// Missing in messages from older providers; default to the safe
    /// (non-retrying) classification.
    #[serde(default = "fatal_class")]
    pub class: ErrorClass,
}

fn fatal_class() -> ErrorClass {
    ErrorClass::Fatal
}

impl From<&CompletionError> for WireError {
    fn from(e: &CompletionError) -> Self {
        Self {
            message: e.to_string(),
            class: e.class(),
        }
    }
}

impl From<WireError> for CompletionError {
    fn from(e: WireError) -> Self {
        CompletionError::provider(e.class, e.message)
    }
}

/// A request sent from the client to the provider server. One request is
/// sent per connection.
#[derive(Debug, Serialize, Deserialize)]
pub enum ProviderRequest {
    /// List the models offered by the provider.
    ListModels,
    /// Invoke a model, streaming the completion response back as
    /// [`ProviderResponse::Chunk`]s.
    InvokeModel {
        model_id: String,
        request: Box<CompletionRequest>,
    },
}

/// A response line sent from the provider server to the client.
#[derive(Debug, Serialize, Deserialize)]
pub enum ProviderResponse {
    /// Reply to [`ProviderRequest::ListModels`].
    Models(Vec<ModelEntry>),
    /// The invocation succeeded; `Chunk`s follow.
    InvokeStarted,
    /// One streamed item of an invocation. `Err` carries a mid-stream
    /// provider error (message plus retry classification) and does not end
    /// the stream.
    Chunk(Result<StreamChunk, WireError>),
    /// The invocation stream finished; the connection will close.
    StreamEnd,
    /// The request failed (sent in place of `Models` / `InvokeStarted`).
    Error(WireError),
}

// ── Framing ──
//
// Connections are framed with tokio's [`LinesCodec`]: one JSON value per
// line in each direction.

/// Serialize `value` as JSON and send it as one line.
async fn send_json<T: Serialize>(framed: &mut JsonLines, value: &T) -> std::io::Result<()> {
    let line = serde_json::to_string(value).map_err(std::io::Error::other)?;
    framed.send(line).await.map_err(std::io::Error::other)
}

/// Receive one line and parse it as JSON. Returns `Ok(None)` on a clean EOF.
async fn recv_json<T: for<'de> Deserialize<'de>>(
    framed: &mut JsonLines,
) -> std::io::Result<Option<T>> {
    match framed.next().await {
        None => Ok(None),
        Some(Ok(line)) => serde_json::from_str(&line)
            .map(Some)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        Some(Err(e)) => Err(std::io::Error::other(e)),
    }
}

// ── Server ──

/// Generate a fresh socket path in the system temp directory. Kept short
/// because Unix socket paths have a low length limit (~104 bytes on macOS).
fn new_socket_path() -> PathBuf {
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    std::env::temp_dir().join(format!("inf-provider-{}.sock", &suffix[..12]))
}

/// Expose a [`ModelProvider`] over a Unix domain socket at a freshly
/// generated temp path.
///
/// Returns the socket path and the server future. Provider binaries should
/// print the path on stdout (so a supervisor like the daemon can discover
/// it) and then await the future, which serves connections forever.
///
/// Must be called from within a tokio runtime.
pub fn serve_provider(
    provider: Arc<dyn ModelProvider>,
) -> std::io::Result<(PathBuf, impl Future<Output = ()> + Send)> {
    let path = new_socket_path();
    let listener = UnixListener::bind(&path)?;
    let serve = async move {
        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    let provider = provider.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(provider, stream).await {
                            tracing::warn!("model provider connection error: {e}");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!("model provider accept error: {e}");
                    // Avoid spinning hot on persistent accept failures.
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
            }
        }
    };
    Ok((path, serve))
}

/// Serve a single connection: read one request, write the response(s).
async fn handle_connection(
    provider: Arc<dyn ModelProvider>,
    stream: UnixStream,
) -> std::io::Result<()> {
    let mut framed = Framed::new(stream, LinesCodec::new());

    let request = match recv_json::<ProviderRequest>(&mut framed).await {
        // Client disconnected without sending a request.
        Ok(None) => return Ok(()),
        Ok(Some(request)) => request,
        Err(e) => {
            send_json(
                &mut framed,
                &ProviderResponse::Error(WireError {
                    message: format!("invalid request: {e}"),
                    class: ErrorClass::Fatal,
                }),
            )
            .await?;
            return Ok(());
        }
    };

    match request {
        ProviderRequest::ListModels => {
            let response = match provider.list_models().await {
                Ok(models) => ProviderResponse::Models(models),
                Err(e) => ProviderResponse::Error(WireError {
                    message: e.to_string(),
                    class: ErrorClass::Fatal,
                }),
            };
            send_json(&mut framed, &response).await?;
        }
        ProviderRequest::InvokeModel { model_id, request } => {
            handle_invoke(provider, &model_id, *request, &mut framed).await?;
        }
    }
    Ok(())
}

/// Invoke the model and forward its stream as `Chunk` lines.
async fn handle_invoke(
    provider: Arc<dyn ModelProvider>,
    model_id: &str,
    request: CompletionRequest,
    framed: &mut JsonLines,
) -> std::io::Result<()> {
    // A client that hangs up is otherwise noticed only on the next write, so a
    // cancelled completion would keep its place in a provider's queue until its
    // first chunk. Dropping the invoke future or the stream is the cancellation.
    let invoked = tokio::select! {
        invoked = provider.invoke_model(model_id, request) => invoked,
        () = client_gone(framed) => return Ok(()),
    };
    let mut stream = match invoked {
        Ok(stream) => stream,
        Err(e) => {
            send_json(framed, &ProviderResponse::Error(WireError::from(&e))).await?;
            return Ok(());
        }
    };
    send_json(framed, &ProviderResponse::InvokeStarted).await?;
    loop {
        let item = tokio::select! {
            item = stream.next() => item,
            () = client_gone(framed) => return Ok(()),
        };
        let Some(item) = item else { break };
        let wire = item.map_err(|e| WireError::from(&e));
        send_json(framed, &ProviderResponse::Chunk(wire)).await?;
    }
    send_json(framed, &ProviderResponse::StreamEnd).await
}

/// Resolve once the client has closed the connection. The client sends nothing
/// after its request, so EOF (or a read error) means it is gone; stray lines are
/// ignored. Cancel-safe: `Framed` keeps partial reads in its own buffer.
async fn client_gone(framed: &mut JsonLines) {
    while let Some(Ok(_)) = framed.next().await {}
}

// ── Client ──

/// A [`ModelProvider`] that forwards all calls to a provider process served
/// over a Unix domain socket (see [`serve_provider`]).
pub struct RemoteModelProvider {
    socket_path: PathBuf,
}

impl RemoteModelProvider {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// Open a fresh connection and send a single request line.
    async fn connect_and_send(&self, request: &ProviderRequest) -> std::io::Result<JsonLines> {
        let stream = UnixStream::connect(&self.socket_path).await?;
        let mut framed = Framed::new(stream, LinesCodec::new());
        send_json(&mut framed, request).await?;
        Ok(framed)
    }
}

#[async_trait]
impl ModelProvider for RemoteModelProvider {
    async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
        let mut framed = self.connect_and_send(&ProviderRequest::ListModels).await?;
        match recv_json::<ProviderResponse>(&mut framed).await? {
            Some(ProviderResponse::Models(models)) => Ok(models),
            Some(ProviderResponse::Error(e)) => Err(e.message.into()),
            Some(_) => Err("unexpected response from model provider".into()),
            None => Err("model provider closed the connection without responding".into()),
        }
    }

    async fn invoke_model(
        &self,
        model_id: &str,
        request: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        let mut framed = self
            .connect_and_send(&ProviderRequest::InvokeModel {
                model_id: model_id.to_owned(),
                request: Box::new(request),
            })
            .await
            .map_err(|e| {
                CompletionError::provider(
                    ErrorClass::Transient,
                    format!(
                        "failed to reach model provider at {}: {e}",
                        self.socket_path.display()
                    ),
                )
            })?;

        match recv_json::<ProviderResponse>(&mut framed)
            .await
            .map_err(|e| {
                CompletionError::ResponseError(format!("failed reading from model provider: {e}"))
            })? {
            Some(ProviderResponse::InvokeStarted) => {}
            Some(ProviderResponse::Error(e)) => return Err(CompletionError::from(e)),
            Some(_) => {
                return Err(CompletionError::ResponseError(
                    "unexpected response from model provider".to_owned(),
                ));
            }
            None => {
                return Err(CompletionError::ResponseError(
                    "model provider closed the connection without responding".to_owned(),
                ));
            }
        }

        let stream = async_stream::stream! {
            loop {
                match recv_json::<ProviderResponse>(&mut framed).await {
                    Ok(Some(ProviderResponse::Chunk(item))) => {
                        yield item.map_err(CompletionError::from);
                    }
                    Ok(Some(ProviderResponse::StreamEnd)) => break,
                    Ok(Some(_)) => {
                        yield Err(CompletionError::ResponseError(
                            "unexpected mid-stream response from model provider".to_owned(),
                        ));
                        break;
                    }
                    Ok(None) => {
                        yield Err(CompletionError::ResponseError(
                            "model provider closed the connection mid-stream".to_owned(),
                        ));
                        break;
                    }
                    Err(e) => {
                        yield Err(CompletionError::ResponseError(format!(
                            "failed reading from model provider: {e}"
                        )));
                        break;
                    }
                }
            }
        };
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mock::mock_model;
    use crate::{SingleModelProvider, Usage};

    fn test_entry() -> ModelEntry {
        ModelEntry {
            model_id: "mock".to_owned(),
            display_name: "mock".to_owned(),
            context_window: 100,
            max_output_tokens: None,
            supports_image_input: true,
        }
    }

    fn test_request(prompt: &str) -> CompletionRequest {
        CompletionRequest {
            preamble: Some("system prompt".to_owned()),
            chat_history: vec![crate::Message::user(prompt)],
            tools: vec![],
            max_tokens: Some(42),
            additional_params: None,
        }
    }

    #[tokio::test]
    async fn round_trip_over_unix_socket() {
        let (model, mut ctrl) = mock_model();
        let provider = Arc::new(SingleModelProvider::new(test_entry(), model));
        let (path, server) = serve_provider(provider).expect("bind provider socket");
        tokio::spawn(server);

        let remote = RemoteModelProvider::new(&path);

        // list_models round-trips the provider's entries.
        let models = remote.list_models().await.expect("list models");
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].model_id, "mock");
        assert_eq!(models[0].context_window, 100);
        assert!(models[0].supports_image_input);

        // invoke_model forwards the request and streams the response back.
        let mut response = remote
            .invoke_model("mock", test_request("hello"))
            .await
            .expect("invoke model");

        let request = ctrl.next_request().await;
        assert_eq!(request.preamble.as_deref(), Some("system prompt"));
        assert_eq!(request.max_tokens, Some(42));

        ctrl.send_text("hello ");
        ctrl.send_text("world");
        ctrl.finish_with_usage(Some(Usage {
            input_tokens: 7,
            output_tokens: 3,
            ..Usage::default()
        }));

        let mut text = String::new();
        let mut usage = None;
        while let Some(item) = response.next().await {
            match item.expect("stream item") {
                StreamChunk::Text(t) => text.push_str(&t),
                StreamChunk::Final(r) => usage = r.usage,
                other => panic!("unexpected stream item: {other:?}"),
            }
        }
        assert_eq!(text, "hello world");
        let usage = usage.expect("final usage");
        assert_eq!(usage.input_tokens, 7);
        assert_eq!(usage.output_tokens, 3);

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn connecting_to_missing_socket_fails_cleanly() {
        let remote = RemoteModelProvider::new("/nonexistent/provider.sock");
        match remote.invoke_model("mock", test_request("hi")).await {
            Err(CompletionError::ProviderError { .. }) => {}
            Err(other) => panic!("unexpected error variant: {other}"),
            Ok(_) => panic!("connect should fail"),
        }
        assert!(remote.list_models().await.is_err());
    }

    /// A provider's retry classification must survive the wire round-trip
    /// so core can react to it (e.g. drop oversized inputs on
    /// [`ErrorClass::ContextOverflow`]).
    #[tokio::test]
    async fn error_class_survives_the_wire() {
        let (model, mut ctrl) = mock_model();
        let provider = Arc::new(SingleModelProvider::new(test_entry(), model));
        let (path, server) = serve_provider(provider).expect("bind provider socket");
        tokio::spawn(server);

        let remote = RemoteModelProvider::new(&path);
        let mut response = remote
            .invoke_model("mock", test_request("hello"))
            .await
            .expect("invoke model");

        let _request = ctrl.next_request().await;
        ctrl.send_error(CompletionError::provider(
            ErrorClass::ContextOverflow,
            "input is too long",
        ));
        ctrl.drop_stream();

        let item = response
            .next()
            .await
            .expect("one stream item")
            .expect_err("stream item should be an error");
        assert_eq!(item.class(), ErrorClass::ContextOverflow);
        assert!(item.to_string().contains("input is too long"));

        std::fs::remove_file(&path).ok();
    }
    /// Reports through `dropped` when it goes out of scope.
    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    /// Where a [`HangingProvider`] stalls.
    #[derive(Clone, Copy)]
    enum Stall {
        /// Inside `invoke_model`, as a queued request would.
        Invoke,
        /// Between stream items, after `InvokeStarted`.
        Stream,
    }

    /// A provider that never finishes. It signals when its work starts and when
    /// the server drops that work.
    struct HangingProvider {
        stall: Stall,
        entered: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
        dropped: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl ModelProvider for HangingProvider {
        async fn list_models(&self) -> Result<Vec<ModelEntry>, BoxError> {
            Ok(vec![test_entry()])
        }

        async fn invoke_model(
            &self,
            _model_id: &str,
            _request: CompletionRequest,
        ) -> Result<ModelStream, CompletionError> {
            let signal = DropSignal(self.dropped.lock().expect("drop signal lock").take());
            if let Some(tx) = self.entered.lock().expect("entry signal lock").take() {
                let _ = tx.send(());
            }
            match self.stall {
                Stall::Invoke => {
                    let _signal = signal;
                    std::future::pending().await
                }
                Stall::Stream => Ok(Box::pin(async_stream::stream! {
                    let _signal = signal;
                    std::future::pending::<()>().await;
                    yield Err(CompletionError::ResponseError("unreachable".to_owned()));
                })),
            }
        }
    }

    async fn assert_hang_up_cancels(stall: Stall) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
        let provider = Arc::new(HangingProvider {
            stall,
            entered: std::sync::Mutex::new(Some(entered_tx)),
            dropped: std::sync::Mutex::new(Some(dropped_tx)),
        });
        let (path, server) = serve_provider(provider).expect("bind provider socket");
        tokio::spawn(server);

        let remote = RemoteModelProvider::new(&path);
        // `async_trait` boxes the future, so dropping `invoke` drops the connection.
        let mut invoke = remote.invoke_model("mock", test_request("hello"));
        let response = match stall {
            Stall::Invoke => {
                tokio::select! {
                    _ = &mut invoke => panic!("a queued invocation should not return"),
                    entered = entered_rx => entered.expect("entry signal"),
                }
                None
            }
            Stall::Stream => {
                let response = (&mut invoke).await.expect("invoke model");
                entered_rx.await.expect("entry signal");
                Some(response)
            }
        };

        // The work must survive while the client is still connected.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            matches!(
                dropped_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "server dropped the provider's work while the client was connected"
        );

        // Hang up: dropping the client side closes the socket.
        drop(response);
        drop(invoke);

        tokio::time::timeout(std::time::Duration::from_secs(5), dropped_rx)
            .await
            .expect("server should drop the provider's work once the client hangs up")
            .expect("drop signal");

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn hang_up_while_queued_cancels_the_invocation() {
        assert_hang_up_cancels(Stall::Invoke).await;
    }

    #[tokio::test]
    async fn hang_up_mid_stream_cancels_the_stream() {
        assert_hang_up_cancels(Stall::Stream).await;
    }
}
