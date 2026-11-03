use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{Method, Request, Response, StatusCode, body::Incoming};
use infinity_protocol::{ClientMessage, DaemonMessage, RapClaimRecord};
use serde::Deserialize;
use tokio::net::{TcpListener, UnixStream};
use tokio_util::codec::Framed;
use uuid::Uuid;

use super::bindings::{SessionBinding, SessionIdentity};
use super::{ControlApi, Result, SendArgs, agent_tools};
use crate::model::ClientKey;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RapConfig {
    pub listen: SocketAddr,
}
impl Default for RapConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 7433)),
        }
    }
}

#[derive(Clone)]
pub(super) struct VerifiedCall {
    identity: SessionIdentity,
    socket: PathBuf,
    record: RapClaimRecord,
}
#[derive(Clone)]
pub(super) struct Completed {
    callback_url: String,
    result: rap_protocol::RapCallback,
    is_error: bool,
    finished: Instant,
}
#[derive(Clone)]
pub(super) enum Execution {
    InFlight {
        call: VerifiedCall,
    },
    Completed {
        call: VerifiedCall,
        result: Box<Completed>,
    },
}
impl Execution {
    fn call(&self) -> &VerifiedCall {
        match self {
            Self::InFlight { call, .. } | Self::Completed { call, .. } => call,
        }
    }
    pub(super) fn identity(&self) -> &SessionIdentity {
        &self.call().identity
    }
    fn expired(&self) -> bool {
        matches!(self, Self::Completed { result, .. } if result.finished.elapsed() >= Duration::from_secs(600))
    }
}

pub struct RapServer {
    listener: TcpListener,
    endpoint: String,
    sockets: Vec<PathBuf>,
    http: reqwest::Client,
}
impl RapServer {
    pub async fn bind(config: &RapConfig, mut sockets: Vec<PathBuf>) -> Result<Self> {
        if !config.listen.ip().is_loopback() || config.listen.port() == 0 {
            return Err("rap.listen must be a fixed loopback address with a nonzero port".into());
        }
        let listener = TcpListener::bind(config.listen)
            .await
            .map_err(|error| format!("cannot bind RAP listener {}: {error}", config.listen))?;
        sockets.sort();
        sockets.dedup();
        Ok(Self {
            listener,
            endpoint: format!("http://{}/invoke", config.listen),
            sockets,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(2))
                .build()?,
        })
    }
    pub async fn serve(self, api: Arc<ControlApi>) -> Result<()> {
        let server = Arc::new(self);
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = server.listener.accept(), if connections.len() < 64 => {
                    let (stream, _) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            tracing::warn!(%error, "RAP accept failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };
                    let server = server.clone();
                    let api = api.clone();
                    connections.spawn(async move {
                        let service = hyper::service::service_fn(move |request| {
                            let server = server.clone();
                            let api = api.clone();
                            async move { Ok::<_, Infallible>(server.handle(request, api).await) }
                        });
                        let mut builder = hyper::server::conn::http1::Builder::new();
                        builder.timer(hyper_util::rt::TokioTimer::new())
                            .header_read_timeout(Duration::from_secs(5)).keep_alive(false);
                        match tokio::time::timeout(Duration::from_secs(30), builder.serve_connection(hyper_util::rt::TokioIo::new(stream), service)).await {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => tracing::debug!(%error, "RAP connection closed"),
                            Err(error) => tracing::debug!(%error, "RAP connection idle timeout"),
                        }
                    });
                }
                done = connections.join_next(), if !connections.is_empty() => {
                    if let Some(Err(error)) = done { tracing::error!(%error, "RAP connection task failed"); }
                }
            }
        }
    }
    async fn handle(
        &self,
        request: Request<Incoming>,
        api: Arc<ControlApi>,
    ) -> Response<Full<Bytes>> {
        if request.method() == Method::GET && request.uri().path() == "/.well-known/rap-toolset" {
            let tool = agent_tools()[0].clone();
            return response(
                StatusCode::OK,
                serde_json::json!({"name":"agentd", "endpoint":self.endpoint, "tools":[tool]})
                    .to_string(),
            );
        }
        if request.method() != Method::POST || request.uri().path() != "/invoke" {
            return response(StatusCode::NOT_FOUND, "Not found");
        }
        let invoke = async {
            let body = Limited::new(request.into_body(), 1024 * 1024)
                .collect()
                .await?;
            let invocation: rap_protocol::RapInvocation = serde_json::from_slice(&body.to_bytes())?;
            let url = reqwest::Url::parse(&invocation.callback_url)?;
            let scope = url
                .path()
                .strip_prefix("/managed/")
                .ok_or("missing managed invocation UUID")?;
            let id = Uuid::parse_str(scope)?;
            self.invoke(api, id).await
        };
        match tokio::time::timeout(Duration::from_secs(25), invoke).await {
            Ok(Ok(())) => response(StatusCode::OK, "OK"),
            Ok(Err(error)) => {
                tracing::warn!(%error, "unverified RAP invocation");
                response(StatusCode::FORBIDDEN, "Unverified RAP invocation")
            }
            Err(error) => {
                tracing::warn!(%error, "RAP invocation timed out");
                response(StatusCode::SERVICE_UNAVAILABLE, "RAP invocation timed out")
            }
        }
    }
    async fn invoke(&self, api: Arc<ControlApi>, id: Uuid) -> Result<()> {
        let cached = {
            let mut calls = api.bindings.rap_calls.lock().await;
            calls.retain(|_, call| !call.expired());
            calls.get(&id).cloned()
        };
        let execution = if let Some(cached) = cached {
            let call = cached.call();
            let binding = api
                .bindings
                .authenticate_rap(&call.socket, &self.endpoint, &call.record)
                .await
                .ok_or("binding revoked")?;
            if binding.identity() != &call.identity {
                return Err("binding replaced".into());
            }
            cached
        } else {
            let mut verified = None;
            let mut claim_errors = Vec::new();
            for socket in &self.sockets {
                let record = match claim(socket, id).await {
                    Ok(Some(record)) => record,
                    Ok(None) => continue,
                    Err(error) => {
                        claim_errors.push(format!("{}: {error}", socket.display()));
                        continue;
                    }
                };
                if let Some(binding) = api
                    .bindings
                    .authenticate_rap(socket, &self.endpoint, &record)
                    .await
                {
                    verified = Some((socket.clone(), record, binding));
                    break;
                }
            }
            let (socket, record, binding) = verified.ok_or_else(|| {
                format!(
                    "no matching daemon dispatch and live binding; claim errors: {}",
                    claim_errors.join(", ")
                )
            })?;
            let call = VerifiedCall {
                identity: binding.identity().clone(),
                socket,
                record,
            };
            let mut calls = api.bindings.rap_calls.lock().await;
            calls.retain(|_, call| !call.expired());
            match calls.entry(id) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.get().clone(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let execution = Execution::InFlight { call: call.clone() };
                    entry.insert(execution.clone());
                    let api = api.clone();
                    let http = self.http.clone();
                    tokio::spawn(async move {
                        let result = execute(&api, id, &call.record, &binding).await;
                        api.bindings.rap_calls.lock().await.insert(
                            id,
                            Execution::Completed {
                                call: call.clone(),
                                result: Box::new(result.clone()),
                            },
                        );
                        drop(binding);
                        Self::deliver(&http, &api, id, &call, &result).await;
                    });
                    execution
                }
            }
        };
        if let Execution::Completed { call, result } = execution {
            let api = api.clone();
            let http = self.http.clone();
            tokio::spawn(async move {
                Self::deliver(&http, &api, id, &call, &result).await;
            });
        }
        Ok(())
    }
    async fn deliver(
        http: &reqwest::Client,
        api: &ControlApi,
        id: Uuid,
        call: &VerifiedCall,
        completed: &Completed,
    ) {
        let mut backoff = Duration::from_millis(100);
        loop {
            if completed.finished.elapsed() >= Duration::from_secs(600) {
                tracing::error!(%id, is_error=completed.is_error, "RAP callback delivery expired");
                return;
            }
            let binding = api
                .bindings
                .authenticate_rap(&call.socket, &call.record.endpoint, &call.record)
                .await;
            if binding
                .as_ref()
                .is_none_or(|binding| binding.identity() != &call.identity)
            {
                tracing::warn!(%id, "RAP callback delivery stopped after binding revocation");
                return;
            }
            drop(binding);
            let remaining = Duration::from_secs(600).saturating_sub(completed.finished.elapsed());
            match tokio::time::timeout(
                remaining.min(Duration::from_secs(2)),
                http.post(&completed.callback_url)
                    .json(&completed.result)
                    .send(),
            )
            .await
            {
                Ok(Ok(response)) if response.status().is_success() => return,
                Ok(Ok(response)) => {
                    tracing::warn!(%id, status=%response.status(), "RAP callback rejected")
                }
                Ok(Err(error)) => tracing::warn!(%id, %error, "RAP callback delivery failed"),
                Err(error) => tracing::warn!(%id, %error, "RAP callback delivery timed out"),
            }
            tokio::time::sleep(
                backoff.min(Duration::from_secs(600).saturating_sub(completed.finished.elapsed())),
            )
            .await;
            backoff = (backoff * 2).min(Duration::from_secs(3));
        }
    }
}

async fn execute(
    api: &ControlApi,
    id: Uuid,
    record: &RapClaimRecord,
    binding: &SessionBinding,
) -> Completed {
    let result = match serde_json::from_value::<SendArgs>(record.arguments.clone()) {
        Ok(args) => {
            api.send(
                binding.sender(),
                &args.to,
                args.body,
                args.reply_to,
                ClientKey(format!("rap:{id}")),
            )
            .await
        }
        Err(_) => Err("invalid send_message arguments".into()),
    };
    let (text, is_error) = match result {
        Ok(value) => (
            value["status"].as_str().unwrap_or("Queued").to_owned(),
            false,
        ),
        Err(error) => (error.to_string(), true),
    };
    Completed {
        callback_url: record.callback_url.clone(),
        is_error,
        finished: Instant::now(),
        result: rap_protocol::RapCallback::ToolResult(rap_protocol::RapToolResult {
            group_id: record
                .thread_stack
                .last()
                .expect("bug: verified call has a root")
                .id
                .clone(),
            id: record.id.clone(),
            call_id: record.call_id.clone(),
            text: Some(text),
            content: None,
            display_as: None,
            subscription: None,
        }),
    }
}

async fn claim(socket: &Path, id: Uuid) -> Result<Option<RapClaimRecord>> {
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut framed = Framed::new(
            UnixStream::connect(socket).await?,
            infinity_protocol::length_delimited_codec(),
        );
        framed
            .send(
                serde_json::to_vec(&ClientMessage::ClaimRapInvocation {
                    invocation: id.to_string(),
                })?
                .into(),
            )
            .await?;
        while let Some(frame) = framed.next().await {
            match serde_json::from_slice::<DaemonMessage>(&frame?)? {
                DaemonMessage::RapInvocationClaim { invocation, record }
                    if invocation == id.to_string() =>
                {
                    return Ok(record);
                }
                _ => {}
            }
        }
        Err("daemon closed claim connection".into())
    })
    .await?
}
fn response(status: StatusCode, text: impl Into<String>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(text.into())))
        .expect("bug: valid HTTP response")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mailroom::Mailroom,
        model::{AgentName, Incarnation},
        supervisor::Broker,
    };

    #[tokio::test]
    async fn verified_invocation_acknowledges_before_slow_send_finishes() {
        let dir = tempfile::tempdir().expect("directory");
        let room = Arc::new(
            Mailroom::open(dir.path().join("mail.db"), Default::default())
                .await
                .expect("mailroom"),
        );
        let agent = room
            .register(AgentName::parse("sender").expect("sender"))
            .await
            .expect("register");
        let receiver = room
            .register(AgentName::parse("receiver").expect("receiver"))
            .await
            .expect("register");
        let (bound, _) = tokio::sync::mpsc::unbounded_channel();
        let bindings = Arc::new(super::super::Bindings::new(
            dir.path().join("mcp.sock"),
            "agentd".into(),
            bound,
        ));
        let broker =
            Arc::new(Broker::new(room.clone(), vec![], Default::default()).expect("broker"));
        let api = Arc::new(ControlApi {
            mailroom: room.clone(),
            broker: broker.clone(),
            bindings: bindings.clone(),
        });
        let socket = dir.path().join("daemon.sock");
        let unix = tokio::net::UnixListener::bind(&socket).expect("Unix listener");
        let root = infinity_protocol::ThreadRef::local("root".into());
        bindings
            .bind_infinity(&socket, root.clone(), agent, Incarnation(1))
            .await;
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let callback = TcpListener::bind("127.0.0.1:0").await.expect("callback");
        let id = Uuid::new_v4();
        let record = RapClaimRecord {
            thread_stack: vec![root],
            operation: "send_message".into(),
            arguments: serde_json::json!({"to":"@receiver","body":"slow once"}),
            endpoint: format!("http://{}/invoke", listener.local_addr().expect("address")),
            callback_url: format!(
                "http://{}/managed/{id}",
                callback.local_addr().expect("callback address")
            ),
            id: "slow-call".into(),
            call_id: None,
        };
        let claim_record = record.clone();
        let claims = tokio::spawn(async move {
            let (stream, _) = unix.accept().await.expect("claim connection");
            let mut wire = Framed::new(stream, infinity_protocol::length_delimited_codec());
            wire.next().await.expect("claim").expect("frame");
            wire.send(
                serde_json::to_vec(&DaemonMessage::RapInvocationClaim {
                    invocation: id.to_string(),
                    record: Some(claim_record),
                })
                .expect("claim JSON")
                .into(),
            )
            .await
            .expect("claim response");
        });
        let server = RapServer {
            listener,
            endpoint: record.endpoint.clone(),
            sockets: vec![socket],
            http: reqwest::Client::new(),
        };
        let pause = broker.pause_sends().await;
        let ack = tokio::time::timeout(Duration::from_secs(1), server.invoke(api, id)).await;
        assert!(
            ack.is_ok(),
            "verified invocation must acknowledge before execution completes"
        );
        ack.expect("ack").expect("verified");
        tokio::time::sleep(Duration::from_secs(16)).await;
        assert_eq!(
            room.pending_for(receiver).await.expect("pending").len(),
            1,
            "one durable message while completion is blocked"
        );
        drop(pause);
        let (stream, _) = tokio::time::timeout(Duration::from_secs(3), callback.accept())
            .await
            .expect("late callback timeout")
            .expect("callback");
        let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
            let body = request
                .into_body()
                .collect()
                .await
                .expect("callback body")
                .to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&body).expect("callback JSON");
            assert!(
                value["text"]
                    .as_str()
                    .expect("result text")
                    .starts_with("Queued")
            );
            Ok::<_, Infallible>(response(StatusCode::OK, "OK"))
        });
        tokio::time::timeout(
            Duration::from_secs(3),
            hyper::server::conn::http1::Builder::new()
                .keep_alive(false)
                .serve_connection(hyper_util::rt::TokioIo::new(stream), service),
        )
        .await
        .expect("callback completes")
        .expect("connection");
        claims.await.expect("claims task");
        assert_eq!(room.pending_for(receiver).await.expect("pending").len(), 1);
    }
}
