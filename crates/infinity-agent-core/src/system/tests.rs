//! Driver and router behavioral tests for interruption, deferral, idling,
//! routing, replay, and auto-compaction.

use async_trait::async_trait;
use infinity_provider_protocol::StreamChunk;
use infinity_provider_protocol::message::UserContent;
use rap_protocol::ThreadId;
use tokio::sync::mpsc;

use super::events::AgentEvent;
use super::local::{ChannelSender, ThreadLifecycleEvent, ThreadLifecycleState};
use super::test_support::*;
use crate::message::{
    InputMessage, InputMessageContent, SyntheticKind, TaggedSyntheticKind, UserChoiceRequired,
};
use crate::tools::{Tool, ToolContext};

#[tokio::test(flavor = "current_thread")]
async fn r5_passive_mail_notification_is_bounded() {
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use std::time::Duration;

    tokio::task::LocalSet::new().run_until(async {
        let (model, mut ctrl) = model_source(None);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listener");
        let url = format!("http://{}", listener.local_addr().expect("address"));
        let (seen, request_seen) = tokio::sync::oneshot::channel();
        let server = tokio::task::spawn_local(async move {
            use tokio::io::AsyncReadExt;
            let (mut stream, _) = listener.accept().await.expect("connection");
            let mut buffer = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let n = stream.read(&mut chunk).await.expect("request");
                assert!(n > 0, "connection closed before request headers");
                buffer.extend_from_slice(&chunk[..n]);
                if buffer.windows(4).any(|w| w == b"\r\n\r\n") { break; }
            }
            assert!(String::from_utf8_lossy(&buffer).contains("/cancel_tool_call"));
            seen.send(()).expect("observed");
            std::future::pending::<()>().await;
        });
        let conv = InMemoryConversationStore::new();
        let mut running = super::AgentSystemBuilder::new_local(
            conv.clone(), InMemoryStateStore::new(), model)
            .rap_notifier(rap_client::notifier::RapNotifier::new(
                vec![url], rap_client::http::SimpleHttpClient::new()))
            .start_with_observer(move |_| TestObserver { tx: tx.clone() });
        running.send_user_text(ThreadId::from_ref("t1"), "work").await;
        ctrl.next_request().await;
        ctrl.send_tool_call("sleeping", "sleep_until_event_or_input", serde_json::json!({}));
        ctrl.finish();
        collect_until_finished(&mut rx).await;
        let mut human = peer_input("t1", "human-mail", "wake from human");
        human.0.synthetic = Some(SyntheticKind::Tagged(TaggedSyntheticKind::PeerMessage { from: "human".into() }));
        running.send(human.0, &human.1).await;
        tokio::time::timeout(Duration::from_secs(2), request_seen).await.expect("notification was sent").expect("server");
        let request = tokio::time::timeout(Duration::from_secs(3), ctrl.next_request()).await
            .expect("human mail must progress after the two-second cancellation notification budget");
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("wake from human") && history.contains("Tool call interrupted by user"));
        ctrl.send_text("awake");
        ctrl.finish();
        loop {
            if matches!(next_evt(&mut rx).await, Evt::E(AgentEvent::InputCommitted { dedup_id }) if dedup_id == human.1) { break; }
        }
        collect_until_finished(&mut rx).await;
        wait_idle(&mut running).await;
        assert_eq!(persisted(&conv, "t1").matches("Tool call interrupted by user").count(), 1);
        running.shutdown().await;
        server.abort();
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn pending_choices_are_thread_local_and_replayed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use super::{AgentSystemBuilder, EventCollector, NoDeferral};
            use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
            use crate::traits::{ConversationStore, StateStore};

            let (model, _ctrl) = model_source(None);
            let conv = InMemoryConversationStore::new();
            conv.ensure_root_thread(ThreadId::from_ref("t1"))
                .await
                .expect("ensure root");
            conv.ensure_root_thread(ThreadId::from_ref("t2"))
                .await
                .expect("ensure root");
            let state = InMemoryStateStore::new();
            let sender = ChannelSender::new_pair().0;
            let mut system = AgentSystemBuilder::new(conv, state.clone(), model, sender).build();
            let choice_input = |thread_id: &str, choice_id: &str| {
                (
                    InputMessage {
                        content: InputMessageContent::UserChoice(UserChoiceRequired {
                            content_type: "user_choice_required".to_owned(),
                            id: choice_id.into(),
                            call_id: None,
                            prompt: "Pick one".to_owned(),
                            choices: vec!["A".to_owned(), "B".to_owned()],
                            default: 0,
                            response_url: "http://example.test/choice".to_owned(),
                        }),
                        group_id: thread_id.into(),
                        metadata: None,
                        synthetic: None,
                        display_as: None,
                        subscription: false,
                    },
                    format!("message-{choice_id}"),
                )
            };
            let collector = EventCollector::new();
            system
                .step(
                    vec![
                        choice_input("t1", "choice-1"),
                        choice_input("t2", "choice-2"),
                    ],
                    &collector,
                    &mut NoDeferral,
                )
                .await
                .expect("persist choices");

            assert_eq!(
                state
                    .get_pending_user_choices(ThreadId::from_ref("t1"))
                    .await
                    .expect("load first thread choices")[0]
                    .id
                    .as_str(),
                "choice-1"
            );
            assert_eq!(
                state
                    .get_pending_user_choices(ThreadId::from_ref("t2"))
                    .await
                    .expect("load second thread choices")[0]
                    .id
                    .as_str(),
                "choice-2"
            );

            let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
            let replay_conv = InMemoryConversationStore::new();
            replay_conv
                .ensure_root_thread(ThreadId::from_ref("t1"))
                .await
                .expect("ensure replay root");
            let running = AgentSystemBuilder::new_local(replay_conv, state, model_source(None).0)
                .start_with_observer(|_| TestObserver {
                    tx: mpsc::unbounded_channel().0,
                });
            running.subscribe(ThreadId::from_ref("t1"), sub_tx).await;
            let Evt::Replay(snapshot) = next_evt(&mut sub_rx).await else {
                panic!("expected replay");
            };
            assert_eq!(snapshot.pending_choices.len(), 1);
            assert_eq!(snapshot.pending_choices[0].id.as_str(), "choice-1");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn driver_idles_after_text_response() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut rx, mut ctrl, _conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "hello")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("hi there");
            ctrl.finish();
            let texts = collect_until_finished(&mut rx).await;
            assert_eq!(texts, vec!["hi there"]);
            wait_idle(&mut running).await;
            assert!(running.is_idle());
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn driver_stays_alive_waiting_for_tool_result() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "use tool")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            assert!(
                !matches!(
                    running.try_next_lifecycle_event(),
                    Ok(ThreadLifecycleEvent {
                        state: ThreadLifecycleState::Idle,
                        ..
                    })
                ),
                "should not idle while a tool call is pending"
            );
            assert!(!running.is_idle());

            // A client attaching while waiting gets a replay whose history
            // ends with the unresolved tool call and no completion in flight.
            let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("t1"), sub_tx).await;
            match next_evt(&mut sub_rx).await {
                Evt::Replay(snapshot) => {
                    assert!(!snapshot.in_progress);
                    assert!(matches!(
                        snapshot.history.last(),
                        Some(crate::message::InfinityMessage::ToolCall { call, .. })
                            if call.function.name == "async_tool"
                    ));
                }
                other => panic!("expected replay, got {other:?}"),
            }

            // Deliver the result; the driver wakes and finishes.
            running
                .send(tool_result_input("t1", "tc-1", "tool done").0, "res-1")
                .await;
            let _req2 = ctrl.next_request().await;
            ctrl.send_text("ok");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn user_text_interrupts_active_completion() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "first")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("partial...");
            // Wait until the chunk is observed so the completion is in flight.
            loop {
                if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                    break;
                }
            }
            running
                .send_user_text(ThreadId::from_ref("t1"), "stop that")
                .await;
            let req2 = ctrl.next_request().await;
            let has_interrupt = req2.chat_history.into_iter().any(|m| {
                if let infinity_provider_protocol::message::Message::User { content } = &m
                    && let Some(UserContent::Text(t)) = content.first()
                {
                    return t.text.contains("<interrupt>");
                }
                false
            });
            assert!(has_interrupt, "interrupting input should carry the marker");
            ctrl.send_text("ok stopped");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
        })
        .await;
}

/// A non-user-text input (e.g. a stale tool result) arriving while a
/// completion is streaming must not interrupt it.
#[tokio::test(flavor = "current_thread")]
async fn non_user_text_during_completion_does_not_interrupt() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![Box::new(AsyncTool)], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "do stuff")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            running
                .send(tool_result_input("t1", "tc-1", "tool output").0, "res-1")
                .await;
            let _req2 = ctrl.next_request().await;
            ctrl.send_text("processing...");
            loop {
                if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                    break;
                }
            }
            // A stale tool result arrives mid-stream — must not interrupt.
            running
                .send(
                    tool_result_input("t1", "tc-other", "stale event").0,
                    "res-stale",
                )
                .await;
            ctrl.send_text(" done");
            ctrl.finish();
            let texts = collect_until_finished(&mut rx).await;
            assert!(
                texts.join("").contains("done"),
                "should not have been interrupted"
            );
        })
        .await;
}

/// An associative subscription event arriving while a completion is streaming
/// is queued and processed in the next round rather than interrupting it.
#[tokio::test(flavor = "current_thread")]
async fn subscription_event_queued_during_completion_processed_after() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) =
                start_system(vec![Box::new(SubscribeTool)], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "subscribe")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-sub", "subscribe_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // The subscription result triggers the next round; leave the model
            // mid-stream while the event arrives.
            let _req2 = ctrl.next_request().await;
            ctrl.send_text("got subscription...");
            loop {
                if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                    break;
                }
            }
            running
                .send(
                    subscription_event_input("t1", "tc-sub", "event payload xyz").0,
                    "ev-1",
                )
                .await;
            ctrl.send_text(" all good");
            ctrl.finish();
            let texts = collect_until_finished(&mut rx).await;
            assert!(
                texts.join("").contains("all good"),
                "should not have been interrupted"
            );

            // The queued event appears in the next round.
            let req3 = ctrl.next_request().await;
            assert!(
                tool_result_texts(&req3)
                    .iter()
                    .any(|t| t.contains("event payload xyz")),
                "queued event should appear in next round"
            );
            ctrl.send_text("processed");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
        })
        .await;
}
/// An unanswered `close_thread` call never receives a result, so it must not
/// keep the driver resident.
#[tokio::test(flavor = "current_thread")]
async fn driver_idles_after_close_thread_tool_call() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            /// Stub close_thread (the built-in one refuses to close a root
            /// thread); registered with built-ins disabled so it is the only
            /// tool by that name.
            struct CloseThreadStub;
            #[async_trait]
            impl Tool<ChannelSender> for CloseThreadStub {
                fn name(&self) -> &str {
                    "close_thread"
                }
                fn description(&self) -> &str {
                    "close"
                }
                fn parameters(&self) -> serde_json::Value {
                    serde_json::json!({"type":"object","properties":{}})
                }
                async fn execute(
                    &self,
                    _: serde_json::Value,
                    _: rap_protocol::ToolCallId,
                    _: Option<rap_protocol::ProviderCallId>,
                    _: &ToolContext<ChannelSender>,
                ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    Ok(())
                }
            }
            let (mut running, mut rx, mut ctrl, _conv) =
                start_system_with(vec![Box::new(CloseThreadStub)], None, false);
            running
                .send_user_text(ThreadId::from_ref("t1"), "close")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-1",
                "close_thread",
                serde_json::json!({"thread_id": "t1"}),
            );
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
            assert!(running.is_idle());
        })
        .await;
}

/// Thread reports arriving while waiting for a non-passive async tool result
/// are deferred until the tool result is processed.
#[tokio::test(flavor = "current_thread")]
async fn thread_report_deferred_during_async_tool_wait() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![Box::new(AsyncTool)], None);

            // 1. User sends input, model calls async_tool.
            running
                .send_user_text(ThreadId::from_ref("t1"), "do async")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-async", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // 2. While waiting for the tool result, a thread report arrives.
            running
                .send(
                    InputMessage {
                        content: InputMessageContent::User(UserContent::ToolResult(
                            infinity_provider_protocol::message::ToolResult {
                                id: String::new(),
                                call_id: None,
                                content: vec![
                                    infinity_provider_protocol::message::ToolResultContent::Text(
                                        infinity_provider_protocol::message::Text {
                                            text: "Report from child thread: progress update"
                                                .into(),
                                        },
                                    ),
                                ],
                            },
                        )),
                        group_id: "t1".into(),
                        metadata: None,
                        synthetic: Some(SyntheticKind::Tagged(TaggedSyntheticKind::ThreadReport {
                            tool_call_id: "tc-async".into(),
                            child_thread_id: "child-1".into(),
                        })),
                        display_as: None,
                        subscription: false,
                    },
                    "report-1",
                )
                .await;
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }

            // 3. The thread report must not have triggered a completion.
            assert!(
                ctrl.try_next_request().is_none(),
                "thread report should not trigger completion while waiting for async tool"
            );

            // 4. The real result arrives; the deferred report is included.
            running
                .send(tool_result_input("t1", "tc-async", "tool done").0, "res-1")
                .await;
            let req2 = ctrl.next_request().await;
            ctrl.send_text("all processed");
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            let texts = tool_result_texts(&req2);
            assert!(texts.iter().any(|t| t.contains("tool done")));
            assert!(texts.iter().any(|t| t.contains("progress update")));
        })
        .await;
}

/// Shutting the system down while a completion is in flight interrupts the
/// completion (stripping trailing reasoning) and waits for every thread
/// driver to flush pending history items before the router task returns.
/// A tool result the model has already produced output for is validated and
/// must be persisted together with the partial output.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_persists_model_validated_tool_result() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, conv) = start_system(vec![Box::new(AsyncTool)], None);

            // 1. User input → model issues an async tool call.
            running
                .send_user_text(ThreadId::from_ref("t1"), "do something")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // 2. The tool result arrives → the driver starts a new
            //    completion, and the model streams some output for it
            //    (validating the result for persistence).
            running
                .send(
                    tool_result_input("t1", "tc-1", "tool execution result").0,
                    "res-1",
                )
                .await;
            let _req2 = ctrl.next_request().await;
            ctrl.send_text("partial reply");
            loop {
                if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                    break;
                }
            }

            // 3. Shut down while the model is mid-response.
            let active_threads = running.active_threads();
            running.shutdown().await;
            assert!(
                active_threads
                    .lock()
                    .expect("bug: active_threads mutex poisoned")
                    .is_empty(),
                "no thread drivers should remain after shutdown"
            );

            // 4. The tool result and the partial output must have been
            //    synced to the store.
            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("tool execution result"),
                "validated in-flight tool result should be persisted on shutdown; history: {stored}"
            );
            assert!(
                stored.contains("partial reply"),
                "partial model output should be persisted on shutdown; history: {stored}"
            );
        })
        .await;
}

/// Orderly shutdown settles an accepted recorded result even before provider
/// validation. Unrelated mail still requires validation before persistence.
#[tokio::test(flavor = "current_thread")]
async fn shutdown_settles_accepted_recorded_tool_result() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, conv) = start_system(vec![Box::new(AsyncTool)], None);

            running
                .send_user_text(ThreadId::from_ref("t1"), "do something")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // The tool result arrives; the completion is in flight but the
            // model has not produced any output yet.
            running
                .send(
                    tool_result_input("t1", "tc-1", "tool execution result").0,
                    "res-1",
                )
                .await;
            let _req2 = ctrl.next_request().await;

            running.shutdown().await;

            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("tc-1"),
                "the tool call itself is model output and must be persisted; history: {stored}"
            );
            assert!(
                stored.contains("tool execution result"),
                "an accepted recorded result must settle before shutdown; history: {stored}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscription_event_deferred_during_async_tool_wait() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![Box::new(AsyncTool)], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "do async")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-async", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // A subscription event arrives while the tool result is pending.
            running
                .send(
                    subscription_event_input("t1", "tc-async", "sub event data").0,
                    "sub-1",
                )
                .await;
            for _ in 0..4 {
                tokio::task::yield_now().await;
            }
            assert!(
                ctrl.try_next_request().is_none(),
                "deferred event must not trigger a completion"
            );

            // The real result arrives; the deferred event is included after it.
            running
                .send(tool_result_input("t1", "tc-async", "tool done").0, "res-1")
                .await;
            let req2 = ctrl.next_request().await;
            ctrl.send_text("all processed");
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            let texts = tool_result_texts(&req2);
            assert!(texts.iter().any(|t| t.contains("tool done")));
            assert!(texts.iter().any(|t| t.contains("sub event data")));
            assert!(
                !texts.iter().any(|t| t.contains("interrupted")),
                "in-flight tool call must not have been interrupted: {texts:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn stale_result_does_not_flush_deferred_events_during_async_tool_wait() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![Box::new(AsyncTool)], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "do async")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-async", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // A deferrable event and a stale tool result arrive together.
            running
                .send(
                    subscription_event_input("t1", "tc-async", "sub event data").0,
                    "sub-1",
                )
                .await;
            running
                .send(
                    tool_result_input("t1", "tc-stale", "stale result").0,
                    "res-stale",
                )
                .await;
            for _ in 0..6 {
                tokio::task::yield_now().await;
            }
            assert!(
                ctrl.try_next_request().is_none(),
                "stale result must not flush the deferred event"
            );

            running
                .send(tool_result_input("t1", "tc-async", "tool done").0, "res-1")
                .await;
            let req2 = ctrl.next_request().await;
            ctrl.send_text("all processed");
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            let texts = tool_result_texts(&req2);
            assert!(texts.iter().any(|t| t.contains("tool done")));
            assert!(texts.iter().any(|t| t.contains("sub event data")));
            assert!(
                !texts.iter().any(|t| t.contains("interrupted")),
                "in-flight tool call must not have been interrupted: {texts:?}"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscribe_mid_thinking_replays_current_thinking() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "think hard")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_chunk(StreamChunk::ReasoningDelta {
                id: None,
                text: "deep ".into(),
            });
            ctrl.send_chunk(StreamChunk::ReasoningDelta {
                id: None,
                text: "thought".into(),
            });
            // Wait until both chunks have been observed.
            let mut seen = String::new();
            while seen != "deep thought" {
                if let Evt::E(AgentEvent::ThinkingChunk { text }) = next_evt(&mut rx).await {
                    seen.push_str(&text);
                }
            }

            let (sub_tx, mut sub_rx) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("t1"), sub_tx).await;
            match next_evt(&mut sub_rx).await {
                Evt::Replay(snapshot) => {
                    assert!(snapshot.in_progress, "completion is in flight");
                    assert_eq!(snapshot.current_thinking.as_deref(), Some("deep thought"));
                }
                other => panic!("expected replay, got {other:?}"),
            }

            // Past the thinking chain, a new subscriber sees no stale thinking.
            ctrl.send_text("the answer");
            loop {
                if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                    break;
                }
            }
            let (sub_tx2, mut sub_rx2) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("t1"), sub_tx2).await;
            match next_evt(&mut sub_rx2).await {
                Evt::Replay(snapshot) => {
                    assert!(snapshot.in_progress);
                    assert!(snapshot.current_thinking.is_none());
                    // The partial text is visible via the in-flight turn.
                    assert!(matches!(
                        snapshot.history.last(),
                        Some(crate::message::InfinityMessage::Assistant { .. })
                    ));
                }
                other => panic!("expected replay, got {other:?}"),
            }

            ctrl.finish();
            collect_until_finished(&mut rx).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn routes_to_separate_threads() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut rx, mut ctrl, _conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "one")
                .await;
            let _r1 = ctrl.next_request().await;
            ctrl.send_text("first");
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            running
                .send_user_text(ThreadId::from_ref("t2"), "two")
                .await;
            let _r2 = ctrl.next_request().await;
            ctrl.send_text("second");
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            wait_idle(&mut running).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn respawns_driver_after_idle() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut rx, mut ctrl, _conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "hello")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("hi");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
            assert!(running.is_idle());

            // A new message respawns the driver; the history is intact.
            running
                .send_user_text(ThreadId::from_ref("t1"), "again")
                .await;
            let req2 = ctrl.next_request().await;
            assert!(req2.chat_history.into_iter().any(|m| {
                if let infinity_provider_protocol::message::Message::User { content } = &m
                    && let Some(UserContent::Text(t)) = content.first()
                {
                    return t.text.contains("hello");
                }
                false
            }));
            ctrl.send_text("welcome back");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
        })
        .await;
}
/// Regression test: a tool result arriving while a compaction child is
/// summarizing must not leave the history with an orphaned tool result whose
/// matching tool call was compacted away. The safe spawn point excludes the
/// trailing unanswered call from the compaction range.
#[tokio::test(flavor = "current_thread")]
async fn compaction_during_tool_call_corrupts_history() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut _rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], Some(small_context_entry()));

            // 1. User sends input, model responds with an async tool call and
            //    usage above the compaction threshold.
            running
                .send_user_text(ThreadId::from_ref("t1"), "do something")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish_with_usage(high_usage());

            // 2. Compaction triggers. Send the tool result BEFORE the
            //    compaction child finishes (simulating a fast tool execution).
            running
                .send(
                    tool_result_input("t1", "tc-1", "tool execution result").0,
                    "res-1",
                )
                .await;

            // Two model requests arrive (compaction child + parent processing
            // the tool result) in scheduling-dependent order.
            let req2 = ctrl.next_request().await;
            if is_compaction_req(&req2) {
                handle_compaction_child(&mut ctrl, &req2, "Summary of conversation so far");
                let _req3 = ctrl.next_request().await;
                ctrl.send_text("processed tool result");
                ctrl.finish();
            } else {
                ctrl.send_text("processed tool result");
                ctrl.finish();
                let compaction_req = ctrl.next_request().await;
                handle_compaction_child(&mut ctrl, &compaction_req, "Summary of conversation so far");
            }

            // 3. After CompactionComplete is applied, trigger a model call so
            //    we can inspect the history.
            running
                .send_user_text(ThreadId::from_ref("t1"), "what happened?")
                .await;
            let req_final =
                tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                    .await
                    .expect("timed out waiting for final model request");

            let history: Vec<_> = req_final.chat_history.into_iter().collect();
            let has_orphaned_tool_result = history.iter().enumerate().any(|(i, m)| {
                if let infinity_provider_protocol::message::Message::User { content } = m
                    && let Some(UserContent::ToolResult(r)) = content.first()
                    && let Some(infinity_provider_protocol::message::ToolResultContent::Text(t)) = r.content.first()
                    && t.text.contains("tool execution result")
                {
                    !history[..i].iter().any(|prev| {
                        if let infinity_provider_protocol::message::Message::Assistant { content, .. } = prev {
                            content.iter().any(|c| {
                                matches!(c, infinity_provider_protocol::message::AssistantContent::ToolCall(tc) if tc.id == "tc-1")
                            })
                        } else {
                            false
                        }
                    })
                } else {
                    false
                }
            });
            assert!(
                !has_orphaned_tool_result,
                "History is corrupted: tool result has no matching tool_call after compaction. History: {history:#?}"
            );

            ctrl.send_text("all good");
            ctrl.finish();
        })
        .await;
}

/// Regression test for issue #64: after compaction is applied, the tracked
/// context usage must be reset so auto-compaction does not immediately
/// re-trigger on the stale pre-compaction token count.
#[tokio::test(flavor = "current_thread")]
async fn compaction_does_not_retrigger_after_applied() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut _rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], Some(small_context_entry()));

            // 1. User input → async tool call + high usage → compaction triggers.
            running
                .send_user_text(ThreadId::from_ref("t1"), "do something")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish_with_usage(high_usage());

            // 2. The compaction child asks for a summary; close it. This sends
            //    CompactionComplete to the parent driver, which is still alive
            //    waiting on the tc-1 tool result.
            let compaction_req = ctrl.next_request().await;
            assert!(is_compaction_req(&compaction_req));
            handle_compaction_child(&mut ctrl, &compaction_req, "Summary of conversation so far");

            // 3. The tool result arrives. The parent applies the compaction
            //    and then processes the tool result. With the bug, the stale
            //    86-token count immediately re-triggers compaction, spawning a
            //    second compaction child whose model request would show up
            //    here.
            running
                .send(
                    tool_result_input("t1", "tc-1", "tool execution result").0,
                    "res-1",
                )
                .await;
            let req = ctrl.next_request().await;
            assert!(
                !is_compaction_req(&req),
                "compaction must not re-trigger after being applied"
            );
            // Finish without usage so the tracked context usage stays at its
            // post-compaction value.
            ctrl.send_text("processed tool result");
            ctrl.finish();

            // 4. With the fix, all drivers wind down. With the bug, a second
            //    compaction child sits waiting on the mock model forever and
            //    the system never idles.
            wait_idle(&mut running).await;
            for _ in 0..100 {
                tokio::task::yield_now().await;
            }
            assert!(
                ctrl.try_next_request().is_none(),
                "no further model requests should be made after compaction is applied"
            );
        })
        .await;
}

/// Regression test: a second compaction on the same (still-loaded) thread
/// after a prior compaction must compute the safe spawn point against
/// absolute store orders, not in-memory indices. The compaction child's
/// inherited history must include everything before the pending tool call and
/// exclude the call itself.
#[tokio::test(flavor = "current_thread")]
async fn second_compaction_during_tool_call_after_prior_compaction() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut _rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], Some(small_context_entry()));

            // ── FIRST ROUND: text response + compaction (no tool call) ──
            running
                .send_user_text(ThreadId::from_ref("t1"), "first message")
                .await;
            let _req1 = ctrl.next_request().await;
            ctrl.send_text("first response");
            ctrl.finish_with_usage(high_usage());

            let compaction_req1 = ctrl.next_request().await;
            assert!(is_compaction_req(&compaction_req1));
            handle_compaction_child(&mut ctrl, &compaction_req1, "Summary of first round");

            // Build more history after the first compaction.
            running
                .send_user_text(ThreadId::from_ref("t1"), "second message")
                .await;
            let _req2 = ctrl.next_request().await;
            ctrl.send_text("second response");
            ctrl.finish();

            running
                .send_user_text(ThreadId::from_ref("t1"), "third message")
                .await;
            let _req3 = ctrl.next_request().await;
            ctrl.send_text("third response");
            ctrl.finish();

            // ── SECOND ROUND: tool call + compaction after prior compaction ──
            running
                .send_user_text(ThreadId::from_ref("t1"), "fourth message")
                .await;
            let _req4 = ctrl.next_request().await;
            ctrl.send_tool_call("tc-2", "async_tool", serde_json::json!({}));
            ctrl.finish_with_usage(high_usage());

            // Send tool result before the compaction child finishes.
            running
                .send(
                    tool_result_input("t1", "tc-2", "second tool result").0,
                    "res-2",
                )
                .await;

            let req_c = ctrl.next_request().await;
            let compaction_child_req = if is_compaction_req(&req_c) {
                let r = req_c;
                handle_compaction_child(&mut ctrl, &r, "Summary of second round");
                let _req_d = ctrl.next_request().await;
                ctrl.send_text("processed second tool");
                ctrl.finish();
                r
            } else {
                ctrl.send_text("processed second tool");
                ctrl.finish();
                let req_d = ctrl.next_request().await;
                handle_compaction_child(&mut ctrl, &req_d, "Summary of second round");
                req_d
            };

            // The compaction child inherits everything before the pending
            // tool call ("fourth message" included, tc-2 excluded).
            insta::assert_json_snapshot!(
                "second_compaction_child_history",
                compaction_child_req.chat_history,
                {
                    "[].content[].id" => "[id]",
                    "[].content[].content[].text" => insta::dynamic_redaction(|value, _| {
                        let s = value.as_str().unwrap_or("");
                        if s.contains("compaction thread") {
                            insta::internals::Content::String("[compaction_instructions]".into())
                        } else {
                            value
                        }
                    })
                }
            );
        })
        .await;
}

/// Regression test for issue #31: compaction spawned inside a child thread
/// must account for ancestor messages when computing the safe spawn point;
/// otherwise the grandchild panics loading history.
#[tokio::test(flavor = "current_thread")]
async fn compaction_inside_child_thread_does_not_panic() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut _rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], Some(small_context_entry()));

            // ── Build root history ──
            running
                .send_user_text(ThreadId::from_ref("root"), "root message one")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("root response one");
            ctrl.finish();

            running
                .send_user_text(ThreadId::from_ref("root"), "root message two")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("root response two");
            ctrl.finish();

            // ── Spawn a child thread from root ──
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "do child work",
                    "child_of": ["root"]
                }),
            );
            ctrl.finish();

            // Parent loops back after the sync spawn — extract the child id.
            let parent_followup = ctrl.next_request().await;
            let child_thread_id = tool_result_texts(&parent_followup)
                .iter()
                .find_map(|t| {
                    let after =
                        t.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find child thread ID in spawn result");
            ctrl.send_text("ok, child spawned");
            ctrl.finish();

            // ── Child thread gets its first model call ──
            let _child_req = ctrl.next_request().await;
            ctrl.send_text("child first response");
            ctrl.finish();

            // ── Another round in the child: tool call + high usage triggers
            //    compaction inside the child thread. ──
            running
                .send_user_text(ThreadId::from_ref(&child_thread_id), "child follow-up")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-child", "async_tool", serde_json::json!({}));
            ctrl.finish_with_usage(high_usage());

            // Send tool result before compaction completes.
            running
                .send(
                    tool_result_input(&child_thread_id, "tc-child", "async tool result").0,
                    "res-child",
                )
                .await;

            // Two model requests arrive (compaction grandchild + child
            // processing the tool result) in scheduling-dependent order.
            let req_a = ctrl.next_request().await;
            let compaction_req = if is_compaction_req(&req_a) {
                handle_compaction_child(&mut ctrl, &req_a, "Summary of child work");
                let _req_b = ctrl.next_request().await;
                ctrl.send_text("processed tool result");
                ctrl.finish();
                req_a
            } else {
                ctrl.send_text("processed tool result");
                ctrl.finish();
                let req_b = ctrl.next_request().await;
                handle_compaction_child(&mut ctrl, &req_b, "Summary of child work");
                req_b
            };

            insta::assert_json_snapshot!(
                "issue31_compaction_child_history",
                compaction_req.chat_history,
                {
                    "[].content[].id" => "[id]",
                    "[].content[].content[].text" => insta::dynamic_redaction(|value, _| {
                        let s = value.as_str().unwrap_or("");
                        if s.contains("compaction thread") {
                            insta::internals::Content::String("[compaction_instructions]".into())
                        } else if s.contains("INSIDE the thread") {
                            insta::internals::Content::String("[spawn_instructions]".into())
                        } else {
                            value
                        }
                    })
                }
            );

            // ── After compaction applies, inspect the child's history ──
            running
                .send_user_text(
                    ThreadId::from_ref(&child_thread_id),
                    "message after compaction",
                )
                .await;
            let post_compaction_req =
                tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                    .await
                    .expect("child should respond after compaction");

            insta::assert_json_snapshot!(
                "issue31_post_compaction_history",
                post_compaction_req.chat_history,
                {
                    "[].content[].id" => "[id]",
                }
            );

            ctrl.send_text("done");
            ctrl.finish();
        })
        .await;
}

// ═══════════════════════════════════════════════════════════════════════
// Oversized-input / persistence-safety driver tests
// ═══════════════════════════════════════════════════════════════════════

/// The messages persisted for a thread, debug-formatted for asserts.
fn persisted(conv: &crate::stores::InMemoryConversationStore, thread_id: &str) -> String {
    format!(
        "{:?}",
        conv.thread_messages(ThreadId::from_ref(thread_id))
            .unwrap_or_default()
    )
}

/// Interrupting a request that produced no model output yet must not
/// persist the interrupted input — but the input stays in memory and is
/// re-sent (together with the interrupting message) on the next round.
/// Everything persists once the model produces output for it.
#[tokio::test(flavor = "current_thread")]
async fn interrupt_before_output_persists_nothing_until_model_output() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut rx, mut ctrl, conv) = start_system(vec![], None);

            // Message A: request goes out, model never answers.
            running
                .send_user_text(ThreadId::from_ref("t1"), "first message")
                .await;
            let _req1 = ctrl.next_request().await;

            // Message B interrupts before any output.
            running
                .send_user_text(ThreadId::from_ref("t1"), "second message")
                .await;
            let req2 = ctrl.next_request().await;

            // Nothing persisted yet: the model never validated A (it could
            // be the poison input blowing up the context window).
            let stored = persisted(&conv, "t1");
            assert!(
                !stored.contains("first message"),
                "interrupted input must not be persisted before model output; store: {stored}"
            );

            // But A is still in memory: the retry request carries A and B.
            let history = format!("{:?}", req2.chat_history);
            assert!(
                history.contains("first message") && history.contains("second message"),
                "the retry must include both inputs; history: {history}"
            );

            ctrl.send_text("model reply");
            ctrl.finish();
            collect_until_finished(&mut rx).await; // interrupted round 1
            collect_until_finished(&mut rx).await; // round 2
            wait_idle(&mut running).await;

            // Model output validated everything: now it all persists.
            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("first message")
                    && stored.contains("second message")
                    && stored.contains("model reply"),
                "validated inputs and output must be persisted; store: {stored}"
            );
        })
        .await;
}

/// End-to-end oversized tool result recovery: the result is replaced with a
/// placeholder and retried; if the placeholder still overflows, the step
/// errors out *without* persisting the poison result, keeping the call
/// answered in memory as "interrupted by user" — so a later user input just
/// works and the thread never hangs.
#[tokio::test(flavor = "current_thread")]
async fn oversized_tool_result_never_wedges_the_thread() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use infinity_provider_protocol::{CompletionError, ErrorClass};

            let (mut running, mut rx, mut ctrl, conv) =
                start_system(vec![Box::new(AsyncTool)], None);

            // Round 1: the model calls an async tool.
            running
                .send_user_text(ThreadId::from_ref("t1"), "use the tool")
                .await;
            let _req1 = ctrl.next_request().await;
            ctrl.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;

            // The tool result is enormous: the model reports an overflow.
            running
                .send(
                    tool_result_input("t1", "tc-1", "HUGE TOOL RESULT").0,
                    "res-1",
                )
                .await;
            let _req2 = ctrl.next_request().await;
            ctrl.send_error(CompletionError::provider(
                ErrorClass::ContextOverflow,
                "input is too long for the model",
            ));
            ctrl.drop_stream();

            // Core retries with the placeholder result...
            let req3 = tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                .await
                .expect("core should retry with a placeholder tool result");
            let history = format!("{:?}", req3.chat_history);
            assert!(
                !history.contains("HUGE TOOL RESULT"),
                "retry must not resend the oversized result; history: {history}"
            );
            assert!(
                history.contains(crate::event_processor::TOOL_RESULT_TOO_LARGE_PLACEHOLDER),
                "retry must answer the call with the placeholder; history: {history}"
            );

            // ...which still overflows: the step gives up.
            ctrl.send_error(CompletionError::provider(
                ErrorClass::ContextOverflow,
                "input is too long for the model",
            ));
            ctrl.drop_stream();
            collect_until_finished(&mut rx).await;

            // The poison result must not have been persisted.
            let stored = persisted(&conv, "t1");
            assert!(
                !stored.contains("HUGE TOOL RESULT"),
                "the oversized result must not be persisted; store: {stored}"
            );

            // A later user input settles the stranded call and the thread
            // keeps working.
            running
                .send_user_text(ThreadId::from_ref("t1"), "are you alive?")
                .await;
            let req4 = tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                .await
                .expect("a later user input must reach the model (no permanent hang)");
            let history = format!("{:?}", req4.chat_history);
            assert!(
                history.contains(crate::event_processor::TOOL_CALL_INTERRUPTED_TEXT),
                "the call must stay answered (settled as interrupted); history: {history}"
            );
            assert!(
                !history.contains(crate::event_processor::TOOL_RESULT_TOO_LARGE_PLACEHOLDER),
                "a 'too large' note would mislead the model; history: {history}"
            );
            assert!(
                !history.contains("HUGE TOOL RESULT"),
                "the poison result must be gone from the model-facing history"
            );
            ctrl.send_text("alive and well");
            ctrl.finish();
            let texts = collect_until_finished(&mut rx).await;
            assert_eq!(texts, vec!["alive and well"]);
            wait_idle(&mut running).await;
        })
        .await;
}

/// An orderly shutdown retains the accepted result. Reload must preserve it
/// without adding a synthetic interruption, and later mail must flow.
#[tokio::test(flavor = "current_thread")]
async fn reloaded_session_preserves_shutdown_settled_tool_result() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use super::AgentSystemBuilder;
            use crate::stores::{InMemoryConversationStore, InMemoryStateStore};

            let conv = InMemoryConversationStore::new();
            let state = InMemoryStateStore::new();

            // Shut down before provider validation.
            let (model1, mut ctrl1) = model_source(None);
            let (tx1, mut rx1) = mpsc::unbounded_channel();
            let running1 = AgentSystemBuilder::new_local(conv.clone(), state.clone(), model1)
                .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
                .start_with_observer(move |_thread_id| TestObserver { tx: tx1.clone() });
            running1
                .send_user_text(ThreadId::from_ref("t1"), "do something")
                .await;
            let _req1 = ctrl1.next_request().await;
            ctrl1.send_tool_call("tc-1", "async_tool", serde_json::json!({}));
            ctrl1.finish();
            collect_until_finished(&mut rx1).await;

            running1
                .send(
                    tool_result_input("t1", "tc-1", "HUGE TOOL RESULT").0,
                    "res-1",
                )
                .await;
            let _req2 = ctrl1.next_request().await;
            running1.shutdown().await;

            // The accepted result settles before driver teardown.
            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("tc-1") && stored.contains("HUGE TOOL RESULT"),
                "shutdown must retain the accepted result; store: {stored}"
            );

            // ── Session 2: boots from the same stores. ──
            let (model2, mut ctrl2) = model_source(None);
            let (tx2, mut rx2) = mpsc::unbounded_channel();
            let mut running2 = AgentSystemBuilder::new_local(conv.clone(), state.clone(), model2)
                .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
                .start_with_observer(move |_thread_id| TestObserver { tx: tx2.clone() });
            running2
                .send_user_text(ThreadId::from_ref("t1"), "hello again")
                .await;

            // Reload sees the settled real result before processing new mail.
            let req = ctrl2.next_request().await;
            let history = format!("{:?}", req.chat_history);
            assert!(
                history.contains("HUGE TOOL RESULT") && !history.contains("interrupted:"),
                "reload must preserve the real result; history: {history}"
            );
            assert!(history.contains("hello again"), "history: {history}");

            ctrl2.send_text("back online");
            ctrl2.finish();
            let texts = collect_until_finished(&mut rx2).await;
            assert_eq!(texts, vec!["back online"]);
            wait_idle(&mut running2).await;

            // Everything from the recovered round is validated + persisted.
            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("HUGE TOOL RESULT")
                    && stored.contains("hello again")
                    && stored.contains("back online"),
                "recovered round must persist; store: {stored}"
            );
        })
        .await;
}

/// Variant of the interrupt test where the retry round overflows the
/// context. User text has no safe placeholder substitute, so the only
/// possible reaction is to drop the pending inputs — and when inputs are
/// dropped the step must *stop* (no blind retry: the user clearly wants to
/// say something, and silently re-running without their words would be
/// worse). The thread stays usable for the next input.
#[tokio::test(flavor = "current_thread")]
async fn overflow_after_interrupt_drops_user_inputs_and_stops() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use infinity_provider_protocol::{CompletionError, ErrorClass};

            let (mut running, mut rx, mut ctrl, conv) = start_system(vec![], None);

            // Message A: request goes out, model never answers.
            running
                .send_user_text(ThreadId::from_ref("t1"), "first message")
                .await;
            let _req1 = ctrl.next_request().await;

            // Message B interrupts; the retry carries A and B...
            running
                .send_user_text(ThreadId::from_ref("t1"), "second message")
                .await;
            let req2 = ctrl.next_request().await;
            let history = format!("{:?}", req2.chat_history);
            assert!(history.contains("first message") && history.contains("second message"));

            // ...and overflows the context.
            ctrl.send_error(CompletionError::provider(
                ErrorClass::ContextOverflow,
                "input is too long for the model",
            ));
            ctrl.drop_stream();

            // The step surfaces the drop and stops — dropping inputs must
            // never be followed by a silent retry. (The first
            // CompletionFinished belongs to the interrupted round; the
            // overflow happens in the second.)
            let mut saw_drop_info = false;
            let mut finished_rounds = 0;
            while finished_rounds < 2 {
                match next_evt(&mut rx).await {
                    Evt::E(AgentEvent::Info { text }) if text.contains("too large") => {
                        saw_drop_info = true;
                    }
                    Evt::E(AgentEvent::CompletionFinished { .. }) => finished_rounds += 1,
                    _ => {}
                }
            }
            assert!(
                saw_drop_info,
                "the dropped input must be surfaced to the user"
            );
            wait_idle(&mut running).await;
            assert!(
                ctrl.try_next_request().is_none(),
                "dropping user input must stop the round, not retry it"
            );

            // Nothing was persisted.
            let stored = persisted(&conv, "t1");
            assert!(
                !stored.contains("first message") && !stored.contains("second message"),
                "dropped inputs must not be persisted; store: {stored}"
            );

            // The thread is not wedged: a fresh (smaller) message works, and
            // the dropped inputs are gone from the model-facing history too.
            running
                .send_user_text(ThreadId::from_ref("t1"), "third message")
                .await;
            let req3 = ctrl.next_request().await;
            let history = format!("{:?}", req3.chat_history);
            assert!(
                history.contains("third message")
                    && !history.contains("first message")
                    && !history.contains("second message"),
                "dropped inputs must not be resent; history: {history}"
            );
            ctrl.send_text("hello!");
            ctrl.finish();
            let texts = collect_until_finished(&mut rx).await;
            assert_eq!(texts, vec!["hello!"]);
            wait_idle(&mut running).await;

            let stored = persisted(&conv, "t1");
            assert!(
                stored.contains("third message") && stored.contains("hello!"),
                "the recovered round must persist; store: {stored}"
            );
        })
        .await;
}

/// A `fresh_context: true` spawn creates a child that inherits no parent
/// history: its first model request contains only the synthetic spawn call
/// and the self-contained instructions, none of the parent's messages.
#[tokio::test(flavor = "current_thread")]
async fn fresh_context_spawn_has_no_parent_history() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut _rx, mut ctrl, _conv) = start_system(vec![], None);

            // Build root history the child must NOT see.
            running
                .send_user_text(ThreadId::from_ref("root"), "root secret message")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("root response");
            ctrl.finish();

            // Spawn a fresh-context child.
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a fresh child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn-fresh",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "summarize the file docs/overview.md",
                    "child_of": ["root"],
                    "fresh_context": true
                }),
            );
            ctrl.finish();

            // Parent loops back after the sync spawn.
            let parent_followup = ctrl.next_request().await;
            assert!(
                tool_result_texts(&parent_followup)
                    .iter()
                    .any(|t| t.contains("Child thread is successfully spawned")),
                "spawn result present in parent follow-up"
            );
            ctrl.send_text("ok, spawned");
            ctrl.finish();

            // The child's first model request: no parent content, only the
            // synthetic spawn call and the fresh instructions.
            let child_req = ctrl.next_request().await;
            let history_json =
                serde_json::to_string(&child_req.chat_history).expect("serialize history");
            assert!(
                !history_json.contains("root secret message"),
                "fresh child must not inherit parent history: {history_json}"
            );
            assert!(
                history_json.contains("summarize the file docs/overview.md"),
                "fresh child sees its instructions: {history_json}"
            );
            assert!(
                history_json.contains("FRESH context"),
                "fresh child gets the fresh-context preamble: {history_json}"
            );
            ctrl.send_text("child done");
            ctrl.finish();
        })
        .await;
}

/// An inheriting grandchild sees its fresh parent's context, but nothing
/// from before the fresh-context boundary: t1 → fresh t2 → inheriting t3.
#[tokio::test(flavor = "current_thread")]
async fn nested_spawn_inherits_only_fresh_parent_context() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut _rx, mut ctrl, _conv) = start_system(vec![], None);

            running
                .send_user_text(ThreadId::from_ref("t1"), "t1 private user message")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_text("t1 private assistant response");
            ctrl.finish();
            wait_idle(&mut running).await;

            running
                .send_user_text(ThreadId::from_ref("t1"), "spawn a fresh child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn-t2",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "t2 independent task",
                    "child_of": ["t1"],
                    "fresh_context": true
                }),
            );
            ctrl.finish();

            let root_followup = ctrl.next_request().await;
            let t2 = tool_result_texts(&root_followup)
                .iter()
                .find_map(|text| {
                    let after =
                        text.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find t2 ID in spawn result");
            ctrl.send_text("t1 spawned t2");
            ctrl.finish();

            let _child_req = ctrl.next_request().await;
            ctrl.send_text("t2 findings to share with descendants");
            ctrl.finish();
            wait_idle(&mut running).await;

            running
                .send_user_text(
                    ThreadId::from_ref(&t2),
                    "t2 follow-up: delegate the next step",
                )
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn-t3",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "t3 continue using t2 findings",
                    "child_of": ["t1", t2],
                    "fresh_context": false
                }),
            );
            ctrl.finish();

            let _child_followup = ctrl.next_request().await;
            ctrl.send_text("t2 spawned t3");
            ctrl.finish();

            let grandchild_req = ctrl.next_request().await;
            let history =
                serde_json::to_string(&grandchild_req.chat_history).expect("serialize t3 history");
            assert!(
                !history.contains("t1 private user message")
                    && !history.contains("t1 private assistant response"),
                "t3 must not inherit t1 context across the fresh boundary: {history}"
            );
            assert!(
                history.contains("t2 independent task")
                    && history.contains("t2 findings to share with descendants")
                    && history.contains("t2 follow-up: delegate the next step"),
                "t3 must inherit t2 instructions and conversation: {history}"
            );
            assert!(
                tool_result_texts(&grandchild_req)
                    .iter()
                    .any(|text| text.contains("t3 continue using t2 findings")),
                "t3 must receive its own spawn instructions: {history}"
            );
            ctrl.send_text("t3 done");
            ctrl.finish();
            wait_idle(&mut running).await;
        })
        .await;
}

/// Reproduction: a child thread parked on `sleep_until_event_or_input`
/// receives a parent message, but the provider fails the completion with a
/// fatal error (e.g. expired auth). Once the provider recovers, a later
/// parent message must still reach the child.
#[tokio::test(flavor = "current_thread")]
async fn parent_message_after_fatal_error_still_reaches_sleeping_child() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut _rx, mut ctrl, _conv) = start_system(vec![], None);

            // ── Spawn a child thread from the root ──
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "wait for my messages",
                    "child_of": ["root"]
                }),
            );
            ctrl.finish();

            // Parent loops back after the sync spawn — extract the child id.
            let parent_followup = ctrl.next_request().await;
            let child_thread_id = tool_result_texts(&parent_followup)
                .iter()
                .find_map(|t| {
                    let after =
                        t.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find child thread ID in spawn result");
            ctrl.send_text("ok, child spawned");
            ctrl.finish();

            // ── Child's first round: it goes to sleep ──
            let child_req = ctrl.next_request().await;
            assert!(
                format!("{:?}", child_req.chat_history).contains("INSIDE the thread"),
                "expected the child's seed round"
            );
            ctrl.send_tool_call(
                "tc-sleep",
                "sleep_until_event_or_input",
                serde_json::json!({}),
            );
            ctrl.finish();

            // ── Parent sends a message to the sleeping child ──
            running
                .send_user_text(ThreadId::from_ref("root"), "message the child")
                .await;
            let _parent_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-msg1",
                "send_message_to_child",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "message": "hello child"
                }),
            );
            ctrl.finish();

            // Two rounds follow in scheduling-dependent order: the parent's
            // follow-up (send_message_to_child tool result) and the child's
            // wake-up with the parent message. The child's round fails with a
            // FATAL provider error (auth).
            for _ in 0..2 {
                let req = ctrl.next_request().await;
                let history = format!("{:?}", req.chat_history);
                if history.contains("Message from parent thread: hello child") {
                    ctrl.send_error(infinity_provider_protocol::CompletionError::provider(
                        infinity_provider_protocol::ErrorClass::Fatal,
                        "auth expired",
                    ));
                    ctrl.drop_stream();
                } else {
                    ctrl.send_text("message sent");
                    ctrl.finish();
                }
            }
            // The child driver idles out after the failed round; the message
            // consumed by the fatal round is lost (it was never validated by
            // model output, so it was never persisted).
            wait_idle(&mut running).await;

            // ── Provider "recovers"; parent sends another message ──
            running
                .send_user_text(ThreadId::from_ref("root"), "message the child again")
                .await;
            let _parent_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-msg2",
                "send_message_to_child",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "message": "hello again"
                }),
            );
            ctrl.finish();

            // Again two rounds: the parent's follow-up and (hopefully) the
            // child's wake-up containing "hello again". Match on the
            // child-side text — the parent's own history also contains
            // "hello again" inside the send_message_to_child call args.
            let mut child_got_message = false;
            for _ in 0..2 {
                let req =
                    tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                        .await
                        .expect("timed out waiting for a model round after provider recovery");
                let history = format!("{:?}", req.chat_history);
                if history.contains("Message from parent thread: hello again") {
                    child_got_message = true;
                    ctrl.send_text("got it");
                    ctrl.finish();
                } else {
                    ctrl.send_text("message sent");
                    ctrl.finish();
                }
            }
            assert!(
                child_got_message,
                "the child must receive parent messages after the provider recovers"
            );
        })
        .await;
}

/// Known issue: the child is waiting on an *active* tool call when the parent
/// message arrives (so the message is deferred). The tool result then lands
/// together with the drained parent message, but the completion fails with a
/// fatal provider error: both inputs were consumed from the queue yet never
/// persisted. The store still ends with the dangling tool call, so after the
/// driver exits every later parent message defers forever against a tool
/// result that can never arrive again.
///
/// The accepted result must survive the provider failure. The unrelated
/// unvalidated parent message has no receipt and must not be promoted with it.
#[tokio::test(flavor = "current_thread")]
async fn parent_message_after_fatal_error_during_active_tool_call() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (mut running, mut _rx, mut ctrl, _conv) =
                start_system(vec![Box::new(AsyncTool)], None);

            // ── Spawn a child thread from the root ──
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "work then wait",
                    "child_of": ["root"]
                }),
            );
            ctrl.finish();

            let parent_followup = ctrl.next_request().await;
            let child_thread_id = tool_result_texts(&parent_followup)
                .iter()
                .find_map(|t| {
                    let after =
                        t.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find child thread ID in spawn result");
            ctrl.send_text("ok, child spawned");
            ctrl.finish();

            // ── Child's first round: it starts an async (active) tool call ──
            let _child_req = ctrl.next_request().await;
            ctrl.send_tool_call("tc-work", "async_tool", serde_json::json!({}));
            ctrl.finish();

            // ── Parent messages the child while tc-work is pending: deferred ──
            running
                .send_user_text(ThreadId::from_ref("root"), "message the child")
                .await;
            let _parent_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-msg1",
                "send_message_to_child",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "message": "hello child"
                }),
            );
            ctrl.finish();
            // Parent's follow-up round after the tool result.
            let parent_req = ctrl.next_request().await;
            assert!(
                format!("{:?}", parent_req.chat_history).contains("Message sent to child thread"),
                "expected the parent's follow-up round"
            );
            ctrl.send_text("message sent");
            ctrl.finish();

            // ── The async tool result arrives; the batch settles the pending
            //    call and drains the deferred parent message. This completion
            //    fails with a FATAL provider error (auth). ──
            running
                .send(
                    tool_result_input(&child_thread_id, "tc-work", "work finished").0,
                    "res-work",
                )
                .await;
            let child_req = ctrl.next_request().await;
            let history = format!("{:?}", child_req.chat_history);
            assert!(
                history.contains("work finished")
                    && history.contains("Message from parent thread: hello child"),
                "the child round must include the tool result and the drained parent message; got {history}"
            );
            ctrl.send_error(infinity_provider_protocol::CompletionError::provider(
                infinity_provider_protocol::ErrorClass::Fatal,
                "auth expired",
            ));
            ctrl.drop_stream();
            // The driver settles the accepted result before idling. The
            // unrelated unvalidated parent message has no durable receipt.
            wait_idle(&mut running).await;

            // ── Provider "recovers"; parent sends another message ──
            running
                .send_user_text(ThreadId::from_ref("root"), "message the child again")
                .await;
            let _parent_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-msg2",
                "send_message_to_child",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "message": "hello again"
                }),
            );
            ctrl.finish();

            // The parent's follow-up and (hopefully) the child's wake-up.
            // Match on the child-side text — the parent's own history also
            // contains "hello again" inside the send_message_to_child args.
            let mut child_got_message = false;
            for _ in 0..2 {
                let Ok(req) = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    ctrl.next_request(),
                )
                .await
                else {
                    break;
                };
                let history = format!("{:?}", req.chat_history);
                if history.contains("Message from parent thread: hello again") {
                    assert!(history.contains("work finished"), "accepted result must survive: {history}");
                    assert!(!history.contains("Message from parent thread: hello child"),
                        "unrelated unvalidated mail must not be promoted: {history}");
                    child_got_message = true;
                    ctrl.send_text("got it");
                    ctrl.finish();
                } else {
                    ctrl.send_text("message sent");
                    ctrl.finish();
                }
            }
            assert!(
                child_got_message,
                "the child must receive parent messages after the provider recovers"
            );
        })
        .await;
}

/// Regression: a sleeping parent must be woken by its child's close report
/// even after a compaction folded the child's `spawn_thread` tool call into
/// the summary. Compaction never deletes store rows, so the original call
/// must be found by unwinding past the compaction boundary instead of
/// dropping the report.
#[tokio::test(flavor = "current_thread")]
async fn child_close_report_wakes_sleeping_parent_after_compaction() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![], None);

            // ── Parent spawns a child, then goes to sleep ──
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "work, then close when asked",
                    "child_of": ["root"]
                }),
            );
            ctrl.finish();

            let parent_followup = ctrl.next_request().await;
            let child_thread_id = tool_result_texts(&parent_followup)
                .iter()
                .find_map(|t| {
                    let after =
                        t.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find child thread ID in spawn result");
            ctrl.send_tool_call(
                "tc-sleep",
                "sleep_until_event_or_input",
                serde_json::json!({}),
            );
            ctrl.finish();

            // ── Child's seed round: it acknowledges and idles ──
            let child_req = ctrl.next_request().await;
            assert!(
                format!("{:?}", child_req.chat_history).contains("INSIDE the thread"),
                "expected the child's seed round"
            );
            ctrl.send_text("child ready");
            ctrl.finish();

            // ── Manually trigger compaction on the sleeping parent ──
            running
                .send(
                    InputMessage {
                        content: InputMessageContent::User(UserContent::text("")),
                        group_id: "root".into(),
                        metadata: None,
                        synthetic: Some(SyntheticKind::Tagged(TaggedSyntheticKind::Compaction)),
                        display_as: None,
                        subscription: false,
                    },
                    "compact-1",
                )
                .await;
            let creq = ctrl.next_request().await;
            assert!(is_compaction_req(&creq), "expected the compaction child");
            handle_compaction_child(&mut ctrl, &creq, "parent spawned a child and went to sleep");

            // Wait until the parent has actually applied the compaction, so
            // the child's report arrives strictly after the spawn call was
            // folded into the summary.
            loop {
                if let Evt::E(AgentEvent::CompactionApplied) = next_evt(&mut rx).await {
                    break;
                }
            }

            // ── The child closes with a report ──
            running
                .send_user_text(ThreadId::from_ref(&child_thread_id), "close now")
                .await;
            let _child_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-close",
                "close_thread",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "report_to_parent": "child finished its work",
                }),
            );
            ctrl.finish();

            // ── The sleeping parent must be woken by the close report ──
            let parent_req =
                tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                    .await
                    .expect("the sleeping parent must be woken by the child's close report");
            let history = format!("{:?}", parent_req.chat_history);
            assert!(
                history.contains("child finished its work"),
                "the parent's wake-up round must include the child's report; got {history}"
            );
            assert!(
                history.contains("[Compacted conversation summary]"),
                "the parent's history must be the compacted view; got {history}"
            );
            ctrl.send_text("resumed");
            ctrl.finish();
        })
        .await;
}

/// Mirror case: a parent message must reach a child whose *own* compaction
/// folded the inherited `spawn_thread` call into its summary. The spawn call
/// lives in the parent's store (the child only ever saw it through its
/// inherited ancestor slice), so resolving it requires scanning ancestor
/// slices, not just this thread's own rows.
#[tokio::test(flavor = "current_thread")]
async fn parent_message_reaches_child_after_child_compaction() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (running, mut rx, mut ctrl, _conv) = start_system(vec![], None);

            // ── Parent spawns a child ──
            running
                .send_user_text(ThreadId::from_ref("root"), "spawn a child")
                .await;
            let _req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-spawn",
                "spawn_thread",
                serde_json::json!({
                    "instructions": "wait for my messages",
                    "child_of": ["root"]
                }),
            );
            ctrl.finish();

            let parent_followup = ctrl.next_request().await;
            let child_thread_id = tool_result_texts(&parent_followup)
                .iter()
                .find_map(|t| {
                    let after =
                        t.strip_prefix("Child thread is successfully spawned and has ID: ")?;
                    Some(after.split('.').next()?.to_owned())
                })
                .expect("should find child thread ID in spawn result");
            ctrl.send_text("ok, child spawned");
            ctrl.finish();

            // ── Child's seed round ──
            let _child_req = ctrl.next_request().await;
            ctrl.send_text("child ready");
            ctrl.finish();

            // ── Compact the CHILD: its inherited ancestor prefix (including
            //    the spawn call) is folded into the summary ──
            running
                .send(
                    InputMessage {
                        content: InputMessageContent::User(UserContent::text("")),
                        group_id: child_thread_id.as_str().into(),
                        metadata: None,
                        synthetic: Some(SyntheticKind::Tagged(TaggedSyntheticKind::Compaction)),
                        display_as: None,
                        subscription: false,
                    },
                    "compact-child-1",
                )
                .await;
            let creq = ctrl.next_request().await;
            assert!(is_compaction_req(&creq), "expected the compaction child");
            handle_compaction_child(&mut ctrl, &creq, "child was spawned and is waiting");
            loop {
                if let Evt::E(AgentEvent::CompactionApplied) = next_evt(&mut rx).await {
                    break;
                }
            }

            // ── Parent messages the compacted child ──
            running
                .send_user_text(ThreadId::from_ref("root"), "message the child")
                .await;
            let _parent_req = ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-msg",
                "send_message_to_child",
                serde_json::json!({
                    "thread_id": child_thread_id,
                    "message": "hello child"
                }),
            );
            ctrl.finish();

            // Two rounds in scheduling-dependent order: the parent's
            // follow-up and the child's wake-up. Match on the child-side
            // text — the parent's history also contains "hello child" in the
            // send_message_to_child call args.
            let mut child_got_message = false;
            for _ in 0..2 {
                let req =
                    tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                        .await
                        .expect("the compacted child must receive the parent's message");
                let history = format!("{:?}", req.chat_history);
                if history.contains("Message from parent thread: hello child") {
                    assert!(
                        history.contains("[Compacted conversation summary]"),
                        "the child's history must be the compacted view; got {history}"
                    );
                    child_got_message = true;
                    ctrl.send_text("got it");
                    ctrl.finish();
                } else {
                    ctrl.send_text("message sent");
                    ctrl.finish();
                }
            }
            assert!(
                child_got_message,
                "the parent's message must reach the child after the child compacts"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn event_before_result_in_batch_preserves_real_result() {
    let (model, mut ctrl) = model_source(None);
    let conv = crate::stores::InMemoryConversationStore::new();
    let state = crate::stores::InMemoryStateStore::new();
    let mut system =
        super::AgentSystemBuilder::new(conv, state, model, ChannelSender::new_pair().0)
            .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
            .build();
    let observer = super::EventCollector::new();
    let mut defer = super::InMemoryDeferQueue::new();
    let first = system.step(vec![user_text_input("t1", "work")], &observer, &mut defer);
    let respond = async {
        ctrl.next_request().await;
        ctrl.send_tool_call("tc-work", "async_tool", serde_json::json!({}));
        ctrl.finish();
    };
    let (result, ()) = tokio::join!(first, respond);
    result.expect("first step");
    let second = system.step(
        vec![
            subscription_event_input("t1", "tc-work", "event data"),
            tool_result_input("t1", "tc-work", "real result"),
        ],
        &observer,
        &mut defer,
    );
    let respond = async {
        let req = ctrl.next_request().await;
        let texts = tool_result_texts(&req);
        assert_eq!(texts, vec!["real result", "event data"]);
        ctrl.send_text("done");
        ctrl.finish();
    };
    let (result, ()) = tokio::join!(second, respond);
    result.expect("second step");
}

#[tokio::test(flavor = "current_thread")]
async fn peer_deferred_until_real_result_and_peer_first_batch_settles() {
    for same_batch in [false, true] {
        let (model, mut ctrl) = model_source(None);
        let conv = crate::stores::InMemoryConversationStore::new();
        let mut system = super::AgentSystemBuilder::new(
            conv,
            crate::stores::InMemoryStateStore::new(),
            model,
            ChannelSender::new_pair().0,
        )
        .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
        .build();
        let observer = super::EventCollector::new();
        let mut defer = super::InMemoryDeferQueue::new();
        let first = system.step(vec![user_text_input("t1", "work")], &observer, &mut defer);
        let respond = async {
            ctrl.next_request().await;
            ctrl.send_tool_call("tc-work", "async_tool", serde_json::json!({}));
            ctrl.finish();
        };
        let (result, ()) = tokio::join!(first, respond);
        result.expect("first step");
        observer.take();
        let peer = peer_input("t1", "mail-1", "peer payload");
        let mut batch = vec![];
        if same_batch {
            batch.push(peer);
        } else {
            system
                .step(vec![peer], &observer, &mut defer)
                .await
                .expect("defer peer");
            assert!(
                ctrl.try_next_request().is_none(),
                "peer must wait for real result"
            );
        }
        assert!(
            observer.take().iter().all(|(_, event)| !matches!(
                event,
                AgentEvent::InputAdmitted { .. } | AgentEvent::InputCommitted { .. }
            )),
            "deferred mail is neither admitted nor committed"
        );
        batch.push(tool_result_input("t1", "tc-work", "real result"));
        let second = system.step(batch, &observer, &mut defer);
        let respond = async {
            let req = ctrl.next_request().await;
            let events = observer.take();
            assert!(
                events.iter().any(|(_, event)| matches!(event,
                AgentEvent::InputAdmitted { dedup_id } if dedup_id == "peer-mail-1")),
                "mail is admitted when it reaches the provider"
            );
            assert!(
                !events.iter().any(|(_, event)| matches!(event,
                AgentEvent::InputCommitted { dedup_id } if dedup_id == "peer-mail-1")),
                "admission is not a durable receipt"
            );
            assert_eq!(tool_result_texts(&req), vec!["real result", "peer payload"]);
            let injected = req
                .chat_history
                .iter()
                .find_map(|m| {
                    if let infinity_provider_protocol::message::Message::Assistant { content } = m {
                        content.iter().find_map(|c| match c {
                            infinity_provider_protocol::message::AssistantContent::ToolCall(
                                call,
                            ) if call.function.name == "receive_event__injected" => Some(call),
                            _ => None,
                        })
                    } else {
                        None
                    }
                })
                .expect("injected peer call");
            assert_eq!(injected.id, "peer-mail-1");
            assert_eq!(injected.function.arguments["from"], "@planner");
            ctrl.send_text("done");
            ctrl.finish();
        };
        let (result, ()) = tokio::join!(second, respond);
        result.expect("second step");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn peer_wakes_passive_sleep() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (running, mut rx, mut ctrl, _) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "sleep")
                .await;
            ctrl.next_request().await;
            ctrl.send_tool_call(
                "tc-sleep",
                "sleep_until_event_or_input",
                serde_json::json!({}),
            );
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            let (peer, id) = peer_input("t1", "wake", "wake up");
            running.send(peer, &id).await;
            let req = tokio::time::timeout(std::time::Duration::from_secs(5), ctrl.next_request())
                .await
                .expect("peer must wake passive sleep");
            assert!(tool_result_texts(&req).iter().any(|s| s == "wake up"));
            ctrl.send_text("awake");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn peer_redelivery_committed_once_in_history_after_reload() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (mut running, mut rx, mut ctrl, conv) = start_system(vec![], None);
            running
                .send_user_text(ThreadId::from_ref("t1"), "hello")
                .await;
            ctrl.next_request().await;
            ctrl.send_text("hi");
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            let (peer, id) = peer_input("t1", "redelivered", "unique peer payload");
            running.send(peer.clone(), &id).await;
            let req = ctrl.next_request().await;
            assert_eq!(tool_result_texts(&req), vec!["unique peer payload"]);
            ctrl.send_text("received");
            ctrl.finish();
            loop {
                if let Evt::E(AgentEvent::InputCommitted { dedup_id }) = next_evt(&mut rx).await {
                    assert_eq!(dedup_id, id);
                    break;
                }
            }
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
            running.send(peer, &id).await;
            loop {
                if let Evt::E(AgentEvent::InputCommitted { dedup_id }) = next_evt(&mut rx).await {
                    assert_eq!(dedup_id, id);
                    break;
                }
            }
            wait_idle(&mut running).await;
            assert!(
                ctrl.try_next_request().is_none(),
                "durable duplicate must not run inference"
            );
            let stored = conv
                .thread_messages(ThreadId::from_ref("t1"))
                .expect("stored history");
            let peers: Vec<_> = stored
                .iter()
                .filter(|(m, _)| {
                    matches!(m, crate::message::InfinityMessage::SubscriptionEvent { .. })
                })
                .collect();
            assert_eq!(peers.len(), 1);
            assert!(format!("{peers:?}").contains("peer-redelivered"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn input_committed_observer_sees_persisted_input_only_after_validation() {
    struct DurableObserver {
        conv: crate::stores::InMemoryConversationStore,
        events: super::EventCollector,
    }
    #[async_trait(?Send)]
    impl super::ThreadObserver for DurableObserver {
        type SubscribeRequest = ();
        fn on_event(&self, thread_id: &ThreadId<str>, event: &AgentEvent) {
            if let AgentEvent::InputCommitted { dedup_id } = event
                && dedup_id == "peer-durable"
            {
                assert!(persisted(&self.conv, thread_id.as_str()).contains("durable payload"));
            }
            super::ThreadObserver::on_event(&self.events, thread_id, event);
        }
    }
    let (model, mut ctrl) = model_source(None);
    let conv = crate::stores::InMemoryConversationStore::new();
    let observer = DurableObserver {
        conv: conv.clone(),
        events: super::EventCollector::new(),
    };
    let system = super::AgentSystemBuilder::new(
        conv.clone(),
        crate::stores::InMemoryStateStore::new(),
        model,
        ChannelSender::new_pair().0,
    )
    .build();
    let thread = super::thread::Thread::load(system.inner.clone(), "t1".into())
        .await
        .expect("load thread");
    let (_cancel1, cancel1) = tokio::sync::oneshot::channel();
    let peer = peer_input("t1", "durable", "durable payload");
    let failed = thread.step_no_defer(vec![peer.clone()], &observer, cancel1);
    let respond = async {
        ctrl.next_request().await;
        assert!(!persisted(&conv, "t1").contains("durable payload"));
        ctrl.send_error(infinity_provider_protocol::CompletionError::provider(
            infinity_provider_protocol::ErrorClass::Fatal,
            "inference failed",
        ));
    };
    let (result, ()) = tokio::join!(failed, respond);
    result.expect("failed completion is reported as an event");
    let events = observer.events.take();
    assert!(
        events
            .iter()
            .any(|(_, e)| matches!(e, AgentEvent::CompletionFinished { .. }))
    );
    assert!(
        !events
            .iter()
            .any(|(_, e)| matches!(e, AgentEvent::InputCommitted { .. }))
    );
    assert!(!persisted(&conv, "t1").contains("durable payload"));
    let (_cancel2, cancel2) = tokio::sync::oneshot::channel();
    thread
        .step_no_defer(vec![peer], &observer, cancel2)
        .await
        .expect("unvalidated duplicate");
    assert!(
        !observer
            .events
            .take()
            .iter()
            .any(|(_, e)| matches!(e, AgentEvent::InputCommitted { .. }))
    );
    let (_cancel3, cancel3) = tokio::sync::oneshot::channel();
    let retry = thread.step_no_defer(vec![user_text_input("t1", "retry")], &observer, cancel3);
    let respond = async {
        ctrl.next_request().await;
        ctrl.send_text("validated");
        ctrl.finish();
    };
    let (result, ()) = tokio::join!(retry, respond);
    result.expect("validated completion");
    assert!(observer.events.take().iter().any(
        |(_, e)| matches!(e, AgentEvent::InputCommitted { dedup_id } if dedup_id == "peer-durable")
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn routed_interrupt_cancels_step_without_input_or_phantom_driver() {
    for with_output in [false, true] {
        tokio::task::LocalSet::new()
            .run_until(async {
                let (mut running, mut rx, mut ctrl, conv) = start_system(vec![], None);
                let interrupt = running.interrupt_handle();
                running
                    .send_user_text(ThreadId::from_ref("t1"), "original input")
                    .await;
                ctrl.next_request().await;
                if with_output {
                    ctrl.send_text("partial output");
                    loop {
                        if let Evt::E(AgentEvent::TextChunk { .. }) = next_evt(&mut rx).await {
                            break;
                        }
                    }
                }
                assert!(interrupt.interrupt(ThreadId::from_ref("t1")));
                let mut committed = false;
                loop {
                    match next_evt(&mut rx).await {
                        Evt::E(AgentEvent::InputCommitted { .. }) => committed = true,
                        Evt::E(AgentEvent::CompletionFinished { .. }) => break,
                        _ => {}
                    }
                }
                assert_eq!(committed, with_output);
                wait_idle(&mut running).await;
                assert_eq!(
                    persisted(&conv, "t1").contains("partial output"),
                    with_output
                );
                assert!(!persisted(&conv, "t1").contains("<interrupt>"));
                assert!(
                    ctrl.try_next_request().is_none(),
                    "interrupt must not start another step"
                );
                assert!(interrupt.interrupt(ThreadId::from_ref("ghost")));
                running
                    .send_user_text(ThreadId::from_ref("t2"), "next input")
                    .await;
                ctrl.next_request().await;
                assert!(conv.thread_info(ThreadId::from_ref("ghost")).is_none());
                ctrl.send_text("done");
                ctrl.finish();
                collect_until_finished(&mut rx).await;
                wait_idle(&mut running).await;
            })
            .await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn overflowing_peer_is_never_replaced_or_receipted_and_can_be_reoffered() {
    for mixed_tail in [false, true] {
        let (model, mut ctrl) = model_source(None);
        let conv = crate::stores::InMemoryConversationStore::new();
        let system = super::AgentSystemBuilder::new(
            conv.clone(),
            crate::stores::InMemoryStateStore::new(),
            model,
            ChannelSender::new_pair().0,
        )
        .build();
        let thread = super::thread::Thread::load(system.inner.clone(), "t1".into())
            .await
            .expect("load thread");
        let observer = super::EventCollector::new();
        let peer = peer_input("t1", "overflow", "full peer payload");
        let mut batch = vec![peer.clone()];
        if mixed_tail {
            batch.push(user_text_input("t1", "other oversized input"));
        }
        let (_cancel, cancel) = tokio::sync::oneshot::channel();
        let step = thread.step_no_defer(batch, &observer, cancel);
        tokio::pin!(step);
        let respond = async {
            ctrl.next_request().await;
            ctrl.send_error(infinity_provider_protocol::CompletionError::provider(
                infinity_provider_protocol::ErrorClass::ContextOverflow,
                "context overflow",
            ));
        };
        tokio::select! {
            result = &mut step => panic!("step ended before overflow response: {result:?}"),
            () = respond => {}
        }
        tokio::select! {
            result = &mut step => { result.expect("overflow is reported as an event"); }
            _ = ctrl.next_request() => {
                ctrl.send_text("accepted replacement"); ctrl.finish();
                step.await.expect("replacement completion");
            }
        }
        assert!(
            !observer.take().iter().any(|(_, event)| matches!(event,
            AgentEvent::InputCommitted { dedup_id } if dedup_id == "peer-overflow")),
            "overflow must not receipt a peer whose full payload was not stored"
        );
        let stored = persisted(&conv, "t1");
        assert!(!stored.contains("full peer payload"));
        assert!(!stored.contains(crate::event_processor::TOOL_RESULT_TOO_LARGE_PLACEHOLDER));
        assert!(!stored.contains(crate::event_processor::TOOL_CALL_INTERRUPTED_TEXT));
        let (_cancel, cancel) = tokio::sync::oneshot::channel();
        let retry = thread.step_no_defer(vec![peer], &observer, cancel);
        let respond = async {
            let req = tokio::time::timeout(std::time::Duration::from_secs(2), ctrl.next_request())
                .await
                .expect("overflow must release peer dedup ID for re-offer");
            assert_eq!(tool_result_texts(&req), vec!["full peer payload"]);
            ctrl.send_text("received full payload");
            ctrl.finish();
        };
        let (result, ()) = tokio::join!(retry, respond);
        result.expect("re-offered peer completion");
        let stored = persisted(&conv, "t1");
        assert_eq!(stored.matches("full peer payload").count(), 1);
        assert!(!stored.contains(crate::event_processor::TOOL_RESULT_TOO_LARGE_PLACEHOLDER));
        assert!(!stored.contains(crate::event_processor::TOOL_CALL_INTERRUPTED_TEXT));
        assert!(observer.take().iter().any(|(_, event)| matches!(event,
            AgentEvent::InputCommitted { dedup_id } if dedup_id == "peer-overflow")));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn routed_interrupt_durably_settles_waiting_tool_and_choice() {
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use crate::traits::{ConversationStore, StateStore};
    tokio::task::LocalSet::new()
        .run_until(async {
            let (model, mut ctrl) = model_source(None);
            let conv = InMemoryConversationStore::new();
            let state = InMemoryStateStore::new();
            conv.ensure_root_thread(ThreadId::from_ref("t1"))
                .await
                .expect("root");
            let (tx, mut rx) = mpsc::unbounded_channel();
            let mut running =
                super::AgentSystemBuilder::new_local(conv.clone(), state.clone(), model)
                    .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
                    .start_with_observer(move |_| TestObserver { tx: tx.clone() });
            running
                .send_user_text(ThreadId::from_ref("t1"), "ask")
                .await;
            ctrl.next_request().await;
            ctrl.send_tool_call("waiting", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            running
                .send(
                    InputMessage {
                        content: InputMessageContent::UserChoice(UserChoiceRequired {
                            content_type: "user_choice_required".into(),
                            id: "waiting".into(),
                            call_id: None,
                            prompt: "Choose".into(),
                            choices: vec!["A".into()],
                            default: 0,
                            response_url: "http://example.test/choice".into(),
                        }),
                        group_id: "t1".into(),
                        metadata: None,
                        synthetic: None,
                        display_as: None,
                        subscription: false,
                    },
                    "choice",
                )
                .await;
            loop {
                if matches!(
                    next_evt(&mut rx).await,
                    Evt::E(AgentEvent::UserChoiceRequired { .. })
                ) {
                    break;
                }
            }
            assert!(
                running
                    .interrupt_handle()
                    .interrupt(ThreadId::from_ref("t1"))
            );
            loop {
                if matches!(
                    next_evt(&mut rx).await,
                    Evt::E(AgentEvent::UserChoiceDismissed { .. })
                ) {
                    break;
                }
            }
            assert!(
                state
                    .get_pending_user_choices(ThreadId::from_ref("t1"))
                    .await
                    .expect("choices")
                    .is_empty()
            );
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
            assert!(persisted(&conv, "t1").contains("Tool call interrupted by user"));
            let (replay_tx, mut replay_rx) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("t1"), replay_tx).await;
            let Evt::Replay(snapshot) = next_evt(&mut replay_rx).await else {
                panic!("replay")
            };
            assert!(snapshot.pending_choices.is_empty());
            assert!(!matches!(
                snapshot.history.last(),
                Some(crate::message::InfinityMessage::ToolCall { .. })
            ));
            assert!(
                ctrl.try_next_request().is_none(),
                "cancel must not start inference"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn r4_interrupt_with_unresponsive_rap_notification() {
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use crate::traits::{ConversationStore, StateStore};
    tokio::task::LocalSet::new()
        .run_until(async {
            let (model, mut ctrl) = model_source(None);
            let conv = InMemoryConversationStore::new();
            let state = InMemoryStateStore::new();
            conv.ensure_root_thread(ThreadId::from_ref("t1"))
                .await
                .expect("root");
            let (tx, mut rx) = mpsc::unbounded_channel();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let url = format!("http://{}", listener.local_addr().expect("address"));
            tokio::task::spawn_local(async move {
                use tokio::io::AsyncReadExt;
                let (mut stream, _) = listener.accept().await.expect("connection");
                let mut buffer = [0u8; 4096];
                let n = stream.read(&mut buffer).await.expect("request");
                eprintln!(
                    "R4 unresponsive RAP received {}",
                    String::from_utf8_lossy(&buffer[..n])
                );
                std::future::pending::<()>().await;
            });
            let mut running =
                super::AgentSystemBuilder::new_local(conv.clone(), state.clone(), model)
                    .tools(vec![Box::new(AsyncTool) as Box<dyn Tool<ChannelSender>>])
                    .rap_notifier(rap_client::notifier::RapNotifier::new(
                        vec![url],
                        rap_client::http::SimpleHttpClient::new(),
                    ))
                    .start_with_observer(move |_| TestObserver { tx: tx.clone() });
            running
                .send_user_text(ThreadId::from_ref("t1"), "ask")
                .await;
            ctrl.next_request().await;
            ctrl.send_tool_call("waiting", "async_tool", serde_json::json!({}));
            ctrl.finish();
            collect_until_finished(&mut rx).await;
            running
                .send(
                    InputMessage {
                        content: InputMessageContent::UserChoice(UserChoiceRequired {
                            content_type: "user_choice_required".into(),
                            id: "waiting".into(),
                            call_id: None,
                            prompt: "Choose".into(),
                            choices: vec!["A".into()],
                            default: 0,
                            response_url: "http://example.test/choice".into(),
                        }),
                        group_id: "t1".into(),
                        metadata: None,
                        synthetic: None,
                        display_as: None,
                        subscription: false,
                    },
                    "choice",
                )
                .await;
            loop {
                if matches!(
                    next_evt(&mut rx).await,
                    Evt::E(AgentEvent::UserChoiceRequired { .. })
                ) {
                    break;
                }
            }
            assert!(
                running
                    .interrupt_handle()
                    .interrupt(ThreadId::from_ref("t1"))
            );
            loop {
                if matches!(
                    next_evt(&mut rx).await,
                    Evt::E(AgentEvent::UserChoiceDismissed { .. })
                ) {
                    break;
                }
            }
            assert!(
                state
                    .get_pending_user_choices(ThreadId::from_ref("t1"))
                    .await
                    .expect("choices")
                    .is_empty()
            );
            collect_until_finished(&mut rx).await;
            wait_idle(&mut running).await;
            assert!(persisted(&conv, "t1").contains("Tool call interrupted by user"));
            let (replay_tx, mut replay_rx) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("t1"), replay_tx).await;
            let Evt::Replay(snapshot) = next_evt(&mut replay_rx).await else {
                panic!("replay")
            };
            assert!(snapshot.pending_choices.is_empty());
            assert!(!matches!(
                snapshot.history.last(),
                Some(crate::message::InfinityMessage::ToolCall { .. })
            ));
            assert!(
                ctrl.try_next_request().is_none(),
                "cancel must not start inference"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn human_deferrable_mail_waits_for_active_tool_and_receipts_its_dedup_id() {
    tokio::task::LocalSet::new().run_until(async {
        let (running, mut rx, mut ctrl, conv) = start_system(vec![Box::new(AsyncTool)], None);
        running.send_user_text(ThreadId::from_ref("t1"), "work").await;
        ctrl.next_request().await;
        ctrl.send_tool_call("active", "async_tool", serde_json::json!({}));
        ctrl.finish();
        collect_until_finished(&mut rx).await;
        let mut human = peer_input("t1", "human-mail", "Mail from the human operator");
        human.0.synthetic = Some(SyntheticKind::Tagged(TaggedSyntheticKind::PeerMessage { from: "human".into() }));
        running.send(human.0.clone(), &human.1).await;
        assert!(tokio::time::timeout(std::time::Duration::from_millis(30), ctrl.next_request()).await.is_err(),
            "deferrable human mail must wait for the real active-tool result");
        assert!(!persisted(&conv, "t1").contains("human operator"));
        let result = tool_result_input("t1", "active", "real active-tool result");
        running.send(result.0, &result.1).await;
        let request = ctrl.next_request().await;
        let history = format!("{:?}", request.chat_history);
        assert!(history.contains("real active-tool result") && history.contains("human operator"));
        assert!(!history.contains("interrupted"));
        ctrl.send_text("received human mail");
        ctrl.finish();
        loop {
            if matches!(next_evt(&mut rx).await, Evt::E(AgentEvent::InputCommitted { dedup_id }) if dedup_id == human.1) { break; }
        }
        collect_until_finished(&mut rx).await;
        running.send(human.0, &human.1).await;
        loop {
            if matches!(next_evt(&mut rx).await, Evt::E(AgentEvent::InputCommitted { dedup_id }) if dedup_id == human.1) { break; }
        }
        assert!(ctrl.try_next_request().is_none(), "durable human mail must dedup without inference");
        running.shutdown().await;
    }).await;
}

#[tokio::test(flavor = "current_thread")]
async fn r7_nonassociative_subscription_child_keeps_event() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for restart in [false, true] {
                let conv = crate::stores::InMemoryConversationStore::new();
                let state = crate::stores::InMemoryStateStore::new();
                let (model, mut ctrl) = model_source(None);
                let (tx, mut rx) = mpsc::unbounded_channel();
                let mut running =
                    super::AgentSystemBuilder::new_local(conv.clone(), state.clone(), model)
                        .tool(Box::new(SubscribeTool))
                        .start_with_observer(move |_| TestObserver { tx: tx.clone() });
                running
                    .send_user_text(ThreadId::from_ref("t1"), "subscribe")
                    .await;
                ctrl.next_request().await;
                ctrl.send_tool_call("tc-sub", "subscribe_tool", serde_json::json!({}));
                ctrl.finish();
                collect_until_finished(&mut rx).await;
                let subscribed = ctrl.next_request().await;
                assert!(
                    tool_result_texts(&subscribed)
                        .iter()
                        .any(|t| t == "subscribed")
                );
                ctrl.send_text("subscribed and ready");
                ctrl.finish();
                wait_idle(&mut running).await;
                while rx.try_recv().is_ok() {}
                let (mut event, id) =
                    subscription_event_input("t1", "tc-sub", "r7 unique subscription payload");
                event.synthetic = Some(SyntheticKind::Tagged(
                    TaggedSyntheticKind::SubscriptionEvent {
                        tool_call_id: "tc-sub".into(),
                        associative: false,
                        r#final: true,
                    },
                ));
                running.send(event, &id).await;
                let request = tokio::time::timeout(
                    std::time::Duration::from_millis(700),
                    ctrl.next_request(),
                )
                .await;
                let child = loop {
                    let event = tokio::time::timeout(
                        std::time::Duration::from_secs(1),
                        running.next_lifecycle_event(),
                    )
                    .await
                    .expect("child lifecycle")
                    .expect("lifecycle open");
                    if event.thread_id.as_str() != "t1" && event.state == ThreadLifecycleState::Live
                    {
                        break event.thread_id;
                    }
                };
                let request = request.expect(
                    "fresh subscription child must process its event without a daemon restart",
                );
                assert!(
                    tool_result_texts(&request)
                        .iter()
                        .any(|t| t.contains("r7 unique subscription payload"))
                );
                assert!(
                    !persisted(&conv, child.as_str()).contains("interrupted: daemon restarted")
                );
                if restart {
                    let before = persisted(&conv, child.as_str());
                    assert!(before.contains("receive_event__injected"));
                    assert!(
                        !before.contains("r7 unique subscription payload"),
                        "payload is still unvalidated"
                    );
                    running.shutdown().await;
                    let (model, mut ctrl) = model_source(None);
                    let (tx, _rx) = mpsc::unbounded_channel();
                    let running = super::AgentSystemBuilder::new_local(conv.clone(), state, model)
                        .start_with_observer(move |_| TestObserver { tx: tx.clone() });
                    let (tx, mut replay) = mpsc::unbounded_channel();
                    running.subscribe(&child, tx).await;
                    let Evt::Replay(snapshot) = next_evt(&mut replay).await else {
                        panic!("replay");
                    };
                    assert!(
                        snapshot.idle,
                        "accepted subscription payload settles before shutdown"
                    );
                    assert!(
                        persisted(&conv, child.as_str()).contains("r7 unique subscription payload")
                    );
                    assert!(ctrl.try_next_request().is_none());
                    running.shutdown().await;
                    continue;
                }
                ctrl.send_text("handled");
                ctrl.finish();
                collect_until_finished(&mut rx).await;
                let history = persisted(&conv, child.as_str());
                assert!(history.contains("r7 unique subscription payload"));
                assert!(!history.contains("interrupted: daemon restarted"));
                running.shutdown().await;
            }
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn unanswered_calls_remain_pending_on_load() {
    use crate::message::InfinityMessage;
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use crate::traits::ConversationStore;
    use infinity_provider_protocol::message::{ToolCall, ToolFunction};

    tokio::task::LocalSet::new()
        .run_until(async {
            for name in [
                "spawn_thread",
                "__harness_begin_compaction__",
                "receive_event__injected",
                "async_tool",
            ] {
                let conv = InMemoryConversationStore::new();
                let tid = ThreadId::from_ref("legacy");
                conv.ensure_root_thread(tid).await.expect("root");
                conv.append_messages(
                    tid,
                    vec![(
                        InfinityMessage::ToolCall {
                            call: ToolCall {
                                id: "unrecorded".into(),
                                call_id: None,
                                function: ToolFunction {
                                    name: name.into(),
                                    arguments: serde_json::json!({}),
                                },
                            },
                            display_as: None,
                        },
                        "legacy-entry".into(),
                    )],
                )
                .await
                .expect("legacy history");
                let before = persisted(&conv, tid.as_str());
                let (model, mut ctrl) = model_source(None);
                let (tx, _rx) = mpsc::unbounded_channel();
                let running = super::AgentSystemBuilder::new_local(
                    conv.clone(),
                    InMemoryStateStore::new(),
                    model,
                )
                .start_with_observer(move |_| TestObserver { tx: tx.clone() });
                let (tx, mut replay) = mpsc::unbounded_channel();
                running.subscribe(tid, tx).await;
                let Evt::Replay(snapshot) = next_evt(&mut replay).await else {
                    panic!("replay")
                };
                assert!(!snapshot.idle, "unrecorded {name} must stay pending");
                assert_eq!(persisted(&conv, tid.as_str()), before);
                assert!(ctrl.try_next_request().is_none());
                running.shutdown().await;
            }
        })
        .await;
}

mod accepted_result {
    use super::*;
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use crate::system::AgentSystemBuilder;
    use crate::traits::ConversationStore;
    use rap_protocol::ToolCallId;
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    struct ProbeTool {
        sync: bool,
        invoked: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Tool<ChannelSender> for ProbeTool {
        fn name(&self) -> &str {
            "probe"
        }
        fn description(&self) -> &str {
            "probe"
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }
        fn supports_sync(&self) -> bool {
            self.sync
        }
        async fn execute(
            &self,
            _: serde_json::Value,
            _: ToolCallId,
            _: Option<rap_protocol::ProviderCallId>,
            _: &ToolContext<ChannelSender>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            self.invoked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
        async fn execute_synchronous(
            &self,
            _: &serde_json::Value,
            id: &ToolCallId<str>,
            _: Option<&rap_protocol::ProviderCallId<str>>,
            _: &ToolContext<ChannelSender>,
        ) -> Option<infinity_provider_protocol::message::ToolResult> {
            self.invoked.fetch_add(1, Ordering::SeqCst);
            Some(infinity_provider_protocol::message::ToolResult {
                id: id.to_string(),
                call_id: None,
                content: vec![
                    infinity_provider_protocol::message::ToolResultContent::text("probe completed"),
                ],
            })
        }
    }

    async fn r9_result_then_provider_failure(sync: bool) {
        tokio::task::LocalSet::new().run_until(async {
            let conv = InMemoryConversationStore::new();
            conv.ensure_root_thread(ThreadId::from_ref("r9")).await.expect("root");
            let state = InMemoryStateStore::new();
            let invoked = Arc::new(AtomicUsize::new(0));
            let (model, mut ctrl) = model_source(None);
            let (tx, mut events) = mpsc::unbounded_channel();
            let running = AgentSystemBuilder::new_local(conv.clone(), state.clone(), model)
                .tool(Box::new(ProbeTool { sync, invoked: invoked.clone() }))
                .start_with_observer(move |_| TestObserver { tx: tx.clone() });
            let peer = peer_input("r9", "first", "first broker mail");
            running.send(peer.0, &peer.1).await;
            ctrl.next_request().await;
            ctrl.send_tool_call("completed-call", "probe", serde_json::json!({}));
            ctrl.finish();
            if !sync {
                collect_until_finished(&mut events).await;
                let unrelated = peer_input("r9", "unvalidated", "unrelated unvalidated mail");
                running.send(unrelated.0, &unrelated.1).await;
                let result = tool_result_input("r9", "completed-call", "probe completed");
                running.send(result.0, &result.1).await;
            }
            let request = ctrl.next_request().await;
            assert!(format!("{:?}", request.chat_history).contains("probe completed"));
            assert_eq!(invoked.load(Ordering::SeqCst), 1);
            ctrl.send_error(infinity_provider_protocol::CompletionError::provider(
                infinity_provider_protocol::ErrorClass::Fatal, "single provider failure"));
            ctrl.drop_stream();
            wait_idle(&mut events).await;
            let stored = persisted(&conv, "r9");
            let peer = peer_input("r9", "after", "human broker mail after recovery");
            running.send(peer.0, &peer.1).await;
            let request = tokio::time::timeout(Duration::from_secs(4), ctrl.next_request()).await
                .expect("completed tool must not permanently block later broker mail after one provider failure");
            assert!(format!("{:?}", request.chat_history).contains("human broker mail after recovery"));
            assert_eq!(stored.matches("probe completed").count(), 1);
            assert!(!stored.contains("interrupted:"));
            assert!(!stored.contains("unrelated unvalidated mail"));
            ctrl.send_text("recovered"); ctrl.finish();
            running.shutdown().await;
        }).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn r9_sync_result_provider_failure() {
        r9_result_then_provider_failure(true).await;
    }
    #[tokio::test(flavor = "current_thread")]
    async fn r9_async_result_provider_failure() {
        r9_result_then_provider_failure(false).await;
    }

    async fn wait_idle(events: &mut mpsc::UnboundedReceiver<Evt>) {
        loop {
            if matches!(next_evt(events).await, Evt::E(AgentEvent::ThreadIdle)) {
                break;
            }
        }
    }
}

mod held_settlement {
    use super::*;
    use crate::stores::{InMemoryConversationStore, InMemoryStateStore};
    use crate::system::UserChoice;
    use crate::traits::{ConversationStore, StateStore};
    use rap_protocol::{ChoiceId, ToolCallId};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    #[derive(Clone)]
    struct GateStore {
        inner: InMemoryStateStore,
        failed: Arc<AtomicBool>,
        attempts: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl StateStore for GateStore {
        type Error = crate::stores::InMemoryStoreError;
        async fn get_processed_ids(
            &self,
            thread_id: &ThreadId<str>,
        ) -> Result<std::collections::HashSet<String>, Self::Error> {
            self.inner.get_processed_ids(thread_id).await
        }
        async fn add_processed_message_ids(
            &self,
            thread_id: &ThreadId<str>,
            message_ids: Vec<String>,
        ) -> Result<(), Self::Error> {
            self.inner
                .add_processed_message_ids(thread_id, message_ids)
                .await
        }
        async fn get_metadata(
            &self,
            root_thread_id: &ThreadId<str>,
        ) -> Result<Option<serde_json::Value>, Self::Error> {
            self.inner.get_metadata(root_thread_id).await
        }
        async fn set_metadata(
            &self,
            root_thread_id: &ThreadId<str>,
            metadata: serde_json::Value,
        ) -> Result<(), Self::Error> {
            self.inner.set_metadata(root_thread_id, metadata).await
        }
        async fn get_active_subscriptions(
            &self,
            thread_id: &ThreadId<str>,
        ) -> Result<Vec<ToolCallId>, Self::Error> {
            self.inner.get_active_subscriptions(thread_id).await
        }
        async fn add_active_subscription(
            &self,
            thread_id: &ThreadId<str>,
            tool_call_id: &ToolCallId<str>,
        ) -> Result<(), Self::Error> {
            self.inner
                .add_active_subscription(thread_id, tool_call_id)
                .await
        }
        async fn remove_active_subscription(
            &self,
            thread_id: &ThreadId<str>,
            tool_call_id: &ToolCallId<str>,
        ) -> Result<(), Self::Error> {
            self.inner
                .remove_active_subscription(thread_id, tool_call_id)
                .await
        }
        async fn add_pending_user_choice(
            &self,
            thread_id: &ThreadId<str>,
            choice: UserChoice,
        ) -> Result<(), Self::Error> {
            self.inner.add_pending_user_choice(thread_id, choice).await
        }
        async fn remove_pending_user_choice(
            &self,
            thread_id: &ThreadId<str>,
            choice_id: &ChoiceId<str>,
        ) -> Result<(), Self::Error> {
            if self.failed.load(Ordering::SeqCst) {
                self.attempts.fetch_add(1, Ordering::SeqCst);
                return Err(crate::stores::InMemoryStoreError(
                    "choice store unavailable".into(),
                ));
            }
            self.inner
                .remove_pending_user_choice(thread_id, choice_id)
                .await
        }
        async fn get_pending_user_choices(
            &self,
            thread_id: &ThreadId<str>,
        ) -> Result<Vec<UserChoice>, Self::Error> {
            self.inner.get_pending_user_choices(thread_id).await
        }
    }

    #[tokio::test(start_paused = true)]
    async fn held_retries_on_input_and_timer_and_exits_when_channels_close() {
        for recovery in ["input", "timer", "shutdown"] {
            tokio::task::LocalSet::new()
                .run_until(async {
                    let conv = InMemoryConversationStore::new();
                    let tid = ThreadId::from_ref("held");
                    conv.ensure_root_thread(tid).await.expect("root");
                    conv.append_messages(
                        tid,
                        vec![(
                            crate::message::InfinityMessage::ToolCall {
                                call: infinity_provider_protocol::message::ToolCall {
                                    id: "waiting".into(),
                                    call_id: None,
                                    function: infinity_provider_protocol::message::ToolFunction {
                                        name: "async_tool".into(),
                                        arguments: serde_json::json!({}),
                                    },
                                },
                                display_as: None,
                            },
                            "call".into(),
                        )],
                    )
                    .await
                    .expect("pending call");
                    let state = GateStore {
                        inner: InMemoryStateStore::new(),
                        failed: Arc::new(AtomicBool::new(false)),
                        attempts: Arc::new(AtomicUsize::new(0)),
                    };
                    state
                        .add_pending_user_choice(
                            tid,
                            UserChoice {
                                id: "waiting".into(),
                                prompt: "Choose".into(),
                                choices: vec!["A".into()],
                                default: 0,
                                response_url: "http://unused.test".into(),
                            },
                        )
                        .await
                        .expect("choice");
                    state.failed.store(true, Ordering::SeqCst);
                    let (model, mut ctrl) = model_source(None);
                    let (tx, mut events) = mpsc::unbounded_channel();
                    let running = super::super::AgentSystemBuilder::new_local(
                        conv.clone(),
                        state.clone(),
                        model,
                    )
                    .tool(Box::new(AsyncTool))
                    .start_with_observer(move |_| TestObserver { tx: tx.clone() });
                    let (tx, mut replay) = mpsc::unbounded_channel();
                    running.subscribe(tid, tx).await;
                    next_evt(&mut replay).await;
                    running.interrupt_handle().interrupt(tid);
                    loop {
                        if matches!(
                            events.recv().await.expect("event"),
                            Evt::E(AgentEvent::SettlementFailed { .. })
                        ) {
                            break;
                        }
                    }
                    assert!(
                        state.attempts.load(Ordering::SeqCst) <= 32,
                        "fast retries are bounded"
                    );
                    assert!(ctrl.try_next_request().is_none(), "Held runs no inference");
                    if recovery == "shutdown" {
                        tokio::time::timeout(Duration::from_secs(1), running.shutdown())
                            .await
                            .expect("closed channels release Held driver");
                        return;
                    }
                    state.failed.store(false, Ordering::SeqCst);
                    if recovery == "input" {
                        let peer = peer_input("held", "repair", "mail after repair");
                        running.send(peer.0, &peer.1).await;
                        tokio::time::timeout(Duration::from_secs(1), ctrl.next_request())
                            .await
                            .expect("any input retries Held promptly");
                        ctrl.send_text("done");
                        ctrl.finish();
                    }
                    tokio::time::timeout(Duration::from_secs(6), async {
                        loop {
                            if matches!(
                                events.recv().await.expect("event"),
                                Evt::E(AgentEvent::ThreadIdle)
                            ) {
                                break;
                            }
                        }
                    })
                    .await
                    .expect("slow timer settles after storage repair");
                    assert!(
                        state
                            .get_pending_user_choices(tid)
                            .await
                            .expect("choices")
                            .is_empty()
                    );
                    assert_eq!(
                        persisted(&conv, "held")
                            .matches("Tool call interrupted by user")
                            .count(),
                        1
                    );
                    running.shutdown().await;
                })
                .await;
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn caught_executor_panic_is_reported_and_can_be_interrupted() {
    struct PanicTool;
    #[async_trait]
    impl Tool<ChannelSender> for PanicTool {
        fn name(&self) -> &str {
            "panic_tool"
        }
        fn description(&self) -> &str {
            "panics"
        }
        fn parameters(&self) -> serde_json::Value {
            serde_json::json!({"type":"object"})
        }
        async fn execute(
            &self,
            _: serde_json::Value,
            _: rap_protocol::ToolCallId,
            _: Option<rap_protocol::ProviderCallId>,
            _: &ToolContext<ChannelSender>,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            panic!("executor panic fixture");
        }
    }
    tokio::task::LocalSet::new()
        .run_until(async {
            let (model, mut ctrl) = model_source(None);
            let conv = crate::stores::InMemoryConversationStore::new();
            let (tx, mut events) = mpsc::unbounded_channel();
            let running = super::AgentSystemBuilder::new_local(
                conv.clone(),
                crate::stores::InMemoryStateStore::new(),
                model,
            )
            .tool(Box::new(PanicTool))
            .start_with_observer(move |_| TestObserver { tx: tx.clone() });
            running
                .send_user_text(ThreadId::from_ref("panic"), "work")
                .await;
            ctrl.next_request().await;
            ctrl.send_tool_call("panic-call", "panic_tool", serde_json::json!({}));
            ctrl.finish();
            loop {
                if let Evt::E(AgentEvent::DriverFailed { text }) = next_evt(&mut events).await {
                    assert!(text.starts_with("Thread driver failed:"));
                    break;
                }
            }
            assert!(!persisted(&conv, "panic").contains("Tool call interrupted by user"));
            let (tx, mut replay) = mpsc::unbounded_channel();
            running.subscribe(ThreadId::from_ref("panic"), tx).await;
            assert!(matches!(next_evt(&mut replay).await, Evt::Replay(snapshot) if !snapshot.idle));
            running
                .interrupt_handle()
                .interrupt(ThreadId::from_ref("panic"));
            loop {
                if matches!(next_evt(&mut events).await, Evt::E(AgentEvent::ThreadIdle)) {
                    break;
                }
            }
            assert!(persisted(&conv, "panic").contains("Tool call interrupted by user"));
            running.shutdown().await;
        })
        .await;
}
