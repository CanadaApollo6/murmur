//! The whole Infinity fork diff: two additive commits, each written to be proposed
//! upstream on its own. Estimated non-test lines per hunk are in each banner; totals:
//! commit 1 about 205, commit 2 about 22 (design budget: 150-250).
//!
//! - Commit 1, "peer messages": lets any daemon client put an event into a thread
//!   without impersonating the user and without a subscription tool call to anchor to,
//!   reports when an input is durable, and adds a real interrupt.
//! - Commit 2, "provider cancellation": `serve_provider` notices a client that went away
//!   while the provider is still queued or streaming.
//!
//! Why a patch at all: today the only non-interrupting inputs are synthetics anchored
//! to a tool call in the recipient's own history (`prepare_input` drops a synthetic
//! whose `find_tool_call` misses, event_processor.rs:1090-1096). A peer has no such call
//! to anchor to. The zero-patch workaround (make every model agent first call an
//! `open_inbox` subscription tool) puts deliverability at the mercy of a local model
//! following a bootstrap instruction; see design.md, "Alternatives considered".
//!
//! Untouched: rap-protocol, rap-client, infinity-rap-bridge, infinity-mcp-bridge,
//! infinity-provider-{bedrock,rig}, sandbox-*, stores and JSON persistence, router
//! admission, defer queue. One forced one-line touch outside the table: see 1(g).
//!
//! Paths are relative to the fork root; line numbers are from the current checkout.

// ######################################################################
// COMMIT 1: peer messages
// ######################################################################

// ======================================================================
// 1(a). crates/infinity-agent-core/src/message.rs  (+14)
//       enum TaggedSyntheticKind (:49), impl SyntheticKind (:73)
// ======================================================================

pub enum TaggedSyntheticKind {
    // ... existing: SubscriptionEvent, ThreadReport, ParentMessage, Compaction,
    //     CompactionComplete ...
    /// An event from outside the thread tree (another agent). Inlined into the
    /// recipient like an associative subscription event, but needs no anchor: the
    /// runtime mints the `receive_event__injected` call itself.
    #[serde(rename = "peer_message")]
    PeerMessage {
        /// Sender's display address, e.g. "@planner". Shown to the model in the
        /// injected call's arguments.
        from: String,
    },
}

// SyntheticKind::tool_call_id() (:74, exhaustive match):
//     SyntheticKind::Tagged(TaggedSyntheticKind::PeerMessage { .. }) => ToolCallId::from_ref(""),
//                                                                (like Compaction)   (+3)
// SyntheticKind::is_peer_message(&self) -> bool, next to is_parent_message (:100) (+6)

// ======================================================================
// 1(b). crates/infinity-agent-core/src/system/thread.rs  (+1)
//       fn is_deferrable_synthetic_event (:63)
// ======================================================================

// Add `| TaggedSyntheticKind::PeerMessage { .. }` to the matches! list, so a peer
// message waits in `InMemoryDeferQueue` while a non-passive tool call is pending and
// never produces a "Tool call interrupted" result.

// ======================================================================
// 1(c). crates/infinity-agent-core/src/system/thread.rs  (+10)
//       Thread::filter_deferrable (:248-290)
// ======================================================================

// Today (:267-281): when the batch settles the pending call, it is returned in arrival
// order. `[peer, result]` then runs the peer first; `handle_content` treats a
// self-contained SubscriptionEvent as non-tool-result input and calls
// `interrupt_pending_tool_call()` (event_processor.rs:282-286), so the real result that
// follows is discarded as a stale answer. Fix: put the settling result first.
//
// let is_settling_result = |msg: &InputMessage| {                 // hoisted from :268
//     msg.synthetic.is_none()
//         && matches!(&msg.content,
//                     InputMessageContent::User(UserContent::ToolResult(r)) if r.id == pending_call_id)
// };
// let settles_pending = inputs.iter().any(|(m, _)| is_user_text_input(m) || is_settling_result(m));
// if settles_pending {
//     let mut batch = inputs;
//     // Settle the pending call before anything that could interrupt it.
//     if let Some(i) = batch.iter().position(|(m, _)| is_settling_result(m)) {
//         let settling = batch.remove(i);
//         batch.insert(0, settling);
//     }
//     batch.extend(defer.drain().await?);
//     return Ok(batch);
// }

// ======================================================================
// 1(d). crates/infinity-agent-core/src/event_processor.rs  (+36)
//       prepare_input (:889), input_event (:1309)
// ======================================================================

// prepare_input, right after `user_content` is extracted (:1042-1047) and before the
// synthetic handling (`subscription_event_meta`, :1051):                     (+32)
//
// if let Some(SyntheticKind::Tagged(TaggedSyntheticKind::PeerMessage { from })) = &input_msg.synthetic {
//     let UserContent::ToolResult(mut result) = user_content else {
//         return Err("peer message is not a tool result".into());
//     };
//     // Derived, not random: `message_id` is the daemon's dedup id "peer-{MessageId}",
//     // so a replayed or re-offered message has the same injected call ID everywhere
//     // (history, display, provider request).
//     let call_id = message_id.clone();
//     let invocation = infinity_provider_protocol::message::ToolCall {
//         id: call_id.clone(),
//         call_id: None,
//         function: infinity_provider_protocol::message::ToolFunction {
//             name: "receive_event__injected".to_owned(),
//             arguments: serde_json::json!({ "original_tool_name": "peer_message", "from": from }),
//         },
//     };
//     result.id = call_id;
//     let msg = InfinityMessage::SubscriptionEvent {
//         result: Box::new(result),
//         tool_call_id: ToolCallId::from(""),     // no originating call
//         child_thread_id: None,
//         invocation: Some(Box::new(invocation)),
//     };
//     // `handle_content` checks processed_message_ids first, so a re-offer after a crash
//     // returns Handled without a completion (and 1(e) still reports it committed).
//     return if current_history.handle_content(msg, message_id)? {
//         Ok(PrepareResult::Ready)
//     } else {
//         Ok(PrepareResult::Handled)
//     };
// }
//
// `handle_content` treats a self-contained SubscriptionEvent as a non-tool-result and
// calls `interrupt_pending_tool_call()`. With 1(b) that path only runs when the pending
// call is passive (sleep), which is the intended "wake on message" behavior.
//
// input_event (:1309): the name branch falls back to "Event for tool call " for an
// empty tool_call_id; add a PeerMessage arm -> `format!("message from {from}")`, so the
// daemon broadcasts `DaemonMessage::SubscriptionEvent` with a readable name.      (+4)

// ======================================================================
// 1(e). AgentEvent::InputCommitted, emitted after sync()  (+27)
// ======================================================================

// crates/infinity-agent-core/src/system/events.rs, enum AgentEvent (:24):          (+4)
pub enum AgentEvent {
    // ... existing variants ...
    /// The input with this dedup id is durable: `sync()` has appended it to the
    /// conversation store and recorded its processed id. Emitted for user text and
    /// subscription events (the inputs `sync()` records dedup ids for), including a
    /// redelivered input that dedup absorbed after it was already durable.
    InputCommitted { dedup_id: String },
}

// crates/infinity-agent-core/src/event_processor.rs, struct HistoryManager (:141):
//   committed_ids: RefCell<Vec<String>>, initialized next to pending_items (:232)   (+2)
//   sync() (:627): after add_processed_message_ids (:664-669),
//     self.committed_ids.borrow_mut().extend(msg_ids)                                (+1)
//   handle_content (:241), dedup early return (:246): if the id is in neither
//     unvalidated_items nor pending_items it is already durable ->
//     self.committed_ids.borrow_mut().push(message_id.clone())                       (+6)
//   pub fn take_committed_ids(&self) -> Vec<String>                                  (+4)
//
// crates/infinity-agent-core/src/system/thread.rs, Thread::step_no_defer (:423):
//   after both `self.history.sync().await?` (:480, :566) call                         (+2)
//   fn emit_committed(&self, observer, thread_id) {                                  (+8)
//       for dedup_id in self.history.take_committed_ids() {
//           observer.on_event(thread_id, &AgentEvent::InputCommitted { dedup_id });
//       }
//   }
//   (`sync()` inside run_completion, event_processor.rs:1837, only adds to the list;
//    the :566 call drains it.)
//
// Unvalidated input is not persisted (`mark_inputs_model_validated`, :510), so a
// completion that fails before output emits `CompletionFinished` but no
// `InputCommitted` for its inputs. That is exactly why the broker keys `Consumed` here.

// ======================================================================
// 1(f). crates/infinity-protocol/src/lib.rs  (+18)
//       enum ClientMessage (:230), enum DaemonMessage (:339)
// ======================================================================

pub enum ClientMessage {
    // ... existing variants ...
    /// Deliver a message from another agent into a thread as a non-interrupting event.
    /// The daemon uses `peer-{message_id}` as the dedup id, so re-sending the same id
    /// after the thread absorbed it is a no-op (answered with `InputCommitted` again).
    DeliverPeerMessage {
        thread_id: ThreadRef,
        message_id: String,
        from: String,
        text: String,
    },
    /// Cancel the thread's in-flight step (as user text would) without adding input.
    Interrupt { thread_id: ThreadRef },
}

pub enum DaemonMessage {
    // ... existing variants ...
    /// An input to this thread is durable (`AgentEvent::InputCommitted`).
    InputCommitted {
        thread_id: Option<ThreadRef>,
        dedup_id: String,
    },
}

// ======================================================================
// 1(g). Daemon dispatch, forwarding, remote strip, interrupt routing  (+99)
// ======================================================================

// crates/infinity-daemon/src/client_handler.rs
//
// strip_client_message (:209), proxying to a remote daemon:                         (+8)
//   DeliverPeerMessage { thread_id, .. } and Interrupt { thread_id } =>
//     same with thread_id.strip(remote_name)
// prefix_daemon_message (:63), forwarding a remote's events back:                   (+4)
//   InputCommitted { thread_id, dedup_id } => thread_id: prefix_thread_id(thread_id, remote_name)
//
// handle_client_channels (:290) dispatch, next to UserInput (:501):               (+30)
//
// ClientMessage::DeliverPeerMessage { thread_id, message_id, from, text } => {
//     if thread_id.remote.is_some() { /* same "remote is not connected" error as UserInput */ }
//     let mgr = session_manager.lock().await;
//     let input = InputMessage {
//         content: InputMessageContent::User(UserContent::ToolResult(ToolResult {
//             id: String::new(),                  // replaced in prepare_input, 1(d)
//             call_id: None,
//             content: vec![ToolResultContent::Text(Text { text })],
//         })),
//         group_id: thread_id.id.clone(),
//         metadata: None,
//         synthetic: Some(SyntheticKind::Tagged(TaggedSyntheticKind::PeerMessage { from })),
//         display_as: None,
//         subscription: false,
//     };
//     if !mgr.send_input((input, Some(format!("peer-{message_id}")))).await { /* Error */ }
// }
// ClientMessage::Interrupt { thread_id } => {
//     if thread_id.remote.is_some() { /* "remote is not connected" */ }
//     session_manager.lock().await.interrupt(&thread_id.id);
// }
//
// crates/infinity-daemon/src/session/mod.rs: `interrupt: InterruptHandle` next to
//   `subscribe` (:98, set from `running.interrupt_handle()` at :274), and
//   `pub fn interrupt(&self, thread_id)` next to send_input (:549)                  (+10)
// crates/infinity-daemon/src/session/display.rs:
//   agent_event_to_daemon (:7, exhaustive): InputCommitted -> DaemonMessage::InputCommitted (+4)
//   history_message_to_daemon (:77): a SubscriptionEvent with an empty tool_call_id
//     takes its name from the invocation's `from` ("message from @x") on replay      (+6)
//
// Interrupt routing needs core too: today a driver cancels its step only when user
// text arrives (system/local/driver.rs:178), and the daemon holds no handle to a
// driver.
// crates/infinity-agent-core/src/system/local/router.rs: `InterruptHandle` (clone of
//   the `SubscribeHandle` shape, :32), `RoutedMessage::Interrupt(ThreadId)` (:234),
//   a per-worker interrupt channel in `WorkerChannels` (:229); route only to a live
//   worker (no driver means no in-flight step: no-op, never spawn one)            (+22)
// crates/infinity-agent-core/src/system/local/driver.rs: select arm next to
//   `subscribe_rx` (:213): `in_flight.take().cancel().await` -> handle_step_result (:366);
//   the inputs that step had not validated stay in memory and run on the next step,
//   as with a user interrupt, but no `<interrupt>` text is injected               (+14)
//
// Forced by an exhaustive match (outside the design's "patched" list):
// crates/infinity-agent-cli/src/daemon_client.rs:128, daemon_msg_to_display:
//   `| DaemonMessage::InputCommitted { .. }` in the `return None` arm               (+1)
//
// Router admission is unchanged and correct for peers: a synthetic to an unknown or
// stopped thread is dropped (router.rs:310-355). The broker never shuts down a live
// agent's session; a dropped peer never produces `InputCommitted`, so it stays Offered
// and is re-offered under the next incarnation.

// ######################################################################
// COMMIT 2: provider cancellation
// ######################################################################

// ======================================================================
// 2. crates/infinity-provider-protocol/src/remote.rs  (+22)
//    serve_provider (:149) -> handle_connection (:177) -> handle_invoke (:219)
// ======================================================================

// Today the server reads one request, then only writes. A client that hangs up is
// noticed on the next `send_json` (:232, :235), never while `provider.invoke_model`
// (:225) is waiting, e.g. in `infinity-provider-local`'s GPU queue. A cancelled
// completion therefore keeps its queue position or slot until its first chunk.
//
// Fix: race both waits against EOF on the socket. The client sends nothing after the
// request, so any readable EOF means it is gone.
//
// async fn client_gone(stream: &UnixStream) {                                      (+12)
//     let mut buf = [0u8; 1];
//     loop {
//         if stream.readable().await.is_err() { return; }
//         match stream.try_read(&mut buf) {
//             Ok(0) => return,                                           // EOF
//             Ok(_) => continue,                                         // ignore stray bytes
//             Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
//             Err(_) => return,
//         }
//     }
// }
//
// handle_invoke:                                                                  (+10)
//   let mut stream = tokio::select! {
//       r = provider.invoke_model(model_id, request) => match r { .. as today .. },
//       () = client_gone(framed.get_ref()) => return Ok(()),  // drops the invoke future
//   };
//   ...
//   loop {
//       tokio::select! {
//           item = stream.next() => { let Some(item) = item else { break };
//                                     send_json(framed, &ProviderResponse::Chunk(..)).await? }
//           () = client_gone(framed.get_ref()) => return Ok(()),  // drops the stream
//       }
//   }
//
// Dropping the invoke future or the stream is the provider's cancellation signal; the
// trait needs no new method, so every provider benefits (bedrock and rig drop their
// HTTP requests the same way).
