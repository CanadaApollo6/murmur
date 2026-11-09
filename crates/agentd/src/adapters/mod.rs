use std::sync::Arc;

use tokio::sync::mpsc;
use tokio::time::Instant;

use crate::adapter::{Adapter, KindConfig, Wire};

pub(crate) mod acp;
pub(crate) mod infinity;

pub(crate) struct AdapterSet;
impl AdapterSet {
    pub(crate) fn build(kind: KindConfig) -> Arc<dyn Adapter> {
        match kind.wire {
            Wire::Acp => Arc::new(acp::AcpAdapter(kind)),
            Wire::Infinity { .. } => Arc::new(infinity::InfinityAdapter(kind)),
        }
    }
}

/// Creates the queue from an adapter's reader task to its session.
///
/// The reader stamps each message with the time when it hands the message to the queue. A session
/// can then judge a deadline by when the peer answered, not by when the supervisor polled the
/// session.
pub(crate) fn inbox<T>(capacity: usize) -> (InboxSender<T>, Inbox<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (InboxSender(tx), Inbox(rx))
}

pub(crate) struct InboxSender<T>(mpsc::Sender<(Instant, T)>);

impl<T> InboxSender<T> {
    /// Stamps `message` with the current time, then queues it. The stamp comes before the wait for
    /// capacity.
    pub(crate) async fn send(&self, message: T) -> Result<(), mpsc::error::SendError<T>> {
        self.0
            .send((Instant::now(), message))
            .await
            .map_err(|mpsc::error::SendError((_, message))| mpsc::error::SendError(message))
    }
}

pub(crate) struct Inbox<T>(mpsc::Receiver<(Instant, T)>);

/// What [`Inbox::recv_by`] found.
pub(crate) enum Inbound<T> {
    Message(T),
    /// The reader task is gone, the queue is empty, and no deadline has passed.
    Closed,
    /// The peer did not answer by the deadline.
    Expired,
}

impl<T> Inbox<T> {
    pub(crate) async fn recv(&mut self) -> Option<T> {
        self.0.recv().await.map(|(_, message)| message)
    }

    /// Returns the next message that the reader stamped at or before `deadline`.
    ///
    /// Queued messages come before the deadline check, so a queued answer still counts when the
    /// caller polls after the deadline. A message stamped after the deadline is dropped and
    /// reported as `Expired`, so a peer that keeps sending cannot postpone the timeout.
    ///
    /// The queue is bounded, and the reader stops reading while it is full. An answer that was
    /// still behind a full queue when the deadline passed can be reported as `Expired`.
    pub(crate) async fn recv_by(&mut self, deadline: Option<Instant>) -> Inbound<T> {
        let Some(deadline) = deadline else {
            return self.recv().await.map_or(Inbound::Closed, Inbound::Message);
        };
        let queued = tokio::select! {
            biased;
            queued = self.0.recv() => queued,
            // On a multi-thread runtime a message can land after the poll above found none.
            () = tokio::time::sleep_until(deadline) => self.0.try_recv().ok(),
        };
        match queued {
            Some((at, message)) if at <= deadline => Inbound::Message(message),
            None if Instant::now() < deadline => Inbound::Closed,
            _ => Inbound::Expired,
        }
    }
}
