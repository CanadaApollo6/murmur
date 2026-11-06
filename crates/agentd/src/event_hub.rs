use crate::model::*;
use crate::supervisor::Event;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::broadcast;

#[derive(Clone)]
pub struct EventHub(Arc<Inner>);
struct Inner {
    legacy: broadcast::Sender<Event>,
    state: broadcast::Sender<Event>,
    activity: broadcast::Sender<Event>,
    book: Mutex<Book>,
}
#[derive(Default)]
struct Book {
    permissions: HashMap<PermissionId, (ulid::Ulid, PermissionRequest)>,
    denials: HashMap<AgentId, Instant>,
}
impl Default for EventHub {
    fn default() -> Self {
        Self(Arc::new(Inner {
            legacy: broadcast::channel(1024).0,
            state: broadcast::channel(1024).0,
            activity: broadcast::channel(1024).0,
            book: Mutex::new(Book::default()),
        }))
    }
}
impl EventHub {
    pub fn events(&self) -> broadcast::Receiver<Event> {
        self.0.legacy.subscribe()
    }
    pub fn state(&self) -> broadcast::Receiver<Event> {
        self.0.state.subscribe()
    }
    pub fn activity(&self) -> broadcast::Receiver<Event> {
        self.0.activity.subscribe()
    }
    pub fn permissions(&self) -> Vec<PermissionRequest> {
        self.0
            .book
            .lock()
            .expect("bug: event book poisoned")
            .permissions
            .values()
            .map(|(_, r)| r.clone())
            .collect()
    }
    pub(crate) fn owner(&self) -> PermissionGuard {
        PermissionGuard {
            hub: self.clone(),
            token: ulid::Ulid::new(),
        }
    }
    pub fn emit(&self, event: Event) {
        self.publish(event, None)
    }
    fn publish(&self, event: Event, owner: Option<ulid::Ulid>) {
        // Map mutation and state publication share the lock so concurrent guard drops cannot reorder them.
        let mut book = self.0.book.lock().expect("bug: event book poisoned");
        match &event {
            Event::PermissionPending(r) => {
                book.permissions.insert(
                    r.id,
                    (owner.expect("bug: permission needs actor owner"), r.clone()),
                );
            }
            Event::PermissionAnswered { id, .. } => {
                book.permissions.remove(id);
            }
            _ => {}
        }
        if self.0.legacy.send(event.clone()).is_err() {
            tracing::trace!("no legacy event subscribers");
        }
        let target = match event {
            Event::Activity {
                agent,
                incarnation,
                text,
            } => {
                let event = Event::Activity {
                    agent,
                    incarnation,
                    text: crate::surfaces::events::tail(&text, 2048),
                };
                if self.0.activity.send(event).is_err() {
                    tracing::trace!("no activity subscribers");
                }
                return;
            }
            Event::EgressDenied { agent, .. } => {
                let now = Instant::now();
                if book
                    .denials
                    .get(&agent)
                    .is_some_and(|last| now.duration_since(*last) < Duration::from_secs(1))
                {
                    return;
                }
                book.denials.insert(agent, now);
                &self.0.state
            }
            _ => &self.0.state,
        };
        if target.send(event).is_err() {
            tracing::trace!("no state subscribers");
        }
    }
}
pub(crate) struct PermissionGuard {
    hub: EventHub,
    token: ulid::Ulid,
}
impl PermissionGuard {
    pub fn emit(&self, event: Event) {
        self.hub.publish(event, Some(self.token));
    }
}
impl Drop for PermissionGuard {
    fn drop(&mut self) {
        let mut book = self.hub.0.book.lock().expect("bug: event book poisoned");
        let ids = book
            .permissions
            .iter()
            .filter(|(_, (token, _))| *token == self.token)
            .map(|(id, (_, r))| (*id, r.agent))
            .collect::<Vec<_>>();
        for (id, agent) in ids {
            book.permissions.remove(&id);
            let event = Event::PermissionAnswered { agent, id };
            if self.hub.0.legacy.send(event.clone()).is_err() {
                tracing::trace!("no legacy event subscribers");
            }
            if self.hub.0.state.send(event).is_err() {
                tracing::trace!("no state subscribers");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(agent: AgentId, incarnation: u64) -> PermissionRequest {
        PermissionRequest {
            id: PermissionId(ulid::Ulid::new()),
            agent,
            incarnation: Incarnation(incarnation),
            tool: "tool".into(),
            title: "title".into(),
            detail: "detail".into(),
            options: vec![],
        }
    }
    #[test]
    fn guard_tracks_actor_token_across_reopen_and_panic() {
        let hub = EventHub::default();
        let agent = AgentId::new();
        let mut state = hub.state();
        let old = hub.owner();
        old.emit(Event::PermissionPending(request(agent, 1)));
        let new = hub.owner();
        let replacement = request(agent, 3);
        new.emit(Event::PermissionPending(replacement.clone()));
        drop(old);
        assert_eq!(hub.permissions().len(), 1);
        assert_eq!(hub.permissions()[0].id, replacement.id);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let guard = new;
            guard.emit(Event::PermissionPending(request(agent, 4)));
            panic!("test actor panic");
        }));
        assert!(result.is_err());
        assert!(hub.permissions().is_empty());
        let mut answers = 0;
        while let Ok(e) = state.try_recv() {
            if matches!(e, Event::PermissionAnswered { .. }) {
                answers += 1;
            }
        }
        assert_eq!(answers, 3);
    }
    #[test]
    fn activity_and_denial_floods_preserve_state_channel() {
        let hub = EventHub::default();
        let agent = AgentId::new();
        let mut state = hub.state();
        let mut activity = hub.activity();
        let mut legacy = hub.events();
        let started = Instant::now();
        for _ in 0..4096 {
            hub.emit(Event::Activity {
                agent,
                incarnation: Incarnation(1),
                text: "x".repeat(8192),
            });
            hub.emit(Event::EgressDenied {
                agent,
                host: "blocked".into(),
                port: 443,
            });
        }
        // A slow runner can legitimately cross the one-second limit during the flood.
        let allowed = started.elapsed().as_secs() + 1;
        assert!(matches!(state.try_recv(), Ok(Event::EgressDenied { .. })));
        let mut denials = 1;
        loop {
            match state.try_recv() {
                Ok(Event::EgressDenied { .. }) => denials += 1,
                Err(broadcast::error::TryRecvError::Empty) => break,
                other => panic!("state channel: {other:?}"),
            }
        }
        assert!(denials <= allowed, "{denials} denials in {allowed} seconds");
        assert!(matches!(
            activity.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        if let Ok(Event::Activity { text, .. }) = activity.try_recv() {
            assert!(serde_json::to_string(&text).expect("text").len() <= 2048);
            assert!(text.ends_with('x'));
        } else {
            panic!("activity");
        }
        assert!(matches!(
            legacy.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
    }
}
