use std::{cell::RefCell, collections::HashMap, rc::Rc};

use rap_protocol::ThreadId;

use crate::{event_processor::PendingItem, traits::ToolDispatch};

/// One slot per thread, matching the durable dispatch slot. Only Awaiting
/// belongs to the live set. Received retains the result until it is durable.
#[derive(Default)]
pub(crate) struct RuntimeDispatches(RefCell<HashMap<ThreadId, (ToolDispatch, DispatchState)>>);

enum DispatchState {
    Awaiting,
    Received(PendingItem),
}

impl RuntimeDispatches {
    pub fn start(self: &Rc<Self>, thread: &ThreadId, dispatch: ToolDispatch) -> DispatchGuard {
        self.0
            .borrow_mut()
            .insert(thread.clone(), (dispatch.clone(), DispatchState::Awaiting));
        DispatchGuard {
            runtime: self.clone(),
            thread: thread.clone(),
            dispatch,
            awaiting_callback: false,
        }
    }

    pub fn awaiting(&self, thread: &ThreadId, dispatch: &ToolDispatch) -> bool {
        self.0.borrow().get(thread).is_some_and(|(record, state)| {
            same_call(record, dispatch) && matches!(state, DispatchState::Awaiting)
        })
    }

    pub fn received(&self, thread: &ThreadId, dispatch: &ToolDispatch, result: PendingItem) {
        self.0.borrow_mut().insert(
            thread.clone(),
            (dispatch.clone(), DispatchState::Received(result)),
        );
    }

    pub fn has_result(&self, thread: &ThreadId, dispatch: &ToolDispatch) -> bool {
        self.0.borrow().get(thread).is_some_and(|(record, state)| {
            same_call(record, dispatch) && matches!(state, DispatchState::Received(_))
        })
    }

    pub fn result(&self, thread: &ThreadId, dispatch: &ToolDispatch) -> Option<PendingItem> {
        self.0.borrow().get(thread).and_then(|(record, state)| {
            if same_call(record, dispatch)
                && let DispatchState::Received(result) = state
            {
                Some(result.clone())
            } else {
                None
            }
        })
    }

    pub fn remove(&self, thread: &ThreadId, dispatch: &ToolDispatch) {
        let mut calls = self.0.borrow_mut();
        if calls
            .get(thread)
            .is_some_and(|(record, _)| same_call(record, dispatch))
        {
            calls.remove(thread);
        }
    }
}

fn same_call(a: &ToolDispatch, b: &ToolDispatch) -> bool {
    a.epoch == b.epoch && a.call_order == b.call_order && a.call_id == b.call_id
}

/// Dropping an invocation future removes its live claim. Async dispatch
/// transfers that claim to its callback, which may arrive after execute returns.
pub(crate) struct DispatchGuard {
    runtime: Rc<RuntimeDispatches>,
    thread: ThreadId,
    dispatch: ToolDispatch,
    awaiting_callback: bool,
}

impl DispatchGuard {
    pub fn await_callback(mut self) {
        self.awaiting_callback = true;
    }
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        if !self.awaiting_callback && self.runtime.awaiting(&self.thread, &self.dispatch) {
            self.runtime.remove(&self.thread, &self.dispatch);
        }
    }
}
