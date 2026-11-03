use infinity_provider_protocol::{CompletionError, ErrorClass};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub(crate) struct Gate {
    permits: Arc<Semaphore>,
    waiting: AtomicUsize,
}
struct Waiter<'a>(&'a AtomicUsize);
impl Drop for Waiter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Gate {
    pub fn new(slots: usize) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(slots)),
            waiting: AtomicUsize::new(0),
        }
    }
    // Discovery is serialized and no dispatch can precede the first successful discovery.
    pub fn resize_initial(&self, old: usize, new: usize) {
        if new > old {
            self.permits.add_permits(new - old);
        } else {
            let removed = self.permits.forget_permits(old - new);
            assert_eq!(removed, old - new, "bug: initial gate has no owners");
        }
    }
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit, CompletionError> {
        if let Ok(permit) = self.permits.clone().try_acquire_owned() {
            return Ok(permit);
        }
        self.waiting
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n < 32).then_some(n + 1)
            })
            .map_err(|_| {
                CompletionError::provider(
                    ErrorClass::Throttled,
                    "local server gate queue is full (32 waiters)",
                )
            })?;
        let waiter = Waiter(&self.waiting);
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("bug: local gate never closes");
        drop(waiter);
        Ok(permit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn initial_resize_grows_and_shrinks_the_gate() {
        let gate = Gate::new(1);
        gate.resize_initial(1, 3);
        assert_eq!(gate.permits.available_permits(), 3);
        let gate = Gate::new(4);
        gate.resize_initial(4, 1);
        assert_eq!(gate.permits.available_permits(), 1);
    }
    #[tokio::test]
    async fn fifo_queue_is_bounded_and_cancelled_waiters_are_pruned() {
        let gate = Arc::new(Gate::new(1));
        let held = gate.acquire().await.expect("first permit");
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let mut tasks = Vec::new();
        for i in 0..32 {
            let gate = gate.clone();
            let tx = tx.clone();
            tasks.push(tokio::spawn(async move {
                let permit = gate.acquire().await.expect("queued permit");
                tx.send(i).expect("record admission order");
                permit
            }));
            tokio::task::yield_now().await;
        }
        assert_eq!(
            gate.acquire()
                .await
                .expect_err("33rd waiter is rejected")
                .class(),
            ErrorClass::Throttled
        );
        tasks[0].abort();
        assert!(
            tasks
                .remove(0)
                .await
                .expect_err("cancelled waiter")
                .is_cancelled()
        );
        let g = gate.clone();
        let replacement = tokio::spawn(async move {
            let permit = g
                .acquire()
                .await
                .expect("cancelled waiter frees queue room");
            tx.send(32).expect("replacement admitted");
            permit
        });
        tokio::task::yield_now().await;
        drop(held);
        for (expected, task) in (1..32).zip(tasks) {
            let permit = tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .expect("FIFO progress")
                .expect("waiter");
            assert_eq!(rx.recv().await, Some(expected));
            drop(permit);
        }
        drop(replacement.await.expect("replacement"));
        assert_eq!(rx.recv().await, Some(32));
    }
}
