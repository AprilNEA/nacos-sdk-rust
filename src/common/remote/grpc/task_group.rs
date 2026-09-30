use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Poll, ready};

use tokio::task::JoinHandle;

use crate::api::error::Error;

#[derive(Default)]
pub(crate) struct TaskGroup {
    state: Mutex<State>,
    shutdown_lock: tokio::sync::Mutex<()>,
}

#[derive(Default)]
struct State {
    closed: bool,
    handles: Vec<JoinHandle<()>>,
}

impl TaskGroup {
    pub(crate) fn spawn<F>(&self, future: F)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let mut state = self.state.lock().expect("task group lock poisoned");
        if state.closed {
            return;
        }
        state.handles.retain(|handle| !handle.is_finished());
        state
            .handles
            .push(crate::common::executor::spawn(async move {
                future.await;
            }));
    }

    pub(crate) fn ensure_running(&self) -> Result<(), Error> {
        if self.state.lock().expect("task group lock poisoned").closed {
            Err(Error::ClientShutdown(
                "client has been shut down".to_owned(),
            ))
        } else {
            Ok(())
        }
    }

    pub(crate) fn cancel(&self) {
        let mut state = self.state.lock().expect("task group lock poisoned");
        state.closed = true;
        for handle in &state.handles {
            handle.abort();
        }
    }

    pub(crate) async fn shutdown(&self) {
        self.cancel();
        let _shutdown = self.shutdown_lock.lock().await;
        // Keep handles in the group while polling so cancelling shutdown does not detach them.
        futures::future::poll_fn(|cx| {
            let mut state = self.state.lock().expect("task group lock poisoned");
            while let Some(handle) = state.handles.last_mut() {
                let _ = ready!(Pin::new(handle).poll(cx));
                let _ = state.handles.pop();
            }
            Poll::Ready(())
        })
        .await;
    }
}

/// Keep the cancellation owner outside tasks that retain the task group.
#[derive(Default)]
pub(crate) struct TaskOwner {
    pub(crate) tasks: Arc<TaskGroup>,
}

impl Drop for TaskOwner {
    fn drop(&mut self) {
        self.tasks.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    struct Dropped(Option<oneshot::Sender<()>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn shutdown_waits_for_tasks_and_rejects_new_tasks() {
        let owner = TaskOwner::default();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        owner.tasks.spawn(async move {
            let _dropped = Dropped(Some(dropped_tx));
            started_tx.send(()).expect("test must receive start");
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("task must start");
        tokio::join!(owner.tasks.shutdown(), owner.tasks.shutdown());
        dropped_rx.await.expect("shutdown must drop task resources");

        let (rejected_tx, rejected_rx) = oneshot::channel::<()>();
        owner.tasks.spawn(async move {
            rejected_tx.send(()).expect("task must not run");
        });
        assert!(rejected_rx.await.is_err());
    }

    #[tokio::test]
    async fn dropping_owner_cancels_tasks_that_retain_the_group() {
        let owner = TaskOwner::default();
        let tasks = owner.tasks.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let (dropped_tx, dropped_rx) = oneshot::channel();
        owner.tasks.spawn(async move {
            let _retained_group = tasks;
            let _dropped = Dropped(Some(dropped_tx));
            started_tx.send(()).expect("test must receive start");
            std::future::pending::<()>().await;
        });
        started_rx.await.expect("task must start");
        drop(owner);
        dropped_rx.await.expect("owner drop must cancel task");
    }
}
