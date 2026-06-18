//! Bounded concurrent task fan-out shared by the fetch/push/copy/restore/
//! prefetch loops.
//!
//! `spawn` blocks once `concurrency` tasks are in flight, then reaps finished
//! tasks as new ones are queued so the live set stays ~`concurrency` deep
//! (not one handle per item). Each completed output is routed through the
//! `on_done` handler in completion order; a handler that returns `Err` aborts,
//! and dropping the set cancels whatever is still running.

use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub struct BoundedTasks<T, H> {
    sem: Arc<Semaphore>,
    set: JoinSet<T>,
    on_done: H,
    /// Tags permit/join error context, e.g. `download` → "acquire download
    /// permit" / "download task join"
    label: &'static str,
}

impl<T, H> BoundedTasks<T, H>
where
    T: Send + 'static,
    H: FnMut(T) -> Result<()>,
{
    pub fn new(concurrency: usize, label: &'static str, on_done: H) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(concurrency.max(1))),
            set: JoinSet::new(),
            on_done,
            label,
        }
    }

    /// Acquire a permit (blocking once `concurrency` are out), spawn `fut`, then
    /// drain any finished tasks through `on_done`
    pub async fn spawn<F>(&mut self, fut: F) -> Result<()>
    where
        F: Future<Output = T> + Send + 'static,
    {
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .with_context(|| format!("acquire {} permit", self.label))?;
        self.set.spawn(async move {
            let _permit = permit;
            fut.await
        });
        while let Some(joined) = self.set.try_join_next() {
            self.report(joined)?;
        }
        Ok(())
    }

    /// Await remaining tasks, routing each output through `on_done`
    pub async fn join(mut self) -> Result<()> {
        while let Some(joined) = self.set.join_next().await {
            self.report(joined)?;
        }
        Ok(())
    }

    fn report(&mut self, joined: std::result::Result<T, tokio::task::JoinError>) -> Result<()> {
        let out = joined.with_context(|| format!("{} task join", self.label))?;
        (self.on_done)(out)
    }
}
