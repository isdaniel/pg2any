//! Replication stream actor — owns `LogicalReplicationStream` on a dedicated
//! single-threaded runtime so the non-`Sync` libpq connection never crosses
//! thread boundaries.
//!
//! # Architecture
//!
//! ```text
//!  ┌─────────────────────────┐         mpsc::channel
//!  │   ReplicationActorHandle│ ───Command──►┐
//!  │   (Send + Sync)         │              │
//!  └─────────────────────────┘              ▼
//!                                  ┌────────────────────┐
//!                                  │  Dedicated Thread   │
//!                                  │  (single-threaded   │
//!                                  │   tokio runtime)    │
//!                                  │                     │
//!                                  │  LogicalReplication │
//!                                  │  Stream (owned)     │
//!                                  └────────────────────┘
//!                                           │
//!                                    Events / Results
//!                                           │
//!                                           ▼
//!                                  mpsc::channel / oneshot
//! ```
//!
//! The handle is fully `Send + Sync` and can be used freely from any tokio task.

use crate::error::{CdcError, Result};
use pg_walstream::{LogicalReplicationStream, SharedLsnFeedback};
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Re-export for convenience.
pub use pg_walstream::types::ChangeEvent;

pub enum ActorCommand {
    /// Start the replication stream with an optional starting LSN.
    Start {
        start_lsn: Option<u64>,
        reply: oneshot::Sender<Result<()>>,
    },
    /// Read the next event (with automatic retry / keepalive handling).
    NextEvent {
        cancel: CancellationToken,
        reply: oneshot::Sender<Result<ChangeEvent>>,
    },
    /// Send feedback (standby status update) to PostgreSQL.
    SendFeedback { reply: oneshot::Sender<Result<()>> },
    /// Get the current LSN position.
    CurrentLsn { reply: oneshot::Sender<u64> },
    /// Gracefully stop the replication stream.
    Stop { reply: oneshot::Sender<Result<()>> },
}

// ---------------------------------------------------------------------------
// Actor handle — the public, Send + Sync interface
// ---------------------------------------------------------------------------

/// A `Send + Sync` handle to the replication actor.
///
/// All interaction with the underlying `LogicalReplicationStream` goes through message-passing, so no `Sync` bound is required on the stream itself.
pub struct ReplicationActorHandle {
    pub cmd_tx: mpsc::Sender<ActorCommand>,
    /// Thread join handle so the caller can wait for the actor to shut down.
    join_handle: Option<std::thread::JoinHandle<()>>,
    /// Shared LSN feedback — thread-safe, can be used directly from any task.
    pub shared_lsn_feedback: Arc<SharedLsnFeedback>,
}

impl ReplicationActorHandle {
    /// Spawn a new actor that owns the given `LogicalReplicationStream`.
    ///
    /// It's own single-threaded tokio runtime, so libpq's `*mut PGconn` never needs to be `Send`/`Sync`.
    pub fn spawn(stream: LogicalReplicationStream) -> Self {
        let shared_lsn_feedback = stream.shared_lsn_feedback.clone();
        // Bounded command channel — back-pressure if the actor is busy.
        let (cmd_tx, cmd_rx) = mpsc::channel::<ActorCommand>(32);

        let join_handle = std::thread::Builder::new()
            .name("replication-actor".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create actor runtime");

                rt.block_on(actor_loop(stream, cmd_rx));
            })
            .expect("Failed to spawn replication actor thread");

        Self {
            cmd_tx,
            join_handle: Some(join_handle),
            shared_lsn_feedback,
        }
    }

    /// Start the replication stream.
    pub async fn start(&self, start_lsn: Option<u64>) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::Start {
                start_lsn,
                reply: tx,
            })
            .await
            .map_err(|_| CdcError::generic("Replication actor has shut down"))?;
        rx.await
            .map_err(|_| CdcError::generic("Replication actor dropped reply"))?
    }

    /// Read the next replication event (blocks until one is available or cancelled).
    pub async fn next_event(&self, cancel: &CancellationToken) -> Result<ChangeEvent> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::NextEvent {
                cancel: cancel.clone(),
                reply: tx,
            })
            .await
            .map_err(|_| CdcError::generic("Replication actor has shut down"))?;
        rx.await
            .map_err(|_| CdcError::generic("Replication actor dropped reply"))?
    }

    /// Send feedback / standby status update to PostgreSQL.
    pub async fn send_feedback(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::SendFeedback { reply: tx })
            .await
            .map_err(|_| CdcError::generic("Replication actor has shut down"))?;
        rx.await
            .map_err(|_| CdcError::generic("Replication actor dropped reply"))?
    }

    /// Get the current LSN position.
    pub async fn current_lsn(&self) -> Result<u64> {
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(ActorCommand::CurrentLsn { reply: tx })
            .await
            .map_err(|_| CdcError::generic("Replication actor has shut down"))?;
        rx.await
            .map_err(|_| CdcError::generic("Replication actor dropped reply"))
    }

    /// Gracefully stop the replication stream and shut down the actor thread.
    pub async fn stop(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        // If the actor is already gone, that's OK.
        let _ = self.cmd_tx.send(ActorCommand::Stop { reply: tx }).await;
        rx.await
            .map_err(|_| CdcError::generic("Replication actor dropped reply"))?
    }

    /// Wait for the actor thread to exit (call after `stop`).
    pub fn join(&mut self) {
        if let Some(handle) = self.join_handle.take() {
            if let Err(e) = handle.join() {
                error!("Replication actor thread panicked: {:?}", e);
            }
        }
    }
}

impl Drop for ReplicationActorHandle {
    fn drop(&mut self) {
        // Best-effort: try to tell the actor to stop. If the channel is already closed the actor is gone and we don't need to do anything.
        let _ = self.cmd_tx.try_send(ActorCommand::Stop {
            reply: oneshot::channel().0,
        });
        self.join();
    }
}

// ---------------------------------------------------------------------------
// Actor event loop — runs on the dedicated thread
// ---------------------------------------------------------------------------

async fn actor_loop(
    mut stream: LogicalReplicationStream,
    mut cmd_rx: mpsc::Receiver<ActorCommand>,
) {
    info!("Replication actor started on dedicated thread");

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ActorCommand::Start { start_lsn, reply } => {
                let result = stream.start(start_lsn).await.map_err(CdcError::from);
                let _ = reply.send(result);
            }

            ActorCommand::NextEvent { cancel, reply } => {
                let result = stream
                    .next_event_with_retry(&cancel)
                    .await
                    .map_err(CdcError::from);
                let _ = reply.send(result);
            }

            ActorCommand::SendFeedback { reply } => {
                let result = stream.send_feedback().await.map_err(CdcError::from);
                let _ = reply.send(result);
            }

            ActorCommand::CurrentLsn { reply } => {
                let _ = reply.send(stream.current_lsn());
            }

            ActorCommand::Stop { reply } => {
                info!("Replication actor received stop command");
                let result = stream.stop().await.map_err(CdcError::from);
                let _ = reply.send(result);
                break; // Exit the actor loop
            }
        }
    }

    debug!("Replication actor loop exited");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that `ReplicationActorHandle` is Send + Sync (compile-time check).
    fn _assert_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<ReplicationActorHandle>();
        assert_sync::<ReplicationActorHandle>();
    }
}
