use crate::error::Result;
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::transaction_manager::{PendingTransactionFile, TransactionManager};
use crate::types::{EventType, Lsn};
use chrono::{DateTime, Utc};
use pg_walstream::LogicalReplicationStream;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

/// Commit a transaction file and notify the consumer.
///
/// Moves the transaction from `sql_received_tx/` to `sql_pending_tx/`, reads the
/// resulting metadata, and sends a notification through the mpsc channel so the
/// consumer can pick it up for execution.
pub(crate) async fn handle_transaction_commit(
    transaction_id: u32,
    event_lsn: Option<Lsn>,
    transaction_type: &str,
    transaction_file_manager: &Arc<TransactionManager>,
    commit_notifier: &mpsc::Sender<PendingTransactionFile>,
    metrics_collector: &Arc<MetricsCollector>,
) -> Result<()> {
    match transaction_file_manager
        .commit_transaction(transaction_id, event_lsn)
        .await
    {
        Ok((pending_path, metadata)) => {
            info!(
                "Committed {} transaction file to: {:?} (commit_lsn: {:?})",
                transaction_type, pending_path, event_lsn
            );

            let notification = PendingTransactionFile {
                file_path: pending_path,
                metadata,
            };
            if let Err(e) = commit_notifier.send(notification).await {
                warn!(
                    "Failed to send commit notification to consumer: {}. Consumer may have stopped.",
                    e
                );
            }
            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to commit {} transaction file for tx {}: {}",
                transaction_type, transaction_id, e
            );
            metrics_collector.record_error("transaction_file_commit_failed", "producer");
            Err(e)
        }
    }
}

/// File-based producer task: reads events from the PostgreSQL replication stream
/// and persists them as transaction files.
///
/// The producer collects events between BEGIN and COMMIT, writing them to
/// transaction files in `sql_data_tx/`. On COMMIT the metadata is moved from
/// `sql_received_tx/` to `sql_pending_tx/`, making it available for the consumer.
///
/// ## Shutdown Coordination
///
/// When the cancellation token fires:
/// 1. Producer flushes buffers and drops the mpsc sender ("no more messages")
/// 2. Sends oneshot signal to notify consumer it has stopped
/// 3. Consumer then drains mpsc channel and processes remaining transactions
pub(crate) async fn run_producer(
    replication_stream: Arc<Mutex<LogicalReplicationStream>>,
    cancellation_token: CancellationToken,
    start_lsn: Lsn,
    metrics_collector: Arc<MetricsCollector>,
    transaction_file_manager: Arc<TransactionManager>,
    commit_notifier: mpsc::Sender<PendingTransactionFile>,
    producer_shutdown_signal: oneshot::Sender<()>,
) -> Result<()> {
    info!(
        "Starting replication producer, last time start_lsn: {}",
        start_lsn
    );

    metrics_collector.update_source_connection_status(true);

    // Restore active transaction files from sql_received_tx/
    let active_tx_files = transaction_file_manager
        .restore_received_transactions()
        .await?;

    info!(
        "Producer starting with {} active transaction file(s) from sql_received_tx/",
        active_tx_files.len()
    );

    let mut current_normal_tx: Option<(u32, DateTime<Utc>)> = None;
    let mut streaming_txs: HashMap<u32, DateTime<Utc>> = HashMap::new();
    let mut active_streaming_dml_txid: Option<u32> = None;

    // Restore transactions from active_tx_files based on their type
    for metadata in active_tx_files {
        let tx_id = metadata.transaction_id;
        let timestamp = metadata.commit_timestamp;

        if metadata.transaction_type == "streaming" {
            streaming_txs.insert(tx_id, timestamp);
            info!(
                "Restored streaming transaction {} from sql_received_tx/",
                tx_id
            );
        } else {
            if current_normal_tx.is_some() {
                warn!(
                    "Multiple normal transactions found in sql_received_tx/, only one expected. Keeping tx {}",
                    tx_id
                );
            }
            current_normal_tx = Some((tx_id, timestamp));
            info!(
                "Restored normal transaction {} from sql_received_tx/",
                tx_id
            );
        }
    }

    while !cancellation_token.is_cancelled() {
        let event_result = {
            let mut stream = replication_stream.lock().await;
            stream.next_event_with_retry(&cancellation_token).await
        };

        match event_result {
            Ok(event) => {
                metrics_collector.record_received_lsn(event.lsn.0);
                metrics_collector.record_event(&event);

                match &event.event_type {
                    EventType::Begin {
                        transaction_id,
                        commit_timestamp,
                        ..
                    } => {
                        debug!(
                            "BEGIN: transaction_id={}, commit_timestamp={}",
                            transaction_id, commit_timestamp
                        );

                        if current_normal_tx.is_some() {
                            warn!("BEGIN received while normal transaction already active - replacing");
                        }

                        match transaction_file_manager
                            .begin_transaction(*transaction_id, *commit_timestamp, "normal")
                            .await
                        {
                            Ok(_) => {
                                current_normal_tx = Some((*transaction_id, *commit_timestamp));
                                debug!("Created normal transaction file for tx {}", transaction_id);
                            }
                            Err(e) => {
                                error!(
                                    "Failed to create transaction file for tx {}: {}",
                                    transaction_id, e
                                );
                                metrics_collector
                                    .record_error("transaction_file_create_failed", "producer");
                                return Err(e);
                            }
                        }
                    }

                    EventType::Commit { .. } => {
                        if let Some((tx_id, _)) = current_normal_tx.take() {
                            info!("Producer: Committing normal transaction {}", tx_id);

                            if let Err(e) = handle_transaction_commit(
                                tx_id,
                                Some(event.lsn),
                                "normal",
                                &transaction_file_manager,
                                &commit_notifier,
                                &metrics_collector,
                            )
                            .await
                            {
                                error!(
                                    "Failed to handle normal transaction commit for tx {}: {}",
                                    tx_id, e
                                );
                                return Err(e);
                            }
                        } else {
                            warn!("Received COMMIT without active normal transaction, ignoring");
                        }
                    }

                    EventType::StreamStart {
                        transaction_id,
                        first_segment,
                    } => {
                        debug!(
                            "StreamStart: transaction_id={}, first_segment={}",
                            transaction_id, first_segment
                        );

                        active_streaming_dml_txid = Some(*transaction_id);

                        if *first_segment {
                            let timestamp = chrono::Utc::now();
                            match transaction_file_manager
                                .begin_transaction(*transaction_id, timestamp, "streaming")
                                .await
                            {
                                Ok(_) => {
                                    debug!(
                                        "Created streaming transaction file for tx {}",
                                        transaction_id
                                    );
                                    streaming_txs.insert(*transaction_id, timestamp);
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to create streaming transaction file for tx {}: {}",
                                        transaction_id, e
                                    );
                                    metrics_collector
                                        .record_error("transaction_file_create_failed", "producer");
                                    return Err(e);
                                }
                            }
                        }
                    }

                    EventType::StreamStop => {
                        if let Some(txid) = active_streaming_dml_txid {
                            info!("Producer: StreamStop for streaming transaction {}", txid);
                            active_streaming_dml_txid = None;
                        } else {
                            warn!("Producer: StreamStop received but no active streaming DML transaction");
                        }
                    }

                    EventType::StreamCommit {
                        transaction_id,
                        commit_timestamp: _,
                        ..
                    } => {
                        info!("Producer: StreamCommit for transaction {}", transaction_id);

                        if streaming_txs.remove(transaction_id).is_some() {
                            if let Err(e) = handle_transaction_commit(
                                *transaction_id,
                                Some(event.lsn),
                                "streaming",
                                &transaction_file_manager,
                                &commit_notifier,
                                &metrics_collector,
                            )
                            .await
                            {
                                error!(
                                    "Failed to handle streaming transaction commit for tx {}: {}",
                                    transaction_id, e
                                );
                                return Err(e);
                            }
                        } else {
                            warn!("StreamCommit for unknown transaction {}", transaction_id);
                        }
                    }

                    EventType::StreamAbort { transaction_id, .. } => {
                        debug!("StreamAbort: transaction_id={}", transaction_id);

                        if let Some(timestamp) = streaming_txs.remove(transaction_id) {
                            match transaction_file_manager
                                .abort_transaction(*transaction_id, timestamp)
                                .await
                            {
                                Ok(_) => {
                                    debug!(
                                        "Aborted streaming transaction file for tx {}",
                                        transaction_id
                                    );
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to abort transaction file for tx {}: {}",
                                        transaction_id, e
                                    );
                                    metrics_collector
                                        .record_error("transaction_file_abort_failed", "producer");
                                }
                            }
                        } else {
                            warn!("StreamAbort for unknown transaction {}", transaction_id);
                        }
                    }

                    EventType::Insert { .. }
                    | EventType::Update { .. }
                    | EventType::Delete { .. }
                    | EventType::Truncate(_) => {
                        let target_tx_id = if let Some((tx_id, _)) = current_normal_tx {
                            debug!("Appending DML to normal transaction {}", tx_id);
                            Some(tx_id)
                        } else if let Some(streaming_txid) = active_streaming_dml_txid {
                            if streaming_txs.contains_key(&streaming_txid) {
                                debug!("Appending DML to streaming transaction {}", streaming_txid);
                                Some(streaming_txid)
                            } else {
                                error!(
                                    "Active streaming DML txid {} not found in streaming_txs map",
                                    streaming_txid
                                );
                                None
                            }
                        } else {
                            warn!(
                                "Received DML event with no active transaction (normal or streaming): {:?}",
                                event.event_type
                            );
                            None
                        };

                        if let Some(tx_id) = target_tx_id {
                            if let Err(e) =
                                transaction_file_manager.append_event(tx_id, &event).await
                            {
                                error!("Failed to append event to transaction {}: {}", tx_id, e);
                                metrics_collector
                                    .record_error("transaction_file_append_failed", "producer");
                                return Err(e);
                            }
                        }
                    }

                    _ => {
                        debug!("Skipping metadata event: {:?}", event.event_type);
                    }
                }
            }
            Err(e) => {
                error!("Error reading from replication stream: {}", e);
                metrics_collector.record_error("replication_stream_error", "producer");
                break;
            }
        }
    }

    info!("File-based producer shutting down");

    if let Err(e) = transaction_file_manager.flush_all_buffers().await {
        error!("Failed to flush buffers during shutdown: {}", e);
        metrics_collector.record_error("buffer_flush_failed", "producer");
    } else {
        info!("Successfully flushed all buffers");
    }

    let total_incomplete = if let Some((tx_id, _)) = current_normal_tx {
        info!(
            "Producer shutdown with incomplete normal transaction {}",
            tx_id
        );
        1 + streaming_txs.len()
    } else {
        streaming_txs.len()
    };

    if total_incomplete > 0 {
        info!(
            "Producer shutdown with {} incomplete transaction(s) in sql_received_tx/ (will be recovered on restart)",
            total_incomplete
        );
    }

    metrics_collector.update_source_connection_status(false);

    if producer_shutdown_signal.send(()).is_err() {
        warn!("Producer: Failed to send shutdown signal (consumer may have already stopped)");
    } else {
        info!("Producer: Sent shutdown notification to consumer");
    }

    info!("Producer shutdown complete");
    Ok(())
}
