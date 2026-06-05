//! Regression test for the shutdown / no-replay invariant.
//!
//! We use the simpler unit-style approach (per the spec): construct a
//! `PendingTransactionFile` whose `commit_lsn` is already covered by the
//! `LsnTracker`'s `flush_lsn`, invoke `process_transaction_file`, and assert
//! that the destination handler is NOT invoked. This exercises the core
//! "already-applied" skip path in `transaction_manager.rs:1763` that prevents
//! replay after a crash/restart at the on-disk LSN.
//!
//! Wiring up a full producer/consumer integration would require significant
//! scaffolding for the LogicalReplicationStream; the unit-style test covers
//! the actual anti-replay invariant from requirement #5 directly.

use async_trait::async_trait;
use chrono::Utc;
use pg2any_lib::destinations::{DestinationHandler, PreCommitHook};
use pg2any_lib::transaction_manager::{
    PendingTransactionFile, TransactionFileMetadata, TransactionManager,
};
use pg2any_lib::{DestinationType, LsnTracker, MetricsCollectorTrait, SharedLsnFeedback};
use pg_walstream::Lsn;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;
use tokio_util::sync::CancellationToken;

type ApplyLog = Arc<Mutex<Vec<(u32, Option<Lsn>)>>>;

struct RecordingHandler {
    log: ApplyLog,
}

#[async_trait]
impl DestinationHandler for RecordingHandler {
    async fn connect(&mut self, _connection_string: &str) -> pg2any_lib::CdcResult<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _hook: Option<PreCommitHook>,
    ) -> pg2any_lib::CdcResult<()> {
        // Recording placeholder: a real call would push (tx_id, commit_lsn).
        // For the skip path we expect this to never be called.
        self.log.lock().unwrap().push((u32::MAX, None));
        Ok(())
    }

    async fn close(&mut self) -> pg2any_lib::CdcResult<()> {
        Ok(())
    }
}

#[tokio::test]
async fn test_skip_already_applied_transaction_no_replay() {
    let dir = tempdir().unwrap();
    let base = dir.path();

    let manager = TransactionManager::new(base, DestinationType::SQLite, None, 64 * 1024 * 1024)
        .await
        .unwrap();
    let manager = Arc::new(manager);

    // Pre-populate the LSN tracker so flush_lsn is ahead of the pending txn's commit_lsn.
    let lsn_file = base.join("lsn.metadata");
    let tracker = LsnTracker::new(Some(lsn_file.to_str().unwrap())).await;
    tracker.commit_lsn(200);
    assert_eq!(tracker.get(), 200);

    // Construct a pending file that should be skipped (commit_lsn=100 <= flush_lsn=200).
    let pending_path = base.join("pg2any_pending_tx").join("tx_42.metadata");
    let pending = PendingTransactionFile {
        file_path: pending_path,
        metadata: TransactionFileMetadata {
            transaction_id: 42,
            commit_timestamp: Utc::now(),
            commit_lsn: Some(Lsn(100)),
            destination_type: DestinationType::SQLite,
            segments: Vec::new(),
            current_segment_index: 0,
            last_executed_command_index: None,
            last_update_timestamp: None,
            transaction_type: "normal".to_string(),
        },
    };

    let log: ApplyLog = Arc::new(Mutex::new(Vec::new()));
    let mut handler: Box<dyn DestinationHandler> = Box::new(RecordingHandler { log: log.clone() });

    let cancel = CancellationToken::new();
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let feedback = SharedLsnFeedback::new_shared();

    let result = manager
        .clone()
        .process_transaction_file(
            &pending,
            &mut handler,
            &cancel,
            &tracker,
            &metrics,
            100,
            &feedback,
        )
        .await;

    assert!(result.is_ok(), "skip path returns Ok: {result:?}");
    assert!(
        log.lock().unwrap().is_empty(),
        "handler must not be invoked for already-applied transaction"
    );
}
