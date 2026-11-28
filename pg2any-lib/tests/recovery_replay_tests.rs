//! Startup recovery re-applies the batch that was in flight when the process was killed. When the
//! destination had already committed it, its inserts hit duplicate keys: recovery must skip them
//! (and only recovery: a live duplicate stays an error).
#![cfg(feature = "sqlite")]

use chrono::Utc;
use pg2any_lib::destinations::{DestinationHandler, SQLiteDestination};
use pg2any_lib::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use pg2any_lib::transaction_manager::{PendingTransactionFile, TransactionManager};
use pg2any_lib::types::{DestinationType, Lsn};
use pg2any_lib::MetricsCollectorTrait;
use pg2any_lib::{ConsumerSettings, GROUP_COMMIT_MAX_TXNS};
use pg_walstream::{ChangeEvent, ColumnValue, ReplicaIdentity, RowData};
use sqlx::SqlitePool;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

const COMMIT_LSN: u64 = 5_000;

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

/// A committed transaction of `events`, a SQLite destination that already holds its result (as if
/// the server finished the COMMIT after the client was killed; `setup` seeds the table first) and
/// the files restored to their pre-apply state (as if the crash came before the checkpoint and
/// LSN were persisted).
struct InDoubt {
    dir: TempDir,
    manager: Arc<TransactionManager>,
    pending: PendingTransactionFile,
    destination: Box<dyn DestinationHandler>,
    pool: SqlitePool,
}

fn row(id: i64) -> RowData {
    RowData::from_pairs(vec![
        ("id", ColumnValue::text(&id.to_string())),
        ("name", ColumnValue::text(&format!("row-{id}"))),
    ])
}

fn insert(id: i64) -> ChangeEvent {
    ChangeEvent::insert("public", "items", 1, row(id), Lsn::from(COMMIT_LSN))
}

fn three_inserts() -> Vec<ChangeEvent> {
    (1..=3).map(insert).collect()
}

async fn in_doubt(setup: &[&'static str], events: Vec<ChangeEvent>) -> InDoubt {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("dest.db");
    let manager = Arc::new(
        TransactionManager::new(
            dir.path().join("tx"),
            DestinationType::SQLite,
            None,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    manager
        .begin_transaction(1, Utc::now(), "normal")
        .await
        .unwrap();
    for event in &events {
        manager.append_event(1, event).await.unwrap();
    }
    manager.flush_all_buffers().await.unwrap();
    let (file_path, metadata) = manager
        .commit_transaction(1, Some(Lsn(COMMIT_LSN)))
        .await
        .unwrap();
    let pending = PendingTransactionFile {
        file_path,
        metadata,
    };

    let mut destination: Box<dyn DestinationHandler> = Box::new(SQLiteDestination::new());
    destination.connect(db.to_str().unwrap()).await.unwrap();
    let pool = SqlitePool::connect(&format!("sqlite://{}", db.display()))
        .await
        .unwrap();
    sqlx::query("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    for sql in setup {
        sqlx::query(*sql).execute(&pool).await.unwrap();
    }

    let backup = dir.path().join("backup");
    copy_dir(
        &dir.path().join("tx/sql_pending_tx"),
        &backup.join("pending"),
    );
    copy_dir(&dir.path().join("tx/sql_data_tx"), &backup.join("data"));
    let applied = apply(&manager, &pending, &mut destination, dir.path()).await;
    assert!(applied.is_ok(), "first apply: {applied:?}");
    copy_dir(
        &backup.join("pending"),
        &dir.path().join("tx/sql_pending_tx"),
    );
    copy_dir(&backup.join("data"), &dir.path().join("tx/sql_data_tx"));

    InDoubt {
        dir,
        manager,
        pending,
        destination,
        pool,
    }
}

/// Applies `pending` with an LSN tracker that has persisted nothing (flush position 0).
async fn apply(
    manager: &Arc<TransactionManager>,
    pending: &PendingTransactionFile,
    destination: &mut Box<dyn DestinationHandler>,
    scratch: &Path,
) -> pg2any_lib::error::Result<()> {
    apply_in_batches(manager, pending, destination, scratch, 1000).await
}

async fn apply_in_batches(
    manager: &Arc<TransactionManager>,
    pending: &PendingTransactionFile,
    destination: &mut Box<dyn DestinationHandler>,
    scratch: &Path,
    batch_size: usize,
) -> pg2any_lib::error::Result<()> {
    let lsn_file = scratch.join(format!(
        "lsn-{}.metadata",
        Utc::now().timestamp_nanos_opt().unwrap()
    ));
    let (lsn_tracker, _) = LsnTracker::new_with_load(Some(lsn_file.to_str().unwrap())).await;
    manager
        .clone()
        .process_transaction_file(
            pending,
            destination,
            &CancellationToken::new(),
            &lsn_tracker,
            &Arc::new(pg2any_lib::MetricsCollector::new()),
            batch_size,
            &SharedLsnFeedback::new_shared(),
        )
        .await
}

/// Another committed transaction (`tx_id`, commit LSN above the first one's) in the same manager.
async fn commit_file(
    manager: &Arc<TransactionManager>,
    tx_id: u32,
    events: &[ChangeEvent],
) -> PendingTransactionFile {
    manager
        .begin_transaction(tx_id, Utc::now(), "normal")
        .await
        .unwrap();
    for event in events {
        manager.append_event(tx_id, event).await.unwrap();
    }
    manager.flush_all_buffers().await.unwrap();
    let (file_path, metadata) = manager
        .commit_transaction(tx_id, Some(Lsn(COMMIT_LSN + u64::from(tx_id))))
        .await
        .unwrap();
    PendingTransactionFile {
        file_path,
        metadata,
    }
}

async fn name_of(pool: &SqlitePool, id: i64) -> Option<String> {
    sqlx::query_scalar("SELECT name FROM items WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
        .unwrap()
}

async fn row_count(pool: &SqlitePool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM items")
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn recovery_skips_the_duplicates_of_a_batch_the_destination_already_committed() {
    let mut s = in_doubt(&[], three_inserts()).await;
    assert_eq!(row_count(&s.pool).await, 3);

    s.manager.set_replay_grace(1);
    let replay = apply(&s.manager, &s.pending, &mut s.destination, s.dir.path()).await;
    s.manager.set_replay_grace(0);

    assert!(replay.is_ok(), "recovery replay must succeed: {replay:?}");
    assert_eq!(row_count(&s.pool).await, 3, "no row applied twice");
    assert!(
        !s.pending.file_path.exists(),
        "the replayed file is finalized"
    );
}

/// The in-memory fast path leaves no file of a transaction the destination committed before the
/// crash: PostgreSQL re-sends it and it arrives as live work, within the grace after startup.
#[tokio::test]
async fn a_resent_committed_batch_is_skipped_within_the_startup_grace() {
    let mut s = in_doubt(&[], three_inserts()).await;

    s.manager.set_replay_grace(1);
    let live = apply(&s.manager, &s.pending, &mut s.destination, s.dir.path()).await;

    assert!(live.is_ok(), "{live:?}");
    assert_eq!(row_count(&s.pool).await, 3, "no row applied twice");
}

#[tokio::test]
async fn a_live_duplicate_is_still_an_error() {
    let mut s = in_doubt(&[], three_inserts()).await;

    let live = apply(&s.manager, &s.pending, &mut s.destination, s.dir.path()).await;

    let err = live.expect_err("outside recovery a duplicate key must fail the batch");
    assert!(
        err.to_string().contains("UNIQUE constraint failed"),
        "{err}"
    );
    assert!(s.pending.file_path.exists(), "the file stays for the retry");
}

async fn ids(pool: &SqlitePool) -> Vec<i64> {
    sqlx::query_scalar("SELECT id FROM items ORDER BY id")
        .fetch_all(pool)
        .await
        .unwrap()
}

/// `DELETE id=2; UPDATE id=1 SET id=2; INSERT id=3` on rows {1, 2}, already committed: replaying it
/// statement by statement would delete the renamed row 2 again and skip the rest.
fn delete_rename_insert() -> Vec<ChangeEvent> {
    let key = || vec![Arc::<str>::from("id")];
    vec![
        ChangeEvent::delete(
            "public",
            "items",
            1,
            row(2),
            ReplicaIdentity::Default,
            key(),
            Lsn::from(COMMIT_LSN),
        ),
        ChangeEvent::update(
            "public",
            "items",
            1,
            Some(row(1)),
            row(2),
            ReplicaIdentity::Default,
            key(),
            Lsn::from(COMMIT_LSN),
        ),
        insert(3),
    ]
}

#[tokio::test]
async fn recovery_never_replays_a_batch_with_updates_or_deletes_statement_by_statement() {
    let setup = [
        "INSERT INTO items VALUES (1, 'row-1')",
        "INSERT INTO items VALUES (2, 'row-2')",
    ];
    let mut s = in_doubt(&setup, delete_rename_insert()).await;
    assert_eq!(ids(&s.pool).await, [2, 3], "the committed result");

    s.manager.set_replay_grace(1);
    let replay = apply(&s.manager, &s.pending, &mut s.destination, s.dir.path()).await;
    s.manager.set_replay_grace(0);

    assert!(
        replay.is_err(),
        "an ambiguous batch stays pending: {replay:?}"
    );
    assert_eq!(
        ids(&s.pool).await,
        [2, 3],
        "nothing of the batch was re-applied"
    );
    assert!(
        s.pending.file_path.exists(),
        "the file stays for the operator"
    );
}

/// A MySQL-shaped destination whose bulk load always works and whose SQL batches always report a
/// duplicate key.
struct BulkOkDuplicateSql;

#[async_trait::async_trait]
impl DestinationHandler for BulkOkDuplicateSql {
    async fn connect(&mut self, _connection_string: &str) -> pg2any_lib::error::Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: std::collections::HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _pre_commit_hook: Option<pg2any_lib::destinations::PreCommitHook>,
    ) -> pg2any_lib::error::Result<()> {
        Err(pg2any_lib::error::CdcError::generic(
            "MySQL execute_sql_batch failed at command 1/1: Server error: \
             `ERROR 1062 (23000): Duplicate entry '1' for key 'items.PRIMARY''",
        ))
    }

    fn supports_bulk_insert(&self) -> bool {
        true
    }

    async fn execute_bulk_insert_with_hook(
        &mut self,
        _table: &str,
        _columns: &[String],
        _rows: &[Vec<String>],
        pre_commit_hook: Option<pg2any_lib::destinations::PreCommitHook>,
    ) -> pg2any_lib::error::Result<()> {
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> pg2any_lib::error::Result<()> {
        Ok(())
    }
}

/// The startup grace is spent once per batch, also by a batch that bulk-loads and never reaches
/// the SQL path: otherwise it stays open and later hides a genuine duplicate.
#[tokio::test]
async fn a_bulk_loaded_batch_spends_the_startup_grace() {
    let dir = tempfile::tempdir().unwrap();
    let mut manager = TransactionManager::new(
        dir.path().join("tx"),
        DestinationType::MySQL,
        None,
        64 * 1024 * 1024,
    )
    .await
    .unwrap();
    manager.set_bulk_insert_config(2);
    let manager = Arc::new(manager);

    let mut files = Vec::new();
    for (tx_id, events) in [(1u32, three_inserts()), (2, vec![insert(9)])] {
        manager
            .begin_transaction(tx_id, Utc::now(), "normal")
            .await
            .unwrap();
        for event in &events {
            manager.append_event(tx_id, event).await.unwrap();
        }
        manager.flush_all_buffers().await.unwrap();
        let (file_path, metadata) = manager
            .commit_transaction(tx_id, Some(Lsn(COMMIT_LSN + u64::from(tx_id))))
            .await
            .unwrap();
        files.push(PendingTransactionFile {
            file_path,
            metadata,
        });
    }

    let mut destination: Box<dyn DestinationHandler> = Box::new(BulkOkDuplicateSql);
    manager.set_replay_grace(1);

    let bulk = apply(&manager, &files[0], &mut destination, dir.path()).await;
    assert!(bulk.is_ok(), "the bulk load succeeds: {bulk:?}");

    let later = apply(&manager, &files[1], &mut destination, dir.path()).await;
    let err = later.expect_err("the grace was spent by the bulk batch: a duplicate is an error");
    assert!(err.to_string().contains("Duplicate entry"), "{err}");
}

/// The grace covers the transactions a crash can leave in doubt (the first ones after the resume
/// position), not every file of the recovery: a later file that hits a duplicate key was never
/// applied, so its duplicate is a genuine conflict and must fail instead of being skipped.
#[tokio::test]
async fn the_grace_of_one_covers_the_in_flight_file_and_not_the_next_one() {
    let mut s = in_doubt(&[], three_inserts()).await;
    let second = commit_file(&s.manager, 2, &[insert(10), insert(11)]).await;
    sqlx::query("INSERT INTO items VALUES (10, 'DESTINATION-OWNED')")
        .execute(&s.pool)
        .await
        .unwrap();

    s.manager.set_replay_grace(1);
    let first = apply(&s.manager, &s.pending, &mut s.destination, s.dir.path()).await;
    assert!(first.is_ok(), "the in-flight file is replayed: {first:?}");

    let next = apply(&s.manager, &second, &mut s.destination, s.dir.path()).await;
    let err = next.expect_err("the second file is outside the grace: a duplicate is an error");
    assert!(
        err.to_string().contains("UNIQUE constraint failed"),
        "{err}"
    );
    assert_eq!(
        name_of(&s.pool, 10).await.as_deref(),
        Some("DESTINATION-OWNED")
    );
    assert_eq!(
        name_of(&s.pool, 11).await,
        None,
        "the failed batch rolled back"
    );
    assert!(second.file_path.exists(), "the file stays for the operator");
}

/// With group commit one group of transactions commits in a single destination transaction, so up
/// to `group_commit_max_txns` files can be in doubt: the grace is that many batches.
#[tokio::test]
async fn the_grace_of_n_covers_n_in_flight_files() {
    let mut s = in_doubt(&[], three_inserts()).await;
    let second = commit_file(&s.manager, 2, &[insert(4), insert(5)]).await;
    let third = commit_file(&s.manager, 3, &[insert(10)]).await;
    for sql in [
        "INSERT INTO items VALUES (4, 'row-4'), (5, 'row-5')",
        "INSERT INTO items VALUES (10, 'DESTINATION-OWNED')",
    ] {
        sqlx::query(sql).execute(&s.pool).await.unwrap();
    }

    s.manager.set_replay_grace(2);
    for (name, file) in [("first", &s.pending), ("second", &second)] {
        let applied = apply(&s.manager, file, &mut s.destination, s.dir.path()).await;
        assert!(applied.is_ok(), "the {name} in-flight file: {applied:?}");
    }
    let applied = apply(&s.manager, &third, &mut s.destination, s.dir.path()).await;
    assert!(applied.is_err(), "the third file is outside the grace");
    assert_eq!(
        name_of(&s.pool, 10).await.as_deref(),
        Some("DESTINATION-OWNED")
    );
}

/// A file with several batches: the crash left at most the first unfinished one in doubt, so a
/// duplicate in a later batch of the same file is a genuine conflict.
#[tokio::test]
async fn the_grace_of_one_covers_the_first_batch_of_a_file_only() {
    let mut s = in_doubt(&[], (1..=4).map(insert).collect()).await;
    // Batch 1 (ids 1, 2) is committed; batch 2 (ids 3, 4) was never applied and id 3 conflicts.
    for sql in [
        "DELETE FROM items WHERE id IN (3, 4)",
        "INSERT INTO items VALUES (3, 'DESTINATION-OWNED')",
    ] {
        sqlx::query(sql).execute(&s.pool).await.unwrap();
    }

    s.manager.set_replay_grace(1);
    let applied =
        apply_in_batches(&s.manager, &s.pending, &mut s.destination, s.dir.path(), 2).await;

    let err = applied.expect_err("batch 2 is outside the grace");
    assert!(
        err.to_string().contains("UNIQUE constraint failed"),
        "{err}"
    );
    assert_eq!(
        name_of(&s.pool, 3).await.as_deref(),
        Some("DESTINATION-OWNED")
    );
    assert_eq!(name_of(&s.pool, 4).await, None, "batch 2 rolled back");
    assert!(
        s.pending.file_path.exists(),
        "the file stays for the operator"
    );
}

/// Delegates to a real destination but fails its first `failures` SQL batches with a connection
/// error: the batch never reaches the server.
struct FailsFirst {
    inner: Box<dyn DestinationHandler>,
    failures: usize,
}

#[async_trait::async_trait]
impl DestinationHandler for FailsFirst {
    async fn connect(&mut self, connection_string: &str) -> pg2any_lib::error::Result<()> {
        self.inner.connect(connection_string).await
    }

    fn set_schema_mappings(&mut self, mappings: std::collections::HashMap<String, String>) {
        self.inner.set_schema_mappings(mappings);
    }

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<pg2any_lib::destinations::PreCommitHook>,
    ) -> pg2any_lib::error::Result<()> {
        if self.failures > 0 {
            self.failures -= 1;
            return Err(pg2any_lib::error::CdcError::generic(
                "connection reset by peer",
            ));
        }
        self.inner
            .execute_sql_batch_with_hook(commands, pre_commit_hook)
            .await
    }

    async fn close(&mut self) -> pg2any_lib::error::Result<()> {
        self.inner.close().await
    }
}

/// The grace counts batches that completed: retries after a connection error (the consumer
/// retries with backoff, a failed group is also retried one by one) must not burn it, or the
/// in-flight batch the destination already committed becomes a permanent duplicate-key error.
#[tokio::test]
async fn failed_attempts_do_not_spend_the_startup_grace() {
    let s = in_doubt(&[], three_inserts()).await;
    let second = commit_file(&s.manager, 2, &[insert(10), insert(11)]).await;
    sqlx::query("INSERT INTO items VALUES (10, 'DESTINATION-OWNED')")
        .execute(&s.pool)
        .await
        .unwrap();
    let mut destination: Box<dyn DestinationHandler> = Box::new(FailsFirst {
        inner: s.destination,
        failures: 3,
    });

    s.manager.set_replay_grace(1);
    for attempt in 1..=3 {
        let failed = apply(&s.manager, &s.pending, &mut destination, s.dir.path()).await;
        assert!(
            failed.is_err(),
            "attempt {attempt} hits the connection error"
        );
    }

    let replay = apply(&s.manager, &s.pending, &mut destination, s.dir.path()).await;
    assert!(
        replay.is_ok(),
        "the in-flight batch is still covered by the grace: {replay:?}"
    );

    let next = apply(&s.manager, &second, &mut destination, s.dir.path()).await;
    assert!(
        next.is_err(),
        "the completed batch spent the grace: a later duplicate is an error"
    );
    assert_eq!(row_count(&s.pool).await, 4, "ids 1-3 and 10, no row twice");
}

/// Through the consumer: a group that fails on a connection error is retried one by one, and the
/// whole round is retried after a backoff. None of those attempts may use up the grace, or the
/// in-flight batch the destination already committed (the first transaction of the backlog)
/// becomes a permanent duplicate-key error and the drain never finishes.
#[tokio::test]
async fn a_failed_group_and_its_one_by_one_retry_do_not_spend_the_startup_grace() {
    let s = in_doubt(&[], three_inserts()).await;
    let second = commit_file(&s.manager, 2, &[insert(10), insert(11)]).await;
    let mut destination: Box<dyn DestinationHandler> = Box::new(FailsFirst {
        inner: s.destination,
        failures: 3,
    });
    s.manager.set_replay_grace(1);

    let mut queue = BinaryHeap::new();
    queue.push(Reverse(s.pending));
    queue.push(Reverse(second));
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(1);
    drop(sender);
    let lsn_file = s.dir.path().join("consumer-lsn.metadata");
    let (lsn_tracker, _) = LsnTracker::new_with_load(Some(lsn_file.to_str().unwrap())).await;
    let settings = ConsumerSettings {
        batch_size: 1000,
        group_commit_max_txns: GROUP_COMMIT_MAX_TXNS,
    };

    // One 2 s backoff separates the two rounds; a spent grace would retry forever.
    let drained = tokio::time::timeout(
        Duration::from_secs(20),
        pg2any_lib::drain_and_shutdown(
            &mut queue,
            &mut receiver,
            &s.manager,
            &mut destination,
            &lsn_tracker,
            &Arc::new(pg2any_lib::MetricsCollector::new()),
            &settings,
            &SharedLsnFeedback::new_shared(),
        ),
    )
    .await;

    assert!(
        drained.is_ok(),
        "the in-flight duplicate stayed an error: the grace was spent by failed attempts"
    );
    assert_eq!(row_count(&s.pool).await, 5, "ids 1-3 once, plus 10 and 11");
    assert_eq!(lsn_tracker.get(), COMMIT_LSN + 2);
}
