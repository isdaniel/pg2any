use async_trait::async_trait;
use chrono::Utc;
use pg2any_lib::destinations::{DestinationHandler, PreCommitHook};
use pg2any_lib::error::{CdcError, Result};
use pg2any_lib::lsn_tracker::{LsnTracker, SharedLsnFeedback};
use pg2any_lib::transaction_manager::{PendingTransactionFile, TransactionManager};
use pg2any_lib::types::{DestinationType, Lsn};
use pg2any_lib::MetricsCollectorTrait;
use pg2any_lib::{ConsumerSettings, GROUP_COMMIT_MAX_TXNS};
use pg_walstream::{ChangeEvent, ColumnValue, EventType, RowData};
use std::collections::{BTreeSet, BinaryHeap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

/// Records every batch it is asked to apply. Calls listed in `fail_calls` (1-based) fail;
/// with `hook_before_fail` the pre-commit hook runs first, like a COMMIT that fails after the
/// checkpoint was staged.
struct Recorder {
    calls: Arc<Mutex<Vec<Vec<String>>>>,
    fail_calls: BTreeSet<usize>,
    fail_all: bool,
    hook_before_fail: bool,
    counter: usize,
}

impl Recorder {
    fn new(fail_calls: &[usize]) -> (Self, Arc<Mutex<Vec<Vec<String>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                calls: calls.clone(),
                fail_calls: fail_calls.iter().copied().collect(),
                fail_all: false,
                hook_before_fail: false,
                counter: 0,
            },
            calls,
        )
    }
}

#[async_trait]
impl DestinationHandler for Recorder {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        self.counter += 1;
        self.calls.lock().unwrap().push(commands.to_vec());

        if self.fail_all || self.fail_calls.contains(&self.counter) {
            if self.hook_before_fail {
                if let Some(hook) = pre_commit_hook {
                    hook().await?;
                }
            }
            return Err(CdcError::generic("simulated destination failure"));
        }
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

fn insert_event(id: usize, lsn: u64) -> ChangeEvent {
    let data = RowData::from_pairs(vec![
        ("id", ColumnValue::text(&id.to_string())),
        ("name", ColumnValue::text(&format!("row-{id}"))),
    ]);
    ChangeEvent::insert("public", "test", 12345, data, Lsn::from(lsn))
}

async fn new_manager(dir: &TempDir) -> Arc<TransactionManager> {
    Arc::new(
        TransactionManager::new(dir.path(), DestinationType::MySQL, None, 64 * 1024 * 1024)
            .await
            .unwrap(),
    )
}

/// Commit one transaction with one INSERT per id; commit_lsn = `lsn`.
async fn commit_tx(
    manager: &Arc<TransactionManager>,
    tx_id: u32,
    lsn: u64,
    ids: std::ops::Range<usize>,
) -> PendingTransactionFile {
    manager
        .begin_transaction(tx_id, Utc::now(), "normal")
        .await
        .unwrap();
    for id in ids {
        manager
            .append_event(tx_id, &insert_event(id, lsn))
            .await
            .unwrap();
    }
    manager.flush_all_buffers().await.unwrap();
    let (file_path, metadata) = manager
        .commit_transaction(tx_id, Some(Lsn(lsn)))
        .await
        .unwrap();
    PendingTransactionFile {
        file_path,
        metadata,
    }
}

fn settings(group: usize, batch_size: usize) -> ConsumerSettings {
    ConsumerSettings {
        batch_size,
        group_commit_max_txns: group,
    }
}

async fn drain(
    manager: &Arc<TransactionManager>,
    txs: Vec<PendingTransactionFile>,
    dest: Recorder,
    lsn_path: &str,
    settings: &ConsumerSettings,
) -> Arc<LsnTracker> {
    let mut queue = BinaryHeap::new();
    for tx in txs {
        queue.push(std::cmp::Reverse(tx));
    }
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(1);
    drop(sender);

    let mut dest: Box<dyn DestinationHandler> = Box::new(dest);
    let (lsn_tracker, _) = LsnTracker::new_with_load(Some(lsn_path)).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let feedback = SharedLsnFeedback::new_shared();

    pg2any_lib::drain_and_shutdown(
        &mut queue,
        &mut receiver,
        manager,
        &mut dest,
        &lsn_tracker,
        &metrics,
        settings,
        &feedback,
    )
    .await;
    lsn_tracker
}

fn ids_of(batch: &[String]) -> Vec<usize> {
    batch
        .iter()
        .map(|stmt| {
            let tail = stmt.split("row-").nth(1).expect("row marker");
            tail.chars()
                .take_while(|c| c.is_ascii_digit())
                .collect::<String>()
                .parse()
                .unwrap()
        })
        .collect()
}

#[tokio::test]
async fn group_commit_merges_queued_transactions_into_one_ordered_batch() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..5usize {
        txs.push(commit_tx(&manager, 100 + i as u32, 1000 + i as u64 * 10, i..i + 1).await);
    }

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 1000),
    )
    .await;

    let calls = calls.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        1,
        "five small transactions, one destination batch"
    );
    assert_eq!(
        ids_of(&calls[0]),
        vec![0, 1, 2, 3, 4],
        "commit order preserved"
    );
    assert_eq!(
        tracker.get(),
        1040,
        "flush_lsn is the last transaction's commit_lsn"
    );
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());
    let (_, on_disk) = LsnTracker::new_with_load(Some(lsn_path.to_str().unwrap())).await;
    assert_eq!(on_disk, Some(Lsn(1040)));
}

/// The cap the client uses: a backlog of small transactions larger than it is applied in groups of
/// at most that many, the rest in a last, smaller one.
#[tokio::test]
async fn the_built_in_cap_splits_a_backlog_into_groups() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let total = GROUP_COMMIT_MAX_TXNS + 5;
    let mut txs = Vec::new();
    for i in 0..total {
        txs.push(commit_tx(&manager, 3000 + i as u32, 30_000 + i as u64, i..i + 1).await);
    }

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(GROUP_COMMIT_MAX_TXNS, 1000),
    )
    .await;

    let sizes: Vec<usize> = calls.lock().unwrap().iter().map(Vec::len).collect();
    assert_eq!(sizes, vec![GROUP_COMMIT_MAX_TXNS, 5]);
    assert_eq!(tracker.get(), 30_000 + total as u64 - 1);
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn a_cap_of_one_applies_every_transaction_on_its_own() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..3usize {
        txs.push(commit_tx(&manager, 200 + i as u32, 2000 + i as u64, i..i + 1).await);
    }

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 1000),
    )
    .await;
    assert_eq!(calls.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn group_commit_respects_transaction_and_statement_caps() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..5usize {
        txs.push(commit_tx(&manager, 300 + i as u32, 3000 + i as u64, i..i + 1).await);
    }
    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn_txn_cap");
    drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(2, 1000),
    )
    .await;
    let sizes: Vec<usize> = calls.lock().unwrap().iter().map(|c| c.len()).collect();
    assert_eq!(sizes, vec![2, 2, 1]);

    // Two statements per transaction, statement cap (batch_size) 3: nothing fits together.
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..3usize {
        txs.push(commit_tx(&manager, 400 + i as u32, 4000 + i as u64, i * 2..i * 2 + 2).await);
    }
    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn_stmt_cap");
    drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 3),
    )
    .await;
    let sizes: Vec<usize> = calls.lock().unwrap().iter().map(|c| c.len()).collect();
    assert_eq!(sizes, vec![2, 2, 2]);
}

#[tokio::test]
async fn failed_group_falls_back_to_one_by_one_and_applies_everything() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..4usize {
        txs.push(commit_tx(&manager, 500 + i as u32, 5000 + i as u64, i..i + 1).await);
    }

    // Call 1 is the group batch; it fails, so the four transactions are applied singly.
    let (dest, calls) = Recorder::new(&[1]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 1000),
    )
    .await;

    let sizes: Vec<usize> = calls.lock().unwrap().iter().map(|c| c.len()).collect();
    assert_eq!(sizes, vec![4, 1, 1, 1, 1]);
    assert_eq!(tracker.get(), 5003);
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn group_commit_skips_transactions_below_flush_lsn() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = Vec::new();
    for i in 0..4usize {
        txs.push(commit_tx(&manager, 600 + i as u32, 6000 + i as u64 * 10, i..i + 1).await);
    }

    let lsn_path = dir.path().join("lsn");
    // The first two were already applied before the restart.
    let pre = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
    pre.commit_lsn(6010);
    pre.persist_async().await.unwrap();

    let (dest, calls) = Recorder::new(&[]);
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 1000),
    )
    .await;

    let calls = calls.lock().unwrap().clone();
    assert_eq!(calls.len(), 1);
    assert_eq!(
        ids_of(&calls[0]),
        vec![2, 3],
        "already-applied transactions are not re-applied"
    );
    assert_eq!(tracker.get(), 6030);
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn drain_retries_a_transient_failure_until_everything_is_applied() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let txs = vec![
        commit_tx(&manager, 700, 7000, 0..1).await,
        commit_tx(&manager, 701, 7010, 1..2).await,
    ];

    // The second call fails once; the drain backs off (2 s) and retries.
    let (dest, calls) = Recorder::new(&[2]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 1000),
    )
    .await;
    assert_eq!(calls.lock().unwrap().len(), 3);
    assert_eq!(tracker.get(), 7010);
    assert!(manager
        .list_pending_transactions()
        .await
        .unwrap()
        .is_empty());
}

/// MySQL commits implicitly at TRUNCATE, so a group holding one would not be all-or-nothing:
/// such a group is never formed and every transaction is applied on its own.
#[tokio::test]
async fn group_commit_never_merges_a_truncate_on_mysql() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let first = commit_tx(&manager, 800, 8000, 0..1).await;
    manager
        .begin_transaction(801, Utc::now(), "normal")
        .await
        .unwrap();
    let truncate = ChangeEvent {
        event_type: EventType::Truncate {
            tables: vec![Arc::from("public.test")],
            cascade: false,
            restart_identity: false,
        },
        lsn: Lsn(8010),
        metadata: None,
        xid: None,
    };
    manager.append_event(801, &truncate).await.unwrap();
    manager.flush_all_buffers().await.unwrap();
    let (file_path, metadata) = manager
        .commit_transaction(801, Some(Lsn(8010)))
        .await
        .unwrap();
    let middle = PendingTransactionFile {
        file_path,
        metadata,
    };
    let last = commit_tx(&manager, 802, 8020, 2..3).await;

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        vec![first, middle, last],
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 1000),
    )
    .await;

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 3, "one batch per transaction: {calls:?}");
    assert!(calls[1][0].starts_with("TRUNCATE"), "{calls:?}");
    assert_eq!(tracker.get(), 8020);
}

#[derive(Clone, Copy)]
enum Apply {
    Group,
    Alone,
}

/// Freezes the consumer between "destination committed" and "files deleted". The LSN file is
/// written through `<lsn file>.tmp`; making that path a FIFO blocks the persist until the test
/// reads it. While it is blocked every transaction file must still exist: a crash there replays
/// (and then skips) the transactions, whereas files deleted before the position reached the disk
/// would lose them. Returns whether every file survived while the persist was blocked.
#[cfg(unix)]
async fn files_survive_until_the_lsn_is_persisted(apply: Apply) -> bool {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let mut txs = vec![
        commit_tx(&manager, 900, 9000, 0..2).await,
        commit_tx(&manager, 901, 9010, 2..4).await,
    ];
    if matches!(apply, Apply::Alone) {
        txs.truncate(1);
    }
    let files: Vec<PathBuf> = txs
        .iter()
        .flat_map(|tx| {
            std::iter::once(tx.file_path.clone())
                .chain(tx.metadata.segments.iter().map(|s| s.path.clone()))
        })
        .collect();
    let last_lsn = txs.last().unwrap().metadata.commit_lsn.unwrap().0;

    let lsn_path = dir.path().join("lsn.metadata");
    let fifo = dir.path().join("lsn.metadata.tmp");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    let (tracker, _) = LsnTracker::new_with_load(Some(lsn_path.to_str().unwrap())).await;
    let (recorder, calls) = Recorder::new(&[]);
    let mut dest: Box<dyn DestinationHandler> = Box::new(recorder);
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let feedback = SharedLsnFeedback::new_shared();
    let token = CancellationToken::new();

    let applying = async {
        match apply {
            Apply::Group => {
                manager
                    .clone()
                    .process_transaction_group(
                        &txs, &mut dest, &token, &tracker, &metrics, &feedback,
                    )
                    .await
            }
            Apply::Alone => {
                manager
                    .clone()
                    .process_transaction_file(
                        &txs[0], &mut dest, &token, &tracker, &metrics, 1000, &feedback,
                    )
                    .await
            }
        }
    };
    let observing = async {
        for _ in 0..500 {
            if !calls.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // The destination has committed; the persist is blocked on the FIFO. Deleting the files
        // first takes microseconds, so this wait is ample to catch a wrong order.
        tokio::time::sleep(Duration::from_millis(300)).await;
        let survived = files.iter().all(|f| f.exists());
        // Unblock the persist before anything can fail, or a failed assertion would hang the
        // runtime on the blocked thread.
        let persisted = tokio::task::spawn_blocking(move || std::fs::read_to_string(fifo))
            .await
            .unwrap()
            .unwrap();
        (survived, persisted)
    };
    let (result, (survived, persisted)) = tokio::join!(applying, observing);
    result.unwrap();

    let persisted_path = dir.path().join("persisted.metadata");
    std::fs::write(&persisted_path, persisted).unwrap();
    let (_, on_disk) = LsnTracker::new_with_load(Some(persisted_path.to_str().unwrap())).await;
    assert_eq!(
        on_disk.map(|lsn| lsn.0),
        Some(last_lsn),
        "the position persisted is the last commit_lsn"
    );
    assert!(
        files.iter().all(|f| !f.exists()),
        "the files are deleted once the position is on disk"
    );
    survived
}

/// The persist-before-delete rule of group commit (CLAUDE.md): after the destination committed
/// the group, `flush_lsn` is persisted ONCE for the last `commit_lsn`, and only then are the
/// group's files deleted.
#[cfg(unix)]
#[tokio::test]
async fn group_commit_persists_the_lsn_before_deleting_the_files() {
    let survived = files_survive_until_the_lsn_is_persisted(Apply::Group).await;
    assert!(
        survived,
        "the group's files were deleted before its LSN was persisted"
    );
}

/// Same rule on the one-transaction path.
#[cfg(unix)]
#[tokio::test]
async fn single_transaction_persists_the_lsn_before_deleting_its_files() {
    let survived = files_survive_until_the_lsn_is_persisted(Apply::Alone).await;
    assert!(
        survived,
        "the transaction's files were deleted before its LSN was persisted"
    );
}

/// The drain has no time limit of its own (the process manager's stop timeout ends it): a
/// destination that keeps failing is retried with backoff, nothing is dropped, nothing advances.
#[tokio::test]
async fn drain_keeps_retrying_a_failing_destination_and_leaves_the_files() {
    let dir = TempDir::new().unwrap();
    let manager = new_manager(&dir).await;
    let txs = vec![
        commit_tx(&manager, 710, 7100, 0..1).await,
        commit_tx(&manager, 711, 7110, 1..2).await,
    ];

    let (mut dest, calls) = Recorder::new(&[]);
    dest.fail_all = true;
    let lsn_path = dir.path().join("lsn");
    // The first retry comes after a 2 s backoff: within 3 s the drain has tried twice and is
    // still going.
    let outcome = tokio::time::timeout(
        Duration::from_secs(3),
        drain(
            &manager,
            txs,
            dest,
            lsn_path.to_str().unwrap(),
            &settings(1, 1000),
        ),
    )
    .await;

    assert!(outcome.is_err(), "the drain must not give up on its own");
    assert!(calls.lock().unwrap().len() >= 2, "the failure was retried");
    assert_eq!(manager.list_pending_transactions().await.unwrap().len(), 2);
}

/// Event-mode (Kafka-style) destination: records `group:<tx ids>` or `single:<tx id>` per call.
struct EventRecorder {
    calls: Arc<Mutex<Vec<String>>>,
    fail_group: bool,
}

#[async_trait]
impl DestinationHandler for EventRecorder {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        Err(CdcError::unsupported("event mode only"))
    }

    fn supports_event_mode(&self) -> bool {
        true
    }

    async fn execute_events_batch_with_hook(
        &mut self,
        events: &[ChangeEvent],
        transaction_id: u32,
        _commit_timestamp: chrono::DateTime<Utc>,
        _commit_lsn: Option<Lsn>,
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("single:{transaction_id}:{}", events.len()));
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn execute_events_group_with_hook(
        &mut self,
        group: &[pg2any_lib::destinations::EventTransaction<'_>],
    ) -> Result<()> {
        let ids: Vec<String> = group
            .iter()
            .map(|t| format!("{}:{}", t.transaction_id, t.events.len()))
            .collect();
        self.calls
            .lock()
            .unwrap()
            .push(format!("group:{}", ids.join(",")));
        if self.fail_group {
            return Err(CdcError::generic("simulated group failure"));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

async fn drain_events(
    dir: &TempDir,
    fail_group: bool,
    settings: &ConsumerSettings,
) -> (Vec<String>, u64, usize) {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let dest = Box::new(EventRecorder {
        calls: calls.clone(),
        fail_group,
    });
    let (lsn, pending) = drain_events_to(dir, dest, settings).await;
    let calls = calls.lock().unwrap().clone();
    (calls, lsn, pending)
}

async fn drain_events_to(
    dir: &TempDir,
    mut dest: Box<dyn DestinationHandler>,
    settings: &ConsumerSettings,
) -> (u64, usize) {
    let mut manager =
        TransactionManager::new(dir.path(), DestinationType::Kafka, None, 64 * 1024 * 1024)
            .await
            .unwrap();
    manager.set_event_mode(true);
    let manager = Arc::new(manager);

    let mut queue = BinaryHeap::new();
    for (i, rows) in [2usize, 1, 3].iter().enumerate() {
        let tx = commit_tx(&manager, 900 + i as u32, 9000 + i as u64 * 10, 0..*rows).await;
        queue.push(std::cmp::Reverse(tx));
    }
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(1);
    drop(sender);

    let lsn_path = dir.path().join("lsn");
    let (lsn_tracker, _) = LsnTracker::new_with_load(Some(lsn_path.to_str().unwrap())).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let feedback = SharedLsnFeedback::new_shared();
    pg2any_lib::drain_and_shutdown(
        &mut queue,
        &mut receiver,
        &manager,
        &mut dest,
        &lsn_tracker,
        &metrics,
        settings,
        &feedback,
    )
    .await;
    let pending = manager.list_pending_transactions().await.unwrap().len();
    (lsn_tracker.get(), pending)
}

/// A custom event destination that does not implement grouped delivery and fails transaction 901
/// when it is delivered without a pre-commit hook, as the default group path does.
struct NoGroupSupport {
    calls: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl DestinationHandler for NoGroupSupport {
    async fn connect(&mut self, _connection_string: &str) -> Result<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        _commands: &[String],
        _pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        Err(CdcError::unsupported("event mode only"))
    }

    fn supports_event_mode(&self) -> bool {
        true
    }

    async fn execute_events_batch_with_hook(
        &mut self,
        _events: &[ChangeEvent],
        transaction_id: u32,
        _commit_timestamp: chrono::DateTime<Utc>,
        _commit_lsn: Option<Lsn>,
        pre_commit_hook: Option<PreCommitHook>,
    ) -> Result<()> {
        if transaction_id == 901 && pre_commit_hook.is_none() {
            return Err(CdcError::generic("simulated failure inside a group"));
        }
        self.calls
            .lock()
            .unwrap()
            .push(format!("tx:{transaction_id}"));
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// The default group path must not commit a prefix of the group before failing: the consumer's
/// one-by-one retry would deliver that prefix a second time.
#[tokio::test]
async fn event_destination_without_group_support_never_delivers_a_transaction_twice() {
    let dir = TempDir::new().unwrap();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let dest = Box::new(NoGroupSupport {
        calls: calls.clone(),
    });
    let (lsn, pending) = drain_events_to(&dir, dest, &settings(100, 1000)).await;
    assert_eq!(*calls.lock().unwrap(), vec!["tx:900", "tx:901", "tx:902"]);
    assert_eq!(lsn, 9020);
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn event_mode_group_commit_sends_queued_transactions_in_one_group() {
    let dir = TempDir::new().unwrap();
    let (calls, lsn, pending) = drain_events(&dir, false, &settings(100, 1000)).await;
    assert_eq!(calls, vec!["group:900:2,901:1,902:3"]);
    assert_eq!(lsn, 9020);
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn event_mode_failed_group_is_resent_one_by_one() {
    let dir = TempDir::new().unwrap();
    let (calls, lsn, pending) = drain_events(&dir, true, &settings(100, 1000)).await;
    assert_eq!(
        calls,
        vec![
            "group:900:2,901:1,902:3",
            "single:900:2",
            "single:901:1",
            "single:902:3"
        ]
    );
    assert_eq!(lsn, 9020);
    assert_eq!(pending, 0);
}

#[tokio::test]
async fn event_mode_without_group_commit_sends_each_transaction_alone() {
    let dir = TempDir::new().unwrap();
    let (calls, lsn, _) = drain_events(&dir, false, &settings(1, 1000)).await;
    assert_eq!(calls, vec!["single:900:2", "single:901:1", "single:902:3"]);
    assert_eq!(lsn, 9020);
}

// ---------------------------------------------------------------------------------------------
// In-memory fast path
// ---------------------------------------------------------------------------------------------

async fn memory_manager(
    dir: &TempDir,
    destination: DestinationType,
    event_mode: bool,
    budget_bytes: usize,
    max_statements: usize,
) -> Arc<TransactionManager> {
    let mut manager = TransactionManager::new(dir.path(), destination, None, 64 * 1024 * 1024)
        .await
        .unwrap();
    manager.set_event_mode(event_mode);
    manager.set_memory_fast_path(budget_bytes, max_statements);
    Arc::new(manager)
}

fn files_in(dir: &TempDir, sub: &str) -> usize {
    std::fs::read_dir(dir.path().join(sub)).unwrap().count()
}

fn files_on_disk(dir: &TempDir) -> usize {
    ["sql_data_tx", "sql_received_tx", "sql_pending_tx"]
        .iter()
        .map(|sub| files_in(dir, sub))
        .sum()
}

#[tokio::test]
async fn memory_transactions_never_touch_the_filesystem() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    let mut txs = Vec::new();
    for i in 0..3usize {
        txs.push(commit_tx(&manager, 1000 + i as u32, 10_000 + i as u64 * 10, i..i + 1).await);
    }
    assert_eq!(
        files_on_disk(&dir),
        0,
        "committed in memory: no file at all"
    );
    assert!(manager.memory_in_use() > 0);
    assert_eq!(manager.pending_count(), 3);

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 1000),
    )
    .await;

    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    assert_eq!(ids_of(&calls[2]), vec![2]);
    assert_eq!(tracker.get(), 10_020);
    assert_eq!(files_on_disk(&dir), 0);
    assert_eq!(
        manager.memory_in_use(),
        0,
        "the budget is returned after the apply"
    );
    assert_eq!(manager.pending_count(), 0);
}

/// A BEGIN for a transaction that is already open (a reconnect re-sends it from the slot's
/// confirmed position) replaces the open one: the budget it held must come back, or every such
/// event would permanently shrink the budget until everything spills to files.
#[tokio::test]
async fn a_repeated_begin_returns_the_budget_of_the_transaction_it_replaces() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;

    manager
        .begin_transaction(2000, Utc::now(), "normal")
        .await
        .unwrap();
    manager
        .append_event(2000, &insert_event(1, 20_000))
        .await
        .unwrap();
    assert!(
        manager.memory_in_use() > 0,
        "the first attempt holds budget"
    );

    manager
        .begin_transaction(2000, Utc::now(), "normal")
        .await
        .unwrap();
    assert_eq!(
        manager.memory_in_use(),
        0,
        "the replaced attempt's budget is returned"
    );
}

#[tokio::test]
async fn memory_transactions_group_commit_into_one_batch() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    let mut txs = Vec::new();
    for i in 0..4usize {
        txs.push(commit_tx(&manager, 1100 + i as u32, 11_000 + i as u64 * 10, i..i + 1).await);
    }
    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(100, 1000),
    )
    .await;
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(ids_of(&calls[0]), vec![0, 1, 2, 3]);
    assert_eq!(tracker.get(), 11_030);
    assert_eq!(manager.memory_in_use(), 0);
}

/// More statements than one batch, or a budget that is used up: the transaction spills to files
/// and everything is still applied once, in commit order, with the memory budget back to zero.
#[tokio::test]
async fn oversized_and_over_budget_transactions_spill_to_files_in_commit_order() {
    let dir = TempDir::new().unwrap();
    // One batch holds 3 statements; the budget fits about one small transaction.
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 400, 3).await;
    let first = commit_tx(&manager, 1200, 12_000, 0..1).await;
    let too_big = commit_tx(&manager, 1201, 12_010, 1..6).await;
    let over_budget = commit_tx(&manager, 1202, 12_020, 6..7).await;
    let later = commit_tx(&manager, 1203, 12_030, 7..8).await;
    assert!(
        files_on_disk(&dir) > 0,
        "the big and the over-budget transactions are files"
    );
    assert!(
        first.metadata.segments[0]
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("mem-"),
        "the first one stayed in memory"
    );

    let (dest, calls) = Recorder::new(&[]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        vec![later, too_big, first, over_budget],
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 3),
    )
    .await;

    let applied: Vec<usize> = calls
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| ids_of(batch))
        .collect();
    assert_eq!(
        applied,
        vec![0, 1, 2, 3, 4, 5, 6, 7],
        "commit order, each once"
    );
    assert_eq!(tracker.get(), 12_030);
    assert_eq!(
        files_on_disk(&dir),
        0,
        "spilled files are cleaned up as before"
    );
    assert_eq!(manager.memory_in_use(), 0);
    assert_eq!(manager.pending_count(), 0);
}

#[tokio::test]
async fn failed_apply_of_a_memory_transaction_is_retried_from_memory() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    let txs = vec![commit_tx(&manager, 1300, 13_000, 0..2).await];
    let (dest, calls) = Recorder::new(&[1]);
    let lsn_path = dir.path().join("lsn");
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 1000),
    )
    .await;
    let calls = calls.lock().unwrap();
    assert_eq!(calls.len(), 2, "one failure, one retry");
    assert_eq!(
        calls[0], calls[1],
        "the retry applies the very same statements"
    );
    assert_eq!(tracker.get(), 13_000);
    assert_eq!(manager.memory_in_use(), 0);
}

#[tokio::test]
async fn already_applied_memory_transaction_is_skipped_and_its_budget_returned() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    let txs = vec![
        commit_tx(&manager, 1400, 14_000, 0..1).await,
        commit_tx(&manager, 1401, 14_010, 1..2).await,
    ];
    let lsn_path = dir.path().join("lsn");
    let pre = LsnTracker::new(Some(lsn_path.to_str().unwrap())).await;
    pre.commit_lsn(14_000);
    pre.persist_async().await.unwrap();

    let (dest, calls) = Recorder::new(&[]);
    let tracker = drain(
        &manager,
        txs,
        dest,
        lsn_path.to_str().unwrap(),
        &settings(1, 1000),
    )
    .await;
    assert_eq!(calls.lock().unwrap().len(), 1);
    assert_eq!(tracker.get(), 14_010);
    assert_eq!(manager.memory_in_use(), 0);
    assert_eq!(manager.pending_count(), 0);
}

#[tokio::test]
async fn unfinished_memory_transaction_leaves_nothing_behind() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    manager
        .begin_transaction(1500, Utc::now(), "normal")
        .await
        .unwrap();
    manager
        .append_event(1500, &insert_event(0, 15_000))
        .await
        .unwrap();
    // The producer's shutdown flush must not trip over an in-memory transaction.
    manager.flush_all_buffers().await.unwrap();
    assert_eq!(files_on_disk(&dir), 0);
}

#[tokio::test]
async fn streaming_transactions_always_use_files() {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 1000).await;
    manager
        .begin_transaction(1600, Utc::now(), "streaming")
        .await
        .unwrap();
    assert!(files_on_disk(&dir) > 0);
    assert_eq!(manager.memory_in_use(), 0);
}

async fn event_memory_calls(group: usize) -> Vec<String> {
    let dir = TempDir::new().unwrap();
    let manager = memory_manager(&dir, DestinationType::Kafka, true, 1 << 20, 1000).await;
    let mut queue = BinaryHeap::new();
    for (i, rows) in [2usize, 1, 3].iter().enumerate() {
        let tx = commit_tx(&manager, 1700 + i as u32, 17_000 + i as u64 * 10, 0..*rows).await;
        queue.push(std::cmp::Reverse(tx));
    }
    assert_eq!(
        files_on_disk(&dir),
        0,
        "event-mode transactions stay in memory too"
    );

    let calls = Arc::new(Mutex::new(Vec::new()));
    let mut dest: Box<dyn DestinationHandler> = Box::new(EventRecorder {
        calls: calls.clone(),
        fail_group: false,
    });
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<PendingTransactionFile>(1);
    drop(sender);
    let lsn_path = dir.path().join("lsn");
    let (lsn_tracker, _) = LsnTracker::new_with_load(Some(lsn_path.to_str().unwrap())).await;
    let metrics = Arc::new(pg2any_lib::MetricsCollector::new());
    let feedback = SharedLsnFeedback::new_shared();
    pg2any_lib::drain_and_shutdown(
        &mut queue,
        &mut receiver,
        &manager,
        &mut dest,
        &lsn_tracker,
        &metrics,
        &settings(group, 1000),
        &feedback,
    )
    .await;
    assert_eq!(lsn_tracker.get(), 17_020);
    assert_eq!(manager.memory_in_use(), 0);
    let calls = calls.lock().unwrap().clone();
    calls
}

#[tokio::test]
async fn event_mode_memory_transactions_are_sent_from_memory() {
    assert_eq!(
        event_memory_calls(1).await,
        vec!["single:1700:2", "single:1701:1", "single:1702:3"]
    );
}

#[tokio::test]
async fn event_mode_memory_transactions_group_commit() {
    assert_eq!(
        event_memory_calls(100).await,
        vec!["group:1700:2,1701:1,1702:3"]
    );
}

/// A crash loses the in-memory transaction A (LSN 15_000) but not the spilled file transaction B
/// (LSN 15_010). Recovery applies B and raises the position to 15_010, so PostgreSQL's re-send
/// of A (15_000 <= flush_lsn) would be dropped as "already applied": A must survive on disk.
#[tokio::test]
async fn memory_transaction_before_a_file_transaction_survives_a_crash() {
    let dir = TempDir::new().unwrap();
    let lsn_path = dir.path().join("lsn");
    let lsn_path = lsn_path.to_str().unwrap();
    let applied = {
        let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 3).await;
        let a = commit_tx(&manager, 1500, 15_000, 0..1).await;
        let b = commit_tx(&manager, 1501, 15_010, 1..6).await;
        assert!(a.metadata.segments[0].path.ends_with("mem-1500-15000.sql"));
        assert!(!b.metadata.segments[0]
            .path
            .to_string_lossy()
            .contains("mem-"));
        // Crash: nothing was applied; the process (and with it the memory) is gone.
        drop((a, b, manager));

        // Restart: recovery applies the pending files, then PostgreSQL re-sends A and B.
        let manager = memory_manager(&dir, DestinationType::MySQL, false, 1 << 20, 3).await;
        let recovered = manager.list_pending_transactions().await.unwrap();
        let (dest, calls) = Recorder::new(&[]);
        drain(&manager, recovered, dest, lsn_path, &settings(1, 3)).await;
        let resent = vec![
            commit_tx(&manager, 1500, 15_000, 0..1).await,
            commit_tx(&manager, 1501, 15_010, 1..6).await,
        ];
        let (dest, resent_calls) = Recorder::new(&[]);
        drain(&manager, resent, dest, lsn_path, &settings(1, 3)).await;
        let mut applied: Vec<usize> = calls
            .lock()
            .unwrap()
            .iter()
            .chain(resent_calls.lock().unwrap().iter())
            .flat_map(|batch| ids_of(batch))
            .collect();
        applied.sort_unstable();
        applied
    };
    assert_eq!(applied, vec![0, 1, 2, 3, 4, 5], "every row once, none lost");
    assert_eq!(files_on_disk(&dir), 0, "recovered files are cleaned up");
}
