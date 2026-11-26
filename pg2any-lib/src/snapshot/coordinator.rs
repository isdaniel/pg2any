//! Orchestrates the initial snapshot: clean up a previous attempt, take the slot's snapshot,
//! prepare the targets, copy, and hand the stream back at the consistent point.

use super::batcher::{BatchMode, Batcher};
use super::budget::ByteBudget;
use super::decision::SnapshotAction;
use super::marker::{MarkerStore, SnapshotMarker, MARKER_FILE};
use super::pipeline::{run_copy, CopyStats, SnapshotSource};
use super::target::prepare_targets;
use crate::config::{sanitize_connection_string, Config, SnapshotTargetPolicy};
use crate::destinations::destination_factory::DestinationHandler;
use crate::error::{CdcError, Result};
use crate::lsn_tracker::LsnTracker;
use crate::monitoring::{MetricsCollector, MetricsCollectorTrait};
use crate::sql_renderer::RenderContext;
use crate::types::{ChangeEvent, Lsn};
use async_trait::async_trait;
use pg_walstream::{LogicalReplicationStream, SnapshotEvents, SnapshotOutcome};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const MIB: usize = 1 << 20;
const PROGRESS_INTERVAL_MS: u64 = 10_000;

pub(crate) struct SnapshotContext {
    pub config: Config,
    pub lsn_tracker: Arc<LsnTracker>,
    pub marker_store: MarkerStore,
    pub metrics: Arc<MetricsCollector>,
    pub cancel: CancellationToken,
}

#[async_trait]
impl SnapshotSource for SnapshotEvents {
    async fn next_event(&mut self) -> Result<Option<ChangeEvent>> {
        Ok(SnapshotEvents::next_event(self).await?)
    }
}

/// Source (schema, table) -> destination (schema, table); unmapped schemas pass through.
fn destination_tables(
    tables: &[(String, String)],
    schema_mappings: &HashMap<String, String>,
) -> Vec<(String, String)> {
    tables
        .iter()
        .map(|(schema, table)| {
            let mapped = schema_mappings.get(schema).unwrap_or(schema);
            (mapped.clone(), table.clone())
        })
        .collect()
}

/// `schema.table` of every source table the key query returned no columns for, so a table that
/// has no replica identity key is told apart from a lookup that missed it.
fn keyless_tables(
    source_tables: &[(String, String)],
    key_columns: &[(String, Vec<Arc<str>>)],
) -> Vec<String> {
    source_tables
        .iter()
        .map(|(schema, table)| format!("{schema}.{table}"))
        .filter(|name| !key_columns.iter().any(|(key, _)| key == name))
        .collect()
}

/// Never more than the destination supports, never fewer than one.
fn writer_count(configured: usize, destination_max: usize) -> usize {
    configured.min(destination_max).max(1)
}

// Batches stay well under the budget (half of one writer's share, at least 1 MiB) so the
// writers pipeline instead of one batch holding most of the buffer.
fn max_batch_bytes(buffer_bytes: usize, writers: usize) -> usize {
    (buffer_bytes / writers.max(1) / 2).max(MIB)
}

/// Allows one log line per interval; shared by the reader-side progress callback.
struct Throttle {
    last_ms: AtomicU64,
}

impl Throttle {
    fn allow(&self, now_ms: u64) -> bool {
        let last = self.last_ms.load(Ordering::Relaxed);
        now_ms.saturating_sub(last) >= PROGRESS_INTERVAL_MS
            && self
                .last_ms
                .compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }
}

/// Rows per second over the span since the previous log line, from cumulative values.
fn rows_per_sec(prev_rows: u64, prev_ms: u64, rows: u64, now_ms: u64) -> f64 {
    match now_ms.saturating_sub(prev_ms) {
        0 => 0.0,
        elapsed_ms => rows.saturating_sub(prev_rows) as f64 * 1000.0 / elapsed_ms as f64,
    }
}

/// Clears the in-progress gauge on every exit path of the copy phase.
struct InProgressGuard<'a>(&'a MetricsCollector);

impl Drop for InProgressGuard<'_> {
    fn drop(&mut self) {
        self.0.set_snapshot_in_progress(false);
    }
}

fn cancelled() -> CdcError {
    CdcError::generic("snapshot cancelled")
}

/// Runs `fut` unless shutdown is requested first, in which case `fut` is dropped mid-flight (a
/// source or destination call then only loses its connection). The callers' error paths already
/// leave a state the next start recovers from.
async fn cancellable<T>(
    cancel: &CancellationToken,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(cancelled()),
        out = fut => out,
    }
}

/// Under `append` the destination tables may hold rows that are not ours, so they must never be
/// listed for automatic clean-up: a later policy switch would otherwise truncate them.
fn tables_to_record(
    policy: SnapshotTargetPolicy,
    tables: Vec<(String, String)>,
) -> Vec<(String, String)> {
    match policy {
        SnapshotTargetPolicy::Append => Vec::new(),
        _ => tables,
    }
}

fn in_progress(
    slot_name: &str,
    consistent_point: Option<u64>,
    tables: Vec<(String, String)>,
    copy_started: bool,
    destination: &str,
) -> SnapshotMarker {
    SnapshotMarker::InProgress {
        slot_name: slot_name.to_string(),
        consistent_point,
        tables,
        copy_started,
        destination: Some(destination.to_string()),
    }
}

async fn close_quietly(mut handler: Box<dyn DestinationHandler>) {
    if let Err(e) = handler.close().await {
        warn!("failed to close a snapshot destination handler: {e}");
    }
}

/// For error paths, where the original error is the one worth returning.
async fn clear_marker_quietly(store: &MarkerStore) {
    if let Err(e) = store.clear().await {
        warn!("failed to clear the snapshot marker: {e}");
    }
}

async fn close_all(handlers: Vec<Box<dyn DestinationHandler>>) {
    for handler in handlers {
        close_quietly(handler).await;
    }
}

async fn new_handler(config: &Config) -> Result<Box<dyn DestinationHandler>> {
    let mut handler = config.create_destination()?;
    config.connect_destination(handler.as_mut()).await?;
    Ok(handler)
}

async fn drop_source_slot(config: &Config) -> Result<()> {
    warn!(
        "dropping replication slot '{}' before the snapshot",
        config.replication_slot_name
    );
    crate::slot::drop_replication_slot(
        &config.source_connection_string,
        &config.replication_slot_name,
        config.connection_timeout,
    )
    .await
}

/// Empties the tables a previous, interrupted snapshot was filling. `tables` are the destination
/// names the marker recorded, used as they are: the schema mappings may have changed since.
async fn clean_tables(
    config: &Config,
    cancel: &CancellationToken,
    tables: &[(String, String)],
) -> Result<()> {
    if tables.is_empty() {
        return Ok(());
    }
    let mut handler = cancellable(cancel, new_handler(config)).await?;
    let result = cancellable(
        cancel,
        prepare_targets(handler.as_mut(), tables, SnapshotTargetPolicy::Truncate),
    )
    .await;
    close_quietly(handler).await;
    result
}

/// Undoes what a previous attempt left behind. `decide` only plans `Recover` and `Resync` outside
/// `append`, and only plans `Resync` under `truncate`.
async fn clear_previous_attempt(ctx: &SnapshotContext, action: SnapshotAction) -> Result<()> {
    let config = &ctx.config;
    match action {
        SnapshotAction::Run => Ok(()),
        SnapshotAction::Recover { drop_slot } => {
            let tables =
                match ctx.marker_store.load().await? {
                    Some(SnapshotMarker::InProgress { tables, .. }) => tables,
                    _ => return Err(CdcError::generic(
                        "snapshot recovery requested but there is no in-progress snapshot marker",
                    )),
                };
            if drop_slot {
                cancellable(&ctx.cancel, drop_source_slot(config)).await?;
            }
            clean_tables(config, &ctx.cancel, &tables).await?;
            ctx.marker_store.clear().await?;
            Ok(())
        }
        SnapshotAction::Resync { drop_slot } => {
            if drop_slot {
                cancellable(&ctx.cancel, drop_source_slot(config)).await?;
            }
            // The new snapshot's tables are emptied by `prepare_targets` (policy is `truncate`).
            ctx.marker_store.clear().await?;
            Ok(())
        }
        other @ (SnapshotAction::Skip | SnapshotAction::Fail(_)) => Err(CdcError::generic(
            format!("the snapshot phase cannot run for action {other:?}"),
        )),
    }
}

/// `LsnTracker::commit_lsn` only moves forward, so a tracker already past the new slot's
/// consistent point would keep the de-dup boundary too high and silently drop live transactions.
fn ensure_tracker_not_ahead(tracker_lsn: u64, consistent_point: u64) -> Result<()> {
    if tracker_lsn <= consistent_point {
        return Ok(());
    }
    Err(CdcError::config(format!(
        "the on-disk LSN position {} is ahead of the new replication slot's consistent point {}; \
         the source was probably restored from a backup or failed over to a lagging replica. \
         Resuming would silently skip every transaction up to the on-disk position. Delete the LSN \
         file (CDC_LAST_LSN_FILE) and {MARKER_FILE} in the data directory, then restart to re-sync; \
         the snapshot then runs under the configured CDC_SNAPSHOT_TARGET_POLICY, so a destination \
         that already holds rows fails under fail_if_not_empty (use \
         CDC_SNAPSHOT_TARGET_POLICY=truncate for a deliberate re-sync)",
        pg_walstream::format_lsn(tracker_lsn),
        pg_walstream::format_lsn(consistent_point),
    )))
}

/// Records the snapshot as done. The order is the crash-safety argument: `Completed` goes to disk
/// before the LSN tracker. A crash between the two writes leaves the tracker behind, but the slot's
/// `confirmed_flush_lsn` (>= the consistent point) seeds the de-dup boundary at the next start.
async fn finalize_snapshot(
    ctx: &SnapshotContext,
    slot_name: &str,
    consistent_point: u64,
) -> Result<()> {
    ctx.marker_store
        .store(&SnapshotMarker::Completed {
            slot_name: slot_name.to_string(),
        })
        .await?;
    ctx.lsn_tracker.commit_lsn(consistent_point);
    ctx.lsn_tracker.persist_async().await?;
    Ok(())
}

pub(crate) async fn run_snapshot_phase(
    ctx: &SnapshotContext,
    action: SnapshotAction,
    stream: LogicalReplicationStream,
) -> Result<(LogicalReplicationStream, Lsn)> {
    let config = &ctx.config;
    warn!(
        "initial snapshot starting: it holds a REPEATABLE READ transaction on the source that pins \
         the database xmin (VACUUM cannot reclaim dead tuples, database-wide) and the replication \
         slot '{}' retains WAL until the snapshot finishes",
        config.replication_slot_name
    );
    if ctx.cancel.is_cancelled() {
        return Err(cancelled());
    }

    clear_previous_attempt(ctx, action).await?;

    let slot_name = config.replication_slot_name.as_str();
    let destination = sanitize_connection_string(&config.destination_connection_string);
    ctx.marker_store
        .store(&in_progress(
            slot_name,
            None,
            Vec::new(),
            false,
            &destination,
        ))
        .await?;
    let outcome = cancellable(&ctx.cancel, async { Ok(stream.snapshot().await?) }).await?;
    let snap = match outcome {
        SnapshotOutcome::Available(snap) => snap,
        SnapshotOutcome::Unavailable(_) => {
            clear_marker_quietly(&ctx.marker_store).await;
            return Err(CdcError::config(format!(
                "replication slot '{slot_name}' appeared while the snapshot was starting, so no \
                 snapshot could be exported; another process may be using this slot name. Stop it, \
                 or drop the slot, and start again"
            )));
        }
    };
    let consistent_point = snap.consistent_point();
    // The slot's identity goes to disk first; recovery matches it against `confirmed_flush_lsn`.
    ctx.marker_store
        .store(&in_progress(
            slot_name,
            Some(consistent_point.0),
            Vec::new(),
            false,
            &destination,
        ))
        .await?;
    if let Err(e) = ensure_tracker_not_ahead(ctx.lsn_tracker.get(), consistent_point.0) {
        // Nothing was copied, so no marker should survive to demand a cleanup later.
        clear_marker_quietly(&ctx.marker_store).await;
        return Err(e);
    }

    let source_tables: Vec<(String, String)> = snap
        .tables()
        .iter()
        .map(|t| {
            let r = t.relation();
            (r.namespace.to_string(), r.relation_name.to_string())
        })
        .collect();
    info!(
        "snapshot of {} table(s) at consistent point {}",
        source_tables.len(),
        pg_walstream::format_lsn(consistent_point.0)
    );

    // Every table is probed/cleared before any row is copied; the first handler also tells how
    // many writers the destination supports.
    // Every error up to the copy leaves the marker cleared: nothing was written, and an
    // `InProgress` marker would make `append` (which cannot clean up) refuse the next start.
    let mut first = match cancellable(&ctx.cancel, new_handler(config)).await {
        Ok(handler) => handler,
        Err(e) => {
            clear_marker_quietly(&ctx.marker_store).await;
            return Err(e);
        }
    };
    let count = writer_count(config.snapshot_writers, first.max_snapshot_writers());
    let target_tables = destination_tables(&source_tables, &config.schema_mappings);
    let prepared = cancellable(
        &ctx.cancel,
        prepare_targets(
            first.as_mut(),
            &target_tables,
            config.snapshot_target_policy,
        ),
    )
    .await;
    if let Err(e) = prepared {
        close_quietly(first).await;
        clear_marker_quietly(&ctx.marker_store).await;
        return Err(e);
    }
    let mut writers = vec![first];
    for _ in 1..count {
        match cancellable(&ctx.cancel, new_handler(config)).await {
            Ok(handler) => writers.push(handler),
            Err(e) => {
                close_all(writers).await;
                clear_marker_quietly(&ctx.marker_store).await;
                return Err(e);
            }
        }
    }
    // The snapshot's relations carry no key flags, so a destination that keys its records (Kafka,
    // the one event-mode destination) gets each table's replica-identity key from the catalog:
    // the key a live UPDATE/DELETE of the same row will carry.
    if writers[0].supports_event_mode() {
        let key_columns = match cancellable(
            &ctx.cancel,
            crate::slot::query_replica_identity_keys(
                &config.source_connection_string,
                &source_tables,
                config.connection_timeout,
            ),
        )
        .await
        {
            Ok(key_columns) => key_columns,
            Err(e) => {
                close_all(writers).await;
                clear_marker_quietly(&ctx.marker_store).await;
                return Err(e);
            }
        };
        let keyless = keyless_tables(&source_tables, &key_columns);
        if !keyless.is_empty() {
            warn!(
                "snapshot messages for these tables will have no key (no replica identity key \
                 columns): {}",
                keyless.join(", ")
            );
        }
        for writer in &mut writers {
            for (table_key, columns) in &key_columns {
                writer.set_snapshot_key_columns(table_key, columns.clone());
            }
        }
    }

    // The tables become ours to clean up only once they are verified empty or truncated, and
    // just before the first row is written; recording them earlier would let a crash in between
    // make recovery truncate tables this snapshot never touched.
    if let Err(e) = ctx
        .marker_store
        .store(&in_progress(
            slot_name,
            Some(consistent_point.0),
            tables_to_record(config.snapshot_target_policy, target_tables),
            true,
            &destination,
        ))
        .await
    {
        close_all(writers).await;
        clear_marker_quietly(&ctx.marker_store).await;
        return Err(e);
    }

    let mode = if writers[0].supports_event_mode() {
        BatchMode::Events
    } else {
        BatchMode::Sql(RenderContext {
            dialect: writers[0].dialect(),
            schema_mappings: &config.schema_mappings,
        })
    };
    let batcher = Batcher::new(
        mode,
        config.snapshot_batch_rows,
        max_batch_bytes(config.snapshot_buffer_bytes, writers.len()),
    );
    let budget = ByteBudget::new(config.snapshot_buffer_bytes);

    let started = Instant::now();
    let throttle = Throttle {
        last_ms: AtomicU64::new(0),
    };
    // (elapsed ms, cumulative rows) at the previous log line, for rows/s.
    let last_logged = Mutex::new((0u64, 0u64));
    let progress = |stats: &CopyStats| {
        ctx.metrics
            .record_snapshot_progress(stats.rows, stats.bytes, stats.tables);
        let now_ms = started.elapsed().as_millis() as u64;
        if throttle.allow(now_ms) {
            let mut last = last_logged.lock().unwrap();
            let rate = rows_per_sec(last.1, last.0, stats.rows, now_ms);
            *last = (now_ms, stats.rows);
            info!(
                "snapshot progress: {} rows ({:.0} rows/s), {:.1} MiB, {} table(s) written so far",
                stats.rows,
                rate,
                stats.bytes as f64 / MIB as f64,
                stats.tables
            );
        }
    };

    ctx.metrics.set_snapshot_in_progress(true);
    let _in_progress = InProgressGuard(&ctx.metrics);

    let mut events = snap.with_cancellation(ctx.cancel.clone()).events();
    // An error from here on leaves the `InProgress` marker; dropping `events` discards the slot.
    let stats = run_copy(
        &mut events,
        writers,
        batcher,
        budget,
        consistent_point,
        &ctx.cancel,
        &progress,
    )
    .await?;

    let stream = events.finish().await?;

    finalize_snapshot(ctx, slot_name, consistent_point.0).await?;

    info!(
        "initial snapshot complete: {} rows, {:.1} MiB, {} batch(es), {} table(s) in {:.1?}; \
         resuming the stream from {}",
        stats.rows,
        stats.bytes as f64 / MIB as f64,
        stats.batches,
        stats.tables,
        started.elapsed(),
        pg_walstream::format_lsn(consistent_point.0)
    );
    Ok((stream, consistent_point))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn tables(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .collect()
    }

    #[test]
    fn marker_tables_use_destination_schema_names() {
        let mappings = HashMap::from([("public".to_string(), "cdc_db".to_string())]);
        assert_eq!(
            destination_tables(
                &tables(&[("public", "users"), ("public", "orders")]),
                &mappings
            ),
            tables(&[("cdc_db", "users"), ("cdc_db", "orders")])
        );
    }

    #[test]
    fn unmapped_schema_passes_through() {
        let mappings = HashMap::from([("public".to_string(), "cdc_db".to_string())]);
        assert_eq!(
            destination_tables(&tables(&[("public", "a"), ("sales", "b")]), &mappings),
            tables(&[("cdc_db", "a"), ("sales", "b")])
        );
        assert_eq!(
            destination_tables(&tables(&[("sales", "b")]), &HashMap::new()),
            tables(&[("sales", "b")])
        );
    }

    #[test]
    fn append_records_no_tables_other_policies_do() {
        let t = tables(&[("public", "a"), ("sales", "b")]);
        assert!(tables_to_record(SnapshotTargetPolicy::Append, t.clone()).is_empty());
        assert_eq!(
            tables_to_record(SnapshotTargetPolicy::Truncate, t.clone()),
            t
        );
        assert_eq!(
            tables_to_record(SnapshotTargetPolicy::FailIfNotEmpty, t.clone()),
            t
        );
    }

    #[tokio::test]
    async fn cancellable_with_a_cancelled_token_fails_without_running_the_future() {
        let cancel = CancellationToken::new();
        cancel.cancel();
        let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = ran.clone();

        let err = cancellable(&cancel, async move {
            flag.store(true, Ordering::SeqCst);
            Ok(1)
        })
        .await
        .unwrap_err();

        assert!(err.to_string().contains("snapshot cancelled"), "{err}");
        assert!(!ran.load(Ordering::SeqCst), "the future must not start");
    }

    #[tokio::test]
    async fn cancellable_aborts_a_pending_future_promptly() {
        struct DropFlag(Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let cancel = CancellationToken::new();
        let canceller = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            canceller.cancel();
        });
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let guard = DropFlag(dropped.clone());

        let out = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cancellable(&cancel, async move {
                let _guard = guard;
                std::future::pending::<Result<()>>().await
            }),
        )
        .await
        .expect("cancellation must end a pending future");

        assert!(out.unwrap_err().to_string().contains("snapshot cancelled"));
        assert!(dropped.load(Ordering::SeqCst), "the future must be dropped");
    }

    #[tokio::test]
    async fn cancellable_passes_a_completing_future_through() {
        let cancel = CancellationToken::new();
        assert_eq!(cancellable(&cancel, async { Ok(7) }).await.unwrap(), 7);

        let err = cancellable(&cancel, async { Err::<(), _>(CdcError::generic("boom")) })
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
    }

    #[test]
    fn tables_without_key_columns_are_listed() {
        let keys = vec![("public.a".to_string(), vec![Arc::<str>::from("id")])];
        assert_eq!(
            keyless_tables(
                &tables(&[("public", "a"), ("public", "b"), ("s", "c")]),
                &keys
            ),
            ["public.b", "s.c"]
        );
        assert!(keyless_tables(&tables(&[("public", "a")]), &keys).is_empty());
    }

    #[test]
    fn writer_count_is_clamped_to_destination_limit_and_at_least_one() {
        assert_eq!(writer_count(4, 8), 4);
        assert_eq!(writer_count(8, 1), 1, "SQLite / Kafka");
        assert_eq!(writer_count(4, usize::MAX), 4);
        assert_eq!(writer_count(0, 8), 1);
        assert_eq!(writer_count(0, 0), 1);
    }

    #[test]
    fn batch_bytes_are_half_a_writers_share_with_a_one_mib_floor() {
        assert_eq!(max_batch_bytes(1024 * MIB, 4), 128 * MIB);
        assert_eq!(max_batch_bytes(64 * MIB, 1), 32 * MIB);
        assert_eq!(max_batch_bytes(4 * MIB, 4), MIB, "floor");
        assert_eq!(max_batch_bytes(0, 4), MIB, "floor");
        assert_eq!(
            max_batch_bytes(64 * MIB, 0),
            32 * MIB,
            "zero writers count as one"
        );
    }

    #[test]
    fn progress_is_logged_at_most_once_per_interval() {
        let t = Throttle {
            last_ms: AtomicU64::new(0),
        };
        assert!(!t.allow(9_999));
        assert!(t.allow(PROGRESS_INTERVAL_MS));
        assert!(!t.allow(15_000));
        assert!(!t.allow(5), "a stale clock reading never logs");
        assert!(t.allow(20_000));
    }

    #[test]
    fn rows_per_sec_is_the_rows_delta_over_the_elapsed_span() {
        assert_eq!(rows_per_sec(0, 0, 10_000, 10_000), 1000.0);
        assert_eq!(rows_per_sec(10_000, 10_000, 15_000, 20_000), 500.0);
        assert_eq!(rows_per_sec(100, 5_000, 100, 15_000), 0.0, "no new rows");
        assert_eq!(rows_per_sec(100, 5_000, 900, 5_000), 0.0, "zero elapsed");
        assert_eq!(rows_per_sec(900, 5_000, 100, 15_000), 0.0, "never negative");
    }

    #[test]
    fn tracker_ahead_of_consistent_point_fails_with_instructions() {
        let err = ensure_tracker_not_ahead(0x2000, 0x1000).unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "got {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("CDC_LAST_LSN_FILE"), "{msg}");
        assert!(msg.contains(MARKER_FILE), "{msg}");
        assert!(msg.contains("0/2000"), "on-disk LSN missing: {msg}");
        assert!(msg.contains("0/1000"), "consistent point missing: {msg}");
        assert!(msg.contains("restart"), "{msg}");
        assert!(msg.contains("fail_if_not_empty"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
    }

    #[test]
    fn tracker_equal_or_behind_is_ok() {
        assert!(ensure_tracker_not_ahead(0x1000, 0x1000).is_ok());
        assert!(ensure_tracker_not_ahead(0x0FFF, 0x1000).is_ok());
        assert!(ensure_tracker_not_ahead(0, 0x1000).is_ok());
    }

    async fn context(dir: &std::path::Path, marker_dir: &std::path::Path) -> SnapshotContext {
        SnapshotContext {
            config: Config::default(),
            lsn_tracker: LsnTracker::new(Some(dir.join("lsn").to_str().unwrap())).await,
            marker_store: MarkerStore::new(marker_dir.to_str().unwrap()),
            metrics: Arc::new(MetricsCollector::new()),
            cancel: CancellationToken::new(),
        }
    }

    #[tokio::test]
    async fn completed_marker_is_written_before_lsn_tracker_is_updated() {
        let dir = tempfile::tempdir().unwrap();
        // A marker directory below a regular file cannot be created, so `store` fails.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"not a directory").unwrap();
        let ctx = context(dir.path(), &blocker.join("data")).await;

        let err = finalize_snapshot(&ctx, "slot_a", 0x1000).await.unwrap_err();

        assert!(matches!(err, CdcError::Io(_)), "got {err:?}");
        assert_eq!(ctx.lsn_tracker.get(), 0, "tracker must not move first");
        assert!(
            !std::path::Path::new(ctx.lsn_tracker.file_path()).exists(),
            "nothing may be persisted"
        );
    }

    #[tokio::test]
    async fn finalize_writes_marker_then_tracker() {
        let dir = tempfile::tempdir().unwrap();
        let ctx = context(dir.path(), dir.path()).await;

        finalize_snapshot(&ctx, "slot_a", 0x1000).await.unwrap();

        assert_eq!(
            ctx.marker_store.load().await.unwrap(),
            Some(SnapshotMarker::Completed {
                slot_name: "slot_a".into(),
            })
        );
        assert_eq!(ctx.lsn_tracker.get(), 0x1000);
        let persisted = ctx.lsn_tracker.load_from_file().await.unwrap();
        assert_eq!(persisted.get_lsn(), 0x1000, "tracker is persisted");
    }

    /// Records every truncate it is asked for into a list the test shares.
    struct TruncateLog(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl DestinationHandler for TruncateLog {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }
        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}
        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _pre_commit_hook: Option<crate::destinations::destination_factory::PreCommitHook>,
        ) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        async fn snapshot_table_is_empty(&mut self, _schema: &str, _table: &str) -> Result<bool> {
            Ok(true)
        }
        async fn snapshot_truncate_table(&mut self, schema: &str, table: &str) -> Result<()> {
            self.0.lock().unwrap().push(format!("{schema}.{table}"));
            Ok(())
        }
    }

    /// The marker lists the tables the interrupted run wrote; a schema mapping changed since then
    /// must not redirect the clean-up to other (possibly populated) tables.
    #[tokio::test]
    async fn recovery_empties_the_tables_the_marker_lists_whatever_the_current_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let truncated = Arc::new(Mutex::new(Vec::new()));
        let log = truncated.clone();
        let config = Config::builder()
            .source_connection_string("postgresql://u:p@localhost/db?replication=database")
            .destination_connection_string("dest")
            .schema_mappings(HashMap::from([(
                "public".to_string(),
                "production".to_string(),
            )]))
            .custom_destination(move || TruncateLog(log.clone()))
            .build()
            .unwrap();
        let mut ctx = context(dir.path(), dir.path()).await;
        ctx.config = config;
        ctx.marker_store
            .store(&SnapshotMarker::InProgress {
                slot_name: "slot_a".into(),
                consistent_point: Some(0x1000),
                tables: tables(&[("public", "t")]),
                copy_started: true,
                destination: None,
            })
            .await
            .unwrap();

        clear_previous_attempt(&ctx, SnapshotAction::Recover { drop_slot: false })
            .await
            .unwrap();

        assert_eq!(*truncated.lock().unwrap(), ["public.t"]);
        assert_eq!(ctx.marker_store.load().await.unwrap(), None);
    }

    /// Compile-only: the exact signature of `run_snapshot_phase`, and that its future is `Send`
    /// so it can be awaited from a spawned task.
    #[test]
    fn run_snapshot_phase_signature_and_future_is_send() {
        fn check<'a>(
            ctx: &'a SnapshotContext,
            action: SnapshotAction,
            stream: LogicalReplicationStream,
        ) -> impl Future<Output = Result<(LogicalReplicationStream, Lsn)>> + Send + 'a {
            run_snapshot_phase(ctx, action, stream)
        }
        let _ = check;
    }
}
