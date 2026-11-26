//! Concurrent snapshot copy: one reader batches rows, several writers apply them.

use crate::destinations::destination_factory::DestinationHandler;
use crate::error::{CdcError, Result};
use crate::snapshot::batcher::{Batcher, SnapshotBatch};
use crate::snapshot::budget::{ByteBudget, BytePermit};
use crate::types::{ChangeEvent, EventType, Lsn};
use async_trait::async_trait;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Mutex as AsyncMutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::warn;

#[async_trait]
pub(crate) trait SnapshotSource: Send {
    async fn next_event(&mut self) -> Result<Option<ChangeEvent>>;
}

#[derive(Debug, Default, Clone)]
pub(crate) struct CopyStats {
    pub rows: u64,
    pub bytes: u64,
    pub batches: u64,
    pub tables: u64,
}

#[derive(Default)]
struct Progress {
    stats: CopyStats,
    tables: HashSet<String>,
}

type Shared = Arc<Mutex<Progress>>;
type Batches = mpsc::Sender<(SnapshotBatch, BytePermit)>;
// A plain mpsc has one consumer; the writers take turns on the lock while waiting in `recv`.
type SharedRx = Arc<AsyncMutex<mpsc::Receiver<(SnapshotBatch, BytePermit)>>>;

fn cancelled() -> CdcError {
    CdcError::generic("snapshot cancelled")
}

fn stats_of(shared: &Shared) -> CopyStats {
    shared.lock().unwrap().stats.clone()
}

/// The first writer failure is returned. `progress` runs on the reader side (the writers are
/// `'static` tasks and cannot borrow it), so it sees the writes completed so far.
pub(crate) async fn run_copy(
    source: &mut dyn SnapshotSource,
    writers: Vec<Box<dyn DestinationHandler>>,
    mut batcher: Batcher<'_>,
    budget: ByteBudget,
    consistent_point: Lsn,
    cancel: &CancellationToken,
    progress: &(dyn Fn(&CopyStats) + Send + Sync),
) -> Result<CopyStats> {
    if writers.is_empty() {
        return Err(CdcError::generic("snapshot copy needs at least one writer"));
    }
    // Cancelled by the caller's token, or by the first failing writer / reader error.
    let stop = cancel.child_token();
    let (tx, rx) = mpsc::channel(writers.len());
    let rx: SharedRx = Arc::new(AsyncMutex::new(rx));
    let shared = Shared::default();

    let mut tasks = JoinSet::new();
    for handler in writers {
        tasks.spawn(write_loop(
            handler,
            rx.clone(),
            shared.clone(),
            stop.clone(),
            consistent_point,
        ));
    }
    drop(rx);

    let read = read_loop(source, &mut batcher, &budget, &tx, &stop, &shared, progress).await;
    if read.is_err() {
        stop.cancel();
    }
    drop(tx);

    let mut writer_error = None;
    while let Some(joined) = tasks.join_next().await {
        let error = match joined {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => e,
            Err(e) => CdcError::generic(format!("snapshot writer task failed: {e}")),
        };
        writer_error.get_or_insert(error);
    }

    if let Some(e) = writer_error {
        return Err(e);
    }
    read?;
    // Writers quit early on cancellation, so a clean read does not mean everything was written.
    if cancel.is_cancelled() {
        return Err(cancelled());
    }
    let stats = stats_of(&shared);
    progress(&stats);
    Ok(stats)
}

async fn read_loop(
    source: &mut dyn SnapshotSource,
    batcher: &mut Batcher<'_>,
    budget: &ByteBudget,
    tx: &Batches,
    stop: &CancellationToken,
    shared: &Shared,
    progress: &(dyn Fn(&CopyStats) + Send + Sync),
) -> Result<()> {
    loop {
        let event = tokio::select! {
            biased;
            _ = stop.cancelled() => return Err(cancelled()),
            event = source.next_event() => event?,
        };
        let eof = event.is_none();
        let ready = match event {
            Some(event) => batcher.push(event)?,
            None => batcher.finish().into_iter().collect(),
        };
        for batch in ready {
            dispatch(batch, budget, tx, stop).await?;
            progress(&stats_of(shared));
        }
        if eof {
            return Ok(());
        }
    }
}

/// Holds one permit at a time, and hands it to the channel before the next acquire.
async fn dispatch(
    batch: SnapshotBatch,
    budget: &ByteBudget,
    tx: &Batches,
    stop: &CancellationToken,
) -> Result<()> {
    let permit = tokio::select! {
        biased;
        _ = stop.cancelled() => return Err(cancelled()),
        permit = budget.acquire(batch.bytes()) => permit?,
    };
    tokio::select! {
        biased;
        _ = stop.cancelled() => Err(cancelled()),
        sent = tx.send((batch, permit)) => {
            sent.map_err(|_| CdcError::generic("snapshot writers stopped"))
        }
    }
}

async fn write_loop(
    mut handler: Box<dyn DestinationHandler>,
    rx: SharedRx,
    shared: Shared,
    stop: CancellationToken,
    consistent_point: Lsn,
) -> Result<()> {
    handler.begin_snapshot_writes();
    // Cancels the siblings and the reader on error or panic; disarmed on a clean finish.
    let guard = stop.clone().drop_guard();
    let result: Result<()> = async {
        loop {
            let next = tokio::select! {
                biased;
                _ = stop.cancelled() => return Ok(()),
                next = async { rx.lock().await.recv().await } => next,
            };
            let Some((batch, permit)) = next else {
                return Ok(());
            };
            let (rows, table) = write_batch(&mut *handler, &batch, consistent_point).await?;
            {
                let mut p = shared.lock().unwrap();
                p.stats.rows += rows;
                p.stats.bytes += batch.bytes() as u64;
                p.stats.batches += 1;
                if p.tables.insert(table) {
                    p.stats.tables += 1;
                }
            }
            drop(permit);
        }
    }
    .await;

    if let Err(e) = handler.close().await {
        warn!("snapshot writer failed to close its destination: {e}");
    }
    if result.is_ok() {
        guard.disarm();
    }
    result
}

/// Returns the number of rows written and a key identifying the target table.
async fn write_batch(
    handler: &mut dyn DestinationHandler,
    batch: &SnapshotBatch,
    consistent_point: Lsn,
) -> Result<(u64, String)> {
    match batch {
        SnapshotBatch::Sql {
            table,
            columns,
            rows,
            ..
        } => {
            handler
                .execute_bulk_insert_with_hook(table, columns, rows, None)
                .await?;
            Ok((rows.len() as u64, table.clone()))
        }
        SnapshotBatch::Events { events, .. } => {
            handler
                .execute_snapshot_events_batch(events, consistent_point)
                .await?;
            let table = match events.first().map(|e| &e.event_type) {
                Some(EventType::Insert { schema, table, .. }) => format!("{schema}.{table}"),
                _ => String::new(),
            };
            Ok((events.len() as u64, table))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::destination_factory::PreCommitHook;
    use crate::destinations::dialects::MySqlDialect;
    use crate::error::CdcError;
    use crate::snapshot::batcher::BatchMode;
    use crate::snapshot::budget::row_byte_estimate;
    use crate::sql_renderer::RenderContext;
    use crate::types::{EventType, RowData};
    use pg_walstream::ColumnValue;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    const SECS_30: Duration = Duration::from_secs(30);

    fn event(table: &str, id: u64, payload: &str) -> ChangeEvent {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&id.to_string())),
            ("name", ColumnValue::text(payload)),
        ]);
        ChangeEvent::insert("public", table, 1, data, Lsn::new(id))
    }

    fn estimate(event: &ChangeEvent) -> usize {
        match &event.event_type {
            EventType::Insert { data, .. } => row_byte_estimate(data),
            _ => unreachable!(),
        }
    }

    /// Emits `total` inserts in runs of 100 rows per table, cycling over `tables` tables.
    struct FakeSource {
        next: u64,
        total: u64,
        tables: u64,
        payload: Option<String>,
        hang_at_end: bool,
        on_eof: Option<Arc<tokio::sync::Notify>>,
        pulled: Arc<AtomicUsize>,
    }

    impl FakeSource {
        fn new(total: u64, tables: u64) -> Self {
            Self {
                next: 0,
                total,
                tables,
                payload: None,
                hang_at_end: false,
                on_eof: None,
                pulled: Arc::default(),
            }
        }
    }

    #[async_trait]
    impl SnapshotSource for FakeSource {
        async fn next_event(&mut self) -> Result<Option<ChangeEvent>> {
            if self.next >= self.total {
                if self.hang_at_end {
                    std::future::pending::<()>().await;
                }
                if let Some(eof) = &self.on_eof {
                    eof.notify_one();
                }
                return Ok(None);
            }
            let id = self.next;
            self.next += 1;
            self.pulled.fetch_add(1, SeqCst);
            let table = format!("t{}", (id / 100) % self.tables);
            let payload = self.payload.clone().unwrap_or_else(|| format!("name-{id}"));
            Ok(Some(event(&table, id, &payload)))
        }
    }

    /// State shared by every `FakeWriter` of one test.
    #[derive(Clone, Default)]
    struct Probe {
        written: Arc<Mutex<Vec<(String, u64)>>>,
        points: Arc<Mutex<Vec<Lsn>>>,
        closed: Arc<AtomicUsize>,
        writes: Arc<AtomicUsize>,
        in_flight: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
        delay: Duration,
        fail_on_write: Option<usize>,
        /// `begin_snapshot_writes` calls, and how many writes that writer had made at each call.
        begun: Arc<Mutex<Vec<usize>>>,
    }

    impl Probe {
        fn writers(&self, n: usize) -> Vec<Box<dyn DestinationHandler>> {
            (0..n)
                .map(|_| {
                    Box::new(FakeWriter(self.clone(), AtomicUsize::new(0)))
                        as Box<dyn DestinationHandler>
                })
                .collect()
        }

        fn written(&self) -> Vec<(String, u64)> {
            let mut all = self.written.lock().unwrap().clone();
            all.sort();
            all
        }

        fn peak(&self) -> usize {
            self.peak.load(SeqCst)
        }

        fn closed(&self) -> usize {
            self.closed.load(SeqCst)
        }

        fn writes(&self) -> usize {
            self.writes.load(SeqCst)
        }
    }

    /// The probe shared by all writers, and this writer's own write count.
    struct FakeWriter(Probe, AtomicUsize);

    impl FakeWriter {
        async fn apply(&self, items: Vec<(String, u64)>, bytes: usize) -> Result<()> {
            let p = &self.0;
            self.1.fetch_add(1, SeqCst);
            let nth = p.writes.fetch_add(1, SeqCst) + 1;
            let now = p.in_flight.fetch_add(bytes, SeqCst) + bytes;
            p.peak.fetch_max(now, SeqCst);
            if p.delay.is_zero() {
                tokio::task::yield_now().await;
            } else {
                sleep(p.delay).await;
            }
            p.in_flight.fetch_sub(bytes, SeqCst);
            if p.fail_on_write == Some(nth) {
                return Err(CdcError::generic("boom"));
            }
            p.written.lock().unwrap().extend(items);
            Ok(())
        }
    }

    #[async_trait]
    impl DestinationHandler for FakeWriter {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }

        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _hook: Option<PreCommitHook>,
        ) -> Result<()> {
            Ok(())
        }

        async fn close(&mut self) -> Result<()> {
            self.0.closed.fetch_add(1, SeqCst);
            Ok(())
        }

        fn begin_snapshot_writes(&mut self) {
            let own_writes = self.1.load(SeqCst);
            self.0.begun.lock().unwrap().push(own_writes);
        }

        async fn execute_bulk_insert_with_hook(
            &mut self,
            table: &str,
            _columns: &[String],
            rows: &[Vec<String>],
            _hook: Option<PreCommitHook>,
        ) -> Result<()> {
            let items = rows
                .iter()
                .map(|r| (table.to_string(), r[0].trim_matches('\'').parse().unwrap()))
                .collect();
            self.apply(items, 0).await
        }

        async fn execute_snapshot_events_batch(
            &mut self,
            events: &[ChangeEvent],
            consistent_point: Lsn,
        ) -> Result<()> {
            self.0.points.lock().unwrap().push(consistent_point);
            let items = events
                .iter()
                .map(|e| match &e.event_type {
                    EventType::Insert { table, .. } => (table.to_string(), e.lsn.value()),
                    _ => unreachable!(),
                })
                .collect();
            self.apply(items, events.iter().map(estimate).sum()).await
        }
    }

    fn no_progress() -> impl Fn(&CopyStats) + Send + Sync {
        |_: &CopyStats| {}
    }

    #[tokio::test]
    async fn copies_every_row_exactly_once_across_four_writers() {
        const TOTAL: u64 = 10_000;
        let (dialect, mappings) = (MySqlDialect, HashMap::new());
        let ctx = RenderContext {
            dialect: &dialect,
            schema_mappings: &mappings,
        };
        let probe = Probe::default();
        let progress_calls = Arc::new(AtomicUsize::new(0));
        let progress = {
            let calls = progress_calls.clone();
            move |_: &CopyStats| {
                calls.fetch_add(1, SeqCst);
            }
        };

        let stats = run_copy(
            &mut FakeSource::new(TOTAL, 3),
            probe.writers(4),
            Batcher::new(BatchMode::Sql(ctx), 50, usize::MAX),
            ByteBudget::new(1 << 20),
            Lsn::new(1),
            &CancellationToken::new(),
            &progress,
        )
        .await
        .unwrap();

        let expected: Vec<(String, u64)> = {
            let mut v: Vec<_> = (0..TOTAL)
                .map(|id| (format!("`public`.`t{}`", (id / 100) % 3), id))
                .collect();
            v.sort();
            v
        };
        assert_eq!(probe.written(), expected, "every row exactly once");

        let count_by_table = |rows: &[(String, u64)]| {
            let mut counts: BTreeMap<String, u64> = BTreeMap::new();
            for (table, _) in rows {
                *counts.entry(table.clone()).or_default() += 1;
            }
            counts
        };
        assert_eq!(count_by_table(&probe.written()), count_by_table(&expected));

        assert_eq!(stats.rows, TOTAL);
        assert_eq!(stats.tables, 3);
        assert_eq!(stats.batches as usize, probe.writes());
        assert!(stats.batches >= TOTAL / 50);
        assert!(stats.bytes > 0);
        assert_eq!(probe.closed(), 4, "every writer is closed");
        assert!(progress_calls.load(SeqCst) > 0);
    }

    #[tokio::test]
    async fn every_writer_is_told_to_begin_before_its_first_write() {
        let (dialect, mappings) = (MySqlDialect, HashMap::new());
        let ctx = RenderContext {
            dialect: &dialect,
            schema_mappings: &mappings,
        };
        let probe = Probe::default();

        run_copy(
            &mut FakeSource::new(2_000, 2),
            probe.writers(4),
            Batcher::new(BatchMode::Sql(ctx), 50, usize::MAX),
            ByteBudget::new(1 << 20),
            Lsn::new(1),
            &CancellationToken::new(),
            &no_progress(),
        )
        .await
        .unwrap();

        assert_eq!(
            *probe.begun.lock().unwrap(),
            vec![0; 4],
            "one call per writer, each before any write"
        );
        assert!(probe.writes() > 0);
    }

    #[tokio::test]
    async fn events_mode_passes_events_to_snapshot_events_batch() {
        let probe = Probe::default();

        let stats = run_copy(
            &mut FakeSource::new(1_000, 2),
            probe.writers(3),
            Batcher::new(BatchMode::Events, 25, usize::MAX),
            ByteBudget::new(1 << 20),
            Lsn::new(777),
            &CancellationToken::new(),
            &no_progress(),
        )
        .await
        .unwrap();

        let expected: Vec<(String, u64)> = {
            let mut v: Vec<_> = (0..1_000u64)
                .map(|id| (format!("t{}", (id / 100) % 2), id))
                .collect();
            v.sort();
            v
        };
        assert_eq!(probe.written(), expected);
        let points = probe.points.lock().unwrap();
        assert_eq!(points.len(), probe.writes());
        assert!(points.iter().all(|p| *p == Lsn::new(777)));
        assert_eq!((stats.rows, stats.tables), (1_000, 2));
        assert_eq!(probe.closed(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn in_flight_bytes_never_exceed_budget_plus_largest_batch() {
        const CAP: usize = 10 * 1024;
        let mut source = FakeSource::new(400, 1);
        let payload = "p".repeat(1000);
        // Ids stay below 1000, so three id digits bound the size of a 4-row batch.
        let largest: usize = 4 * estimate(&event("t0", 999, &payload));
        source.payload = Some(payload);
        let probe = Probe {
            delay: Duration::from_millis(10),
            ..Probe::default()
        };

        let stats = timeout(
            SECS_30,
            run_copy(
                &mut source,
                probe.writers(4),
                Batcher::new(BatchMode::Events, 4, usize::MAX),
                ByteBudget::new(CAP),
                Lsn::new(1),
                &CancellationToken::new(),
                &no_progress(),
            ),
        )
        .await
        .expect("must not hang")
        .unwrap();

        assert_eq!(stats.rows, 400);
        assert!(probe.peak() <= CAP + largest, "peak {}", probe.peak());
        // Four writers would hold four batches at once without the budget.
        assert!(probe.peak() <= CAP, "peak {}", probe.peak());
        assert!(probe.peak() > largest, "writers never overlapped");
    }

    #[tokio::test(start_paused = true)]
    async fn oversize_row_does_not_deadlock() {
        let mut events = Vec::new();
        let mut id = 0;
        let mut push = |payload: &str, n: u64| {
            for _ in 0..n {
                events.push(event("t0", id, payload));
                id += 1;
            }
        };
        push("small", 20);
        push(&"b".repeat(50_000), 1);
        push("small", 20);
        let big = estimate(&events[20]);

        struct VecSource(std::vec::IntoIter<ChangeEvent>);
        #[async_trait]
        impl SnapshotSource for VecSource {
            async fn next_event(&mut self) -> Result<Option<ChangeEvent>> {
                Ok(self.0.next())
            }
        }

        let probe = Probe {
            delay: Duration::from_millis(5),
            ..Probe::default()
        };
        let stats = timeout(
            SECS_30,
            run_copy(
                &mut VecSource(events.into_iter()),
                probe.writers(3),
                Batcher::new(BatchMode::Events, 10, 2048),
                ByteBudget::new(1024),
                Lsn::new(1),
                &CancellationToken::new(),
                &no_progress(),
            ),
        )
        .await
        .expect("oversize row must not deadlock")
        .unwrap();

        assert_eq!(stats.rows, 41);
        assert_eq!(probe.written().len(), 41);
        assert_eq!(probe.peak(), big, "oversize batch is admitted alone");
    }

    #[tokio::test]
    async fn writer_error_aborts_and_is_returned() {
        const TOTAL: u64 = 100_000;
        let mut source = FakeSource::new(TOTAL, 2);
        let pulled = source.pulled.clone();
        let probe = Probe {
            fail_on_write: Some(3),
            ..Probe::default()
        };

        let err = run_copy(
            &mut source,
            probe.writers(4),
            Batcher::new(BatchMode::Events, 10, usize::MAX),
            ByteBudget::new(1 << 20),
            Lsn::new(1),
            &CancellationToken::new(),
            &no_progress(),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("boom"), "got: {err}");
        assert!(
            (pulled.load(SeqCst) as u64) < TOTAL,
            "reader must stop early, pulled {}",
            pulled.load(SeqCst)
        );
        assert_eq!(probe.closed(), 4, "every writer is closed on failure");
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_stops_reader_and_writers_without_hanging() {
        // Endless source with slow writers (reader blocks on the full channel / budget),
        // then a source that goes silent (reader blocks in `next_event`).
        for hang_at_end in [false, true] {
            let mut source = FakeSource::new(if hang_at_end { 50 } else { u64::MAX }, 2);
            source.hang_at_end = hang_at_end;
            let probe = Probe {
                delay: Duration::from_millis(10),
                ..Probe::default()
            };
            let cancel = CancellationToken::new();
            let progress = no_progress();

            let (result, ()) = timeout(SECS_30, async {
                tokio::join!(
                    run_copy(
                        &mut source,
                        probe.writers(4),
                        Batcher::new(BatchMode::Events, 10, usize::MAX),
                        ByteBudget::new(64 * 1024),
                        Lsn::new(1),
                        &cancel,
                        &progress,
                    ),
                    async {
                        sleep(Duration::from_millis(100)).await;
                        cancel.cancel();
                    }
                )
            })
            .await
            .expect("cancellation must not hang");

            let err = result.unwrap_err();
            assert_eq!(
                err.to_string(),
                CdcError::generic("snapshot cancelled").to_string()
            );
            assert_eq!(probe.closed(), 4, "hang_at_end={hang_at_end}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn external_cancel_after_source_eof_is_reported_as_cancelled() {
        // 40 rows in one table at 10 rows per batch: the batcher has nothing left to flush at
        // EOF, so the reader finishes cleanly while two batches are still queued behind the
        // two slow writers.
        let eof = Arc::new(tokio::sync::Notify::new());
        let mut source = FakeSource::new(40, 1);
        source.on_eof = Some(eof.clone());
        let probe = Probe {
            delay: Duration::from_millis(10),
            ..Probe::default()
        };
        let cancel = CancellationToken::new();
        let progress = no_progress();

        let (result, ()) = timeout(SECS_30, async {
            tokio::join!(
                run_copy(
                    &mut source,
                    probe.writers(2),
                    Batcher::new(BatchMode::Events, 10, usize::MAX),
                    ByteBudget::new(1 << 20),
                    Lsn::new(1),
                    &cancel,
                    &progress,
                ),
                async {
                    eof.notified().await;
                    cancel.cancel();
                }
            )
        })
        .await
        .expect("must not hang");

        let err = result.unwrap_err();
        assert_eq!(err.to_string(), cancelled().to_string());
        assert!(probe.written().len() < 40, "queued batches were dropped");
        assert_eq!(probe.closed(), 2);
    }

    fn assert_send<T: Send>(_: &T) {}

    #[test]
    fn run_copy_future_is_send() {
        let (dialect, mappings) = (MySqlDialect, HashMap::new());
        let ctx = RenderContext {
            dialect: &dialect,
            schema_mappings: &mappings,
        };
        let mut source = FakeSource::new(0, 1);
        let cancel = CancellationToken::new();
        let progress = no_progress();

        let future = run_copy(
            &mut source,
            Probe::default().writers(1),
            Batcher::new(BatchMode::Sql(ctx), 10, 1024),
            ByteBudget::new(1024),
            Lsn::new(1),
            &cancel,
            &progress,
        );
        assert_send(&future);
    }

    #[tokio::test]
    async fn empty_source_returns_zero_stats() {
        let probe = Probe::default();

        let stats = run_copy(
            &mut FakeSource::new(0, 1),
            probe.writers(4),
            Batcher::new(BatchMode::Events, 10, usize::MAX),
            ByteBudget::new(1 << 20),
            Lsn::new(1),
            &CancellationToken::new(),
            &no_progress(),
        )
        .await
        .unwrap();

        assert_eq!(
            (stats.rows, stats.bytes, stats.batches, stats.tables),
            (0, 0, 0, 0)
        );
        assert_eq!(probe.writes(), 0);
        assert_eq!(probe.closed(), 4);
    }

    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn copies_into_real_sqlite_file() {
        use crate::destinations::dialects::SqliteDialect;
        use crate::destinations::SQLiteDestination;
        use sqlx::SqlitePool;

        const TOTAL: u64 = 20_000;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot.db").display().to_string();

        let mut setup = SQLiteDestination::new();
        setup.connect(&path).await.unwrap();
        let ddl: Vec<String> = ["t0", "t1"]
            .iter()
            .map(|t| format!("CREATE TABLE \"{t}\" (id INTEGER PRIMARY KEY, name TEXT);"))
            .collect();
        setup.execute_sql_batch_with_hook(&ddl, None).await.unwrap();
        setup.close().await.unwrap();

        let mut writer = SQLiteDestination::new();
        writer.connect(&path).await.unwrap();

        let (dialect, mappings) = (SqliteDialect, HashMap::new());
        let ctx = RenderContext {
            dialect: &dialect,
            schema_mappings: &mappings,
        };
        let stats = run_copy(
            &mut FakeSource::new(TOTAL, 2),
            vec![Box::new(writer)],
            Batcher::new(BatchMode::Sql(ctx), 500, 1 << 20),
            ByteBudget::new(4 << 20),
            Lsn::new(1),
            &CancellationToken::new(),
            &no_progress(),
        )
        .await
        .unwrap();
        assert_eq!((stats.rows, stats.tables), (TOTAL, 2));

        let pool = SqlitePool::connect(&format!("sqlite://{path}"))
            .await
            .unwrap();
        for (table, sql) in [
            ("t0", "SELECT COUNT(*) FROM \"t0\""),
            ("t1", "SELECT COUNT(*) FROM \"t1\""),
        ] {
            let count: i64 = sqlx::query_scalar(sql).fetch_one(&pool).await.unwrap();
            assert_eq!(count as u64, TOTAL / 2, "{table}");
        }
        // id 0..100 -> t0, 100..200 -> t1, 200..300 -> t0 ...
        let name: String = sqlx::query_scalar("SELECT name FROM \"t1\" WHERE id = 150")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(name, "name-150");
        let missing: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM \"t0\" WHERE id = 150")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(missing, 0);
        let max_id: i64 = sqlx::query_scalar("SELECT MAX(id) FROM \"t1\"")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(max_id as u64, TOTAL - 1);
        pool.close().await;
    }
}
