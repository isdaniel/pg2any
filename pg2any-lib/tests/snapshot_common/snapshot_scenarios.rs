//! Destination-independent initial-snapshot scenarios. Each takes the `Target` under test, so the
//! MySQL, SQL Server and Kafka suites can reuse them.
#![allow(dead_code)]

use super::snapshot_harness::{
    client_config, start_client, Digest, PgFixture, RunningClient, Scenario, TableSet, Target,
    MIXED_CASE_TABLE,
};
use pg2any_lib::{Config, SnapshotTargetPolicy};
use pg_walstream::PgReplicationConnection;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

/// Standard tables the scenarios copy; the publication covers all of them.
const S1_TABLES: [&str; 4] = ["snap_items", "snap_wide", "snap_part", "snap_empty"];

const SEED_ITEMS: i64 = 200_000;
/// Rows of ~64 KiB (`repeat(md5, 4096)` is 64 KiB of bytea).
const SEED_WIDE: i64 = 1_000;
/// Rows per `snap_part` partition: ids 1..=10_000 (`snap_part_lo`) and 100_001..=110_000 (`_hi`).
const SEED_PART_PER_PARTITION: i64 = 10_000;
const PART_HI_BASE: i64 = 100_000;

/// The writer's own ids start here, above every seeded id.
const FIRST_LIVE_ID: i64 = 1_000_000;
/// Writer inserts required inside the copy window and after the handoff.
const MIN_WRITES: u64 = 500;
const POLL: Duration = Duration::from_millis(20);
const WRITER_PAUSE: Duration = Duration::from_millis(2);
/// Bounds the copy itself (it took ~15 s here; the writer runs meanwhile).
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(300);
/// Bounds how long the writer may need for `MIN_WRITES + 100` more inserts after the handoff.
const LIVE_PHASE_TIMEOUT: Duration = Duration::from_secs(120);
/// Bounds draining the backlog after the writer stops. Live transactions wait for the handoff, so
/// about `c2` of them are queued then and are applied one commit each: SQLite at ~100 tx/s
/// (~26 s observed), MySQL at ~40 tx/s (110 s observed; its commit is one fsync, 16 ms on the CI
/// disk). A timeout only matters when the destination hangs, so it leaves room for slow disks.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(300);

/// Sets the flag when dropped, so every exit of the scenario (an assertion unwinding included)
/// stops the writer. A `spawn_blocking` loop that keeps running would make the test runtime's
/// drop wait forever instead of reporting the failure.
pub struct StopOnDrop(pub Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

/// Everything the concurrent writer changed besides its inserts.
#[derive(Default)]
struct WriterLog {
    updated: Vec<i64>,
    deleted: Vec<i64>,
}

/// xorshift64: deterministic ids without a `rand` dependency.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Sequential autocommit writer on its own long-lived connection (`PgFixture` opens one per
/// statement, ~55 ms with SCRAM, far too slow to write during a copy window). `inserted` counts
/// inserts that have committed; ids are `FIRST_LIVE_ID + 0, 1, 2, ...`. Besides inserting it
/// updates `note` of a random seeded `snap_items` row every 4th round and deletes a random id
/// < 1000 every 25th round. The short pause per round only keeps the rate near ~115 inserts/s;
/// it does not bound the backlog (see `CONVERGENCE_TIMEOUT`): the writer is stopped once the
/// scenario has its `MIN_WRITES` after the handoff.
fn spawn_writer(
    url: String,
    schema: String,
    inserted: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<WriterLog> {
    tokio::task::spawn_blocking(move || {
        let mut conn = PgReplicationConnection::connect(&url).expect("writer connection");
        let mut run = |sql: String| {
            conn.exec(&sql)
                .unwrap_or_else(|e| panic!("writer statement failed: {e}\n  SQL: {sql}"));
        };
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        let mut log = WriterLog::default();
        let mut round = 0u64;
        while !stop.load(Ordering::Relaxed) {
            let id = FIRST_LIVE_ID + inserted.load(Ordering::Relaxed) as i64;
            run(format!(
                "INSERT INTO {schema}.snap_items (id, name, payload, note) VALUES \
                 ({id}, 'live-{id}', decode(md5('{id}'), 'hex'), \
                 CASE WHEN {id} % 5 = 0 THEN NULL ELSE 'live-note-{id}' END)"
            ));
            inserted.fetch_add(1, Ordering::Relaxed);
            round += 1;
            if round.is_multiple_of(4) {
                let id = 1 + (rng.next() % SEED_ITEMS as u64) as i64;
                run(format!(
                    "UPDATE {schema}.snap_items SET note = 'upd-{round}' WHERE id = {id}"
                ));
                log.updated.push(id);
            }
            if round.is_multiple_of(25) {
                let id = 1 + (rng.next() % 999) as i64;
                run(format!("DELETE FROM {schema}.snap_items WHERE id = {id}"));
                log.deleted.push(id);
            }
            std::thread::sleep(WRITER_PAUSE);
        }
        log
    })
}

async fn seed(pg: &PgFixture, sc: &Scenario) {
    let s = &sc.schema;
    TableSet::seed_items(pg, sc, SEED_ITEMS as u64).await;
    pg.exec(&format!(
        "INSERT INTO {s}.snap_wide SELECT g, decode(repeat(md5(g::text), 4096), 'hex') \
         FROM generate_series(1, {SEED_WIDE}) g"
    ))
    .await;
    pg.exec(&format!(
        "INSERT INTO {s}.snap_part SELECT g, 'lo-' || g FROM generate_series(1, {SEED_PART_PER_PARTITION}) g"
    ))
    .await;
    pg.exec(&format!(
        "INSERT INTO {s}.snap_part SELECT {PART_HI_BASE} + g, 'hi-' || g \
         FROM generate_series(1, {SEED_PART_PER_PARTITION}) g"
    ))
    .await;
}

fn triangular(n: i64) -> i64 {
    n * (n + 1) / 2
}

/// Fails the scenario with the client's own error if it (or the writer) ended unexpectedly.
async fn assert_alive(client: &mut RunningClient, writer: &JoinHandle<WriterLog>, phase: &str) {
    if client.is_finished() {
        let err = client.wait_err(Duration::from_secs(1)).await;
        panic!("the client ended during {phase}: {err:?}");
    }
    assert!(!writer.is_finished(), "the writer ended during {phase}");
}

/// Canonical text of each id's row must be identical in PostgreSQL and at the target; an id
/// missing on both sides (deleted) counts as equal. Rendering: integers in decimal, text as is,
/// NULL as `NULL_TEXT`, bytea as `\x<hex>`. SQLite and MySQL (LONGBLOB) keep replicated bytea as the
/// same `\x<hex>` text they received, so nothing is normalised here: every difference is a real one.
pub async fn rows_equal(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
    table: &str,
    ids: &[i64],
    what: &str,
) {
    let source = pg.rows(sc, table, ids).await;
    for &id in ids {
        assert_eq!(
            target.row(table, id).await.as_ref(),
            source.get(&id),
            "{what}: {table} id {id} differs between the target (left) and PostgreSQL (right)"
        );
    }
}

/// `n` random rows of `table` (as PostgreSQL has them now) compared column by column.
pub async fn sample_equal(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
    table: &str,
    n: usize,
) {
    let rows = pg
        .query(&format!(
            "SELECT id FROM {}.{table} ORDER BY random() LIMIT {n}",
            sc.schema
        ))
        .await;
    let ids: Vec<i64> = rows
        .iter()
        .map(|r| r[0].as_deref().and_then(|v| v.parse().ok()).expect("id"))
        .collect();
    assert!(!ids.is_empty(), "no rows of {table} to sample");
    rows_equal(pg, sc, target, table, &ids, "random sample").await;
}

/// S1: no row missing and none duplicated across the snapshot -> live-stream handoff while a
/// writer keeps committing. The writer starts before the client, and the scenario fails as vacuous
/// unless at least `MIN_WRITES` of its inserts committed inside the copy window (between the
/// slot's creation and the `completed` marker) and `MIN_WRITES` after the handoff.
pub async fn s1_consistent_handoff_under_writes(target: &dyn Target) {
    let begun = Instant::now();
    let pg = Arc::new(PgFixture::connect().await);
    let tag = format!("s1_{}", target.dest_type().to_string().to_lowercase());
    let sc = pg.reset(&tag).await;
    target.reset().await;
    TableSet::create_pg(&pg, &sc, &S1_TABLES).await;
    target.create_tables(&sc, &S1_TABLES).await;
    seed(&pg, &sc).await;
    println!("S1 seeded in {:?}", begun.elapsed());

    let inserted = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_writer_on_exit = StopOnDrop(stop.clone());
    let writer = spawn_writer(
        pg.url().to_string(),
        sc.schema.clone(),
        inserted.clone(),
        stop.clone(),
    );

    let cfg = client_config(&pg, &sc, target)
        .build()
        .expect("valid config");
    let lsn_file = sc.data_dir.path().join("lsn.metadata");
    let mut client = start_client(cfg, &lsn_file).await;
    let mut writer = writer;

    // `c0`: inserts committed by the time the slot's consistent point was on disk (an upper bound
    // of the count at slot creation, which precedes it). `c1_lo`: the count read just before the
    // marker check that first saw `completed`; completion can have happened up to one `POLL`
    // earlier, so it is NOT a strict lower bound (it can exceed the true count by the few inserts
    // of one poll, ~2-3). `c1`: read after `completed` was seen, an upper bound of that count.
    let copy_started = Instant::now();
    let mut c0: Option<u64> = None;
    let (c1_lo, c1, window) = loop {
        assert_alive(&mut client, &writer, "the snapshot").await;
        let before_check = inserted.load(Ordering::Relaxed);
        let state = sc.marker_state();
        if c0.is_none() && sc.marker_consistent_point().is_some() {
            c0 = Some(inserted.load(Ordering::Relaxed));
        }
        if state.as_deref() == Some("completed") {
            break (
                before_check,
                inserted.load(Ordering::Relaxed),
                copy_started.elapsed(),
            );
        }
        assert!(
            copy_started.elapsed() < SNAPSHOT_TIMEOUT,
            "snapshot not completed after {SNAPSHOT_TIMEOUT:?} (marker {state:?})"
        );
        tokio::time::sleep(POLL).await;
    };
    target.on_snapshot_completed().await;
    let c0 = c0.expect("the marker never showed the slot's consistent point");
    println!(
        "S1 snapshot took {window:?}; writer inserts: c0={c0} c1_lo={c1_lo} c1={c1} \
         (window {})",
        c1_lo.saturating_sub(c0)
    );

    // Hand-off phase: keep writing until enough inserts went through the live stream.
    let after_deadline = Instant::now() + LIVE_PHASE_TIMEOUT;
    while inserted.load(Ordering::Relaxed) < c1 + MIN_WRITES + 100 {
        assert_alive(&mut client, &writer, "the live phase").await;
        assert!(
            Instant::now() < after_deadline,
            "writer too slow after the handoff"
        );
        tokio::time::sleep(POLL).await;
    }
    stop.store(true, Ordering::Relaxed);
    let log = (&mut writer).await.expect("writer task");
    let c2 = inserted.load(Ordering::Relaxed);
    println!("S1 writer total: c2={c2}, after handoff {}", c2 - c1);

    assert!(
        c1_lo.saturating_sub(c0) >= MIN_WRITES,
        "snapshot window too short: increase seed rows (only {} inserts between the slot's \
         creation and the completed marker, need {MIN_WRITES})",
        c1_lo.saturating_sub(c0)
    );
    assert!(
        c2 - c1 >= MIN_WRITES,
        "too few inserts after the handoff: {} (need {MIN_WRITES})",
        c2 - c1
    );

    let converge_started = Instant::now();
    client
        .wait_converged(&pg, target, &S1_TABLES, CONVERGENCE_TIMEOUT)
        .await;
    println!(
        "S1 converged {:?} after the writer stopped",
        converge_started.elapsed()
    );

    // Per-table digests, against PostgreSQL and against closed forms that do not depend on it.
    for table in S1_TABLES {
        assert_eq!(
            target.digest(table).await,
            pg.digest(table).await,
            "{table}: target digest differs from PostgreSQL"
        );
    }
    let last = FIRST_LIVE_ID + c2 as i64 - 1;
    let n = c2 as i64;
    let survivors = pg
        .query_i64(&format!(
            "SELECT count(*) FROM {}.snap_items WHERE id < 1000",
            sc.schema
        ))
        .await;
    let survivors_sum = pg
        .query_i64(&format!(
            "SELECT coalesce(sum(id), 0)::bigint FROM {}.snap_items WHERE id < 1000",
            sc.schema
        ))
        .await;
    let items = target.digest("snap_items").await;
    assert_eq!(
        items.count as i64,
        SEED_ITEMS - 999 + survivors + n,
        "snap_items: seeded rows (minus deleted ids) plus every live insert exactly once"
    );
    assert_eq!(
        items.sum_id,
        triangular(SEED_ITEMS) - triangular(999)
            + survivors_sum
            + n * FIRST_LIVE_ID
            + triangular(n - 1),
        "snap_items: id sum"
    );
    assert_eq!(items.max_id, last, "snap_items: highest live id");
    assert_eq!(
        target.digest("snap_wide").await,
        Digest {
            count: SEED_WIDE as u64,
            min_id: 1,
            max_id: SEED_WIDE,
            sum_id: triangular(SEED_WIDE),
        }
    );
    assert_eq!(
        target.digest("snap_part").await,
        Digest {
            count: 2 * SEED_PART_PER_PARTITION as u64,
            min_id: 1,
            max_id: PART_HI_BASE + SEED_PART_PER_PARTITION,
            sum_id: 2 * triangular(SEED_PART_PER_PARTITION)
                + SEED_PART_PER_PARTITION * PART_HI_BASE,
        }
    );
    assert_eq!(target.digest("snap_empty").await.count, 0);

    // Column-by-column comparison: random rows and the first and last live rows, then the
    // writer's updates and deletes.
    sample_equal(&pg, &sc, target, "snap_items", 200).await;
    sample_equal(&pg, &sc, target, "snap_part", 200).await;
    sample_equal(&pg, &sc, target, "snap_wide", 20).await;
    let live_edges: Vec<i64> = (FIRST_LIVE_ID..FIRST_LIVE_ID + 50)
        .chain(last - 49..=last)
        .collect();
    rows_equal(&pg, &sc, target, "snap_items", &live_edges, "live edges").await;
    // EVERY id the writer updated (not a prefix: the id-only digest cannot see updates, and the
    // ones made inside the copy window are what the handoff must not lose or reorder) and EVERY
    // id it deleted, in a couple of statements each.
    let mut updated = log.updated.clone();
    updated.sort_unstable();
    updated.dedup();
    let mut deleted = log.deleted.clone();
    deleted.sort_unstable();
    deleted.dedup();
    assert!(
        updated.len() >= 100 && deleted.len() >= 20,
        "the writer made too few updates/deletes ({}/{}) for the check to mean anything",
        updated.len(),
        deleted.len()
    );
    rows_equal(&pg, &sc, target, "snap_items", &updated, "updated rows").await;
    rows_equal(&pg, &sc, target, "snap_items", &deleted, "deleted ids").await;
    for id in &deleted {
        assert!(
            target.row("snap_items", *id).await.is_none(),
            "snap_items id {id} was deleted at the source but is still at the target"
        );
    }

    assert!(
        client.wait_err(Duration::from_secs(1)).await.is_none(),
        "the client reported an error"
    );
    assert!(!client.is_finished(), "the client stopped on its own");
    assert_eq!(sc.marker_state().as_deref(), Some("completed"));
    assert_eq!(
        pg.slot_count(&sc.slot).await,
        1,
        "exactly one replication slot"
    );
    client.stop().await.expect("clean shutdown");
    println!("S1 total {:?}", begun.elapsed());
}

// ---------------------------------------------------------------------------------------------
// S2-S10: policy, guards, recovery. Small seeds (S10 excepted); every scenario starts from
// `PgFixture::reset` + `Target::reset`.
// ---------------------------------------------------------------------------------------------

pub const SMALL_ROWS: i64 = 2_000;
/// A destination-only row that no source row can collide with.
pub const SENTINEL_ID: i64 = 999_999_999;
/// Bounds a small copy plus handoff (a couple of seconds observed).
pub const SMALL_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds a client that is expected to refuse or fail (slot creation + probe: a few seconds).
const FAILURE_TIMEOUT: Duration = Duration::from_secs(60);
/// Bounds one live row reaching the destination after the handoff.
pub const LIVE_ROW_TIMEOUT: Duration = Duration::from_secs(30);

fn tag(name: &str, target: &dyn Target) -> String {
    format!("{name}_{}", target.dest_type().to_string().to_lowercase())
}

/// Clean PostgreSQL schema/publication/slot and an empty destination; the caller creates tables.
async fn fresh(pg: &PgFixture, target: &dyn Target, name: &str) -> Scenario {
    let sc = pg.reset(&tag(name, target)).await;
    target.reset().await;
    sc
}

/// `fresh` + `snap_items` on both sides + `SMALL_ROWS` source rows.
pub async fn fresh_items(pg: &PgFixture, target: &dyn Target, name: &str) -> Scenario {
    let sc = fresh(pg, target, name).await;
    TableSet::create_pg(pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    TableSet::seed_items(pg, &sc, SMALL_ROWS as u64).await;
    sc
}

fn config(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
    policy: SnapshotTargetPolicy,
) -> Config {
    client_config(pg, sc, target)
        .snapshot_target_policy(policy)
        .build()
        .expect("valid config")
}

async fn start(cfg: Config, sc: &Scenario) -> RunningClient {
    start_client(cfg, &sc.data_dir.path().join("lsn.metadata")).await
}

pub async fn start_with(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
    policy: SnapshotTargetPolicy,
) -> RunningClient {
    start(config(pg, sc, target, policy), sc).await
}

/// The text of the error the client ends with; fails the scenario if it does not end in one.
pub async fn fail_with(client: &mut RunningClient, expectation: &str) -> String {
    match client.wait_err(FAILURE_TIMEOUT).await {
        Some(e) => e.to_string(),
        None => panic!(
            "the client should have failed ({expectation}) but it {}",
            if client.is_finished() {
                "ended cleanly"
            } else {
                "kept running"
            }
        ),
    }
}

/// The operator-facing facts an error must carry (table, env var, ...), not whole sentences.
pub fn assert_mentions(msg: &str, needles: &[&str]) {
    for needle in needles {
        assert!(msg.contains(needle), "error lacks {needle:?}: {msg}");
    }
}

/// Snapshot done (marker `completed`) and every table's digest equal to PostgreSQL's.
pub async fn complete(
    client: &mut RunningClient,
    pg: &PgFixture,
    target: &dyn Target,
    sc: &Scenario,
    tables: &[&str],
) {
    client
        .wait_marker_state(sc, "completed", SMALL_SNAPSHOT_TIMEOUT)
        .await;
    client
        .wait_converged(pg, target, tables, SMALL_SNAPSHOT_TIMEOUT)
        .await;
}

/// Writes a marker file the way a crashed run would have left it (the store type is crate-private).
pub fn write_marker(sc: &Scenario, marker: serde_json::Value) {
    let file = serde_json::json!({ "version": 1, "marker": marker });
    std::fs::write(
        sc.marker_path(),
        serde_json::to_vec(&file).expect("marker json"),
    )
    .expect("write marker file");
}

pub fn marker_bytes(sc: &Scenario) -> Vec<u8> {
    std::fs::read(sc.marker_path()).expect("marker file")
}

fn only_sentinel() -> Digest {
    Digest {
        count: 1,
        min_id: SENTINEL_ID,
        max_id: SENTINEL_ID,
        sum_id: SENTINEL_ID,
    }
}

/// S2: the three target policies against a destination table that already holds a row.
/// `fail_if_not_empty` must refuse before touching anything (no slot, no marker, row kept),
/// `truncate` clears it, `append` keeps it next to every source row.
pub async fn s2_policy_matrix(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;

    let sc = fresh_items(&pg, target, "s2_fail").await;
    target.insert_sentinel("snap_items", SENTINEL_ID).await;
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "non-empty target under fail_if_not_empty").await;
    assert_mentions(&msg, &["snap_items", "CDC_SNAPSHOT_TARGET_POLICY=truncate"]);
    assert_eq!(pg.slot_count(&sc.slot).await, 0, "no slot may survive");
    assert!(!sc.marker_path().exists(), "no marker may survive");
    assert_eq!(target.digest("snap_items").await, only_sentinel());

    // Same state, other policy: the refusal left nothing that blocks the next start.
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::Truncate).await;
    complete(&mut client, &pg, target, &sc, &["snap_items"]).await;
    assert_eq!(
        target.digest("snap_items").await,
        pg.digest("snap_items").await
    );
    assert!(
        target.row("snap_items", SENTINEL_ID).await.is_none(),
        "truncate must remove the destination-only row"
    );
    client.stop().await.expect("clean shutdown");

    let sc = fresh_items(&pg, target, "s2_append").await;
    target.insert_sentinel("snap_items", SENTINEL_ID).await;
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::Append).await;
    client
        .wait_marker_state(&sc, "completed", SMALL_SNAPSHOT_TIMEOUT)
        .await;
    client
        .wait_until(
            "sentinel plus every source row",
            SMALL_SNAPSHOT_TIMEOUT,
            || async {
                let digest = target.digest("snap_items").await;
                (digest.count != SMALL_ROWS as u64 + 1).then(|| format!("{digest:?}"))
            },
        )
        .await;
    assert_eq!(
        target.digest("snap_items").await,
        Digest {
            count: SMALL_ROWS as u64 + 1,
            min_id: 1,
            max_id: SENTINEL_ID,
            sum_id: triangular(SMALL_ROWS) + SENTINEL_ID,
        }
    );
    assert!(
        target.row("snap_items", SENTINEL_ID).await.is_some(),
        "append must keep the destination-only row"
    );
    rows_equal(
        &pg,
        &sc,
        target,
        "snap_items",
        &[1, 7, 1_000, 2_000],
        "append",
    )
    .await;
    client.stop().await.expect("clean shutdown");
    println!("S2 total {:?}", begun.elapsed());
}

/// S3: a slot exists but no completed snapshot marker vouches for the destination. Starting would
/// stream with no baseline, so the client must refuse and touch nothing.
pub async fn s3_existing_slot_without_marker_fails_fast(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh_items(&pg, target, "s3").await;
    pg.create_slot(&sc.slot).await;

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "existing slot, no marker").await;
    assert_mentions(
        &msg,
        &[&sc.slot, "drop the slot", "CDC_INITIAL_SNAPSHOT=false"],
    );
    assert_eq!(
        target.digest("snap_items").await.count,
        0,
        "destination untouched"
    );
    assert_eq!(
        pg.slot_count(&sc.slot).await,
        1,
        "the foreign slot is not ours to drop"
    );
    assert!(!sc.marker_path().exists(), "no marker may be created");
    println!("S3 total {:?}", begun.elapsed());
}

const STRAY_ROWS: i64 = 1_000;
const STRAY_BASE: i64 = 500_000_000;

/// What a crash mid-copy leaves behind, injected: a slot made by SQL, an `in_progress` marker
/// naming it (its `consistent_point` is the slot's `confirmed_flush_lsn`) and the table list, and
/// `STRAY_ROWS` partly loaded rows at the destination. Returns the slot's LSN.
async fn crashed_snapshot(pg: &PgFixture, target: &dyn Target, name: &str) -> (Scenario, u64) {
    let sc = fresh_items(pg, target, name).await;
    let point = pg.create_slot(&sc.slot).await;
    // A real run lists destination names (MySQL database, SQL Server `dbo`), not source schemas.
    let schema = target
        .schema_mapping(&sc)
        .map_or_else(|| sc.schema.clone(), |(_, destination)| destination);
    write_marker(
        &sc,
        serde_json::json!({
            "state": "in_progress",
            "slot_name": sc.slot,
            "consistent_point": point,
            "tables": [[schema, "snap_items"]],
        }),
    );
    for id in 1..=STRAY_ROWS {
        target.insert_sentinel("snap_items", STRAY_BASE + id).await;
    }
    (sc, point)
}

/// S4: restart after a crashed snapshot. `truncate` and `fail_if_not_empty` clean up our own
/// leftovers (slot and partly loaded rows) and re-run; `append` cannot tell snapshot rows from
/// pre-existing ones, so it refuses and changes nothing.
pub async fn s4_crash_recovery_in_progress(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;

    for (name, policy) in [
        ("s4_truncate", SnapshotTargetPolicy::Truncate),
        ("s4_fail", SnapshotTargetPolicy::FailIfNotEmpty),
    ] {
        let (sc, old_point) = crashed_snapshot(&pg, target, name).await;
        assert_eq!(target.digest("snap_items").await.count, STRAY_ROWS as u64);
        let mut client = start_with(&pg, &sc, target, policy).await;
        complete(&mut client, &pg, target, &sc, &["snap_items"]).await;
        assert!(
            target.row("snap_items", STRAY_BASE + 1).await.is_none(),
            "{name}: stray rows must be cleaned up"
        );
        assert_eq!(pg.slot_count(&sc.slot).await, 1, "{name}: exactly one slot");
        assert!(
            pg.slot_confirmed_flush(&sc.slot).await > old_point,
            "{name}: the injected slot must have been replaced by a new one"
        );
        client.stop().await.expect("clean shutdown");
    }

    let (sc, point) = crashed_snapshot(&pg, target, "s4_append").await;
    let marker_before = marker_bytes(&sc);
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::Append).await;
    let msg = fail_with(&mut client, "crash recovery under append").await;
    assert_mentions(&msg, &["CDC_SNAPSHOT_TARGET_POLICY=append", "by hand"]);
    assert_eq!(
        target.digest("snap_items").await.count,
        STRAY_ROWS as u64,
        "the stray rows must be left for the operator"
    );
    assert_eq!(
        pg.slot_count(&sc.slot).await,
        1,
        "the slot must be untouched"
    );
    assert_eq!(pg.slot_confirmed_flush(&sc.slot).await, point);
    assert_eq!(
        marker_bytes(&sc),
        marker_before,
        "the marker must be untouched"
    );
    println!("S4 total {:?}", begun.elapsed());
}

/// S5: a `completed` marker with its slot in place means the baseline exists; a restart must not
/// snapshot again. The restart runs under `truncate`, so a (wrongly) repeated snapshot would
/// wipe the destination-only row and rewrite the marker.
pub async fn s5_completed_marker_skips_snapshot_on_restart(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh_items(&pg, target, "s5").await;
    let policy = SnapshotTargetPolicy::Truncate;

    let mut client = start_with(&pg, &sc, target, policy).await;
    complete(&mut client, &pg, target, &sc, &["snap_items"]).await;
    let digest_before = target.digest("snap_items").await;
    let marker_before = marker_bytes(&sc);
    client.stop().await.expect("clean shutdown");
    assert_eq!(marker_bytes(&sc), marker_before);

    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name, payload, note) \
         VALUES ({}, 'while-stopped', NULL, 'n')",
        sc.schema,
        SMALL_ROWS + 1
    ))
    .await;
    target.insert_sentinel("snap_items", SENTINEL_ID).await;

    let mut client = start_with(&pg, &sc, target, policy).await;
    client
        .wait_for_row(
            target,
            "snap_items",
            SMALL_ROWS + 1,
            |row| row[1] == "while-stopped",
            LIVE_ROW_TIMEOUT,
        )
        .await;
    assert_eq!(
        target.digest("snap_items").await,
        Digest {
            count: digest_before.count + 2,
            min_id: digest_before.min_id,
            max_id: SENTINEL_ID,
            sum_id: digest_before.sum_id + SMALL_ROWS + 1 + SENTINEL_ID,
        },
        "the earlier rows, the destination-only row and the new row: nothing re-copied or wiped"
    );
    assert!(target.row("snap_items", SENTINEL_ID).await.is_some());
    rows_equal(
        &pg,
        &sc,
        target,
        "snap_items",
        &[1, 7, 1_000, 2_000, 2_001],
        "after restart",
    )
    .await;
    assert_eq!(marker_bytes(&sc), marker_before, "marker unchanged");
    assert_eq!(pg.slot_count(&sc.slot).await, 1);
    assert!(!client.is_finished(), "the client stopped on its own");
    client.stop().await.expect("clean shutdown");
    println!("S5 total {:?}", begun.elapsed());
}

/// S6: the slot is gone (dropped by an operator, or invalidated) while the marker says
/// `completed`: WAL since the snapshot may be lost. `fail_if_not_empty` must say so and change
/// nothing; `truncate` re-syncs from scratch.
pub async fn s6_resync_after_slot_loss(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh_items(&pg, target, "s6").await;

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    complete(&mut client, &pg, target, &sc, &["snap_items"]).await;
    let first_point = pg.slot_confirmed_flush(&sc.slot).await;
    let marker_before = marker_bytes(&sc);
    client.stop().await.expect("clean shutdown");

    pg.drop_slot_if_exists(&sc.slot).await;
    assert_eq!(pg.slot_count(&sc.slot).await, 0);
    // A change nobody streamed, and a stale destination row a re-sync must clear.
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name) VALUES ({}, 'after-slot-loss')",
        sc.schema,
        SMALL_ROWS + 1
    ))
    .await;
    target.insert_sentinel("snap_items", SENTINEL_ID).await;

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "slot lost under fail_if_not_empty").await;
    assert_mentions(&msg, &["CDC_SNAPSHOT_TARGET_POLICY=truncate"]);
    assert_eq!(
        marker_bytes(&sc),
        marker_before,
        "the marker must be untouched"
    );
    assert_eq!(pg.slot_count(&sc.slot).await, 0, "no slot may be created");
    assert!(
        target.row("snap_items", SENTINEL_ID).await.is_some(),
        "the destination must be untouched"
    );

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::Truncate).await;
    // The old `completed` marker is cleared by the re-sync; the new slot starts past the old one.
    let (pg_ref, sc_ref) = (&pg, &sc);
    client
        .wait_until("re-synced marker", SMALL_SNAPSHOT_TIMEOUT, || async move {
            let state = sc_ref.marker_state();
            let point = match pg_ref.slot_count(&sc_ref.slot).await {
                1 => pg_ref.slot_confirmed_flush(&sc_ref.slot).await,
                _ => 0,
            };
            match (state.as_deref(), point) {
                (Some("completed"), p) if p > first_point => None,
                _ => Some(format!(
                    "marker {state:?}, slot at {point}, old slot point {first_point}"
                )),
            }
        })
        .await;
    client
        .wait_converged(&pg, target, &["snap_items"], SMALL_SNAPSHOT_TIMEOUT)
        .await;
    assert_eq!(
        target.digest("snap_items").await.count,
        SMALL_ROWS as u64 + 1,
        "every row, including the one written while the slot was gone"
    );
    assert!(
        target.row("snap_items", SENTINEL_ID).await.is_none(),
        "truncate must clear the stale row"
    );
    assert_eq!(pg.slot_count(&sc.slot).await, 1, "exactly one slot");
    client.stop().await.expect("clean shutdown");
    println!("S6 total {:?}", begun.elapsed());
}

/// S7: a published table without a destination table must fail before the first row is copied,
/// and leave neither a slot nor an `in_progress` marker that would demand a cleanup.
pub async fn s7_missing_destination_table_fails_before_copy(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh(&pg, target, "s7").await;
    TableSet::create_pg(&pg, &sc, &["snap_items", "snap_wide", "snap_part"]).await;
    target
        .create_tables(&sc, &["snap_items", "snap_part"])
        .await;
    TableSet::seed_items(&pg, &sc, SMALL_ROWS as u64).await;
    pg.exec(&format!(
        "INSERT INTO {}.snap_wide SELECT g, decode(md5(g::text), 'hex') \
         FROM generate_series(1, 50) g",
        sc.schema
    ))
    .await;

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "no destination table for snap_wide").await;
    assert_mentions(&msg, &["snap_wide"]);
    assert_eq!(
        target.digest("snap_items").await.count,
        0,
        "nothing may be copied before every table is verified"
    );
    assert_eq!(pg.slot_count(&sc.slot).await, 0, "no slot may survive");
    assert_eq!(
        sc.marker_state(),
        None,
        "no marker may survive a failure before the first row"
    );
    println!("S7 total {:?}", begun.elapsed());
}

/// S8: empty tables copy as empty; a publication that resolves to no table at all is refused
/// cleanly and recovers once it has one.
pub async fn s8_empty_tables_and_empty_publication(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let none = Digest {
        count: 0,
        min_id: 0,
        max_id: 0,
        sum_id: 0,
    };

    // (a) every published table is empty.
    let sc = fresh(&pg, target, "s8a").await;
    TableSet::create_pg(&pg, &sc, &["snap_items", "snap_empty"]).await;
    target
        .create_tables(&sc, &["snap_items", "snap_empty"])
        .await;
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    complete(&mut client, &pg, target, &sc, &["snap_items", "snap_empty"]).await;
    assert_eq!(target.digest("snap_items").await, none);
    assert_eq!(target.digest("snap_empty").await, none);
    assert_eq!(pg.slot_count(&sc.slot).await, 1);
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name) VALUES (1, 'first-live')",
        sc.schema
    ))
    .await;
    client
        .wait_for_row(
            target,
            "snap_items",
            1,
            |row| row[1] == "first-live",
            LIVE_ROW_TIMEOUT,
        )
        .await;
    client.stop().await.expect("clean shutdown");

    // (b) `FOR TABLES IN SCHEMA` an empty schema: `PgFixture::reset` makes exactly that.
    // pg_walstream refuses to snapshot a publication without tables ("contain no tables to
    // snapshot"), so the contract here is: a clear refusal that leaves no slot, an `in_progress`
    // marker without tables that the next start cleans up by itself, and a normal snapshot once
    // the publication has a table.
    let sc = fresh(&pg, target, "s8b").await;
    let published = pg
        .query_i64(&format!(
            "SELECT count(*) FROM pg_publication_tables WHERE pubname = '{}'",
            sc.publication
        ))
        .await;
    assert_eq!(published, 0, "the publication must resolve to zero tables");
    // The destination table exists up front; the failed start must not have written to it.
    target.create_tables(&sc, &["snap_items"]).await;
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    let msg = fail_with(&mut client, "publication without tables").await;
    assert_mentions(&msg, &[&sc.publication, "no tables to snapshot"]);
    assert_eq!(pg.slot_count(&sc.slot).await, 0, "no slot may survive");
    assert_eq!(
        target.digest("snap_items").await.count,
        0,
        "no destination writes"
    );
    assert_eq!(sc.marker_state().as_deref(), Some("in_progress"));
    let marker: serde_json::Value = serde_json::from_slice(&marker_bytes(&sc)).expect("marker");
    assert_eq!(
        marker["marker"]["tables"],
        serde_json::json!([]),
        "nothing was loaded, so nothing is left to clean"
    );

    // The first table appears in the schema (a schema publication takes it in on its own);
    // the same config now recovers the empty marker and snapshots it.
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name) VALUES (1, 'in-snapshot')",
        sc.schema
    ))
    .await;
    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    complete(&mut client, &pg, target, &sc, &["snap_items"]).await;
    assert_eq!(pg.slot_count(&sc.slot).await, 1);
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name) VALUES (2, 'live')",
        sc.schema
    ))
    .await;
    client
        .wait_for_row(
            target,
            "snap_items",
            2,
            |row| row[1] == "live",
            LIVE_ROW_TIMEOUT,
        )
        .await;
    client.stop().await.expect("clean shutdown");
    println!("S8 total {:?}", begun.elapsed());
}

/// Text cells exercising quoting, escaping and the COPY text format; every value is NUL-free and
/// none is exactly `t` or `f` (the boolean rewrite of the MySQL/ANSI dialects is a known
/// pre-existing non-goal). Each is a PostgreSQL expression.
const SPECIAL_TEXT: [&str; 13] = [
    "$q$it's$q$",
    "$q$back\\slash \\\\ \\n stays literal$q$",
    "$q$a$q$ || chr(9) || $q$b$q$ || chr(10) || $q$c$q$ || chr(13) || chr(10) || $q$d$q$",
    "$q$héllo wörld — 日本語 🚀 ✓$q$",
    "$q$$q$",
    "$q$\\N$q$",
    "$q$\\.$q$",
    "$q$'; DROP TABLE \"snap_special\"; --$q$",
    "$q$$1 ? %s {} :name @p1$q$",
    "$q$\"double\" 'single' `backtick` [bracket]$q$",
    "$q$NULL$q$",
    "$q$  padded  $q$",
    "repeat($q$ab'\\$q$, 5000)",
];

/// S9: identifiers that need quoting (reserved-word and mixed-case columns, a mixed-case table
/// name) and values that stress escaping; every row must come out identical.
///
/// Nothing is normalised: `Target::row` renders integers in decimal, text as is, NULL as
/// `NULL_TEXT` and binary as `\x<lowercase hex>`. SQLite and MySQL store replicated bytea as that
/// very `\x<hex>` text, so PostgreSQL's `bytea::text` and the target's cell coincide; that hex-text
/// storage is the only destination-specific form involved.
pub async fn s9_special_identifiers_and_values(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh(&pg, target, "s9").await;
    let tables = ["snap_special", MIXED_CASE_TABLE];
    TableSet::create_pg(&pg, &sc, &tables).await;
    target.create_tables(&sc, &tables).await;

    // The text cases: `order` is the value, `Mixed Case` its reverse, `raw` the bytes of its md5.
    let mut ids = Vec::new();
    for (i, text) in SPECIAL_TEXT.iter().enumerate() {
        let id = i as i64 + 1;
        ids.push(id);
        pg.exec(&format!(
            "INSERT INTO {}.snap_special (id, \"order\", \"Mixed Case\", raw) \
             VALUES ({id}, {text}, reverse({text}), decode(md5({text}), 'hex'))",
            sc.schema
        ))
        .await;
    }
    // Empty string vs NULL in each text column, and binary edges: an empty bytea, every byte
    // value, and NUL / 0xff / a literal backslash-x as bytes.
    let first_edge = SPECIAL_TEXT.len() as i64 + 1;
    let edges: [(&str, &str, &str); 5] = [
        ("''", "NULL", "decode('', 'hex')"),
        ("NULL", "''", "NULL"),
        ("NULL", "NULL", "NULL"),
        (
            "'all 256 byte values'",
            "NULL",
            "(SELECT decode(string_agg(lpad(to_hex(g), 2, '0'), ''), 'hex') FROM generate_series(0, 255) g)",
        ),
        (
            "'edge bytes'",
            "NULL",
            "decode('00ff005c7800ffff', 'hex')",
        ),
    ];
    for (i, (order, mixed, raw)) in edges.iter().enumerate() {
        let id = first_edge + i as i64;
        ids.push(id);
        pg.exec(&format!(
            "INSERT INTO {}.snap_special (id, \"order\", \"Mixed Case\", raw) \
             VALUES ({id}, {order}, {mixed}, {raw})",
            sc.schema
        ))
        .await;
    }
    pg.exec(&format!(
        "INSERT INTO {}.\"{MIXED_CASE_TABLE}\" (id, \"group\") \
         VALUES (1, $q$it's \"quoted\"$q$), (2, NULL), (3, '')",
        sc.schema
    ))
    .await;

    let mut client = start_with(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty).await;
    complete(&mut client, &pg, target, &sc, &tables).await;
    assert_eq!(
        target.digest("snap_special").await.count,
        ids.len() as u64,
        "every special row must arrive"
    );
    rows_equal(&pg, &sc, target, "snap_special", &ids, "special values").await;
    rows_equal(
        &pg,
        &sc,
        target,
        MIXED_CASE_TABLE,
        &[1, 2, 3],
        "mixed-case table",
    )
    .await;
    assert_eq!(target.digest(MIXED_CASE_TABLE).await.count, 3);
    // The pairs that matter, read back from the target on their own (not via the shared diff):
    assert_eq!(
        target
            .row("snap_special", first_edge)
            .await
            .expect("empty-string row")[1..3],
        [
            String::new(),
            super::snapshot_harness::NULL_TEXT.to_string()
        ],
        "empty string and NULL stay distinct"
    );
    client.stop().await.expect("clean shutdown");
    println!("S9 total {:?}", begun.elapsed());
}

const CANCEL_ROWS: u64 = 300_000;
/// Cancel once a real partial copy exists (ten 1,000-row batches) ...
const CANCEL_AFTER_ROWS: u64 = 10_000;
/// ... but fail the scenario if the copy outran the poll and is nearly done.
const CANCEL_LATEST_ROWS: u64 = 250_000;
const CANCEL_POLL: Duration = Duration::from_millis(100);
/// Bounds a 300,000-row copy (~20 s here) and the client's shutdown after the cancel.
const BIG_COPY_TIMEOUT: Duration = Duration::from_secs(300);

/// S10 and S12: a clean scenario with `CANCEL_ROWS` source rows, copied slowly enough to be
/// interrupted (one writer, 1,000-row batches).
async fn seeded_big_copy(pg: &PgFixture, target: &dyn Target, name: &str) -> Scenario {
    let begun = Instant::now();
    let sc = fresh(pg, target, name).await;
    TableSet::create_pg(pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    TableSet::seed_items(pg, &sc, CANCEL_ROWS).await;
    println!("{} seeded in {:?}", name.to_uppercase(), begun.elapsed());
    sc
}

fn slow_copy(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
    policy: SnapshotTargetPolicy,
) -> Config {
    client_config(pg, sc, target)
        .snapshot_target_policy(policy)
        .snapshot_writers(1)
        .snapshot_batch_rows(1_000)
        .build()
        .expect("valid config")
}

/// The end of S10 and S12: a restart under `truncate` cleans the partial rows and completes with
/// exactly one slot. Returns the running client for the caller's last checks.
async fn restart_under_truncate_completes(
    pg: &PgFixture,
    sc: &Scenario,
    target: &dyn Target,
) -> RunningClient {
    let mut client = start(
        slow_copy(pg, sc, target, SnapshotTargetPolicy::Truncate),
        sc,
    )
    .await;
    client
        .wait_marker_state(sc, "completed", BIG_COPY_TIMEOUT)
        .await;
    client
        .wait_converged(pg, target, &["snap_items"], BIG_COPY_TIMEOUT)
        .await;
    assert_eq!(target.digest("snap_items").await.count, CANCEL_ROWS);
    assert_eq!(pg.slot_count(&sc.slot).await, 1, "exactly one slot");
    client
}

/// S10: shutdown in the middle of the copy is a failure the next start recovers from: the library
/// discards the slot, the `in_progress` marker stays, and a restart under `truncate` cleans the
/// partial rows and completes.
pub async fn s10_cancel_mid_copy_then_restart_completes(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = seeded_big_copy(&pg, target, "s10").await;

    let mut client = start(
        slow_copy(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty),
        &sc,
    )
    .await;
    let copy_started = Instant::now();
    let copied = loop {
        let copied = target.digest("snap_items").await.count;
        if copied >= CANCEL_AFTER_ROWS {
            break copied;
        }
        assert!(
            !client.is_finished(),
            "the client ended before the copy was under way: {:?}",
            client.wait_err(Duration::from_secs(1)).await
        );
        assert!(
            copy_started.elapsed() < BIG_COPY_TIMEOUT,
            "the copy never reached {CANCEL_AFTER_ROWS} rows (at {copied})"
        );
        tokio::time::sleep(CANCEL_POLL).await;
    };
    assert!(
        copied <= CANCEL_LATEST_ROWS,
        "the copy was already at {copied} rows when first seen; polling is too slow for this test"
    );
    println!(
        "S10 cancelling at {copied} rows after {:?}",
        copy_started.elapsed()
    );
    client.token().cancel();

    let stopped = tokio::time::timeout(BIG_COPY_TIMEOUT, client.stop())
        .await
        .expect("the client did not stop after the cancel");
    stopped.expect("a cancelled snapshot is a clean shutdown");
    assert_eq!(
        sc.marker_state().as_deref(),
        Some("in_progress"),
        "the marker must keep demanding a cleanup"
    );
    assert_eq!(
        pg.slot_count(&sc.slot).await,
        0,
        "the slot must be discarded"
    );
    let partial = target.digest("snap_items").await.count;
    assert!(
        (1..CANCEL_ROWS).contains(&partial),
        "expected a partial copy at the destination, found {partial} rows"
    );
    let cancelled_point = sc.marker_consistent_point().expect("the slot's point");

    let client = restart_under_truncate_completes(&pg, &sc, target).await;
    assert!(pg.slot_confirmed_flush(&sc.slot).await > cancelled_point);
    client.stop().await.expect("clean shutdown");
    println!("S10 total {:?}", begun.elapsed());
}

/// S12: a destination write that fails in the middle of the copy ends the client with an error
/// (never a clean exit or a hang), and, like every failure after the slot's snapshot was taken, it
/// leaves the `in_progress` marker and no orphaned slot; a restart under `truncate` cleans up and
/// completes. Needs a target that implements `break_destination`; others skip.
pub async fn s12_writer_failure_mid_copy_leaves_no_orphan_slot(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = seeded_big_copy(&pg, target, "s12").await;

    let mut client = start(
        slow_copy(&pg, &sc, target, SnapshotTargetPolicy::FailIfNotEmpty),
        &sc,
    )
    .await;
    let copy_started = Instant::now();
    loop {
        let copied = target.digest("snap_items").await.count;
        if copied >= 1 {
            assert!(
                copied <= CANCEL_LATEST_ROWS,
                "the copy was already at {copied} rows when first seen; polling is too slow for this test"
            );
            println!(
                "S12 breaking the destination at {copied} rows after {:?}",
                copy_started.elapsed()
            );
            break;
        }
        assert!(
            !client.is_finished(),
            "the client ended before the copy was under way: {:?}",
            client.wait_err(Duration::from_secs(1)).await
        );
        assert!(
            copy_started.elapsed() < BIG_COPY_TIMEOUT,
            "the copy never started"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    if !target.break_destination("snap_items").await {
        println!(
            "S12 skipped for {:?}: the target cannot break its destination mid-copy",
            target.dest_type()
        );
        client.stop().await.expect("clean shutdown");
        return;
    }

    let failure = client
        .wait_err(BIG_COPY_TIMEOUT)
        .await
        .expect("a destination write failure must end the client with an error");
    println!("S12 the client failed as expected: {failure}");
    assert!(
        !failure.to_string().contains("snapshot cancelled"),
        "the failure is the destination's, not a cancellation: {failure}"
    );
    assert_eq!(
        pg.slot_count(&sc.slot).await,
        0,
        "no orphaned slot after a writer failure"
    );
    assert_eq!(
        sc.marker_state().as_deref(),
        Some("in_progress"),
        "the marker must keep demanding a cleanup"
    );

    target.repair_destination(&sc, "snap_items").await;
    let client = restart_under_truncate_completes(&pg, &sc, target).await;
    client.stop().await.expect("clean shutdown");
    println!("S12 total {:?}", begun.elapsed());
}

// ---------------------------------------------------------------------------------------------
// S13: the client process is killed (SIGKILL) while the live stream is running
// ---------------------------------------------------------------------------------------------

const KILL_WRITERS: usize = 4;
const KILL_TXNS_PER_WRITER: usize = 1_000;
const KILL_CYCLES: usize = 3;
/// Row ids of writer `w` start at `(w + 1) * KILL_ID_STRIDE`; id 0 is the sentinel.
const KILL_ID_STRIDE: i64 = 1_000_000;
const KILL_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);

/// What a kill writer changed besides its inserts.
#[derive(Default)]
struct KillLog {
    updated: Vec<i64>,
    deleted: Vec<i64>,
}

/// Autocommit writer of small mixed transactions (1-8 or 30 inserted rows, a 5-row UPDATE, a
/// DELETE) on its own connection, so that the source out-runs a destination that commits once per
/// transaction and committed-but-unapplied transactions exist whenever the client is killed.
fn spawn_kill_writer(
    url: String,
    schema: String,
    base: i64,
    committed: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<KillLog> {
    tokio::task::spawn_blocking(move || {
        let mut conn = PgReplicationConnection::connect(&url).expect("writer connection");
        let mut next = base + 1;
        let mut log = KillLog::default();
        for txn in 0..KILL_TXNS_PER_WRITER {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let sql = match txn % 10 {
                7 => {
                    let first = next - 15;
                    log.updated.extend(first..first + 5);
                    format!(
                        "UPDATE {schema}.snap_items SET note = 'u{txn}' \
                         WHERE id BETWEEN {first} AND {}",
                        first + 4
                    )
                }
                8 => {
                    let id = next - 12;
                    log.deleted.push(id);
                    format!("DELETE FROM {schema}.snap_items WHERE id = {id}")
                }
                kind => {
                    let rows = if kind == 9 { 30 } else { (txn % 8) as i64 + 1 };
                    let (first, last) = (next, next + rows - 1);
                    next += rows;
                    format!(
                        "INSERT INTO {schema}.snap_items (id, name, payload, note) \
                         SELECT g, 'name-' || g, decode(md5(g::text), 'hex'), \
                         CASE WHEN g % 7 = 0 THEN NULL ELSE 'note-' || g END \
                         FROM generate_series({first}, {last}) AS g"
                    )
                }
            };
            conn.exec(&sql)
                .unwrap_or_else(|e| panic!("writer statement failed: {e}\n  SQL: {sql}"));
            committed.fetch_add(1, Ordering::Relaxed);
        }
        log
    })
}

/// A client running in a child process (the test binary re-executed, see `crash_child_client`),
/// so that `kill` is a real SIGKILL: no shutdown code runs, the memory is gone.
struct CrashChild(std::process::Child);

impl CrashChild {
    fn is_running(&mut self) -> bool {
        matches!(self.0.try_wait(), Ok(None))
    }

    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Drop for CrashChild {
    fn drop(&mut self) {
        self.kill();
    }
}

fn spawn_crash_child(cfg: &Config, lsn_file: &std::path::Path, sc: &Scenario) -> CrashChild {
    let spec = serde_json::json!({
        "source": cfg.source_connection_string,
        "dest_type": format!("{:?}", cfg.destination_type),
        "dest": cfg.destination_connection_string,
        "slot": cfg.replication_slot_name,
        "publication": cfg.publication_name,
        "data_dir": cfg.transaction_file_base_path,
        "lsn_file": lsn_file,
        "schema_mappings": cfg.schema_mappings,
    });
    let log = std::fs::File::create(sc.data_dir.path().join("crash_child.log")).expect("log file");
    let child = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "crash_child_client",
            "--ignored",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("PG2ANY_CRASH_CHILD", spec.to_string())
        .stdout(log.try_clone().expect("log handle"))
        .stderr(log)
        .spawn()
        .expect("spawn the client child process");
    CrashChild(child)
}

/// Waits until PostgreSQL no longer lists the slot as active (a killed client's walsender takes a
/// moment to notice the dead connection) and reports whether it is.
async fn wait_slot_inactive(pg: &PgFixture, sc: &Scenario) {
    let q = format!(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}' AND active",
        sc.slot
    );
    let begun = Instant::now();
    while pg.query_i64(&q).await != 0 {
        assert!(
            begun.elapsed() < KILL_STARTUP_TIMEOUT,
            "the killed client's slot stayed active"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Starts a client child once the slot is free and returns when it has the slot.
async fn start_crash_child(
    pg: &PgFixture,
    sc: &Scenario,
    cfg: &Config,
    lsn_file: &std::path::Path,
) -> CrashChild {
    wait_slot_inactive(pg, sc).await;
    let mut child = spawn_crash_child(cfg, lsn_file, sc);
    let q = format!(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}' AND active",
        sc.slot
    );
    let begun = Instant::now();
    loop {
        if pg.query_i64(&q).await == 1 {
            return child;
        }
        assert!(
            child.is_running(),
            "the client child ended before it had the slot, see {}",
            sc.data_dir.path().join("crash_child.log").display()
        );
        assert!(
            begun.elapsed() < KILL_STARTUP_TIMEOUT,
            "the client child did not take the slot in {KILL_STARTUP_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// S13: a client killed with SIGKILL while the source keeps committing, three times, then restarted
/// in this process, must leave the destination equal to PostgreSQL: nothing lost (committed
/// transactions that were only in memory or in files are re-applied from the slot or the pending
/// files) and nothing applied twice (that fails an INSERT or leaves a duplicate).
pub async fn s13_kill_mid_stream_then_restart_converges(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh(&pg, target, "kill").await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    let cfg = client_config(&pg, &sc, target)
        .initial_snapshot(false)
        .build()
        .expect("valid config");
    let lsn_file = sc.data_dir.path().join("lsn.metadata");

    // The first child creates the slot; the sentinel proves the stream works before any write.
    let mut child = spawn_crash_child(&cfg, &lsn_file, &sc);
    let slot_ready = format!(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}' \
         AND confirmed_flush_lsn IS NOT NULL",
        sc.slot
    );
    let started = Instant::now();
    while pg.query_i64(&slot_ready).await != 1 {
        assert!(
            child.is_running(),
            "the client child ended before its slot existed, see {}",
            sc.data_dir.path().join("crash_child.log").display()
        );
        assert!(started.elapsed() < KILL_STARTUP_TIMEOUT, "no slot in time");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name, payload, note) VALUES (0, 'sentinel', '\\x00', NULL)",
        sc.schema
    ))
    .await;
    let started = Instant::now();
    while target.row("snap_items", 0).await.is_none() {
        assert!(
            child.is_running(),
            "the client child ended before the sentinel arrived"
        );
        assert!(
            started.elapsed() < LIVE_PHASE_TIMEOUT,
            "the sentinel did not reach the destination"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let committed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_guard = StopOnDrop(stop.clone());
    let writers: Vec<_> = (0..KILL_WRITERS)
        .map(|w| {
            spawn_kill_writer(
                pg.url().to_string(),
                sc.schema.clone(),
                (w as i64 + 1) * KILL_ID_STRIDE,
                committed.clone(),
                stop.clone(),
            )
        })
        .collect();
    let total = (KILL_WRITERS * KILL_TXNS_PER_WRITER) as u64;

    let mut behind = 0;
    for cycle in 1..=KILL_CYCLES {
        let kill_at = total * cycle as u64 / (KILL_CYCLES as u64 + 1);
        while committed.load(Ordering::Relaxed) < kill_at {
            assert!(
                child.is_running(),
                "the client child ended on its own, see {}",
                sc.data_dir.path().join("crash_child.log").display()
            );
            tokio::time::sleep(POLL).await;
        }
        if crate::snapshot_common::snapshot_harness::digest_diffs(&pg, target, &["snap_items"])
            .await
            .is_some()
        {
            behind += 1;
        }
        child.kill();
        println!(
            "S13 cycle {cycle}: killed after {} of {total} writer transactions",
            committed.load(Ordering::Relaxed)
        );
        child = start_crash_child(&pg, &sc, &cfg, &lsn_file).await;
    }

    let mut logs = Vec::new();
    for writer in writers {
        logs.push(writer.await.expect("writer"));
    }
    assert_eq!(
        committed.load(Ordering::Relaxed),
        total,
        "every writer finished"
    );
    child.kill();
    println!("S13 the destination was behind PostgreSQL at {behind} of {KILL_CYCLES} kills");
    assert!(
        behind > 0,
        "no kill landed while the destination was behind PostgreSQL: the crash window was never exercised"
    );

    wait_slot_inactive(&pg, &sc).await;
    let mut client = start(cfg, &sc).await;
    client
        .wait_converged(&pg, target, &["snap_items"], CONVERGENCE_TIMEOUT)
        .await;
    let mut ids: Vec<i64> = logs
        .iter()
        .flat_map(|l| l.updated.iter().chain(l.deleted.iter()).copied())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    rows_equal(&pg, &sc, target, "snap_items", &ids, "after the kills").await;
    sample_equal(&pg, &sc, target, "snap_items", 300).await;
    client.stop().await.expect("clean shutdown");
    println!("S13 total {:?}", begun.elapsed());
}

/// S14: a graceful stop (what SIGTERM triggers) in the middle of the stream, with the source still
/// committing and a backlog queued: `stop()` returns only after everything the client had received
/// was applied, so no committed transaction file is left (a transaction still streaming at the stop
/// stays in `sql_received_tx` and is re-sent), the position on disk equals the slot's
/// confirmed position, and a restarted client converges without applying anything twice.
pub async fn s14_graceful_stop_mid_stream_applies_everything(target: &dyn Target) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh(&pg, target, "graceful").await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    let cfg = client_config(&pg, &sc, target)
        .initial_snapshot(false)
        .build()
        .expect("valid config");
    let lsn_file = sc.data_dir.path().join("lsn.metadata");

    let mut client = start(cfg.clone(), &sc).await;
    let slot_ready = format!(
        "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}' \
         AND confirmed_flush_lsn IS NOT NULL",
        sc.slot
    );
    let started = Instant::now();
    while pg.query_i64(&slot_ready).await != 1 {
        assert!(
            !client.is_finished(),
            "the client ended before its slot existed"
        );
        assert!(started.elapsed() < KILL_STARTUP_TIMEOUT, "no slot in time");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name, payload, note) VALUES (0, 'sentinel', '\\x00', NULL)",
        sc.schema
    ))
    .await;
    client
        .wait_for_row(target, "snap_items", 0, |_| true, LIVE_PHASE_TIMEOUT)
        .await;

    let committed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let _stop_guard = StopOnDrop(stop.clone());
    let writers: Vec<_> = (0..KILL_WRITERS)
        .map(|w| {
            spawn_kill_writer(
                pg.url().to_string(),
                sc.schema.clone(),
                (w as i64 + 1) * KILL_ID_STRIDE,
                committed.clone(),
                stop.clone(),
            )
        })
        .collect();
    let total = (KILL_WRITERS * KILL_TXNS_PER_WRITER) as u64;
    while committed.load(Ordering::Relaxed) < total / 2 {
        assert!(!client.is_finished(), "the client ended on its own");
        tokio::time::sleep(POLL).await;
    }

    let stopping = Instant::now();
    client.stop().await.expect("a graceful stop must end Ok");
    println!(
        "S14 graceful stop after {} of {total} writer transactions took {:?}",
        committed.load(Ordering::Relaxed),
        stopping.elapsed()
    );
    // Only sql_pending_tx holds committed work. sql_received_tx keeps the transaction that was
    // mid-stream when the producer stopped; PostgreSQL re-sends it (checked after the restart).
    let left: Vec<_> = std::fs::read_dir(sc.data_dir.path().join("sql_pending_tx"))
        .expect("the transaction directory exists")
        .flatten()
        .map(|e| e.file_name())
        .collect();
    assert!(
        left.is_empty(),
        "committed transactions left unapplied in sql_pending_tx: {left:?}"
    );
    let (_, on_disk) =
        pg2any_lib::LsnTracker::new_with_load(Some(lsn_file.to_str().expect("path"))).await;
    let on_disk = on_disk.expect("a position was persisted").0;
    let confirmed_q = format!(
        "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '{}'",
        sc.slot
    );
    let started = Instant::now();
    loop {
        let rows = pg.query(&confirmed_q).await;
        let confirmed = rows[0][0]
            .as_deref()
            .map(|text| pg_walstream::parse_lsn(text).expect("an LSN"))
            .unwrap_or_default();
        if confirmed == on_disk {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "slot confirmed position {confirmed} differs from the applied position {on_disk}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let mut logs = Vec::new();
    for writer in writers {
        logs.push(writer.await.expect("writer"));
    }
    let mut client = start(cfg, &sc).await;
    client
        .wait_converged(&pg, target, &["snap_items"], CONVERGENCE_TIMEOUT)
        .await;
    let mut ids: Vec<i64> = logs
        .iter()
        .flat_map(|l| l.updated.iter().chain(l.deleted.iter()).copied())
        .collect();
    ids.sort_unstable();
    ids.dedup();
    rows_equal(&pg, &sc, target, "snap_items", &ids, "after the restart").await;
    client.stop().await.expect("clean shutdown");
    println!("S14 total {:?}", begun.elapsed());
}

/// Source transactions of S15: ids `1..=S15_ROWS` inserted, updated, a third of them updated again
/// and every seventh deleted, one autocommit statement (= one source transaction) each.
const S15_ROWS: i64 = 250;
/// The transactions committed after the backlog, with the source silent afterwards.
const S15_TAIL: i64 = 3;

/// S15: a backlog of small transactions (the source committed them while no client ran) is applied
/// in order, including several updates of the same row in different transactions and deletes, and
/// converges to PostgreSQL; with the destination commits counted where the target can (MySQL), far
/// fewer than the transactions, which is what group commit is for. Then the source goes silent
/// after a few more transactions: they must become visible at once, nothing waits for a group to
/// fill up.
pub async fn s15_small_transaction_backlog_converges_and_the_tail_is_not_held_back(
    target: &dyn Target,
) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    let sc = fresh(&pg, target, "s15").await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    pg.create_slot(&sc.slot).await;

    let mut statements: Vec<String> = Vec::new();
    for id in 1..=S15_ROWS {
        statements.push(format!(
            "INSERT INTO {}.snap_items (id, name, payload, note) VALUES ({id}, 'v1-{id}', '\\x00', NULL)",
            sc.schema
        ));
    }
    for id in 1..=S15_ROWS {
        statements.push(format!(
            "UPDATE {}.snap_items SET name = 'v2-{id}' WHERE id = {id}",
            sc.schema
        ));
    }
    for id in (3..=S15_ROWS).step_by(3) {
        statements.push(format!(
            "UPDATE {}.snap_items SET name = 'v3-{id}' WHERE id = {id}",
            sc.schema
        ));
    }
    for id in (7..=S15_ROWS).step_by(7) {
        statements.push(format!(
            "DELETE FROM {}.snap_items WHERE id = {id}",
            sc.schema
        ));
    }
    for statement in &statements {
        pg.exec(statement).await;
    }
    let transactions = statements.len() as u64;

    let cfg = client_config(&pg, &sc, target)
        .initial_snapshot(false)
        .build()
        .expect("valid config");
    let counters_before = target.bench_counters().await;
    let mut client = start(cfg, &sc).await;
    client
        .wait_converged(&pg, target, &["snap_items"], CONVERGENCE_TIMEOUT)
        .await;
    // The digest compares ids only: wait for the last transaction (the delete of the highest
    // multiple of seven) before the content is compared.
    let last_deleted = (7..=S15_ROWS).step_by(7).last().expect("a delete");
    client
        .wait_until(
            "the last transaction of the backlog",
            CONVERGENCE_TIMEOUT,
            || async move {
                target
                    .row("snap_items", last_deleted)
                    .await
                    .map(|_| format!("row {last_deleted} is still there"))
            },
        )
        .await;
    let ids: Vec<i64> = (1..=S15_ROWS).collect();
    rows_equal(&pg, &sc, target, "snap_items", &ids, "the drained backlog").await;
    let name = |id: i64| target.row("snap_items", id);
    assert_eq!(
        name(6).await.map(|r| r[1].clone()).as_deref(),
        Some("v3-6"),
        "the last of three updates of one row wins"
    );
    assert_eq!(name(7).await, None, "a deleted row is gone");

    let counters_after = target.bench_counters().await;
    if let (Some((_, before)), Some((_, after))) = (
        counters_before.iter().find(|(n, _)| *n == "Com_commit"),
        counters_after.iter().find(|(n, _)| *n == "Com_commit"),
    ) {
        let commits = after - before;
        println!("S15 {transactions} source transactions, {commits} destination commits");
        assert!(
            commits * 5 <= transactions,
            "{commits} destination commits for {transactions} source transactions: a queued \
             backlog was not merged"
        );
    }

    // The silent tail: a few transactions, then nothing.
    let tail_started = Instant::now();
    for id in S15_ROWS + 1..=S15_ROWS + S15_TAIL {
        pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name, payload, note) VALUES ({id}, 'tail-{id}', '\\x00', NULL)",
            sc.schema
        ))
        .await;
    }
    client
        .wait_for_row(
            target,
            "snap_items",
            S15_ROWS + S15_TAIL,
            |row| row[1].starts_with("tail-"),
            Duration::from_secs(15),
        )
        .await;
    println!(
        "S15 the silent tail of {S15_TAIL} transactions was visible after {:?}",
        tail_started.elapsed()
    );
    client.stop().await.expect("clean shutdown");
    println!("S15 total {:?}", begun.elapsed());
}

/// The client process of S13 (the parent kills it). Runs the live client from the configuration
/// the parent passes in `PG2ANY_CRASH_CHILD`; a no-op in any other run.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "child process of S13, run by the S13 test"]
async fn crash_child_client() {
    let Ok(spec) = std::env::var("PG2ANY_CRASH_CHILD") else {
        return;
    };
    let spec: serde_json::Value = serde_json::from_str(&spec).expect("child spec");
    let text = |key: &str| spec[key].as_str().expect(key).to_string();
    let destination_type = match text("dest_type").as_str() {
        "MySQL" => pg2any_lib::DestinationType::MySQL,
        "SqlServer" => pg2any_lib::DestinationType::SqlServer,
        "SQLite" => pg2any_lib::DestinationType::SQLite,
        "Kafka" => pg2any_lib::DestinationType::Kafka,
        other => panic!("unknown destination type {other}"),
    };
    let mut builder = Config::builder()
        .source_connection_string(text("source"))
        .destination_type(destination_type)
        .destination_connection_string(text("dest"))
        .replication_slot_name(text("slot"))
        .publication_name(text("publication"))
        .transaction_file_base_path(text("data_dir"))
        .initial_snapshot(false);
    for (source, destination) in spec["schema_mappings"].as_object().expect("mappings") {
        builder = builder.schema_mapping(source.clone(), destination.as_str().expect("schema"));
    }
    let cfg = builder.build().expect("valid config");
    let lsn_file = text("lsn_file");
    let (mut client, start_lsn) = pg2any_lib::CdcClient::new(cfg, Some(&lsn_file))
        .await
        .expect("client");
    // Runs until the parent kills the process.
    client
        .start_replication_from_lsn(start_lsn)
        .await
        .expect("replication");
}

/// Peak-RSS measurement (Linux only: it reads and resets `/proc/self`).
#[cfg(target_os = "linux")]
mod rss {
    /// A `kB` field of `/proc/self/status`, in bytes.
    pub fn status_bytes(field: &str) -> u64 {
        let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
        let line = status
            .lines()
            .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
            .unwrap_or_else(|| panic!("{field} missing from /proc/self/status"));
        let kb: u64 = line
            .trim()
            .trim_end_matches("kB")
            .trim()
            .parse()
            .expect("kB value");
        kb * 1024
    }

    #[cfg(target_env = "gnu")]
    pub fn release_free_heap() {
        extern "C" {
            fn malloc_trim(pad: usize) -> i32;
        }
        // SAFETY: glibc's `malloc_trim` takes no pointers and is safe to call at any time.
        unsafe { malloc_trim(0) };
    }

    #[cfg(not(target_env = "gnu"))]
    pub fn release_free_heap() {}

    /// Resets the process's peak RSS (`VmHWM`) to its current RSS and returns that RSS. Freed
    /// heap that malloc still holds is returned to the OS first, so earlier tests in this
    /// process cannot hide allocations of the measured one by leaving reusable memory behind.
    pub fn reset_peak() -> u64 {
        release_free_heap();
        std::fs::write("/proc/self/clear_refs", "5").expect("reset the peak RSS (clear_refs)");
        let (rss, peak) = (status_bytes("VmRSS"), status_bytes("VmHWM"));
        assert!(
            peak <= rss + (1 << 20),
            "the peak RSS was not reset: VmHWM {peak} B, VmRSS {rss} B"
        );
        rss
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
pub enum MemoryCase {
    /// One row of 64 MB, against the destination's per-row amplification.
    OneHugeRow,
    /// The wide-row scenario with 50 and 400 rows: the peak must not follow the table size, and
    /// stays under an absolute bound.
    TableSizeIndependence,
}

/// 64 MB, not 64 MiB: pg_walstream refuses a COPY message over 128 MiB, and a bytea travels as two
/// hex digits per byte plus framing, so a 64 MiB (67,108,864 byte) value is just over the limit.
/// The largest bytea a snapshot can carry is therefore about 64 MiB; a larger row fails the
/// snapshot with that protocol error and leaves an `in_progress` marker.
#[cfg(target_os = "linux")]
const HUGE_ROW_BYTES: u64 = 64_000_000;
#[cfg(target_os = "linux")]
const WIDE_ROW_BYTES: u64 = 512 * 1024;
/// The byte budget the snapshot is given in S11.
#[cfg(target_os = "linux")]
const MEMORY_BUFFER_BYTES: u64 = 8 * 1024 * 1024;
/// What the process spends on a snapshot regardless of the data: the client itself (12.5 MiB
/// measured with a single 512 KiB row) plus glibc arena retention (15-35 MiB measured), and a
/// run-to-run variance of about 8 MiB. S11a observed 41-55 MiB on SQLite against `FIXED_OVERHEAD +
/// 4 x 8 MiB buffer + 8 x 0.5 MiB row` = 76 MiB (the factor 4 is `Target::buffer_copies`'s default).
#[cfg(target_os = "linux")]
const FIXED_OVERHEAD: u64 = 40 * 1024 * 1024;
/// S11c: the larger table may peak at `1.5 x` the smaller one's delta plus this. Measured with the
/// statement-cache fix: 74.2 / 77.0 / 77.3 MiB for 50 / 200 / 800 rows; without it the delta
/// followed the data (838 MiB at 200 rows, 848 vs 222 MiB for 400 vs 50 rows). Run-to-run variance
/// is about 8 MiB and left a 7 MiB margin once, so the slack is twice that: the guard still has
/// teeth against a bug that moves the delta by hundreds of MiB.
#[cfg(target_os = "linux")]
const SIZE_INDEPENDENCE_SLACK: u64 = 16 * 1024 * 1024;
/// Bounds the copy of the largest S11 case (a few seconds observed) plus a deadlock, which
/// would otherwise hang.
#[cfg(target_os = "linux")]
const MEMORY_SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(300);

#[cfg(target_os = "linux")]
struct CopyMeasurement {
    /// Peak RSS reached during the copy minus the RSS just before it, in bytes.
    delta: u64,
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Seeds `rows` rows of `row_bytes` into `snap_wide` (inside PostgreSQL, before the peak is
/// reset, so the data never counts), snapshots them with an 8 MiB buffer and measures the peak
/// RSS growth of this whole process (PostgreSQL client, destination pool and `CdcClient`
/// included). Also proves the data arrived intact; a row larger than the whole buffer is admitted
/// alone, so a huge row also proves the copy completes instead of waiting for budget that can
/// never be free.
#[cfg(target_os = "linux")]
async fn measure_wide_copy(
    pg: &PgFixture,
    target: &dyn Target,
    name: &str,
    rows: u64,
    row_bytes: u64,
) -> CopyMeasurement {
    let sc = fresh(pg, target, name).await;
    TableSet::create_pg(pg, &sc, &["snap_wide"]).await;
    target.create_tables(&sc, &["snap_wide"]).await;
    // `repeat(md5(id), n)` is 16 * n bytes that differ per row, built by PostgreSQL.
    pg.exec(&format!(
        "INSERT INTO {}.snap_wide SELECT g, decode(repeat(md5(g::text), {}), 'hex') \
         FROM generate_series(1, {rows}) g",
        sc.schema,
        row_bytes / 16
    ))
    .await;
    assert_eq!(
        pg.query_i64(&format!(
            "SELECT min(octet_length(blob)) FROM {}.snap_wide",
            sc.schema
        ))
        .await as u64,
        row_bytes,
        "the seeded rows have the intended size"
    );

    let cfg = client_config(pg, &sc, target)
        .snapshot_buffer_bytes(MEMORY_BUFFER_BYTES as usize)
        .snapshot_writers(2)
        .build()
        .expect("valid config");
    let writers = cfg
        .create_destination()
        .expect("destination")
        .max_snapshot_writers()
        .min(cfg.snapshot_writers);

    let rss_before = rss::reset_peak();
    let started = Instant::now();
    let mut client = start(cfg, &sc).await;
    client
        .wait_marker_state(&sc, "completed", MEMORY_SNAPSHOT_TIMEOUT)
        .await;
    client
        .wait_converged(pg, target, &["snap_wide"], MEMORY_SNAPSHOT_TIMEOUT)
        .await;
    let copy_time = started.elapsed();
    let peak = rss::status_bytes("VmHWM");
    let delta = peak.saturating_sub(rss_before);
    eprintln!(
        "S11 {name}: delta {:.1} MiB (buffer {:.0} MiB, row {:.1} MiB), rows {rows}, \
         writers {writers}, copy {copy_time:.1?}, rss before {:.1} MiB, peak {:.1} MiB",
        mib(delta),
        mib(MEMORY_BUFFER_BYTES),
        mib(row_bytes),
        mib(rss_before),
        mib(peak),
    );

    assert_eq!(
        target.digest("snap_wide").await.count,
        rows,
        "every row arrived"
    );
    // After the measurement: these reads pull whole rows into this process.
    let mut check_ids = vec![1, rows.div_ceil(2) as i64, rows as i64];
    check_ids.dedup();
    rows_equal(pg, &sc, target, "snap_wide", &check_ids, "wide rows").await;
    let copied = target
        .row("snap_wide", 1)
        .await
        .expect("row 1 at the target");
    // `\x` + two hex digits per byte, the form both sides render a bytea in.
    assert_eq!(
        copied[1].len() as u64,
        2 + 2 * row_bytes,
        "row 1 size at the target"
    );
    client.stop().await.expect("clean shutdown");
    CopyMeasurement { delta }
}

/// S11: the snapshot's memory is bounded by its configuration, not by the data.
/// - `OneHugeRow`: peak growth <= `target.row_amplification() x row`; one row far larger than the
///   whole buffer, which is admitted alone and held in several copies by construction.
/// - `TableSizeIndependence`: the contract itself: 400 rows peak at most `1.5 x` the 50-row delta
///   plus a small slack, and at most `FIXED_OVERHEAD + buffer_copies x buffer + 8 x largest row`
///   (the buffer is the configured byte budget, the row term allows the copies of one row no budget
///   can bound). Each run is a fresh scenario and its own measurement window.
#[cfg(target_os = "linux")]
pub async fn s11_memory_bound(target: &dyn Target, case: MemoryCase) {
    let begun = Instant::now();
    let pg = PgFixture::connect().await;
    match case {
        MemoryCase::OneHugeRow => {
            let measured = measure_wide_copy(&pg, target, "s11b", 1, HUGE_ROW_BYTES).await;
            let bound = target.row_amplification() * HUGE_ROW_BYTES;
            assert!(
                measured.delta <= bound,
                "snapshot peak RSS grew by {:.1} MiB, over the bound of {:.1} MiB \
                 ({} x the {:.1} MiB row)",
                mib(measured.delta),
                mib(bound),
                target.row_amplification(),
                mib(HUGE_ROW_BYTES),
            );
            eprintln!(
                "S11b bound {:.1} MiB ({} x row)",
                mib(bound),
                target.row_amplification()
            );
        }
        MemoryCase::TableSizeIndependence => {
            let small = measure_wide_copy(&pg, target, "s11c_50", 50, WIDE_ROW_BYTES).await;
            let large = measure_wide_copy(&pg, target, "s11c_400", 400, WIDE_ROW_BYTES).await;
            let allowed = small.delta + small.delta / 2 + SIZE_INDEPENDENCE_SLACK;
            eprintln!(
                "S11c: delta {:.1} MiB for 50 rows, {:.1} MiB for 400 rows, allowed {:.1} MiB",
                mib(small.delta),
                mib(large.delta),
                mib(allowed)
            );
            assert!(
                large.delta <= allowed,
                "an 8x larger table grew the peak RSS by {:.1} MiB, over 1.5 x {:.1} MiB + {:.0} MiB: \
                 snapshot memory follows the table size",
                mib(large.delta),
                mib(small.delta),
                mib(SIZE_INDEPENDENCE_SLACK),
            );
            let bound =
                FIXED_OVERHEAD + target.buffer_copies() * MEMORY_BUFFER_BYTES + 8 * WIDE_ROW_BYTES;
            assert!(
                large.delta <= bound,
                "snapshot peak RSS grew by {:.1} MiB, over the bound of {:.1} MiB \
                 ({:.0} MiB fixed + {} x {:.0} MiB buffer + 8 x {:.1} MiB largest row)",
                mib(large.delta),
                mib(bound),
                mib(FIXED_OVERHEAD),
                target.buffer_copies(),
                mib(MEMORY_BUFFER_BYTES),
                mib(WIDE_ROW_BYTES),
            );
        }
    }
    println!("S11 total {:?}", begun.elapsed());
}

// ---------------------------------------------------------------------------------------------
// Throughput benchmarks: `bench_snapshot_throughput_<dest>` in each e2e file, run by
// `make snapshot-bench-<dest>`. They are #[ignore]d and the regular targets skip `bench_`.
// ---------------------------------------------------------------------------------------------

#[cfg(target_os = "linux")]
#[allow(unused_imports)]
pub use bench::{
    bench_grid, bench_live, bench_live_drain_grid, bench_live_grid, bench_live_latency,
    bench_snapshot_reference, bench_throughput, BenchDataset, BenchResult, LiveKind, LiveLatency,
    LiveResult, LiveWorkload,
};

#[cfg(target_os = "linux")]
mod bench {
    use super::*;

    /// Bounds one point's copy (and, separately, its convergence check).
    const BENCH_TIMEOUT: Duration = Duration::from_secs(900);
    const MARKER_POLL: Duration = Duration::from_millis(20);
    /// A `Wide` blob is built from 64-byte SHA-512 digests, which no compressor shrinks (the
    /// Kafka producer compresses with lz4 by default; `repeat(md5(..))` data would shrink ~250:1).
    const WIDE_UNIT: u64 = 64;
    const WIDE_ROW_BYTES_DEFAULT: u64 = 8 * 1024;
    /// What the published grids used, named here rather than read from `Config::default()` so that
    /// changing a library default cannot silently change what `make snapshot-bench-*` regenerates:
    /// writers swept at `WRITER_SWEEP_BATCH_ROWS`, batch sizes swept at `GRID_WRITERS`, all of it at a
    /// `GRID_BUFFER_BYTES` byte budget (override: `PG2ANY_BENCH_BUFFER_MB`); the buffer sweep runs
    /// `GRID_WRITERS` writers x `BUFFER_SWEEP_BATCH_ROWS` rows at the buffers it is given.
    const GRID_WRITERS: usize = 4;
    const WRITER_SWEEP_BATCH_ROWS: usize = 5_000;
    const BUFFER_SWEEP_BATCH_ROWS: usize = 20_000;
    const GRID_BUFFER_BYTES: usize = 64 * 1024 * 1024;

    #[derive(Clone, Copy, Debug)]
    pub enum BenchDataset {
        /// `snap_items`: id, short name, 16-byte payload, short note.
        Narrow { rows: u64 },
        /// `snap_wide`: id and an incompressible blob of `row_bytes` (a multiple of 64).
        Wide { rows: u64, row_bytes: u64 },
    }

    impl BenchDataset {
        fn rows(&self) -> u64 {
            match *self {
                Self::Narrow { rows } | Self::Wide { rows, .. } => rows,
            }
        }

        fn name(&self) -> &'static str {
            match self {
                Self::Narrow { .. } => "narrow",
                Self::Wide { .. } => "wide",
            }
        }

        fn table(&self) -> &'static str {
            match self {
                Self::Narrow { .. } => "snap_items",
                Self::Wide { .. } => "snap_wide",
            }
        }
    }

    pub struct BenchResult {
        pub rows: u64,
        /// Column data as PostgreSQL holds it (an id counts 8 bytes, text and bytea their octets),
        /// not the COPY text or the destination's own size.
        pub source_bytes: u64,
        /// From starting the client until the marker says `completed`.
        pub seconds: f64,
        pub rows_per_sec: f64,
        /// MiB of `source_bytes` per second.
        pub mb_per_sec: f64,
        /// Peak RSS of this whole process during the copy minus its RSS just before it.
        pub peak_rss_delta_mib: f64,
        /// CPU time of this whole process (client, destination pool, test harness) during
        /// `seconds`, in cores: 1.0 is one core busy throughout.
        pub client_cores: f64,
        /// RSS growth of this process still left after the client stopped (before / after
        /// `malloc_trim(0)`), against the RSS before the copy: freed-but-retained heap shows as
        /// the difference between the two. Includes what the verification read.
        pub rss_after_stop_mib: f64,
        pub rss_after_trim_mib: f64,
        /// Effective snapshot writers (the configured count, clamped by the destination).
        pub writers: usize,
        /// Of `seconds`, the part before the slot exists (the marker file appears): client start,
        /// source connection, slot creation with its exported snapshot.
        pub slot_seconds: f64,
        /// Of `seconds`, the part before the first row: `slot_seconds` plus the destination's
        /// connection, probe and writer setup.
        pub setup_seconds: f64,
        /// Untimed: how long convergence took to show after `completed`.
        pub verify_seconds: f64,
    }

    /// CPU seconds (user + system) this process has used so far; USER_HZ is 100 on Linux.
    fn process_cpu_seconds() -> f64 {
        let stat = std::fs::read_to_string("/proc/self/stat").expect("/proc/self/stat");
        // Fields 14 (utime) and 15 (stime); the process name (field 2) may hold spaces, so count
        // from the closing parenthesis, after which the fields start at 3.
        let fields: Vec<&str> = stat[stat.rfind(')').expect("process name") + 2..]
            .split(' ')
            .collect();
        let ticks = |i: usize| fields[i].parse::<f64>().expect("CPU ticks");
        (ticks(11) + ticks(12)) / 100.0
    }

    /// Seeds `dataset` inside PostgreSQL, then VACUUM ANALYZEs it so hint bits are set and
    /// autovacuum has no reason to run during the copy. Returns the data's `source_bytes`.
    async fn seed_dataset(pg: &PgFixture, sc: &Scenario, dataset: BenchDataset) -> u64 {
        let s = &sc.schema;
        let table = dataset.table();
        let size = match dataset {
            BenchDataset::Narrow { rows } => {
                TableSet::seed_items(pg, sc, rows).await;
                "8 + octet_length(name) + octet_length(payload) + coalesce(octet_length(note), 0)"
            }
            BenchDataset::Wide { rows, row_bytes } => {
                assert_eq!(
                    row_bytes % WIDE_UNIT,
                    0,
                    "a wide row is built from 64-byte units"
                );
                // Building the blobs costs ~0.6 ms per row, so they are made once per size and
                // every point copies them from this template.
                let template = format!("snap_bench_tpl.wide_{rows}_{row_bytes}");
                pg.exec("CREATE SCHEMA IF NOT EXISTS snap_bench_tpl").await;
                pg.exec(&format!(
                    "CREATE TABLE IF NOT EXISTS {template} AS SELECT g AS id, \
                     (SELECT string_agg(sha512(convert_to(g::text || ':' || i::text, 'UTF8')), ''::bytea) \
                      FROM generate_series(1, {}) i) AS blob \
                     FROM generate_series(1, {rows}) g",
                    row_bytes / WIDE_UNIT
                ))
                .await;
                pg.exec(&format!(
                    "INSERT INTO {s}.snap_wide SELECT id, blob FROM {template}"
                ))
                .await;
                "8 + octet_length(blob)"
            }
        };
        pg.exec(&format!("VACUUM ANALYZE {s}.{table}")).await;
        let count = pg.digest(table).await.count;
        assert_eq!(count, dataset.rows(), "the seeded row count");
        pg.query_i64(&format!(
            "SELECT coalesce(sum({size}), 0)::bigint FROM {s}.{table}"
        ))
        .await as u64
    }

    /// One snapshot of a fresh `dataset` with `writers` snapshot writers and batches of
    /// `batch_rows` rows (a `GRID_BUFFER_BYTES` byte budget unless `PG2ANY_BENCH_BUFFER_MB` is set). The
    /// copy is timed from starting the client to the `completed` marker; convergence of the
    /// destination with PostgreSQL, a sample of whole rows and the destination's bulk-path check
    /// (`Target::on_bench_completed`) follow, untimed.
    pub async fn bench_throughput(
        target: &dyn Target,
        dataset: BenchDataset,
        writers: usize,
        batch_rows: usize,
    ) -> BenchResult {
        let buffer_bytes = std::env::var("PG2ANY_BENCH_BUFFER_MB").map_or_else(
            |_| GRID_BUFFER_BYTES,
            |mb| {
                mb.parse::<usize>()
                    .expect("PG2ANY_BENCH_BUFFER_MB is a MiB count")
                    << 20
            },
        );
        bench_point(target, dataset, writers, batch_rows, buffer_bytes).await
    }

    async fn bench_point(
        target: &dyn Target,
        dataset: BenchDataset,
        writers: usize,
        batch_rows: usize,
        buffer_bytes: usize,
    ) -> BenchResult {
        let pg = PgFixture::connect().await;
        let (table, rows) = (dataset.table(), dataset.rows());
        let sc = fresh(&pg, target, "bench").await;
        TableSet::create_pg(&pg, &sc, &[table]).await;
        target.create_tables(&sc, &[table]).await;
        let source_bytes = seed_dataset(&pg, &sc, dataset).await;

        let cfg = client_config(&pg, &sc, target)
            .snapshot_buffer_bytes(buffer_bytes)
            .snapshot_writers(writers)
            .snapshot_batch_rows(batch_rows)
            .build()
            .expect("valid config");
        let writers = cfg
            .create_destination()
            .expect("destination")
            .max_snapshot_writers()
            .min(cfg.snapshot_writers);

        let rss_before = rss::reset_peak();
        let cpu_before = process_cpu_seconds();
        let started = Instant::now();
        let mut client = start(cfg, &sc).await;
        let (mut slot_made, mut copy_began) = (None, None);
        loop {
            let marker = std::fs::read(sc.marker_path())
                .ok()
                .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok());
            if marker.is_some() {
                slot_made.get_or_insert(started.elapsed());
            }
            match marker.as_ref().map(|m| &m["marker"]) {
                Some(m) if m["state"] == "completed" => break,
                Some(m) if m["tables"].as_array().is_some_and(|t| !t.is_empty()) => {
                    copy_began.get_or_insert(started.elapsed());
                }
                _ => {}
            }
            if client.is_finished() {
                let err = client.wait_err(Duration::from_secs(1)).await;
                panic!("the client ended before the snapshot completed: {err:?}");
            }
            assert!(
                started.elapsed() < BENCH_TIMEOUT,
                "the snapshot did not complete within {BENCH_TIMEOUT:?}"
            );
            tokio::time::sleep(MARKER_POLL).await;
        }
        let elapsed = started.elapsed();
        let cpu = process_cpu_seconds() - cpu_before;
        let peak = rss::status_bytes("VmHWM");

        let verify_started = Instant::now();
        client
            .wait_converged(&pg, target, &[table], BENCH_TIMEOUT)
            .await;
        let verify = verify_started.elapsed();
        let mut ids = vec![1, rows.div_ceil(2) as i64, rows as i64];
        ids.dedup();
        rows_equal(&pg, &sc, target, table, &ids, "benchmark rows").await;
        client.stop().await.expect("clean shutdown");
        let rss_after_stop = rss::status_bytes("VmRSS");
        rss::release_free_heap();
        let rss_after_trim = rss::status_bytes("VmRSS");
        target.on_bench_completed(table, rows, batch_rows).await;

        let seconds = elapsed.as_secs_f64();
        BenchResult {
            rows,
            source_bytes,
            seconds,
            rows_per_sec: rows as f64 / seconds,
            mb_per_sec: mib(source_bytes) / seconds,
            peak_rss_delta_mib: mib(peak.saturating_sub(rss_before)),
            client_cores: cpu / seconds,
            rss_after_stop_mib: mib(rss_after_stop.saturating_sub(rss_before)),
            rss_after_trim_mib: mib(rss_after_trim.saturating_sub(rss_before)),
            writers,
            slot_seconds: slot_made.unwrap_or(elapsed).as_secs_f64(),
            setup_seconds: copy_began.unwrap_or(elapsed).as_secs_f64(),
            verify_seconds: verify.as_secs_f64(),
        }
    }

    /// `(writers, batch_rows)` points: 1, 2, 4, 8 writers at `WRITER_SWEEP_BATCH_ROWS`, then 1,000 /
    /// 5,000 / 20,000 rows at `GRID_WRITERS`. Writers are clamped to what the destination supports
    /// and a point is never repeated.
    fn bench_points(max_writers: usize) -> Vec<(usize, usize)> {
        let mut points = Vec::new();
        let mut add = |writers: usize, batch_rows: usize| {
            let point = (writers.min(max_writers), batch_rows);
            if !points.contains(&point) {
                points.push(point);
            }
        };
        for writers in [1, 2, 4, 8] {
            add(writers, WRITER_SWEEP_BATCH_ROWS);
        }
        for batch_rows in [1_000, 5_000, 20_000] {
            add(GRID_WRITERS, batch_rows);
        }
        points
    }

    fn env_u64(name: &str, default: u64) -> u64 {
        std::env::var(name)
            .ok()
            .map(|v| {
                v.parse()
                    .unwrap_or_else(|_| panic!("{name} is a row count"))
            })
            .unwrap_or(default)
    }

    /// Runs the whole grid against `target`, printing one markdown row per point. A warm-up copy of
    /// 1,000 rows comes first; it also shows the fixed cost of a snapshot. `PG2ANY_BENCH_NARROW_ROWS`
    /// (default 1,000,000) and `PG2ANY_BENCH_WIDE_ROWS` (default 100,000 rows of 8 KiB) size the
    /// datasets; `PG2ANY_BENCH_DATASETS` (`narrow` or `wide`) runs only one of them;
    /// `PG2ANY_BENCH_BUFFER_MB` replaces the default byte budget. `PG2ANY_BENCH_BUFFERS` (a comma
    /// separated list of MiB counts) replaces the grid by a buffer sweep at `GRID_WRITERS` writers x
    /// `BUFFER_SWEEP_BATCH_ROWS` rows, `PG2ANY_BENCH_REPEATS` times round-robin over the buffers.
    pub async fn bench_grid(target: &dyn Target) {
        let datasets = [
            BenchDataset::Narrow {
                rows: env_u64("PG2ANY_BENCH_NARROW_ROWS", 1_000_000),
            },
            BenchDataset::Wide {
                rows: env_u64("PG2ANY_BENCH_WIDE_ROWS", 100_000),
                row_bytes: WIDE_ROW_BYTES_DEFAULT,
            },
        ];
        let max_writers = Config::builder()
            .source_connection_string("postgresql://unused")
            .destination_type(target.dest_type())
            .destination_connection_string(target.connection_string())
            .replication_slot_name("unused")
            .publication_name("unused")
            .build()
            .expect("valid config")
            .create_destination()
            .expect("destination")
            .max_snapshot_writers();
        let points = bench_points(max_writers);

        let warm = bench_throughput(
            target,
            BenchDataset::Narrow { rows: 1_000 },
            GRID_WRITERS,
            WRITER_SWEEP_BATCH_ROWS,
        )
        .await;
        println!(
            "warm-up: 1,000 rows in {:.2} s (slot {:.2} s, setup {:.2} s)",
            warm.seconds, warm.slot_seconds, warm.setup_seconds
        );
        let only = std::env::var("PG2ANY_BENCH_DATASETS").unwrap_or_default();
        let buffers: Vec<usize> = std::env::var("PG2ANY_BENCH_BUFFERS")
            .map(|list| {
                list.split(',')
                    .map(|mb| {
                        mb.trim()
                            .parse()
                            .expect("PG2ANY_BENCH_BUFFERS lists MiB counts")
                    })
                    .collect()
            })
            .unwrap_or_default();
        let repeats = env_u64("PG2ANY_BENCH_REPEATS", 1);
        for dataset in datasets {
            if !only.is_empty() && only != dataset.name() {
                continue;
            }
            if !buffers.is_empty() {
                let (writers, batch_rows) = (GRID_WRITERS, BUFFER_SWEEP_BATCH_ROWS);
                println!("\n{dataset:?}, {writers} writers x {batch_rows} rows, buffer sweep\n");
                println!(
                    "| buffer MiB | run | writers | rows | seconds | rows/s | MiB/s | copy rows/s | peak RSS delta MiB | RSS delta after stop MiB | after malloc_trim MiB | client cores | setup s |"
                );
                println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|");
                for run in 1..=repeats {
                    for &mb in &buffers {
                        let r = bench_point(target, dataset, writers, batch_rows, mb << 20).await;
                        println!(
                            "| {mb} | {run} | {} | {} | {:.1} | {:.0} | {:.2} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} |",
                            r.writers,
                            r.rows,
                            r.seconds,
                            r.rows_per_sec,
                            r.mb_per_sec,
                            r.rows as f64 / (r.seconds - r.setup_seconds).max(1e-3),
                            r.peak_rss_delta_mib,
                            r.rss_after_stop_mib,
                            r.rss_after_trim_mib,
                            r.client_cores,
                            r.setup_seconds,
                        );
                    }
                }
                continue;
            }
            println!("\n{dataset:?}\n");
            println!(
                "| writers | batch_rows | rows | source MiB | seconds | rows/s | MiB/s | copy rows/s | peak RSS delta MiB | client cores | slot s | setup s | verify s |"
            );
            println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|");
            for &(writers, batch_rows) in &points {
                let r = bench_throughput(target, dataset, writers, batch_rows).await;
                println!(
                    "| {} | {batch_rows} | {} | {:.1} | {:.1} | {:.0} | {:.2} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} |",
                    r.writers,
                    r.rows,
                    mib(r.source_bytes),
                    r.seconds,
                    r.rows_per_sec,
                    r.mb_per_sec,
                    r.rows as f64 / (r.seconds - r.setup_seconds).max(1e-3),
                    r.peak_rss_delta_mib,
                    r.client_cores,
                    r.slot_seconds,
                    r.setup_seconds,
                    r.verify_seconds,
                );
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // Live path (`initial_snapshot(false)`): producer -> transaction files -> consumer ->
    // destination, measured like the snapshot so the two can be compared. `bench_live_<dest>` in
    // each e2e file runs `bench_live_grid`; `bench_snapshot_reference_<dest>` runs one snapshot
    // point per dataset at the final defaults on the same day and machine.
    // -----------------------------------------------------------------------------------------

    /// Proves the live stream is up before the clock starts; no workload uses this id.
    const LIVE_SENTINEL_ID: i64 = 0;
    /// UPDATE and DELETE workloads start from this many rows, replicated through the live path
    /// first (transactions of `LIVE_PREFILL_ROWS_PER_TXN` rows).
    const LIVE_PREFILL_ROWS: usize = 100_000;
    const LIVE_PREFILL_ROWS_PER_TXN: usize = 1_000;
    const LIVE_POLL: Duration = Duration::from_millis(100);
    /// Bounds the slot appearing, the sentinel arriving and the final digest converging.
    const LIVE_READY_TIMEOUT: Duration = Duration::from_secs(90);
    /// Bounds one workload (a nominal point takes at most ~4 minutes), so a stall fails loudly.
    const LIVE_WINDOW_TIMEOUT: Duration = Duration::from_secs(900);
    /// Ids compared column by column with PostgreSQL after a point, besides the digest.
    const LIVE_VERIFY_SAMPLE: usize = 200;
    /// A point whose window took at least this long is run once, not twice.
    const LIVE_REPEAT_LIMIT_SECS: f64 = 90.0;
    /// What the snapshot reference runs: the final defaults.
    const REFERENCE_WRITERS: usize = 4;
    const REFERENCE_BATCH_ROWS: usize = 20_000;
    const REFERENCE_BUFFER_BYTES: usize = 16 * 1024 * 1024;

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum LiveKind {
        Insert,
        Update,
        Delete,
    }

    /// `txns` PostgreSQL transactions of `rows_per_txn` rows each, one statement per transaction
    /// (autocommit), on `snap_items` ids `1..=rows_per_txn * txns`. An UPDATE or DELETE workload
    /// changes rows an earlier live INSERT phase replicated.
    #[derive(Clone, Copy, Debug)]
    pub struct LiveWorkload {
        pub kind: LiveKind,
        pub rows_per_txn: usize,
        pub txns: usize,
    }

    impl LiveWorkload {
        pub fn total_rows(&self) -> u64 {
            (self.rows_per_txn * self.txns) as u64
        }
    }

    pub struct LiveResult {
        pub workload: LiveWorkload,
        pub total_rows: u64,
        /// Wall time to commit every transaction at PostgreSQL.
        pub t_source: f64,
        /// From the first transaction until the destination reflects the last change.
        pub t_total: f64,
        /// `t_total - t_source`: how far the destination lagged when the source finished.
        pub tail: f64,
        pub source_tps: f64,
        /// Transactions per second over `t_total`.
        pub apply_tps: f64,
        pub rows_per_sec: f64,
        /// MiB of column data per second (an id counts 8 bytes), over `t_total`.
        pub mb_per_sec: f64,
        /// Peak RSS of this process during the window minus its RSS just before (client,
        /// destination pool and harness).
        pub peak_rss_delta_mib: f64,
        /// CPU time of this process during `t_total`, in cores.
        pub client_cores: f64,
        /// Untimed: replicating the UPDATE/DELETE starting rows (0 for INSERT workloads).
        pub prefill_seconds: f64,
        /// Untimed: convergence and sample checks after the window.
        pub verify_seconds: f64,
        /// `Target::bench_counters` growth over the window.
        pub counters: Vec<(&'static str, u64)>,
    }

    /// The PostgreSQL statement of transaction `txn` (0-based).
    fn live_sql(schema: &str, kind: LiveKind, rows_per_txn: usize, txn: usize) -> String {
        let first = (txn * rows_per_txn) as i64 + 1;
        let last = first + rows_per_txn as i64 - 1;
        match kind {
            LiveKind::Insert if rows_per_txn == 1 => {
                let note = if first % 7 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'note-{first}'")
                };
                format!(
                    "INSERT INTO {schema}.snap_items (id, name, payload, note) VALUES \
                     ({first}, 'name-{first}', decode(md5('{first}'), 'hex'), {note})"
                )
            }
            LiveKind::Insert => format!(
                "INSERT INTO {schema}.snap_items (id, name, payload, note) \
                 SELECT g, 'name-' || g, decode(md5(g::text), 'hex'), \
                 CASE WHEN g % 7 = 0 THEN NULL ELSE 'note-' || g END \
                 FROM generate_series({first}, {last}) AS g"
            ),
            LiveKind::Update => format!(
                "UPDATE {schema}.snap_items SET note = 'u{}' WHERE id BETWEEN {first} AND {last}",
                txn + 1
            ),
            LiveKind::Delete => {
                format!("DELETE FROM {schema}.snap_items WHERE id BETWEEN {first} AND {last}")
            }
        }
    }

    /// `n` ids spread evenly over `lo..=hi`, both ends included.
    fn sample_ids(lo: i64, hi: i64, n: usize) -> Vec<i64> {
        let steps = (n as i64 - 1).max(1);
        let mut ids: Vec<i64> = (0..n as i64).map(|i| lo + (hi - lo) * i / steps).collect();
        ids.extend([lo, hi]);
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// The source: its own connection (like S1's writer), connected before the clock starts, then
    /// every transaction back to back. Returns the seconds from `go` to the last commit.
    fn spawn_live_source(
        url: String,
        schema: String,
        workload: LiveWorkload,
        ready: tokio::sync::oneshot::Sender<()>,
        go: std::sync::mpsc::Receiver<()>,
        stop: Arc<AtomicBool>,
    ) -> JoinHandle<f64> {
        tokio::task::spawn_blocking(move || {
            let mut conn = PgReplicationConnection::connect(&url).expect("source connection");
            let _ = ready.send(());
            go.recv().expect("the start signal");
            let started = Instant::now();
            for txn in 0..workload.txns {
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                let sql = live_sql(&schema, workload.kind, workload.rows_per_txn, txn);
                conn.exec(&sql)
                    .unwrap_or_else(|e| panic!("source statement failed: {e}\n  SQL: {sql}"));
            }
            started.elapsed().as_secs_f64()
        })
    }

    /// Whether the destination already reflects the workload's last change (the one applied last,
    /// since the destination applies in commit order).
    async fn live_probe(target: &dyn Target, kind: LiveKind, id: i64, note: &str) -> bool {
        let row = target.row("snap_items", id).await;
        match kind {
            LiveKind::Insert => row.is_some(),
            LiveKind::Update => row.is_some_and(|r| r[3] == note),
            LiveKind::Delete => row.is_none(),
        }
    }

    struct LiveWindow {
        t_source: f64,
        t_total: f64,
        peak_rss_delta_mib: f64,
        client_cores: f64,
        counters: Vec<(&'static str, u64)>,
    }

    /// Runs `workload` against the running client: the source starts at once, the destination is
    /// polled every `LIVE_POLL` for the last change, and a client that ends first fails the wait.
    async fn live_window(
        pg: &PgFixture,
        sc: &Scenario,
        target: &dyn Target,
        client: &mut RunningClient,
        workload: LiveWorkload,
        stop: &Arc<AtomicBool>,
    ) -> LiveWindow {
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let mut source = Some(spawn_live_source(
            pg.url().to_string(),
            sc.schema.clone(),
            workload,
            ready_tx,
            go_rx,
            stop.clone(),
        ));
        ready_rx.await.expect("the source connected");

        let last_id = workload.total_rows() as i64;
        let last_note = format!("u{}", workload.txns);
        let counters_before = target.bench_counters().await;
        let rss_before = rss::reset_peak();
        let cpu_before = process_cpu_seconds();
        let started = Instant::now();
        go_tx.send(()).expect("the source thread is alive");

        let mut t_source = None;
        let t_total = loop {
            if live_probe(target, workload.kind, last_id, &last_note).await {
                break started.elapsed();
            }
            if source.as_ref().is_some_and(|h| h.is_finished()) {
                t_source = Some(
                    source
                        .take()
                        .expect("the source handle")
                        .await
                        .expect("the source thread panicked"),
                );
            }
            if client.is_finished() {
                let err = client.wait_err(Duration::from_secs(1)).await;
                panic!("the client ended during the {workload:?} workload: {err:?}");
            }
            assert!(
                started.elapsed() < LIVE_WINDOW_TIMEOUT,
                "the destination did not reflect the last change of {workload:?} within \
                 {LIVE_WINDOW_TIMEOUT:?}"
            );
            tokio::time::sleep(LIVE_POLL).await;
        };
        let cpu = process_cpu_seconds() - cpu_before;
        let peak = rss::status_bytes("VmHWM");
        let counters_after = target.bench_counters().await;
        let t_source = match (t_source, source.take()) {
            (Some(seconds), _) => seconds,
            (None, Some(handle)) => handle.await.expect("the source thread panicked"),
            (None, None) => unreachable!("the source result is kept once taken"),
        };
        let counters = counters_after
            .iter()
            .zip(&counters_before)
            .map(|((name, after), (_, before))| (*name, after - before))
            .collect();
        let seconds = t_total.as_secs_f64();
        LiveWindow {
            t_source,
            t_total: seconds,
            peak_rss_delta_mib: mib(peak.saturating_sub(rss_before)),
            client_cores: cpu / seconds,
            counters,
        }
    }

    /// Mean bytes of column data per `snap_items` row as PostgreSQL holds them (an id counts 8).
    async fn live_avg_row_bytes(pg: &PgFixture, sc: &Scenario) -> f64 {
        let rows = pg
            .query(&format!(
                "SELECT coalesce(avg(8 + octet_length(name) + octet_length(payload) \
                 + coalesce(octet_length(note), 0)), 45)::float8 FROM {}.snap_items WHERE id > 0",
                sc.schema
            ))
            .await;
        rows[0][0]
            .as_deref()
            .and_then(|v| v.parse().ok())
            .expect("average row bytes")
    }

    /// A fresh scenario with `snap_items` on both sides and a client running the plain live path,
    /// proven up: a row committed once the slot has its consistent point is delivered whenever the
    /// stream starts, and its arrival shows the whole pipeline works, so what follows starts with
    /// an empty backlog.
    async fn live_setup(target: &dyn Target) -> (PgFixture, Scenario, RunningClient) {
        let pg = PgFixture::connect().await;
        let sc = fresh(&pg, target, "live").await;
        TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
        target.create_tables(&sc, &["snap_items"]).await;

        let cfg = client_config(&pg, &sc, target)
            .initial_snapshot(false)
            .build()
            .expect("valid config");
        let mut client = start(cfg, &sc).await;
        let slot_ready = format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{}' \
             AND confirmed_flush_lsn IS NOT NULL",
            sc.slot
        );
        let begun = Instant::now();
        while pg.query_i64(&slot_ready).await != 1 {
            if client.is_finished() {
                let err = client.wait_err(Duration::from_secs(1)).await;
                panic!("the client ended before its slot existed: {err:?}");
            }
            assert!(
                begun.elapsed() < LIVE_READY_TIMEOUT,
                "the replication slot did not appear within {LIVE_READY_TIMEOUT:?}"
            );
            tokio::time::sleep(LIVE_POLL).await;
        }
        pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name, payload, note) \
             VALUES ({LIVE_SENTINEL_ID}, 'sentinel', '\\x00', NULL)",
            sc.schema
        ))
        .await;
        client
            .wait_for_row(
                target,
                "snap_items",
                LIVE_SENTINEL_ID,
                |_| true,
                LIVE_READY_TIMEOUT,
            )
            .await;
        (pg, sc, client)
    }

    /// One live-path measurement on a fresh scenario and an empty destination (see the section
    /// comment). The destination must converge to PostgreSQL's digest and a sample of whole rows
    /// must match before a result is returned, so a broken run cannot produce a number.
    pub async fn bench_live(target: &dyn Target, workload: LiveWorkload) -> LiveResult {
        let LiveWorkload {
            kind,
            rows_per_txn,
            txns,
        } = workload;
        assert!(rows_per_txn > 0 && txns > 0, "an empty workload");
        let total = workload.total_rows() as i64;
        let (pg, sc, mut client) = live_setup(target).await;
        let stop = Arc::new(AtomicBool::new(false));
        let _stop_on_drop = StopOnDrop(stop.clone());

        let mut prefill_seconds = 0.0;
        let mut avg_row_bytes = 0.0;
        if kind != LiveKind::Insert {
            let prefill = LiveWorkload {
                kind: LiveKind::Insert,
                rows_per_txn: LIVE_PREFILL_ROWS_PER_TXN,
                txns: LIVE_PREFILL_ROWS / LIVE_PREFILL_ROWS_PER_TXN,
            };
            let begun = Instant::now();
            live_window(&pg, &sc, target, &mut client, prefill, &stop).await;
            client
                .wait_converged(&pg, target, &["snap_items"], LIVE_READY_TIMEOUT)
                .await;
            prefill_seconds = begun.elapsed().as_secs_f64();
            avg_row_bytes = live_avg_row_bytes(&pg, &sc).await;
            println!(
                "  [live] {} starting rows replicated in {prefill_seconds:.1} s",
                LIVE_PREFILL_ROWS
            );
        }

        let window = live_window(&pg, &sc, target, &mut client, workload, &stop).await;

        let verify_started = Instant::now();
        client
            .wait_converged(&pg, target, &["snap_items"], LIVE_READY_TIMEOUT)
            .await;
        let mut ids = match kind {
            LiveKind::Insert => {
                let mut ids = sample_ids(1, total, LIVE_VERIFY_SAMPLE);
                ids.push(LIVE_SENTINEL_ID);
                ids
            }
            LiveKind::Update => {
                let mut ids = sample_ids(1, total, LIVE_VERIFY_SAMPLE);
                if total < LIVE_PREFILL_ROWS as i64 {
                    ids.extend(sample_ids(total + 1, LIVE_PREFILL_ROWS as i64, 20));
                }
                ids
            }
            LiveKind::Delete => {
                let mut ids = sample_ids(1, total, 20);
                if total < LIVE_PREFILL_ROWS as i64 {
                    ids.extend(sample_ids(
                        total + 1,
                        LIVE_PREFILL_ROWS as i64,
                        LIVE_VERIFY_SAMPLE,
                    ));
                }
                ids
            }
        };
        ids.dedup();
        rows_equal(&pg, &sc, target, "snap_items", &ids, "live benchmark rows").await;
        let verify_seconds = verify_started.elapsed().as_secs_f64();
        if kind == LiveKind::Insert {
            avg_row_bytes = live_avg_row_bytes(&pg, &sc).await;
        }
        client.stop().await.expect("clean shutdown");

        let rows = workload.total_rows();
        LiveResult {
            workload,
            total_rows: rows,
            t_source: window.t_source,
            t_total: window.t_total,
            tail: (window.t_total - window.t_source).max(0.0),
            source_tps: txns as f64 / window.t_source,
            apply_tps: txns as f64 / window.t_total,
            rows_per_sec: rows as f64 / window.t_total,
            mb_per_sec: rows as f64 * avg_row_bytes / (1024.0 * 1024.0) / window.t_total,
            peak_rss_delta_mib: window.peak_rss_delta_mib,
            client_cores: window.client_cores,
            prefill_seconds,
            verify_seconds,
            counters: window.counters,
        }
    }

    /// The published live grid: `(name, workload)`.
    pub fn live_grid() -> Vec<(&'static str, LiveWorkload)> {
        use LiveKind::{Delete, Insert, Update};
        let point = |name, kind, rows_per_txn, txns| {
            (
                name,
                LiveWorkload {
                    kind,
                    rows_per_txn,
                    txns,
                },
            )
        };
        vec![
            point("insert_1x5000", Insert, 1, 5_000),
            point("insert_10x5000", Insert, 10, 5_000),
            point("insert_100x1000", Insert, 100, 1_000),
            point("insert_1000x300", Insert, 1_000, 300),
            point("insert_10000x30", Insert, 10_000, 30),
            point("update_1x5000", Update, 1, 5_000),
            point("update_100x500", Update, 100, 500),
            point("update_1000x100", Update, 1_000, 100),
            point("delete_100x500", Delete, 100, 500),
        ]
    }

    fn scaled(workload: LiveWorkload, factor: f64) -> LiveWorkload {
        LiveWorkload {
            txns: ((workload.txns as f64 * factor).round() as usize).max(1),
            ..workload
        }
    }

    fn print_live_row(name: &str, run: &str, r: &LiveResult) {
        let counters: Vec<String> = r.counters.iter().map(|(n, v)| format!("{n}={v}")).collect();
        println!(
            "| {name} | {run} | {} | {} | {} | {:.0} | {:.0} | {:.0} | {:.2} | {:.1} | {:.1} | {:.1} | {:.0} | {:.1} | {} |",
            r.workload.rows_per_txn,
            r.workload.txns,
            r.total_rows,
            r.source_tps,
            r.apply_tps,
            r.rows_per_sec,
            r.mb_per_sec,
            r.tail,
            r.t_source,
            r.t_total,
            r.peak_rss_delta_mib,
            r.client_cores,
            counters.join(" "),
        );
    }

    /// Runs the live grid against `target`, printing one markdown row per run. A warm-up of 1,000
    /// rows comes first. Each point runs `PG2ANY_BENCH_LIVE_REPEATS` times (default 2) unless its
    /// window took `LIVE_REPEAT_LIMIT_SECS` or more; a `mean` row follows a repeated point.
    /// `PG2ANY_BENCH_LIVE_POINTS` (comma separated point names, see `live_grid`) selects points,
    /// `PG2ANY_BENCH_LIVE_SCALE` multiplies every point's transaction count (a scaled point is
    /// printed with the transaction count it actually ran).
    pub async fn bench_live_grid(target: &dyn Target) {
        let only: Vec<String> = std::env::var("PG2ANY_BENCH_LIVE_POINTS")
            .unwrap_or_default()
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        let repeats = env_u64("PG2ANY_BENCH_LIVE_REPEATS", 2).max(1);
        let scale: f64 = std::env::var("PG2ANY_BENCH_LIVE_SCALE")
            .map(|s| s.parse().expect("PG2ANY_BENCH_LIVE_SCALE is a number"))
            .unwrap_or(1.0);

        let warm = bench_live(
            target,
            LiveWorkload {
                kind: LiveKind::Insert,
                rows_per_txn: 10,
                txns: 100,
            },
        )
        .await;
        println!(
            "warm-up: 1,000 rows in {:.2} s (source {:.2} s)",
            warm.t_total, warm.t_source
        );
        println!(
            "\n| point | run | rows/txn | txns | rows | source TPS | apply TPS | rows/s | MiB/s | tail s | t_source s | t_total s | peak RSS delta MiB | client cores | destination counters |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
        for (name, nominal) in live_grid() {
            if !only.is_empty() && !only.iter().any(|p| p == name) {
                continue;
            }
            let workload = scaled(nominal, scale);
            let mut runs: Vec<LiveResult> = Vec::new();
            for run in 1..=repeats {
                let r = bench_live(target, workload).await;
                print_live_row(name, &run.to_string(), &r);
                let long = r.t_total >= LIVE_REPEAT_LIMIT_SECS;
                runs.push(r);
                if long {
                    break;
                }
            }
            if runs.len() > 1 {
                let n = runs.len() as f64;
                let mean = |f: fn(&LiveResult) -> f64| runs.iter().map(f).sum::<f64>() / n;
                println!(
                    "| {name} | mean of {} | {} | {} | {} | {:.0} | {:.0} | {:.0} | {:.2} | {:.1} | {:.1} | {:.1} | {:.0} | {:.1} | |",
                    runs.len(),
                    workload.rows_per_txn,
                    workload.txns,
                    workload.total_rows(),
                    mean(|r| r.source_tps),
                    mean(|r| r.apply_tps),
                    mean(|r| r.rows_per_sec),
                    mean(|r| r.mb_per_sec),
                    mean(|r| r.tail),
                    mean(|r| r.t_source),
                    mean(|r| r.t_total),
                    mean(|r| r.peak_rss_delta_mib),
                    mean(|r| r.client_cores),
                );
            }
        }
    }

    /// Commit-to-visible latency of an idle live pipeline, in milliseconds.
    pub struct LiveLatency {
        pub samples: usize,
        pub min_ms: f64,
        pub p50_ms: f64,
        pub p95_ms: f64,
        pub max_ms: f64,
    }

    /// `samples` single-row INSERT transactions, 200 ms apart so the pipeline is idle for each,
    /// timed from the source's COMMIT returning until the destination shows the row (polled every
    /// millisecond; the poll's own query adds to the latency it measures).
    pub async fn bench_live_latency(target: &dyn Target, samples: usize) -> LiveLatency {
        let (pg, sc, mut client) = live_setup(target).await;
        let url = pg.url().to_string();
        let conn = tokio::task::spawn_blocking(move || {
            PgReplicationConnection::connect(&url).expect("source connection")
        })
        .await
        .expect("connect task");
        let conn = Arc::new(std::sync::Mutex::new(conn));
        let mut latencies = Vec::with_capacity(samples);
        for id in 1..=samples as i64 {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let sql = live_sql(&sc.schema, LiveKind::Insert, 1, (id - 1) as usize);
            let source = conn.clone();
            let committed = tokio::task::spawn_blocking(move || {
                let mut conn = source.lock().expect("source connection lock");
                conn.exec(&sql)
                    .unwrap_or_else(|e| panic!("source statement failed: {e}\n  SQL: {sql}"));
                Instant::now()
            })
            .await
            .expect("source task");
            let deadline = Instant::now() + LIVE_READY_TIMEOUT;
            let visible = loop {
                if target.row("snap_items", id).await.is_some() {
                    break Instant::now();
                }
                if client.is_finished() {
                    let err = client.wait_err(Duration::from_secs(1)).await;
                    panic!("the client ended while waiting for row {id}: {err:?}");
                }
                assert!(Instant::now() < deadline, "row {id} never arrived");
                tokio::time::sleep(Duration::from_millis(1)).await;
            };
            latencies.push(visible.duration_since(committed).as_secs_f64() * 1000.0);
        }
        client
            .wait_converged(&pg, target, &["snap_items"], LIVE_READY_TIMEOUT)
            .await;
        client.stop().await.expect("clean shutdown");
        latencies.sort_by(f64::total_cmp);
        let at = |q: usize| latencies[(latencies.len() * q / 100).min(latencies.len() - 1)];
        LiveLatency {
            samples,
            min_ms: latencies[0],
            p50_ms: at(50),
            p95_ms: at(95),
            max_ms: latencies[latencies.len() - 1],
        }
    }

    /// Polls (every `poll`) until the destination reflects the change `live_probe` looks for;
    /// returns when it did. A client that ends first fails the wait.
    async fn wait_visible(
        client: &mut RunningClient,
        target: &dyn Target,
        kind: LiveKind,
        id: i64,
        note: &str,
        poll: Duration,
    ) -> Instant {
        let begun = Instant::now();
        loop {
            if live_probe(target, kind, id, note).await {
                return Instant::now();
            }
            if client.is_finished() {
                let err = client.wait_err(Duration::from_secs(1)).await;
                panic!("the client ended while waiting for row {id}: {err:?}");
            }
            assert!(
                begun.elapsed() < LIVE_WINDOW_TIMEOUT,
                "row {id} did not arrive within {LIVE_WINDOW_TIMEOUT:?}"
            );
            tokio::time::sleep(poll).await;
        }
    }

    pub struct DrainResult {
        pub workload: LiveWorkload,
        /// The source committing the whole workload with no client running.
        pub t_source: f64,
        /// From starting the client until the sentinel (the first transaction of the backlog) is
        /// visible: client start-up plus one transaction.
        pub t_start: f64,
        /// From the sentinel being visible until the last row is.
        pub t_drain: f64,
        pub apply_tps: f64,
        pub rows_per_sec: f64,
        pub peak_rss_delta_mib: f64,
        pub client_cores: f64,
        pub counters: Vec<(&'static str, u64)>,
    }

    /// The pipeline's own capacity, with the source taken out of the picture: the slot is created
    /// first (like an operator would), the whole INSERT workload commits at PostgreSQL with no
    /// client running (the WAL accumulates behind the slot), then the client starts and drains
    /// the backlog. `bench_live` measures source and pipeline together, and when the source is the
    /// slower of the two its number is only a lower bound of what the pipeline can do.
    pub async fn bench_live_drain(target: &dyn Target, workload: LiveWorkload) -> DrainResult {
        assert_eq!(
            workload.kind,
            LiveKind::Insert,
            "only INSERT backlogs are drained"
        );
        let total = workload.total_rows() as i64;
        let pg = PgFixture::connect().await;
        let sc = fresh(&pg, target, "drain").await;
        TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
        target.create_tables(&sc, &["snap_items"]).await;
        pg.create_slot(&sc.slot).await;
        pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name, payload, note) \
             VALUES ({LIVE_SENTINEL_ID}, 'sentinel', '\\x00', NULL)",
            sc.schema
        ))
        .await;

        let stop = Arc::new(AtomicBool::new(false));
        let _stop_on_drop = StopOnDrop(stop.clone());
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (go_tx, go_rx) = std::sync::mpsc::channel();
        let source = spawn_live_source(
            pg.url().to_string(),
            sc.schema.clone(),
            workload,
            ready_tx,
            go_rx,
            stop.clone(),
        );
        ready_rx.await.expect("the source connected");
        go_tx.send(()).expect("the source thread is alive");
        let t_source = source.await.expect("the source thread panicked");

        let cfg = client_config(&pg, &sc, target)
            .initial_snapshot(false)
            .build()
            .expect("valid config");
        let counters_before = target.bench_counters().await;
        let rss_before = rss::reset_peak();
        let cpu_before = process_cpu_seconds();
        let started = Instant::now();
        let mut client = start(cfg, &sc).await;
        let poll = Duration::from_millis(10);
        let first = wait_visible(
            &mut client,
            target,
            LiveKind::Insert,
            LIVE_SENTINEL_ID,
            "",
            poll,
        )
        .await;
        let last = wait_visible(&mut client, target, LiveKind::Insert, total, "", poll).await;
        let cpu = process_cpu_seconds() - cpu_before;
        let peak = rss::status_bytes("VmHWM");
        let counters = target
            .bench_counters()
            .await
            .iter()
            .zip(&counters_before)
            .map(|((name, after), (_, before))| (*name, after - before))
            .collect();

        client
            .wait_converged(&pg, target, &["snap_items"], LIVE_READY_TIMEOUT)
            .await;
        let mut ids = sample_ids(1, total, LIVE_VERIFY_SAMPLE);
        ids.push(LIVE_SENTINEL_ID);
        rows_equal(&pg, &sc, target, "snap_items", &ids, "drained rows").await;
        client.stop().await.expect("clean shutdown");

        let t_drain = last.duration_since(first).as_secs_f64();
        DrainResult {
            workload,
            t_source,
            t_start: first.duration_since(started).as_secs_f64(),
            t_drain,
            apply_tps: workload.txns as f64 / t_drain,
            rows_per_sec: workload.total_rows() as f64 / t_drain,
            peak_rss_delta_mib: mib(peak.saturating_sub(rss_before)),
            client_cores: cpu / last.duration_since(started).as_secs_f64(),
            counters,
        }
    }

    /// `bench_live_drain` over the INSERT points of `live_grid` (`PG2ANY_BENCH_LIVE_POINTS`
    /// selects, default: the points of 100 rows per transaction and more), each run
    /// `PG2ANY_BENCH_LIVE_REPEATS` times (default 2) unless its drain took 90 s or more.
    pub async fn bench_live_drain_grid(target: &dyn Target) {
        let only: Vec<String> = std::env::var("PG2ANY_BENCH_LIVE_POINTS")
            .unwrap_or_default()
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        let repeats = env_u64("PG2ANY_BENCH_LIVE_REPEATS", 2).max(1);
        println!(
            "\n| point (backlog drain) | run | rows/txn | txns | rows | source s | start-up s | drain s | apply TPS | rows/s | peak RSS delta MiB | client cores | destination counters |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|");
        for (name, workload) in live_grid() {
            let wanted = if only.is_empty() {
                workload.kind == LiveKind::Insert && workload.rows_per_txn >= 100
            } else {
                only.iter().any(|p| p == name)
            };
            if !wanted {
                continue;
            }
            let workload = LiveWorkload {
                txns: env_u64("PG2ANY_BENCH_DRAIN_TXNS", workload.txns as u64) as usize,
                ..workload
            };
            for run in 1..=repeats {
                let r = bench_live_drain(target, workload).await;
                let counters: Vec<String> =
                    r.counters.iter().map(|(n, v)| format!("{n}={v}")).collect();
                println!(
                    "| {name} | {run} | {} | {} | {} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} | {:.0} | {:.1} | {} |",
                    workload.rows_per_txn,
                    workload.txns,
                    workload.total_rows(),
                    r.t_source,
                    r.t_start,
                    r.t_drain,
                    r.apply_tps,
                    r.rows_per_sec,
                    r.peak_rss_delta_mib,
                    r.client_cores,
                    counters.join(" "),
                );
                if r.t_drain >= LIVE_REPEAT_LIMIT_SECS {
                    break;
                }
            }
        }
    }

    /// One snapshot point per dataset at the final defaults (4 writers x 20,000 rows, 16 MiB
    /// buffer), twice unless a run took 90 s or more: the reference the live numbers are compared
    /// with on the same machine and day. `PG2ANY_BENCH_NARROW_ROWS` (default 1,000,000) and
    /// `PG2ANY_BENCH_WIDE_ROWS` (default 25,000 rows of 8 KiB) size the datasets.
    pub async fn bench_snapshot_reference(target: &dyn Target) {
        let warm = bench_point(
            target,
            BenchDataset::Narrow { rows: 1_000 },
            REFERENCE_WRITERS,
            REFERENCE_BATCH_ROWS,
            REFERENCE_BUFFER_BYTES,
        )
        .await;
        println!("warm-up: 1,000 rows in {:.2} s", warm.seconds);
        println!(
            "\n| dataset | run | writers | rows | seconds | rows/s | MiB/s | copy rows/s | peak RSS delta MiB | client cores |"
        );
        println!("|---|---|---|---|---|---|---|---|---|---|");
        let datasets = [
            BenchDataset::Narrow {
                rows: env_u64("PG2ANY_BENCH_NARROW_ROWS", 1_000_000),
            },
            BenchDataset::Wide {
                rows: env_u64("PG2ANY_BENCH_WIDE_ROWS", 25_000),
                row_bytes: WIDE_ROW_BYTES_DEFAULT,
            },
        ];
        for dataset in datasets {
            let mut rates = Vec::new();
            for run in 1..=2 {
                let r = bench_point(
                    target,
                    dataset,
                    REFERENCE_WRITERS,
                    REFERENCE_BATCH_ROWS,
                    REFERENCE_BUFFER_BYTES,
                )
                .await;
                println!(
                    "| {} | {run} | {} | {} | {:.1} | {:.0} | {:.2} | {:.0} | {:.1} | {:.1} |",
                    dataset.name(),
                    r.writers,
                    r.rows,
                    r.seconds,
                    r.rows_per_sec,
                    r.mb_per_sec,
                    r.rows as f64 / (r.seconds - r.setup_seconds).max(1e-3),
                    r.peak_rss_delta_mib,
                    r.client_cores,
                );
                rates.push(r.rows_per_sec);
                if r.seconds >= LIVE_REPEAT_LIMIT_SECS {
                    break;
                }
            }
            println!(
                "| {} | mean of {} | | | | {:.0} | | | | |",
                dataset.name(),
                rates.len(),
                rates.iter().sum::<f64>() / rates.len() as f64
            );
        }
    }

    #[test]
    fn live_statements_grid_and_samples() {
        assert_eq!(
            live_sql("s", LiveKind::Insert, 1, 6),
            "INSERT INTO s.snap_items (id, name, payload, note) VALUES \
             (7, 'name-7', decode(md5('7'), 'hex'), NULL)",
            "a single-row INSERT with the 7th note NULL"
        );
        assert!(live_sql("s", LiveKind::Insert, 100, 2).ends_with("generate_series(201, 300) AS g"));
        assert_eq!(
            live_sql("s", LiveKind::Update, 100, 2),
            "UPDATE s.snap_items SET note = 'u3' WHERE id BETWEEN 201 AND 300"
        );
        assert_eq!(
            live_sql("s", LiveKind::Delete, 1, 4),
            "DELETE FROM s.snap_items WHERE id BETWEEN 5 AND 5"
        );

        let grid = live_grid();
        let mut names: Vec<_> = grid.iter().map(|(n, _)| *n).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), grid.len(), "point names are unique");
        let rows: Vec<u64> = grid.iter().map(|(_, w)| w.total_rows()).collect();
        assert_eq!(
            rows,
            [5_000, 50_000, 100_000, 300_000, 300_000, 5_000, 50_000, 100_000, 50_000],
            "the grid of the task"
        );
        assert_eq!(scaled(grid[0].1, 0.1).txns, 500);
        assert_eq!(scaled(grid[0].1, 0.0).txns, 1, "never an empty workload");

        assert_eq!(sample_ids(1, 5, 200), vec![1, 2, 3, 4, 5]);
        let ids = sample_ids(1, 100_000, 200);
        assert_eq!((ids.first(), ids.last()), (Some(&1), Some(&100_000)));
        assert!(ids.windows(2).all(|w| w[0] < w[1]) && ids.len() == 200);
    }

    #[test]
    fn bench_points_never_repeat_and_respect_the_writer_limit() {
        for max in [1, 2, usize::MAX] {
            let points = bench_points(max);
            assert!(points.iter().all(|&(writers, _)| writers <= max));
            let mut unique = points.clone();
            unique.sort();
            unique.dedup();
            assert_eq!(unique.len(), points.len(), "max {max}: {points:?}");
        }
        assert_eq!(
            bench_points(usize::MAX),
            vec![
                (1, 5_000),
                (2, 5_000),
                (4, 5_000),
                (8, 5_000),
                (4, 1_000),
                (4, 20_000)
            ],
            "the published grid"
        );
        assert_eq!(
            bench_points(1),
            vec![(1, 5_000), (1, 1_000), (1, 20_000)],
            "one writer: only the batch sweep"
        );
    }
}
