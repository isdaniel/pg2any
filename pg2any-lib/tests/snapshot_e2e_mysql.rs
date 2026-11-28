//! Initial-snapshot end-to-end scenarios: live PostgreSQL source, MySQL destination.
//!
//! Needs running PostgreSQL and MySQL 8 (see `docker-compose.snapshot-test.yml`, profile `mysql`);
//! run with `make snapshot-test-mysql`.
#![cfg(feature = "mysql")]

#[allow(dead_code)]
mod snapshot_common;

use async_trait::async_trait;
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Value};
use pg2any_lib::DestinationType;
use snapshot_common::snapshot_harness::{
    client_config, start_client, Digest, PgFixture, Scenario, TableSet, Target, NULL_TEXT,
};
use snapshot_common::snapshot_scenarios::{self, rows_equal};
use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

const DEFAULT_MYSQL_URL: &str = "mysql://cdc_user:test.123@127.0.0.1:3306/cdc_db";

/// MySQL has no schemas: the destination "schema" of every scenario is this one database.
struct MySqlTarget {
    url: String,
    database: String,
    pool: Pool,
    /// `(Com_load, Com_insert)` as of the latest `reset`, for `on_snapshot_completed`.
    baseline: Mutex<(u64, u64)>,
}

impl MySqlTarget {
    fn new() -> Self {
        let url = std::env::var("PG2ANY_TEST_MYSQL_URL")
            .unwrap_or_else(|_| DEFAULT_MYSQL_URL.to_string());
        let opts = Opts::from_url(&url).expect("PG2ANY_TEST_MYSQL_URL is a MySQL URL");
        let database = opts
            .db_name()
            .expect("PG2ANY_TEST_MYSQL_URL names a database")
            .to_string();
        let pool_opts = PoolOpts::default()
            .with_constraints(PoolConstraints::new(1, 4).expect("pool constraints"));
        Self {
            url,
            database,
            pool: Pool::new(OptsBuilder::from_opts(opts).pool_opts(pool_opts)),
            baseline: Mutex::new((0, 0)),
        }
    }

    async fn run(&self, sql: &str) {
        let mut conn = self.pool.get_conn().await.expect("MySQL connection");
        conn.query_drop(sql)
            .await
            .unwrap_or_else(|e| panic!("MySQL statement failed: {e}\n  SQL: {sql}"));
    }

    /// Statements the server has run so far: `(LOAD DATA, INSERT)`.
    async fn load_counters(&self) -> (u64, u64) {
        (
            self.status("Com_load").await,
            self.status("Com_insert").await,
        )
    }

    async fn status(&self, name: &str) -> u64 {
        let mut conn = self.pool.get_conn().await.expect("MySQL connection");
        let row: Option<(String, String)> = conn
            .query_first(format!("SHOW GLOBAL STATUS LIKE '{name}'"))
            .await
            .expect("SHOW GLOBAL STATUS");
        row.unwrap_or_else(|| panic!("no status variable {name}"))
            .1
            .parse()
            .expect("numeric status value")
    }
}

/// How many INSERT statements may reach the server between the copy's end and S1's next look at
/// the counters: live transactions start only after the handoff, and S1 polls the marker every
/// 20 ms, which fits at most a couple of 16 ms commits. A fallback adds one INSERT per batch.
const LIVE_INSERTS_AFTER_HANDOFF: u64 = 20;
/// S1 copies at least 200,000 `snap_items` rows in batches of at most 200 rows.
const S1_MIN_BATCHES: u64 = 1_000;

/// Canonical text of one cell: integers in decimal, text as is, NULL as `NULL_TEXT`. A bytea the
/// source sends in text form (`\x<hex>`, what both snapshot and live stream carry) is stored as
/// exactly that text in its LONGBLOB, so the stored bytes read back as the very same text; bytes
/// that are not UTF-8 show up as a marker that matches no source value.
fn cell_text(value: Value) -> String {
    match value {
        Value::NULL => NULL_TEXT.to_string(),
        Value::Bytes(bytes) => String::from_utf8(bytes)
            .unwrap_or_else(|e| format!("<{} bytes that are not UTF-8>", e.as_bytes().len())),
        Value::Int(i) => i.to_string(),
        Value::UInt(u) => u.to_string(),
        other => panic!("unexpected MySQL value {other:?}"),
    }
}

#[async_trait]
impl Target for MySqlTarget {
    fn dest_type(&self) -> DestinationType {
        DestinationType::MySQL
    }

    fn connection_string(&self) -> String {
        self.url.clone()
    }

    async fn create_tables(&self, _s: &Scenario, which: &[&str]) {
        for table in which {
            self.run(&TableSet::mysql_ddl(table)).await;
        }
    }

    async fn digest(&self, table: &str) -> Digest {
        let sql = format!(
            "SELECT CAST(count(*) AS SIGNED), CAST(coalesce(min(id), 0) AS SIGNED), \
             CAST(coalesce(max(id), 0) AS SIGNED), CAST(coalesce(sum(id), 0) AS SIGNED) \
             FROM `{table}`"
        );
        let mut conn = self.pool.get_conn().await.expect("MySQL connection");
        let (count, min_id, max_id, sum_id): (i64, i64, i64, i64) = conn
            .query_first(&sql)
            .await
            .unwrap_or_else(|e| panic!("MySQL digest failed: {e}\n  SQL: {sql}"))
            .expect("an aggregate query returns a row");
        Digest {
            count: count as u64,
            min_id,
            max_id,
            sum_id,
        }
    }

    async fn row(&self, table: &str, id: i64) -> Option<Vec<String>> {
        let sql = format!("SELECT * FROM `{table}` WHERE id = {id}");
        let mut conn = self.pool.get_conn().await.expect("MySQL connection");
        let row: Option<mysql_async::Row> = conn
            .query_first(&sql)
            .await
            .unwrap_or_else(|e| panic!("MySQL row query failed: {e}\n  SQL: {sql}"));
        row.map(|r| r.unwrap().into_iter().map(cell_text).collect())
    }

    async fn insert_sentinel(&self, table: &str, id: i64) {
        self.run(&format!("INSERT INTO `{table}` (id) VALUES ({id})"))
            .await;
    }

    async fn reset(&self) {
        for table in TableSet::ALL {
            self.run(&format!("DROP TABLE IF EXISTS `{table}`")).await;
        }
        *self.baseline.lock().unwrap() = self.load_counters().await;
    }

    /// S1: the copy is over and no live row was applied yet (give or take the poll window), so the
    /// server must have seen a LOAD DATA per batch and no INSERT: a silent fallback to
    /// multi-value INSERT (the destination does that on any LOAD DATA error) would show here.
    async fn on_snapshot_completed(&self) {
        let (loads0, inserts0) = *self.baseline.lock().unwrap();
        let (loads, inserts) = self.load_counters().await;
        let (loads, inserts) = (loads - loads0, inserts - inserts0);
        println!("S1 at the handoff: Com_load delta {loads}, Com_insert delta {inserts}");
        assert!(
            loads >= S1_MIN_BATCHES,
            "{loads} LOAD DATA statements for the S1 copy, expected at least {S1_MIN_BATCHES} batches"
        );
        assert!(
            inserts <= LIVE_INSERTS_AFTER_HANDOFF,
            "{inserts} INSERT statements during the S1 copy: the snapshot fell back from LOAD DATA"
        );
    }

    /// Every batch of the benchmark copy must have been a LOAD DATA: at least one per `batch_rows`
    /// rows, and no INSERT at all since the `reset` that took the baseline.
    async fn on_bench_completed(&self, _table: &str, rows: u64, batch_rows: usize) {
        let (loads0, inserts0) = *self.baseline.lock().unwrap();
        let (loads, inserts) = self.load_counters().await;
        let (loads, inserts) = (loads - loads0, inserts - inserts0);
        println!("bench: Com_load delta {loads}, Com_insert delta {inserts} for {rows} rows");
        assert!(
            loads >= rows.div_ceil(batch_rows as u64),
            "{loads} LOAD DATA statements for {rows} rows in batches of at most {batch_rows}"
        );
        assert_eq!(
            inserts, 0,
            "INSERT statements reached the server: the benchmark fell back from LOAD DATA"
        );
    }

    /// Statement and commit counters the live-path benchmark diffs: `Com_load` shows the LOAD
    /// DATA fast path, `Com_insert` the INSERT statements, `Com_commit` the commits and
    /// `Innodb_os_log_fsyncs` the redo-log fsyncs.
    async fn bench_counters(&self) -> Vec<(&'static str, u64)> {
        let mut counters = Vec::new();
        for name in [
            "Com_load",
            "Com_insert",
            "Com_update",
            "Com_delete",
            "Com_commit",
            "Innodb_os_log_fsyncs",
        ] {
            counters.push((name, self.status(name).await));
        }
        counters
    }

    /// Measured by S11b with the LOAD DATA path proven active (`Com_load` +1, `Com_insert` +0,
    /// see `with_load_data_guard`): the 64 MB row peaked at 614.0 MiB (613.5-615.2 over seven
    /// runs), 10.06x its size: the COPY frame, the rendered literal, the TSV image and the driver's packet copy
    /// each hold the row's 2x hex text. Rounded up with ~15% headroom.
    fn row_amplification(&self) -> u64 {
        12
    }

    /// Measured by S11a with the LOAD DATA path proven active (100 LOAD DATA, 0 INSERT): 69-83
    /// MiB over eight runs (SQLite 41-55 MiB). Besides the budgeted rendered
    /// text, each in-flight batch exists as a TSV image (up to 2x its size with the buffer's growth
    /// slack) and as the driver's packet copy, so 4 + 3 = 7 times the buffer: a 100 MiB bound.
    fn buffer_copies(&self) -> u64 {
        7
    }

    fn schema_mapping(&self, s: &Scenario) -> Option<(String, String)> {
        Some((s.schema.clone(), self.database.clone()))
    }
}

/// Runs a scenario whose window holds no live or sentinel INSERT and asserts the server ran at
/// least `min_loads` LOAD DATA statements and not one INSERT. The destination silently falls back
/// to multi-value INSERT when a LOAD DATA fails, which would let the scenario pass without ever
/// exercising the LOAD DATA path; any INSERT here means exactly that.
async fn with_load_data_guard(
    target: &MySqlTarget,
    what: &str,
    min_loads: u64,
    scenario: impl Future<Output = ()>,
) {
    let (loads0, inserts0) = target.load_counters().await;
    scenario.await;
    let (loads, inserts) = target.load_counters().await;
    let (loads, inserts) = (loads - loads0, inserts - inserts0);
    println!("{what}: Com_load delta {loads}, Com_insert delta {inserts}");
    assert!(
        loads >= min_loads,
        "{what}: {loads} LOAD DATA statements, expected at least {min_loads} batches"
    );
    assert_eq!(
        inserts, 0,
        "{what}: INSERT statements reached the server: the snapshot fell back from LOAD DATA"
    );
}

macro_rules! mysql_scenario {
    ($test:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires live services; run via make snapshot-test-*"]
        async fn $test() {
            snapshot_scenarios::$scenario(&MySqlTarget::new()).await;
        }
    };
}

mysql_scenario!(mysql_s1, s1_consistent_handoff_under_writes);
mysql_scenario!(mysql_s2, s2_policy_matrix);
mysql_scenario!(mysql_s3, s3_existing_slot_without_marker_fails_fast);
mysql_scenario!(mysql_s4, s4_crash_recovery_in_progress);
mysql_scenario!(mysql_s5, s5_completed_marker_skips_snapshot_on_restart);
mysql_scenario!(mysql_s6, s6_resync_after_slot_loss);
mysql_scenario!(mysql_s7, s7_missing_destination_table_fails_before_copy);
mysql_scenario!(mysql_s8, s8_empty_tables_and_empty_publication);
mysql_scenario!(mysql_s13, s13_kill_mid_stream_then_restart_converges);
mysql_scenario!(mysql_s14, s14_graceful_stop_mid_stream_applies_everything);
mysql_scenario!(
    mysql_s15,
    s15_small_transaction_backlog_converges_and_the_tail_is_not_held_back
);

/// The second run of S10 alone copies 300,000 rows in batches of at most 1,000.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn mysql_s10() {
    let target = MySqlTarget::new();
    let scenario = snapshot_scenarios::s10_cancel_mid_copy_then_restart_completes(&target);
    with_load_data_guard(&target, "S10", 300, scenario).await;
}

/// S9 only snapshots, so any INSERT the server sees means the destination silently fell back from
/// `LOAD DATA` (it does so on any `LOAD DATA` error), which would hide a TSV escaping bug.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn mysql_s9() {
    let target = MySqlTarget::new();
    let (loads_before, inserts_before) = (
        target.status("Com_load").await,
        target.status("Com_insert").await,
    );
    snapshot_scenarios::s9_special_identifiers_and_values(&target).await;
    let loads = target.status("Com_load").await - loads_before;
    let inserts = target.status("Com_insert").await - inserts_before;
    println!("S9 LOAD DATA statements {loads}, INSERT statements {inserts}");
    assert!(
        loads >= 2,
        "S9 copies two tables with LOAD DATA, saw {loads} statements"
    );
    assert_eq!(
        inserts, 0,
        "S9 fell back to INSERT: LOAD DATA failed on a special value"
    );
}

#[cfg(target_os = "linux")]
mod memory {
    use super::*;
    use snapshot_scenarios::MemoryCase;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn mysql_s11b_one_huge_row() {
        let target = MySqlTarget::new();
        let scenario = snapshot_scenarios::s11_memory_bound(&target, MemoryCase::OneHugeRow);
        with_load_data_guard(&target, "S11b", 1, scenario).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    // 50 + 400 rows, two to a batch: a batch holds at most two of these ~1 MiB (hex text) rows,
    // the 2 MiB batch limit flushes at the second.
    async fn mysql_s11c_memory_independent_of_table_size() {
        let target = MySqlTarget::new();
        let scenario =
            snapshot_scenarios::s11_memory_bound(&target, MemoryCase::TableSizeIndependence);
        with_load_data_guard(&target, "S11c", 225, scenario).await;
    }
}

const LOAD_DATA_ROWS: u64 = 2_000;
const LOAD_DATA_BATCH_ROWS: u64 = 200;

/// The snapshot must really take the `LOAD DATA LOCAL INFILE` path: the handler reports bulk
/// support against this server, and during a copy the server counts one `LOAD DATA` statement per
/// batch and no INSERT at all. A silent fallback to multi-value INSERT (which the destination does
/// when `LOAD DATA` fails) moves `Com_insert` and fails the test.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn mysql_uses_load_data_path() {
    let target = MySqlTarget::new();
    let pg = PgFixture::connect().await;
    let sc = pg.reset("load_data_mysql").await;
    target.reset().await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    TableSet::seed_items(&pg, &sc, LOAD_DATA_ROWS).await;

    let cfg = client_config(&pg, &sc, &target)
        .snapshot_batch_rows(LOAD_DATA_BATCH_ROWS as usize)
        .build()
        .expect("valid config");

    let mut handler = cfg.create_destination().expect("MySQL destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose MySQL");
    assert!(
        handler.supports_bulk_insert(),
        "LOAD DATA LOCAL INFILE is unavailable (local_infile off?); the snapshot would use INSERT"
    );
    handler.close().await.expect("close");

    let (loads_before, inserts_before) = (
        target.status("Com_load").await,
        target.status("Com_insert").await,
    );
    let mut client = start_client(cfg, &sc.data_dir.path().join("lsn.metadata")).await;
    client
        .wait_marker_state(&sc, "completed", Duration::from_secs(60))
        .await;
    let (loads, inserts) = (
        target.status("Com_load").await - loads_before,
        target.status("Com_insert").await - inserts_before,
    );
    println!("LOAD DATA statements {loads}, INSERT statements {inserts} for {LOAD_DATA_ROWS} rows");
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;

    assert!(
        loads >= LOAD_DATA_ROWS / LOAD_DATA_BATCH_ROWS,
        "{loads} LOAD DATA statements for {LOAD_DATA_ROWS} rows in batches of at most \
         {LOAD_DATA_BATCH_ROWS}: the snapshot did not use LOAD DATA"
    );
    assert_eq!(inserts, 0, "the snapshot fell back to INSERT statements");
    rows_equal(
        &pg,
        &sc,
        &target,
        "snap_items",
        &[1, 7, 1_000, LOAD_DATA_ROWS as i64],
        "after LOAD DATA",
    )
    .await;
    client.stop().await.expect("clean shutdown");
}

const DUP_TABLE: &str = "snap_dup";

/// A handler straight from the factory, optionally told that the snapshot begins, bulk-loads
/// `(id, v)` rows into `snap_dup` through `LOAD DATA LOCAL INFILE`.
async fn load_dup_rows(
    target: &MySqlTarget,
    snapshot: bool,
    rows: &[(i64, &str)],
) -> pg2any_lib::error::Result<()> {
    let cfg = pg2any_lib::Config::builder()
        .source_connection_string("postgresql://unused")
        .destination_type(DestinationType::MySQL)
        .destination_connection_string(target.connection_string())
        .replication_slot_name("unused")
        .publication_name("unused")
        .build()
        .expect("valid config");
    let mut handler = cfg.create_destination().expect("MySQL destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose MySQL");
    assert!(
        handler.supports_bulk_insert(),
        "LOAD DATA LOCAL INFILE is unavailable; this test would not reach it"
    );
    if snapshot {
        handler.begin_snapshot_writes();
    }
    let columns = vec!["`id`".to_string(), "`v`".to_string()];
    let cells: Vec<Vec<String>> = rows
        .iter()
        .map(|(id, v)| vec![id.to_string(), format!("'{v}'")])
        .collect();
    let result = handler
        .execute_bulk_insert_with_hook(&format!("`{DUP_TABLE}`"), &columns, &cells, None)
        .await;
    handler.close().await.expect("close");
    result
}

/// `LOAD DATA LOCAL` behaves like IGNORE: a row that violates the primary key is skipped without
/// an error. A snapshot writer must turn that into a failure and roll the whole batch back; the
/// live path keeps its behaviour (the batch commits, minus the skipped row).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn mysql_load_data_duplicate_keys_fail_snapshot_writers_only() {
    let target = MySqlTarget::new();
    target
        .run(&format!("DROP TABLE IF EXISTS `{DUP_TABLE}`"))
        .await;
    target
        .run(&format!(
            "CREATE TABLE `{DUP_TABLE}` (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(20) NULL)"
        ))
        .await;
    target
        .run(&format!("INSERT INTO `{DUP_TABLE}` VALUES (1, 'old')"))
        .await;
    let batch = [(1, "new1"), (2, "new2")];

    let err = load_dup_rows(&target, true, &batch)
        .await
        .expect_err("a snapshot writer must fail when LOAD DATA skipped a row");
    let msg = err.to_string();
    println!("snapshot writer error: {msg}");
    assert!(msg.contains(DUP_TABLE), "{msg}");
    assert!(msg.contains("sent 2 row(s)"), "{msg}");
    assert!(msg.contains("inserted 1"), "{msg}");
    assert_eq!(
        target.row(DUP_TABLE, 2).await,
        None,
        "the failed batch must be rolled back"
    );
    assert_eq!(
        target.row(DUP_TABLE, 1).await,
        Some(vec!["1".to_string(), "old".to_string()])
    );

    load_dup_rows(&target, false, &batch)
        .await
        .expect("the live path keeps committing the batch");
    assert_eq!(
        target.row(DUP_TABLE, 2).await,
        Some(vec!["2".to_string(), "new2".to_string()]),
        "the live path loads the non-colliding row"
    );
    assert_eq!(
        target.row(DUP_TABLE, 1).await,
        Some(vec!["1".to_string(), "old".to_string()]),
        "the colliding row is skipped, the old value stays"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS `{DUP_TABLE}`"))
        .await;
}

/// `LOAD DATA LOCAL` also turns "too long" and "out of range" into warnings and still reports
/// every row as inserted, so the row count alone does not prove a lossless copy. A snapshot
/// writer must fail and roll back; the live path keeps committing.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn mysql_load_data_warnings_fail_snapshot_writers_only() {
    let target = MySqlTarget::new();
    target
        .run(&format!("DROP TABLE IF EXISTS `{DUP_TABLE}`"))
        .await;
    target
        .run(&format!(
            "CREATE TABLE `{DUP_TABLE}` (id BIGINT NOT NULL PRIMARY KEY, v VARCHAR(3) NULL)"
        ))
        .await;

    let err = load_dup_rows(&target, true, &[(1, "abcdef")])
        .await
        .expect_err("a snapshot writer must fail when LOAD DATA truncated a value");
    let msg = err.to_string();
    println!("snapshot writer error: {msg}");
    assert!(msg.contains(DUP_TABLE), "{msg}");
    assert!(msg.contains("warning"), "{msg}");
    assert_eq!(
        target.row(DUP_TABLE, 1).await,
        None,
        "the failed batch must be rolled back"
    );

    load_dup_rows(&target, false, &[(1, "abcdef")])
        .await
        .expect("the live path keeps committing the batch");
    assert_eq!(
        target.row(DUP_TABLE, 1).await,
        Some(vec!["1".to_string(), "abc".to_string()]),
        "the live path stores the truncated value"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS `{DUP_TABLE}`"))
        .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-mysql"]
async fn bench_snapshot_throughput_mysql() {
    snapshot_scenarios::bench_grid(&MySqlTarget::new()).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-mysql"]
async fn bench_live_throughput_mysql() {
    snapshot_scenarios::bench_live_grid(&MySqlTarget::new()).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-mysql"]
async fn bench_live_latency_mysql() {
    let l = snapshot_scenarios::bench_live_latency(&MySqlTarget::new(), 100).await;
    println!(
        "live latency, idle pipeline, commit to visible: {} single-row transactions, min {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, max {:.1} ms",
        l.samples, l.min_ms, l.p50_ms, l.p95_ms, l.max_ms
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-mysql"]
async fn bench_live_drain_mysql() {
    snapshot_scenarios::bench_live_drain_grid(&MySqlTarget::new()).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-mysql"]
async fn bench_snapshot_reference_mysql() {
    snapshot_scenarios::bench_snapshot_reference(&MySqlTarget::new()).await;
}
