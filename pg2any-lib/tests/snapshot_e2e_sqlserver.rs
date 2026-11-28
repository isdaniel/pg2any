//! Initial-snapshot end-to-end scenarios: live PostgreSQL source, SQL Server destination.
//!
//! Needs running PostgreSQL and SQL Server 2022 (see `docker-compose.snapshot-test.yml`, profile
//! `sqlserver`); run with `make snapshot-test-sqlserver`.
//!
//! The tests must not run in parallel (`--test-threads=1`, as the Makefile does): they share one
//! database, its server-wide bulk-load counter, the audit table and the standard tables.
#![cfg(feature = "sqlserver")]

#[allow(dead_code)]
mod snapshot_common;

use async_trait::async_trait;
use pg2any_lib::DestinationType;
use snapshot_common::snapshot_harness::{
    client_config, start_client, Digest, PgFixture, Scenario, TableSet, Target, NULL_TEXT,
};
use snapshot_common::snapshot_scenarios::{self, rows_equal};
use std::future::Future;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use tiberius::{Client, ColumnData, Config};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const DEFAULT_SQLSERVER_URL: &str = "Server=tcp:127.0.0.1,1433;Database=cdc_db;User Id=sa;\
                                     Password=Test.123!;TrustServerCertificate=true";

type TdsClient = Client<Compat<TcpStream>>;

/// The database part of an ADO.NET connection string.
fn database_of(url: &str) -> String {
    url.split(';')
        .filter_map(|part| part.split_once('='))
        .find(|(key, _)| {
            let key = key.trim().to_ascii_lowercase();
            key == "database" || key == "initial catalog"
        })
        .map(|(_, value)| value.trim().to_string())
        .expect("PG2ANY_TEST_SQLSERVER_URL names a Database")
}

async fn open(config: Config) -> Result<TdsClient, tiberius::error::Error> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

/// Statements that reach the server through an `INSERT` (as opposed to TDS Bulk Load) are
/// counted by an `AFTER INSERT` trigger on every standard table. A bulk load does not fire
/// triggers (tiberius sets no `FIRE_TRIGGERS`), so the audit row only grows when the destination
/// inserts with plain `INSERT` statements: the multi-value INSERT fallback of the snapshot (or the
/// live stream, or a test's sentinel).
const AUDIT_TABLE: &str = "snap_audit";

/// What the server has loaded so far: rows through TDS Bulk Load and rows through INSERT.
#[derive(Clone, Copy, Debug, Default)]
struct Loaded {
    bulk_rows: u64,
    insert_rows: u64,
}

/// All the tables of every scenario live in `dbo` of one database: SQL Server has schemas, so each
/// scenario's PostgreSQL schema is mapped onto `dbo`.
struct SqlServerTarget {
    url: String,
    database: String,
    /// One long-lived connection for the harness's own statements, used one at a time.
    conn: Mutex<Option<TdsClient>>,
    /// `Loaded` as of the latest `reset`, for `on_snapshot_completed`.
    baseline: StdMutex<Loaded>,
    /// Whether `create_tables` adds the INSERT-counting audit trigger. The live-path benchmark
    /// switches it off: the trigger adds an UPDATE of the audit row to every INSERT statement.
    audit_trigger: bool,
}

impl SqlServerTarget {
    async fn new() -> Self {
        let url = std::env::var("PG2ANY_TEST_SQLSERVER_URL")
            .unwrap_or_else(|_| DEFAULT_SQLSERVER_URL.to_string());
        let database = database_of(&url);
        let target = Self {
            url,
            database,
            conn: Mutex::new(None),
            baseline: StdMutex::new(Loaded::default()),
            audit_trigger: true,
        };
        target.ensure_database().await;
        target
            .run(&format!(
                "IF OBJECT_ID(N'dbo.{AUDIT_TABLE}', N'U') IS NULL \
                 BEGIN CREATE TABLE [dbo].[{AUDIT_TABLE}] (inserted_rows BIGINT NOT NULL); \
                 INSERT INTO [dbo].[{AUDIT_TABLE}] VALUES (0); END"
            ))
            .await;
        target
    }

    /// The live-path benchmark's target: tables without the audit trigger.
    fn without_audit_trigger(mut self) -> Self {
        self.audit_trigger = false;
        self
    }

    /// Creates the test database through `master` when the server does not have it yet.
    async fn ensure_database(&self) {
        let mut config = Config::from_ado_string(&self.url).expect("SQL Server connection string");
        config.database("master");
        let mut master = open(config).await.expect("connect to SQL Server master");
        master
            .simple_query(format!(
                "IF DB_ID(N'{}') IS NULL CREATE DATABASE [{}]",
                self.database.replace('\'', "''"),
                self.database.replace(']', "]]")
            ))
            .await
            .unwrap_or_else(|e| panic!("create database {}: {e}", self.database))
            .into_results()
            .await
            .unwrap_or_else(|e| panic!("create database {}: {e}", self.database));
    }

    async fn create_audit_trigger(&self, table: &str) {
        self.run(&format!(
            "CREATE TRIGGER [dbo].[audit_{table}] ON [dbo].[{table}] AFTER INSERT AS \
             BEGIN SET NOCOUNT ON; \
             UPDATE [dbo].[{AUDIT_TABLE}] \
             SET inserted_rows = inserted_rows + (SELECT COUNT_BIG(*) FROM inserted); END"
        ))
        .await;
    }

    /// Runs `sql` and returns the first result set's rows.
    async fn query(&self, sql: &str) -> Vec<tiberius::Row> {
        let mut guard = self.conn.lock().await;
        if guard.is_none() {
            let config = Config::from_ado_string(&self.url).expect("SQL Server connection string");
            *guard = Some(open(config).await.expect("connect to SQL Server"));
        }
        let client = guard.as_mut().expect("connection");
        let result = match client.simple_query(sql).await {
            Ok(stream) => stream.into_first_result().await,
            Err(e) => Err(e),
        };
        result.unwrap_or_else(|e| panic!("SQL Server statement failed: {e}\n  SQL: {sql}"))
    }

    async fn run(&self, sql: &str) {
        self.query(sql).await;
    }

    async fn scalar(&self, sql: &str) -> u64 {
        let rows = self.query(sql).await;
        let row = rows.first().unwrap_or_else(|| panic!("no row from {sql}"));
        row.get::<i64, _>(0)
            .unwrap_or_else(|| panic!("NULL from {sql}")) as u64
    }

    /// A cumulative `sys.dm_os_performance_counters` value of the test database.
    async fn db_counter(&self, counter: &str) -> u64 {
        self.scalar(&format!(
            "SELECT cntr_value FROM sys.dm_os_performance_counters \
             WHERE RTRIM(object_name) LIKE '%:Databases' \
             AND RTRIM(counter_name) = '{counter}' \
             AND RTRIM(instance_name) = N'{}'",
            self.database.replace('\'', "''")
        ))
        .await
    }

    /// `Bulk Copy Rows/sec` is a cumulative row count per database despite its name; it moves only
    /// for TDS Bulk Load (`INSERT BULK`).
    async fn loaded(&self) -> Loaded {
        Loaded {
            bulk_rows: self.db_counter("Bulk Copy Rows/sec").await,
            insert_rows: self
                .scalar(&format!("SELECT inserted_rows FROM [dbo].[{AUDIT_TABLE}]"))
                .await,
        }
    }
}

/// Canonical text of one cell: integers in decimal, text as is, NULL as `NULL_TEXT`. A bytea the
/// source sends in text form (`\x<hex>`, what both snapshot and live stream carry) is stored as
/// exactly that text in its VARCHAR(MAX), so it reads back as the very same text.
fn cell_text(value: ColumnData<'static>) -> String {
    match value {
        ColumnData::U8(Some(i)) => i.to_string(),
        ColumnData::I16(Some(i)) => i.to_string(),
        ColumnData::I32(Some(i)) => i.to_string(),
        ColumnData::I64(Some(i)) => i.to_string(),
        ColumnData::String(Some(s)) => s.into_owned(),
        ColumnData::U8(None)
        | ColumnData::I16(None)
        | ColumnData::I32(None)
        | ColumnData::I64(None)
        | ColumnData::String(None) => NULL_TEXT.to_string(),
        other => panic!("unexpected SQL Server value {other:?}"),
    }
}

#[async_trait]
impl Target for SqlServerTarget {
    fn dest_type(&self) -> DestinationType {
        DestinationType::SqlServer
    }

    fn connection_string(&self) -> String {
        self.url.clone()
    }

    async fn create_tables(&self, _s: &Scenario, which: &[&str]) {
        for table in which {
            self.run(&TableSet::sqlserver_ddl(table)).await;
            if self.audit_trigger {
                self.create_audit_trigger(table).await;
            }
        }
    }

    async fn digest(&self, table: &str) -> Digest {
        let sql = format!(
            "SELECT COUNT_BIG(*), COALESCE(MIN(id), 0), COALESCE(MAX(id), 0), \
             COALESCE(SUM(id), 0) FROM [dbo].[{table}]"
        );
        let rows = self.query(&sql).await;
        let row = rows.first().expect("an aggregate query returns a row");
        let cell = |i: usize| row.get::<i64, _>(i).expect("non-NULL aggregate");
        Digest {
            count: cell(0) as u64,
            min_id: cell(1),
            max_id: cell(2),
            sum_id: cell(3),
        }
    }

    async fn row(&self, table: &str, id: i64) -> Option<Vec<String>> {
        let rows = self
            .query(&format!("SELECT * FROM [dbo].[{table}] WHERE id = {id}"))
            .await;
        rows.into_iter()
            .next()
            .map(|r| r.into_iter().map(cell_text).collect())
    }

    async fn insert_sentinel(&self, table: &str, id: i64) {
        self.run(&format!("INSERT INTO [dbo].[{table}] (id) VALUES ({id})"))
            .await;
    }

    async fn reset(&self) {
        for table in TableSet::ALL {
            self.run(&format!("DROP TABLE IF EXISTS [dbo].[{table}]"))
                .await;
        }
        *self.baseline.lock().unwrap() = self.loaded().await;
    }

    /// S1: the copy is over and no live row was applied yet (give or take the poll window), so
    /// every row of the copy went through the bulk load and at most a few live inserts ran: a
    /// silent fallback to multi-value INSERT (the destination does that on any TDS failure, with
    /// only a warning) would put rows into the audit counter instead.
    async fn on_snapshot_completed(&self) {
        let before = *self.baseline.lock().unwrap();
        let now = self.loaded().await;
        let (bulk, inserted) = (
            now.bulk_rows - before.bulk_rows,
            now.insert_rows - before.insert_rows,
        );
        println!("S1 at the handoff: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
        assert!(
            bulk >= S1_COPIED_ROWS,
            "{bulk} rows through TDS Bulk Load for the S1 copy, expected the {S1_COPIED_ROWS} seeded rows"
        );
        assert!(
            inserted <= LIVE_INSERTS_AFTER_HANDOFF,
            "{inserted} rows inserted by INSERT statements during the S1 copy: the snapshot fell back from TDS Bulk Load"
        );
    }

    /// Every row of the benchmark copy must have gone through TDS Bulk Load and none through
    /// INSERT, since the `reset` that took the baseline.
    async fn on_bench_completed(&self, _table: &str, rows: u64, _batch_rows: usize) {
        let before = *self.baseline.lock().unwrap();
        let now = self.loaded().await;
        let (bulk, inserted) = (
            now.bulk_rows - before.bulk_rows,
            now.insert_rows - before.insert_rows,
        );
        println!("bench: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
        assert_eq!(
            bulk, rows,
            "every benchmark row must go through TDS Bulk Load"
        );
        assert_eq!(
            inserted, 0,
            "rows were inserted by INSERT statements: the benchmark fell back from TDS Bulk Load"
        );
    }

    /// What the live-path benchmark diffs: rows loaded through TDS Bulk Load, rows inserted by
    /// INSERT (the audit trigger, so 0 without it), and the database's log flushes and
    /// transactions (the latter also count the harness's own statements).
    async fn bench_counters(&self) -> Vec<(&'static str, u64)> {
        let loaded = self.loaded().await;
        vec![
            ("bulk_rows", loaded.bulk_rows),
            ("audit_insert_rows", loaded.insert_rows),
            ("log_flushes", self.db_counter("Log Flushes/sec").await),
            ("transactions", self.db_counter("Transactions/sec").await),
        ]
    }

    /// Measured by S11b with the TDS Bulk Load path proven active (`Bulk Copy Rows` +1, no
    /// INSERT, see `with_bulk_guard`): the 64 MB row (61.04 MiB) peaked at 610.9-616.4 MiB over
    /// four runs, 10.0-10.1x its size (MySQL 10.06x, SQLite 14x): the 2x hex text is held as the
    /// COPY frame, the rendered literal, the parsed cell and the encoded TDS row. Rounded up with
    /// ~19% headroom: 12 x 61.04 = 732 MiB.
    fn row_amplification(&self) -> u64 {
        12
    }

    // `buffer_copies` keeps the default (4): S11a on the bulk path measured 30.6-34.2 MiB over
    // four runs against its 76 MiB bound (SQLite 41-55 MiB, MySQL 69-83 MiB), because a bulk load
    // streams each row into the TDS packet buffer instead of building one SQL text or TSV image
    // per batch. The bound still sits well under the 100 MiB of the table, so unbounded
    // buffering would fail it.

    fn schema_mapping(&self, s: &Scenario) -> Option<(String, String)> {
        Some((s.schema.clone(), "dbo".to_string()))
    }
}

/// S1 seeds 200,000 `snap_items`, 1,000 `snap_wide` and 20,000 `snap_part` rows; all of them are
/// copied, plus the few rows the writer had committed before the slot existed.
const S1_COPIED_ROWS: u64 = 221_000;
/// How many rows may be inserted by INSERT statements between the copy's end and S1's next look at
/// the counters: live transactions start only after the handoff, and S1 polls the marker every
/// 20 ms, which fits a handful of single-row commits. A fallback adds up to 200 rows per batch.
const LIVE_INSERTS_AFTER_HANDOFF: u64 = 20;

/// Runs a scenario whose window holds no live or sentinel INSERT and asserts the server bulk-loaded
/// at least `min_rows` rows and ran no INSERT that added a row. The destination silently falls
/// back to multi-value INSERT when a TDS Bulk Load fails (`failed_bulk_tables`, a `warn!` only),
/// which would let the scenario pass without ever exercising the bulk path.
async fn with_bulk_guard(
    target: &SqlServerTarget,
    what: &str,
    min_rows: u64,
    scenario: impl Future<Output = ()>,
) {
    let before = target.loaded().await;
    scenario.await;
    let after = target.loaded().await;
    let (bulk, inserted) = (
        after.bulk_rows - before.bulk_rows,
        after.insert_rows - before.insert_rows,
    );
    println!("{what}: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
    assert!(
        bulk >= min_rows,
        "{what}: {bulk} rows through TDS Bulk Load, expected at least {min_rows}"
    );
    assert_eq!(
        inserted, 0,
        "{what}: rows were inserted by INSERT statements: the snapshot fell back from TDS Bulk Load"
    );
}

macro_rules! sqlserver_scenario {
    ($test:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires live services; run via make snapshot-test-*"]
        async fn $test() {
            snapshot_scenarios::$scenario(&SqlServerTarget::new().await).await;
        }
    };
}

sqlserver_scenario!(sqlserver_s1, s1_consistent_handoff_under_writes);
sqlserver_scenario!(sqlserver_s2, s2_policy_matrix);
sqlserver_scenario!(sqlserver_s3, s3_existing_slot_without_marker_fails_fast);
sqlserver_scenario!(sqlserver_s4, s4_crash_recovery_in_progress);
sqlserver_scenario!(sqlserver_s5, s5_completed_marker_skips_snapshot_on_restart);
sqlserver_scenario!(sqlserver_s6, s6_resync_after_slot_loss);
sqlserver_scenario!(sqlserver_s7, s7_missing_destination_table_fails_before_copy);
sqlserver_scenario!(sqlserver_s8, s8_empty_tables_and_empty_publication);
sqlserver_scenario!(sqlserver_s13, s13_kill_mid_stream_then_restart_converges);
sqlserver_scenario!(
    sqlserver_s14,
    s14_graceful_stop_mid_stream_applies_everything
);
sqlserver_scenario!(
    sqlserver_s15,
    s15_small_transaction_backlog_converges_and_the_tail_is_not_held_back
);

/// S9 only snapshots, so any INSERT-inserted row means the destination silently fell back from
/// TDS Bulk Load, which would hide a type or escaping bug of the bulk path. S9 holds 13 + 5 rows in
/// `snap_special` and 3 in `SnapMixed`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_s9() {
    let target = SqlServerTarget::new().await;
    let scenario = snapshot_scenarios::s9_special_identifiers_and_values(&target);
    with_bulk_guard(&target, "S9", 21, scenario).await;
}

/// The second run of S10 alone copies 300,000 rows.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_s10() {
    let target = SqlServerTarget::new().await;
    let scenario = snapshot_scenarios::s10_cancel_mid_copy_then_restart_completes(&target);
    with_bulk_guard(&target, "S10", 300_000, scenario).await;
}

#[cfg(target_os = "linux")]
mod memory {
    use super::*;
    use snapshot_scenarios::MemoryCase;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlserver_s11b_one_huge_row() {
        let target = SqlServerTarget::new().await;
        let scenario = snapshot_scenarios::s11_memory_bound(&target, MemoryCase::OneHugeRow);
        with_bulk_guard(&target, "S11b", 1, scenario).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    // 50 + 400 rows.
    async fn sqlserver_s11c_memory_independent_of_table_size() {
        let target = SqlServerTarget::new().await;
        let scenario =
            snapshot_scenarios::s11_memory_bound(&target, MemoryCase::TableSizeIndependence);
        with_bulk_guard(&target, "S11c", 450, scenario).await;
    }
}

const BULK_ROWS: u64 = 2_000;
const BULK_BATCH_ROWS: u64 = 200;

/// The snapshot must really take the TDS Bulk Load path: the handler reports bulk support, and a
/// copy of `BULK_ROWS` rows moves the server's `Bulk Copy Rows` counter by that many rows while no
/// INSERT statement inserts any (the audit trigger stays at zero). A silent fallback to multi-value
/// INSERT (taken after one TDS failure, per table) leaves the counter short and the audit non-zero.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_uses_tds_bulk_load() {
    let target = SqlServerTarget::new().await;
    let pg = PgFixture::connect().await;
    let sc = pg.reset("bulk_sqlserver").await;
    target.reset().await;
    TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
    target.create_tables(&sc, &["snap_items"]).await;
    TableSet::seed_items(&pg, &sc, BULK_ROWS).await;

    let cfg = client_config(&pg, &sc, &target)
        .snapshot_batch_rows(BULK_BATCH_ROWS as usize)
        .build()
        .expect("valid config");

    let mut handler = cfg.create_destination().expect("SQL Server destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose SQL Server");
    assert!(handler.supports_bulk_insert(), "SQL Server bulk support");
    handler.close().await.expect("close");

    let before = target.loaded().await;
    let mut client = start_client(cfg, &sc.data_dir.path().join("lsn.metadata")).await;
    client
        .wait_marker_state(&sc, "completed", Duration::from_secs(60))
        .await;
    let after = target.loaded().await;
    let (bulk, inserted) = (
        after.bulk_rows - before.bulk_rows,
        after.insert_rows - before.insert_rows,
    );
    println!("bulk-loaded rows {bulk}, rows inserted by INSERT {inserted} for {BULK_ROWS} rows");
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;

    assert_eq!(
        bulk, BULK_ROWS,
        "every snapshot row must go through TDS Bulk Load"
    );
    assert_eq!(inserted, 0, "the snapshot fell back to INSERT statements");
    rows_equal(
        &pg,
        &sc,
        &target,
        "snap_items",
        &[1, 7, 1_000, BULK_ROWS as i64],
        "after bulk load",
    )
    .await;
    client.stop().await.expect("clean shutdown");
}

/// Physical order `id, c_text, c_def, c_int, c_small, c_tiny`; `c_def` is nullable WITH a DEFAULT.
const PROBE_TABLE: &str = "snap_probe";
const PROBE_DDL: &str = "CREATE TABLE [dbo].[snap_probe] (id BIGINT NOT NULL PRIMARY KEY, \
    c_text NVARCHAR(100) NULL, c_def INT NULL CONSTRAINT [df_snap_probe_def] DEFAULT 77, \
    c_int INT NULL, c_small SMALLINT NULL, c_tiny TINYINT NULL)";
/// The column list of the bulk call: not the physical order.
const PROBE_COLUMNS: [&str; 6] = [
    "[c_tiny]",
    "[c_int]",
    "[c_def]",
    "[id]",
    "[c_small]",
    "[c_text]",
];

struct ProbeRow {
    id: i64,
    tiny: Option<&'static str>,
    int: Option<&'static str>,
    def: Option<&'static str>,
    small: Option<&'static str>,
    text: Option<&'static str>,
}

impl ProbeRow {
    /// The cells as the renderer hands them to the destination, in `PROBE_COLUMNS` order.
    fn rendered(&self) -> Vec<String> {
        let quote = |v: Option<&str>| v.map_or("NULL".to_string(), |v| format!("'{v}'"));
        vec![
            quote(self.tiny),
            quote(self.int),
            quote(self.def),
            format!("'{}'", self.id),
            quote(self.small),
            quote(self.text),
        ]
    }

    /// What the table must hold, as `SELECT id, c_tiny, c_int, c_def, c_small, c_text` reads it.
    fn canonical(&self) -> Vec<String> {
        let cell = |v: Option<&str>| v.map_or(NULL_TEXT.to_string(), str::to_string);
        vec![
            self.id.to_string(),
            cell(self.tiny),
            cell(self.int),
            cell(self.def),
            cell(self.small),
            cell(self.text),
        ]
    }
}

fn probe_rows(first_id: i64, text: &'static str) -> Vec<ProbeRow> {
    vec![
        ProbeRow {
            id: first_id,
            tiny: Some("255"),
            int: Some("-2147483648"),
            def: None,
            small: Some("-32768"),
            text: Some(text),
        },
        ProbeRow {
            id: first_id + 1,
            tiny: None,
            int: None,
            def: Some("5"),
            small: None,
            text: None,
        },
        ProbeRow {
            id: first_id + 2,
            tiny: Some("0"),
            int: Some("2147483647"),
            def: None,
            small: Some("32767"),
            text: Some(""),
        },
        ProbeRow {
            id: first_id + 3,
            tiny: None,
            int: None,
            def: None,
            small: None,
            text: None,
        },
    ]
}

/// A handler straight from the factory, optionally told that the snapshot begins, loads `rows`
/// into `snap_probe`; returns how many rows the server took by bulk load and by INSERT.
async fn load_probe(target: &SqlServerTarget, snapshot: bool, rows: &[ProbeRow]) -> (u64, u64) {
    let cfg = pg2any_lib::Config::builder()
        .source_connection_string("postgresql://unused")
        .destination_type(DestinationType::SqlServer)
        .destination_connection_string(target.connection_string())
        .replication_slot_name("unused")
        .publication_name("unused")
        .build()
        .expect("valid config");
    let mut handler = cfg.create_destination().expect("SQL Server destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose SQL Server");
    if snapshot {
        handler.begin_snapshot_writes();
    }
    let columns: Vec<String> = PROBE_COLUMNS.iter().map(|c| c.to_string()).collect();
    let cells: Vec<Vec<String>> = rows.iter().map(ProbeRow::rendered).collect();
    let before = target.loaded().await;
    handler
        .execute_bulk_insert_with_hook(&format!("[dbo].[{PROBE_TABLE}]"), &columns, &cells, None)
        .await
        .expect("bulk insert or its fallback");
    let after = target.loaded().await;
    handler.close().await.expect("close");
    (
        after.bulk_rows - before.bulk_rows,
        after.insert_rows - before.insert_rows,
    )
}

async fn probe_contents(target: &SqlServerTarget, rows: &[ProbeRow]) -> Vec<Vec<String>> {
    let ids: Vec<String> = rows.iter().map(|r| r.id.to_string()).collect();
    target
        .query(&format!(
            "SELECT id, c_tiny, c_int, c_def, c_small, c_text FROM [dbo].[{PROBE_TABLE}] \
             WHERE id IN ({}) ORDER BY id",
            ids.join(", ")
        ))
        .await
        .into_iter()
        .map(|r| r.into_iter().map(cell_text).collect())
        .collect()
}

/// The bulk load maps values to the NAMED columns (the column list is not the table's physical
/// order) and keeps NULLs NULL (a nullable column WITH a DEFAULT does not get it), over INT,
/// SMALLINT, TINYINT and NVARCHAR columns.
/// (i) A handler told the snapshot began converts the quoted integers and goes through TDS Bulk
/// Load: the server counts the rows as bulk rows and no INSERT touched the table.
/// (ii) A handler that was not told (the live path) bulk-loads the integer columns too, and unlike
/// the snapshot it fires the table's trigger: every value lands in the right column, NULLs kept.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_bulk_load_maps_named_columns_and_keeps_nulls() {
    let target = SqlServerTarget::new().await;
    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{PROBE_TABLE}]"))
        .await;
    target.run(PROBE_DDL).await;
    target.create_audit_trigger(PROBE_TABLE).await;

    let snapshot_rows = probe_rows(1, "héllo 日本語");
    let (bulk, inserted) = load_probe(&target, true, &snapshot_rows).await;
    println!("snapshot handler: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
    assert_eq!(
        (bulk, inserted),
        (snapshot_rows.len() as u64, 0),
        "after begin_snapshot_writes the integer-bearing table must be bulk-loaded"
    );
    assert_eq!(
        probe_contents(&target, &snapshot_rows).await,
        snapshot_rows
            .iter()
            .map(ProbeRow::canonical)
            .collect::<Vec<_>>(),
        "bulk-loaded values in the named columns, NULLs kept, Unicode exact"
    );

    let live_rows = probe_rows(11, "plain ascii");
    let (bulk, inserted) = load_probe(&target, false, &live_rows).await;
    println!("live handler: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
    assert_eq!(
        (bulk, inserted),
        (live_rows.len() as u64, live_rows.len() as u64),
        "without begin_snapshot_writes the integer-bearing table is bulk-loaded AND its trigger fires"
    );
    assert_eq!(
        probe_contents(&target, &live_rows).await,
        live_rows
            .iter()
            .map(ProbeRow::canonical)
            .collect::<Vec<_>>(),
        "live bulk-loaded values in the named columns, NULLs kept"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{PROBE_TABLE}]"))
        .await;
}

const LIVE_TEXT_TABLE: &str = "snap_live_text";
/// All-text table; `c_extra` is a destination column the bulk call does not name, with a DEFAULT,
/// and `c_def` is named but also has a DEFAULT.
const LIVE_TEXT_DDL: &str = "CREATE TABLE [dbo].[snap_live_text] (id NVARCHAR(20) NOT NULL PRIMARY KEY, \
    c_text NVARCHAR(100) NULL, c_def NVARCHAR(20) NULL CONSTRAINT [df_snap_live_text_def] DEFAULT N'named-default', \
    c_extra NVARCHAR(20) NULL CONSTRAINT [df_snap_live_text_extra] DEFAULT N'extra-default')";
/// The column list of the bulk call: not the physical order, and without `c_extra`.
const LIVE_TEXT_COLUMNS: [&str; 3] = ["[c_def]", "[id]", "[c_text]"];

/// The live path (the handler is NOT told the snapshot began) on a table with only text columns
/// and one extra destination column the call does not name: it is bulk-loaded and the trigger
/// fires. The named columns hold their values, a NULL stays NULL even where the column has a
/// DEFAULT, and the column the call does not name gets its DEFAULT.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_live_path_bulk_loads_text_table_with_unnamed_column() {
    let target = SqlServerTarget::new().await;
    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_TEXT_TABLE}]"))
        .await;
    target.run(LIVE_TEXT_DDL).await;
    target.create_audit_trigger(LIVE_TEXT_TABLE).await;

    let cfg = pg2any_lib::Config::builder()
        .source_connection_string("postgresql://unused")
        .destination_type(DestinationType::SqlServer)
        .destination_connection_string(target.connection_string())
        .replication_slot_name("unused")
        .publication_name("unused")
        .build()
        .expect("valid config");
    let mut handler = cfg.create_destination().expect("SQL Server destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose SQL Server");
    let columns: Vec<String> = LIVE_TEXT_COLUMNS.iter().map(|c| c.to_string()).collect();
    let cells: Vec<Vec<String>> = vec![
        vec!["NULL".into(), "'a1'".into(), "'plain ascii'".into()],
        vec!["'given'".into(), "'a2'".into(), "NULL".into()],
        vec!["NULL".into(), "'a3'".into(), "''".into()],
    ];
    let before = target.loaded().await;
    handler
        .execute_bulk_insert_with_hook(
            &format!("[dbo].[{LIVE_TEXT_TABLE}]"),
            &columns,
            &cells,
            None,
        )
        .await
        .expect("bulk insert");
    let after = target.loaded().await;
    handler.close().await.expect("close");

    let (bulk, inserted) = (
        after.bulk_rows - before.bulk_rows,
        after.insert_rows - before.insert_rows,
    );
    println!("live handler: bulk-loaded rows {bulk}, rows inserted by INSERT {inserted}");
    assert_eq!(
        (bulk, inserted),
        (3, 3),
        "a table with a column the call does not name is bulk-loaded and its trigger fires"
    );
    let contents: Vec<Vec<String>> = target
        .query(&format!(
            "SELECT id, c_text, c_def, c_extra FROM [dbo].[{LIVE_TEXT_TABLE}] ORDER BY id"
        ))
        .await
        .into_iter()
        .map(|r| r.into_iter().map(cell_text).collect())
        .collect();
    let row = |cells: [&str; 4]| cells.map(str::to_string).to_vec();
    assert_eq!(
        contents,
        vec![
            row(["a1", "plain ascii", NULL_TEXT, "extra-default"]),
            row(["a2", NULL_TEXT, "given", "extra-default"]),
            row(["a3", "", NULL_TEXT, "extra-default"]),
        ],
        "named columns hold their values, NULL stays NULL, the unnamed column got its DEFAULT"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_TEXT_TABLE}]"))
        .await;
}

const LIVE_FULL_TABLE: &str = "snap_live_full";
/// Same as `LIVE_TEXT_DDL` without the extra column: the call names every column.
const LIVE_FULL_DDL: &str = "CREATE TABLE [dbo].[snap_live_full] (id NVARCHAR(20) NOT NULL PRIMARY KEY, \
    c_text NVARCHAR(100) NULL, c_def NVARCHAR(20) NULL CONSTRAINT [df_snap_live_full_def] DEFAULT N'named-default')";

/// The live path on an all-text table whose every column is named, in an order that is not the
/// table's: TDS Bulk Load with each value in its named column and a NULL kept NULL (the positional
/// load put the values into the wrong columns), and the table's trigger fires.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_live_path_bulk_loads_text_table_naming_every_column() {
    let target = SqlServerTarget::new().await;
    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_FULL_TABLE}]"))
        .await;
    target.run(LIVE_FULL_DDL).await;
    target.create_audit_trigger(LIVE_FULL_TABLE).await;

    let mut handler = live_handler(&target).await;
    let columns: Vec<String> = LIVE_TEXT_COLUMNS.iter().map(|c| c.to_string()).collect();
    let cells: Vec<Vec<String>> = vec![
        vec!["NULL".into(), "'a1'".into(), "'héllo 日本語'".into()],
        vec!["'given'".into(), "'a2'".into(), "NULL".into()],
    ];
    let before = target.loaded().await;
    handler
        .execute_bulk_insert_with_hook(
            &format!("[dbo].[{LIVE_FULL_TABLE}]"),
            &columns,
            &cells,
            None,
        )
        .await
        .expect("bulk insert");
    let after = target.loaded().await;
    handler.close().await.expect("close");

    assert_eq!(
        (
            after.bulk_rows - before.bulk_rows,
            after.insert_rows - before.insert_rows
        ),
        (2, 2),
        "a call that names every column bulk-loads on the live path and fires the trigger"
    );
    let contents: Vec<Vec<String>> = target
        .query(&format!(
            "SELECT id, c_text, c_def FROM [dbo].[{LIVE_FULL_TABLE}] ORDER BY id"
        ))
        .await
        .into_iter()
        .map(|r| r.into_iter().map(cell_text).collect())
        .collect();
    let row = |cells: [&str; 3]| cells.map(str::to_string).to_vec();
    assert_eq!(
        contents,
        vec![
            row(["a1", "héllo 日本語", NULL_TEXT]),
            row(["a2", NULL_TEXT, "given"]),
        ],
        "each value is in its named column and NULL stays NULL"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_FULL_TABLE}]"))
        .await;
}

const LIVE_INT_TABLE: &str = "snap_live_int";
const LIVE_INT_DDL: &str = "CREATE TABLE [dbo].[snap_live_int] (id BIGINT NOT NULL PRIMARY KEY, \
    n INT NOT NULL CONSTRAINT [ck_snap_live_int_n] CHECK (n >= 0), name NVARCHAR(50) NULL)";

async fn live_handler(
    target: &SqlServerTarget,
) -> Box<dyn pg2any_lib::destinations::DestinationHandler> {
    let cfg = pg2any_lib::Config::builder()
        .source_connection_string("postgresql://unused")
        .destination_type(DestinationType::SqlServer)
        .destination_connection_string(target.connection_string())
        .replication_slot_name("unused")
        .publication_name("unused")
        .build()
        .expect("valid config");
    let mut handler = cfg.create_destination().expect("SQL Server destination");
    handler
        .connect(&cfg.destination_connection_string)
        .await
        .expect("connect to the compose SQL Server");
    handler
}

/// On a table with BIGINT/INT columns the live path bulk-loads (the quoted integers are converted),
/// keeps non-ASCII text intact, still fires the table's triggers (the audit counter grows by the
/// loaded rows) and still enforces its CHECK constraint (a violating batch fails and loads
/// nothing).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_live_path_bulk_loads_integer_tables_with_triggers_and_constraints() {
    let target = SqlServerTarget::new().await;
    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_INT_TABLE}]"))
        .await;
    target.run(LIVE_INT_DDL).await;
    target.create_audit_trigger(LIVE_INT_TABLE).await;
    let table = format!("[dbo].[{LIVE_INT_TABLE}]");
    let columns: Vec<String> = ["[id]", "[n]", "[name]"].map(str::to_string).to_vec();
    let rows = |first: u32, name: &str| -> Vec<Vec<String>> {
        (first..first + 3)
            .map(|id| {
                vec![
                    format!("'{id}'"),
                    format!("'{}'", id * 10),
                    format!("'{name}'"),
                ]
            })
            .collect()
    };

    // Bulk-loaded, and the trigger still counted the rows.
    let mut on = live_handler(&target).await;
    let before = target.loaded().await;
    on.execute_bulk_insert_with_hook(&table, &columns, &rows(11, "héllo 日本語"), None)
        .await
        .expect("typed bulk load");
    let after = target.loaded().await;
    assert_eq!(
        (
            after.bulk_rows - before.bulk_rows,
            after.insert_rows - before.insert_rows
        ),
        (3, 3),
        "the rows are bulk-loaded AND the AFTER INSERT trigger fired"
    );
    let contents: Vec<Vec<String>> = target
        .query(&format!(
            "SELECT id, n, name FROM {table} WHERE id >= 11 ORDER BY id"
        ))
        .await
        .into_iter()
        .map(|r| r.into_iter().map(cell_text).collect())
        .collect();
    let expected: Vec<Vec<String>> = (11..14u32)
        .map(|id| {
            vec![
                id.to_string(),
                (id * 10).to_string(),
                "héllo 日本語".to_string(),
            ]
        })
        .collect();
    assert_eq!(contents, expected);

    // A row violating the CHECK constraint fails the batch and nothing of it stays.
    let violating = vec![
        vec!["'21'".to_string(), "'5'".to_string(), "'ok'".to_string()],
        vec!["'22'".to_string(), "'-5'".to_string(), "'bad'".to_string()],
    ];
    let result = on
        .execute_bulk_insert_with_hook(&table, &columns, &violating, None)
        .await;
    on.close().await.expect("close");
    assert!(result.is_err(), "a CHECK violation must fail the batch");
    let left = target
        .query(&format!("SELECT COUNT_BIG(*) FROM {table} WHERE id >= 21"))
        .await;
    assert_eq!(
        left[0].get::<i64, _>(0),
        Some(0),
        "nothing of the failed batch stays"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_INT_TABLE}]"))
        .await;
}

/// A batch that fails on its rows (a duplicate key, a CHECK violation) says nothing about the
/// table's column types: the handler must go on bulk-loading that table. Only a failure of the
/// bulk load itself (a type it cannot send) sends the table to the INSERT fallback for good.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlserver_a_row_level_error_does_not_end_the_bulk_load_of_the_table() {
    let target = SqlServerTarget::new().await;
    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_INT_TABLE}]"))
        .await;
    target.run(LIVE_INT_DDL).await;
    target.create_audit_trigger(LIVE_INT_TABLE).await;
    let table = format!("[dbo].[{LIVE_INT_TABLE}]");
    let columns: Vec<String> = ["[id]", "[n]", "[name]"].map(str::to_string).to_vec();
    let rows = |ids: &[u32], n: i64| -> Vec<Vec<String>> {
        ids.iter()
            .map(|id| vec![format!("'{id}'"), format!("'{n}'"), "'x'".to_string()])
            .collect()
    };
    let mut handler = live_handler(&target).await;

    handler
        .execute_bulk_insert_with_hook(&table, &columns, &rows(&[1, 2, 3], 1), None)
        .await
        .expect("the first batch is bulk-loaded");
    handler
        .execute_bulk_insert_with_hook(&table, &columns, &rows(&[3, 4, 5], 1), None)
        .await
        .expect_err("id 3 exists: a duplicate key fails the batch");
    handler
        .execute_bulk_insert_with_hook(&table, &columns, &rows(&[21, 22], -1), None)
        .await
        .expect_err("n < 0 violates the CHECK constraint");

    let before = target.loaded().await;
    handler
        .execute_bulk_insert_with_hook(&table, &columns, &rows(&[11, 12, 13], 1), None)
        .await
        .expect("a valid batch after the failed ones");
    let after = target.loaded().await;
    handler.close().await.expect("close");
    assert_eq!(
        (
            after.bulk_rows - before.bulk_rows,
            after.insert_rows - before.insert_rows
        ),
        (3, 3),
        "the table is still bulk-loaded (and its trigger fired), not sent to the INSERT fallback"
    );

    target
        .run(&format!("DROP TABLE IF EXISTS [dbo].[{LIVE_INT_TABLE}]"))
        .await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlserver"]
async fn bench_snapshot_throughput_sqlserver() {
    snapshot_scenarios::bench_grid(&SqlServerTarget::new().await).await;
}

/// The live path's inserts would pay for the audit trigger (an UPDATE of the audit row per INSERT
/// statement), so the grid runs on tables without it; `PG2ANY_BENCH_LIVE_SQLSERVER_AUDIT=1` keeps
/// it, to measure what it costs.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlserver"]
async fn bench_live_throughput_sqlserver() {
    let target = SqlServerTarget::new().await;
    let target = if std::env::var("PG2ANY_BENCH_LIVE_SQLSERVER_AUDIT").is_ok_and(|v| v == "1") {
        target
    } else {
        target.without_audit_trigger()
    };
    snapshot_scenarios::bench_live_grid(&target).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlserver"]
async fn bench_live_latency_sqlserver() {
    let target = SqlServerTarget::new().await.without_audit_trigger();
    let l = snapshot_scenarios::bench_live_latency(&target, 100).await;
    println!(
        "live latency, idle pipeline, commit to visible: {} single-row transactions, min {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, max {:.1} ms",
        l.samples, l.min_ms, l.p50_ms, l.p95_ms, l.max_ms
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlserver"]
async fn bench_live_drain_sqlserver() {
    let target = SqlServerTarget::new().await.without_audit_trigger();
    snapshot_scenarios::bench_live_drain_grid(&target).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlserver"]
async fn bench_snapshot_reference_sqlserver() {
    snapshot_scenarios::bench_snapshot_reference(&SqlServerTarget::new().await).await;
}
