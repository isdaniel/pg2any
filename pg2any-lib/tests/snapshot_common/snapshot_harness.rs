//! Live-database harness for the initial-snapshot end-to-end tests.
//!
//! One PostgreSQL source (`PG2ANY_TEST_PG_URL`, a `replication=database` URL) plus a destination
//! `Target`. Every scenario works in its own schema/publication/slot, so a failed run leaves
//! nothing the next `PgFixture::reset` cannot drop.
#![allow(dead_code)]

use async_trait::async_trait;
use pg2any_lib::{CdcClient, CdcError, Config, ConfigBuilder, DestinationType};
use pg_walstream::PgReplicationConnection;
use std::future::Future;
use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_PG_URL: &str =
    "postgresql://postgres:test.123@127.0.0.1:5432/postgres?replication=database";

/// Canonical text of a SQL NULL in `Target::row` / `PgFixture::row`; no test data contains it.
pub const NULL_TEXT: &str = "\u{2400}";

const POLL_INTERVAL: Duration = Duration::from_millis(250);

fn literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Cheap fingerprint of a table's primary keys. It covers ids only: it cannot see pending
/// updates or stale non-id columns, so a test that waits for an update to be applied must poll
/// `Target::row` (see `RunningClient::wait_for_row`) instead of `wait_converged`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Digest {
    pub count: u64,
    pub min_id: i64,
    pub max_id: i64,
    pub sum_id: i64,
}

pub struct Scenario {
    pub schema: String,
    pub publication: String,
    pub slot: String,
    pub data_dir: tempfile::TempDir,
}

impl Scenario {
    pub fn marker_path(&self) -> std::path::PathBuf {
        self.data_dir.path().join("snapshot_marker.json")
    }

    /// The slot's consistent point once the marker records it (right after the slot exists).
    pub fn marker_consistent_point(&self) -> Option<u64> {
        let bytes = std::fs::read(self.marker_path()).ok()?;
        let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        json["marker"]["consistent_point"].as_u64()
    }

    /// `"in_progress"` / `"completed"`, or `None` while no marker file exists.
    pub fn marker_state(&self) -> Option<String> {
        let bytes = std::fs::read(self.marker_path()).ok()?;
        let json: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
        json["marker"]["state"].as_str().map(str::to_owned)
    }
}

/// Blocking libpq over a `replication=database` connection: plain SQL (DDL, DML, SELECT) runs
/// through the simple-query protocol on such a connection, which is how `slot.rs` already uses it.
/// A new connection per call keeps the fixture `Sync` and free of shared session state.
pub struct PgFixture {
    url: String,
    /// Schema of the latest `reset`, used by `digest`/`row` and so by `wait_converged`.
    schema: Mutex<String>,
}

impl PgFixture {
    pub async fn connect() -> Self {
        let url =
            std::env::var("PG2ANY_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_PG_URL.to_string());
        let fixture = Self {
            url,
            schema: Mutex::new(String::new()),
        };
        fixture.query_i64("SELECT 1").await;
        fixture.drop_leftover_slots().await;
        fixture
    }

    /// Every scenario leaves its slot behind (the client keeps it on shutdown), and the test
    /// server has only a handful of slots, so each test starts by dropping the earlier ones.
    /// Tests run one at a time (`--test-threads=1`), so none of these slots is in use.
    async fn drop_leftover_slots(&self) {
        let slots = self
            .query(
                "SELECT slot_name FROM pg_replication_slots WHERE slot_name LIKE 'snap\\_%\\_slot'",
            )
            .await;
        for row in slots {
            self.drop_slot_if_exists(row[0].as_deref().expect("slot name"))
                .await;
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Rows of text cells; `None` is SQL NULL.
    pub async fn query(&self, sql: &str) -> Vec<Vec<Option<String>>> {
        let (url, query) = (self.url.clone(), sql.to_string());
        tokio::task::spawn_blocking(move || {
            let mut conn = PgReplicationConnection::connect(&url).map_err(|e| e.to_string())?;
            let result = conn.exec(&query).map_err(|e| e.to_string())?;
            Ok::<_, String>(
                (0..result.ntuples())
                    .map(|r| {
                        (0..result.nfields())
                            .map(|c| result.get_value(r, c))
                            .collect()
                    })
                    .collect(),
            )
        })
        .await
        .expect("blocking PostgreSQL task panicked")
        .unwrap_or_else(|e| panic!("PostgreSQL statement failed: {e}\n  SQL: {sql}"))
    }

    pub async fn exec(&self, sql: &str) {
        self.query(sql).await;
    }

    pub async fn query_i64(&self, sql: &str) -> i64 {
        let rows = self.query(sql).await;
        let cell = rows.first().and_then(|r| r.first()).cloned().flatten();
        cell.as_deref()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| panic!("expected one integer from {sql}, got {rows:?}"))
    }

    /// Fresh schema `snap_<tag>` (with a publication over all its tables, partitions published
    /// through their root), slot name `snap_<tag>_slot`; leftovers of an earlier run are dropped.
    pub async fn reset(&self, tag: &str) -> Scenario {
        let schema = format!("snap_{tag}");
        let publication = format!("{schema}_pub");
        let slot = format!("{schema}_slot");

        self.drop_slot_if_exists(&slot).await;
        self.exec(&format!(
            "DROP PUBLICATION IF EXISTS {}",
            ident(&publication)
        ))
        .await;
        self.exec(&format!("DROP SCHEMA IF EXISTS {} CASCADE", ident(&schema)))
            .await;
        self.exec(&format!("CREATE SCHEMA {}", ident(&schema)))
            .await;
        self.exec(&format!(
            "CREATE PUBLICATION {} FOR TABLES IN SCHEMA {} WITH (publish_via_partition_root = true)",
            ident(&publication),
            ident(&schema)
        ))
        .await;
        *self.schema.lock().unwrap() = schema.clone();

        Scenario {
            schema,
            publication,
            slot,
            data_dir: tempfile::tempdir().expect("scenario temp dir"),
        }
    }

    /// A slot still held by a dying client is released by terminating its backend first.
    pub async fn drop_slot_if_exists(&self, slot: &str) {
        let name = literal(slot);
        for _ in 0..40 {
            if self.slot_count(slot).await == 0 {
                return;
            }
            self.exec(&format!(
                "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots \
                 WHERE slot_name = {name} AND active_pid IS NOT NULL"
            ))
            .await;
            let (url, sql) = (
                self.url.clone(),
                format!("SELECT pg_drop_replication_slot({name})"),
            );
            let dropped = tokio::task::spawn_blocking(move || {
                PgReplicationConnection::connect(&url)
                    .and_then(|mut c| c.exec(&sql).map(|_| ()))
                    .is_ok()
            })
            .await
            .expect("blocking PostgreSQL task panicked");
            if dropped {
                return;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        panic!("could not drop replication slot {slot}");
    }

    pub async fn slot_count(&self, slot: &str) -> i64 {
        self.query_i64(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = {}",
            literal(slot)
        ))
        .await
    }

    /// Creates a logical `pgoutput` slot the way an operator would (no snapshot export) and
    /// returns its `confirmed_flush_lsn`.
    pub async fn create_slot(&self, slot: &str) -> u64 {
        self.exec(&format!(
            "SELECT pg_create_logical_replication_slot({}, 'pgoutput')",
            literal(slot)
        ))
        .await;
        self.slot_confirmed_flush(slot).await
    }

    /// `confirmed_flush_lsn` of an existing slot as a plain u64 (the marker's representation).
    pub async fn slot_confirmed_flush(&self, slot: &str) -> u64 {
        self.query_i64(&format!(
            "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint FROM pg_replication_slots \
             WHERE slot_name = {}",
            literal(slot)
        ))
        .await as u64
    }

    fn schema(&self) -> String {
        self.schema.lock().unwrap().clone()
    }

    pub async fn digest(&self, table: &str) -> Digest {
        let rows = self
            .query(&format!(
                "SELECT count(*), coalesce(min(id), 0), coalesce(max(id), 0), \
                 coalesce(sum(id), 0)::bigint FROM {}.{}",
                ident(&self.schema()),
                ident(table)
            ))
            .await;
        let cell = |i: usize| -> i64 {
            rows[0][i]
                .as_deref()
                .and_then(|v| v.parse().ok())
                .expect("digest cell")
        };
        Digest {
            count: cell(0) as u64,
            min_id: cell(1),
            max_id: cell(2),
            sum_id: cell(3),
        }
    }

    /// Canonical text columns (see `Target::row`): every column as PostgreSQL's own text form,
    /// `bytea` as `\x<hex>`, NULL as [`NULL_TEXT`].
    pub async fn row(&self, s: &Scenario, table: &str, id: i64) -> Option<Vec<String>> {
        self.rows(s, table, &[id]).await.remove(&id)
    }

    /// `row` for many ids in two statements (each statement costs a connection); ids that do not
    /// exist are absent from the map.
    pub async fn rows(
        &self,
        s: &Scenario,
        table: &str,
        ids: &[i64],
    ) -> std::collections::HashMap<i64, Vec<String>> {
        if ids.is_empty() {
            return Default::default();
        }
        let columns = self
            .query(&format!(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = {} AND table_name = {} ORDER BY ordinal_position",
                literal(&s.schema),
                literal(table)
            ))
            .await;
        let select: Vec<String> = columns
            .iter()
            .map(|c| {
                format!(
                    "coalesce({}::text, {})",
                    ident(c[0].as_deref().expect("column name")),
                    literal(NULL_TEXT)
                )
            })
            .collect();
        let id_list: Vec<String> = ids.iter().map(i64::to_string).collect();
        let rows = self
            .query(&format!(
                "SELECT {} FROM {}.{} WHERE id IN ({})",
                select.join(", "),
                ident(&s.schema),
                ident(table),
                id_list.join(", ")
            ))
            .await;
        rows.into_iter()
            .map(|r| {
                let cells: Vec<String> = r.into_iter().map(|c| c.unwrap_or_default()).collect();
                (cells[0].parse().expect("id is the first column"), cells)
            })
            .collect()
    }
}

/// A table name that needs quoting everywhere because of its upper case, with a column named like
/// a reserved word. No space or other character a Kafka topic name forbids, so Kafka can reuse it.
pub const MIXED_CASE_TABLE: &str = "SnapMixed";

/// The standard tables. All columns besides `id` are nullable, so `Target::insert_sentinel`
/// only has to supply an id.
pub struct TableSet;

impl TableSet {
    pub const ALL: [&'static str; 6] = [
        "snap_items",
        "snap_wide",
        "snap_part",
        "snap_empty",
        "snap_special",
        MIXED_CASE_TABLE,
    ];

    /// PostgreSQL statements creating `table` in `schema`.
    pub fn pg_ddl(schema: &str, table: &str) -> Vec<String> {
        let q = ident(schema);
        match table {
            "snap_items" => vec![format!(
                "CREATE TABLE {q}.snap_items (id bigint PRIMARY KEY, name text, payload bytea, note text)"
            )],
            "snap_wide" => vec![format!(
                "CREATE TABLE {q}.snap_wide (id bigint PRIMARY KEY, blob bytea)"
            )],
            "snap_part" => vec![
                format!(
                    "CREATE TABLE {q}.snap_part (id bigint NOT NULL, label text, PRIMARY KEY (id)) \
                     PARTITION BY RANGE (id)"
                ),
                format!(
                    "CREATE TABLE {q}.snap_part_lo PARTITION OF {q}.snap_part \
                     FOR VALUES FROM (MINVALUE) TO (100000)"
                ),
                format!(
                    "CREATE TABLE {q}.snap_part_hi PARTITION OF {q}.snap_part \
                     FOR VALUES FROM (100000) TO (MAXVALUE)"
                ),
            ],
            "snap_empty" => vec![format!(
                "CREATE TABLE {q}.snap_empty (id bigint PRIMARY KEY, v text)"
            )],
            "snap_special" => vec![format!(
                "CREATE TABLE {q}.snap_special (id bigint PRIMARY KEY, \"order\" text, \
                 \"Mixed Case\" text, raw bytea)"
            )],
            MIXED_CASE_TABLE => vec![format!(
                "CREATE TABLE {q}.{} (id bigint PRIMARY KEY, \"group\" text)",
                ident(MIXED_CASE_TABLE)
            )],
            other => panic!("unknown standard table {other}"),
        }
    }

    /// MySQL statement creating `table` in the connected database. BIGINT keys, TEXT/LONGTEXT and
    /// LONGBLOB (the destination type of `bytea`), `utf8mb4` throughout; `blob`, `order` and
    /// `group` are reserved words, hence the backticks.
    pub fn mysql_ddl(table: &str) -> String {
        let columns = match table {
            "snap_items" => "id BIGINT PRIMARY KEY, name TEXT, payload LONGBLOB, note TEXT",
            "snap_wide" => "id BIGINT PRIMARY KEY, `blob` LONGBLOB",
            "snap_part" => "id BIGINT PRIMARY KEY, label TEXT",
            "snap_empty" => "id BIGINT PRIMARY KEY, v TEXT",
            "snap_special" => {
                "id BIGINT PRIMARY KEY, `order` LONGTEXT, `Mixed Case` LONGTEXT, raw LONGBLOB"
            }
            MIXED_CASE_TABLE => "id BIGINT PRIMARY KEY, `group` LONGTEXT",
            other => panic!("unknown standard table {other}"),
        };
        format!("CREATE TABLE `{table}` ({columns}) DEFAULT CHARSET=utf8mb4")
    }

    /// SQL Server statement creating `[dbo].[table]`. BIGINT keys; text is NVARCHAR(MAX), so any
    /// Unicode round-trips whatever the database collation. A bytea reaches the destination as
    /// the ASCII text `\x<hex>` (snapshot COPY text and live pgoutput text alike) and is stored as
    /// exactly that text, in a VARCHAR(MAX) (a VARBINARY column could not take it: the TDS bulk
    /// path sends a string). The one exception is `snap_wide.blob`, a TEXT: tiberius 0.13 rejects
    /// a string over 65,535 encoded bytes in any (N)VARCHAR(MAX) column of a bulk load (it checks
    /// the length against the 0xFFFF "max" marker) but takes any size into TEXT, and the wide
    /// scenarios (S1's 128 KiB cells, S11's up to 128 MB) must run on the TDS Bulk Load path. A
    /// VARCHAR(MAX) with such a cell makes the destination fall back to multi-value INSERT. `blob`,
    /// `order` and `group` are reserved words, hence the brackets.
    pub fn sqlserver_ddl(table: &str) -> String {
        let columns = match table {
            "snap_items" => {
                "id BIGINT NOT NULL PRIMARY KEY, name NVARCHAR(MAX) NULL, \
                 payload VARCHAR(MAX) NULL, note NVARCHAR(MAX) NULL"
            }
            "snap_wide" => "id BIGINT NOT NULL PRIMARY KEY, [blob] TEXT NULL",
            "snap_part" => "id BIGINT NOT NULL PRIMARY KEY, label NVARCHAR(MAX) NULL",
            "snap_empty" => "id BIGINT NOT NULL PRIMARY KEY, v NVARCHAR(MAX) NULL",
            "snap_special" => {
                "id BIGINT NOT NULL PRIMARY KEY, [order] NVARCHAR(MAX) NULL, \
                 [Mixed Case] NVARCHAR(MAX) NULL, raw VARCHAR(MAX) NULL"
            }
            MIXED_CASE_TABLE => "id BIGINT NOT NULL PRIMARY KEY, [group] NVARCHAR(MAX) NULL",
            other => panic!("unknown standard table {other}"),
        };
        format!("CREATE TABLE [dbo].[{table}] ({columns})")
    }

    /// SQLite statement creating `table`; INTEGER/TEXT/BLOB affinities convert the quoted
    /// literals the renderer emits.
    pub fn sqlite_ddl(table: &str) -> String {
        let columns = match table {
            "snap_items" => "id INTEGER PRIMARY KEY, name TEXT, payload BLOB, note TEXT",
            "snap_wide" => "id INTEGER PRIMARY KEY, blob BLOB",
            "snap_part" => "id INTEGER PRIMARY KEY, label TEXT",
            "snap_empty" => "id INTEGER PRIMARY KEY, v TEXT",
            "snap_special" => {
                "id INTEGER PRIMARY KEY, \"order\" TEXT, \"Mixed Case\" TEXT, raw BLOB"
            }
            MIXED_CASE_TABLE => "id INTEGER PRIMARY KEY, \"group\" TEXT",
            other => panic!("unknown standard table {other}"),
        };
        format!("CREATE TABLE {} ({columns})", ident(table))
    }

    pub async fn create_pg(pg: &PgFixture, s: &Scenario, which: &[&str]) {
        for table in which {
            for statement in Self::pg_ddl(&s.schema, table) {
                pg.exec(&statement).await;
            }
        }
    }

    /// Rows `1..=rows` in `snap_items`; every 7th `note` is NULL.
    pub async fn seed_items(pg: &PgFixture, s: &Scenario, rows: u64) {
        pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name, payload, note) \
             SELECT g, 'name-' || g, decode(md5(g::text), 'hex'), \
             CASE WHEN g % 7 = 0 THEN NULL ELSE 'note-' || g END \
             FROM generate_series(1, {rows}) AS g",
            ident(&s.schema)
        ))
        .await;
    }
}

#[async_trait]
pub trait Target: Send + Sync {
    fn dest_type(&self) -> DestinationType;
    fn connection_string(&self) -> String;
    /// Destination DDL for the standard tables in `which`.
    async fn create_tables(&self, s: &Scenario, which: &[&str]);
    async fn digest(&self, table: &str) -> Digest;
    /// Canonical text columns in table order, comparable with `PgFixture::row`: integers in
    /// decimal, text as is, binary as `\x<lowercase hex>`, NULL as [`NULL_TEXT`].
    async fn row(&self, table: &str, id: i64) -> Option<Vec<String>>;
    async fn insert_sentinel(&self, table: &str, id: i64);
    /// Empties the destination: drops every standard table.
    async fn reset(&self);
    /// Peak memory per oversize row as a multiple of the row's source bytes for this destination;
    /// measured 14.1x on SQLite's text-SQL route, 10.1x on MySQL's `LOAD DATA` route and on SQL
    /// Server's TDS Bulk Load route. Kafka tunes it in its task.
    fn row_amplification(&self) -> u64 {
        16
    }
    /// How many times the destination's write path holds the snapshot byte budget in flight, the
    /// budgeted rendered text itself included (S11a bound: fixed overhead + this x buffer + 8 x
    /// row). SQLite peaks at 41-55 MiB against 4; MySQL needs more, see `MySqlTarget`; SQL Server's
    /// 30-34 MiB fit the default.
    fn buffer_copies(&self) -> u64 {
        4
    }
    /// S1 calls this as soon as it sees the `completed` marker: the copy is over and live
    /// transactions only start after that, so a destination can check how the copy was written.
    async fn on_snapshot_completed(&self) {}
    /// The throughput benchmark calls this once a point's copy converged and its client stopped:
    /// a destination checks that `rows` rows of `table` took its bulk path (a silent fallback to
    /// INSERT makes a benchmark meaningless) and cleans up after the point.
    async fn on_bench_completed(&self, _table: &str, _rows: u64, _batch_rows: usize) {}
    /// Cumulative destination-side counters (statements, commits, log flushes) that the live-path
    /// benchmark diffs around its timed window, to show how the destination was written. Empty
    /// for destinations without such counters.
    async fn bench_counters(&self) -> Vec<(&'static str, u64)> {
        Vec::new()
    }
    /// The destination database the client is configured to write `s.schema` into, when it is not
    /// the schema's own name (MySQL has no schemas: its "schema" is a database).
    fn schema_mapping(&self, _s: &Scenario) -> Option<(String, String)> {
        None
    }
    /// S12: makes every later write to `table` fail (SQLite drops it). `false` means this target
    /// cannot, and S12 skips itself for it.
    async fn break_destination(&self, _table: &str) -> bool {
        false
    }
    /// S12: undoes `break_destination` (SQLite recreates the empty table).
    async fn repair_destination(&self, _s: &Scenario, _table: &str) {}
}

#[cfg(feature = "sqlite")]
#[allow(unused_imports)]
pub use sqlite_target::SqliteTarget;

#[cfg(feature = "sqlite")]
mod sqlite_target {
    use super::*;
    use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
    use sqlx::{AssertSqlSafe, Row, SqlitePool};

    /// The destination database file lives in the scenario's temp dir. The harness pool and the
    /// client's own pool share it (WAL mode, so readers never block the writer).
    pub struct SqliteTarget {
        path: std::path::PathBuf,
        pool: SqlitePool,
    }

    impl SqliteTarget {
        pub fn new(dir: &Path) -> Self {
            let path = dir.join("destination.db");
            let options = SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_secs(30));
            let pool = SqlitePoolOptions::new()
                .max_connections(2)
                .connect_lazy_with(options);
            Self { path, pool }
        }

        async fn run(&self, sql: &str) {
            sqlx::query(AssertSqlSafe(sql.to_string()))
                .execute(&self.pool)
                .await
                .unwrap_or_else(|e| panic!("SQLite statement failed: {e}\n  SQL: {sql}"));
        }
    }

    #[async_trait]
    impl Target for SqliteTarget {
        fn dest_type(&self) -> DestinationType {
            DestinationType::SQLite
        }

        fn connection_string(&self) -> String {
            self.path.display().to_string()
        }

        async fn create_tables(&self, _s: &Scenario, which: &[&str]) {
            for table in which {
                self.run(&TableSet::sqlite_ddl(table)).await;
            }
        }

        async fn digest(&self, table: &str) -> Digest {
            let sql = format!(
                "SELECT count(*), coalesce(min(id), 0), coalesce(max(id), 0), \
                 coalesce(sum(id), 0) FROM {}",
                ident(table)
            );
            let row = sqlx::query(AssertSqlSafe(sql.clone()))
                .fetch_one(&self.pool)
                .await
                .unwrap_or_else(|e| panic!("SQLite digest failed: {e}\n  SQL: {sql}"));
            Digest {
                count: row.get::<i64, _>(0) as u64,
                min_id: row.get(1),
                max_id: row.get(2),
                sum_id: row.get(3),
            }
        }

        async fn row(&self, table: &str, id: i64) -> Option<Vec<String>> {
            let columns: Vec<String> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT name FROM pragma_table_info({}) ORDER BY cid",
                literal(table)
            )))
            .fetch_all(&self.pool)
            .await
            .expect("SQLite table_info");
            let select: Vec<String> = columns
                .iter()
                .map(|c| {
                    let c = ident(c);
                    format!(
                        "CASE typeof({c}) WHEN 'null' THEN {} WHEN 'blob' THEN '\\x' || lower(hex({c})) \
                         ELSE CAST({c} AS TEXT) END",
                        literal(NULL_TEXT)
                    )
                })
                .collect();
            let sql = format!(
                "SELECT {} FROM {} WHERE id = {id}",
                select.join(", "),
                ident(table)
            );
            let row = sqlx::query(AssertSqlSafe(sql.clone()))
                .fetch_optional(&self.pool)
                .await
                .unwrap_or_else(|e| panic!("SQLite row query failed: {e}\n  SQL: {sql}"))?;
            Some(
                (0..columns.len())
                    .map(|i| row.get::<String, _>(i))
                    .collect(),
            )
        }

        async fn insert_sentinel(&self, table: &str, id: i64) {
            self.run(&format!("INSERT INTO {} (id) VALUES ({id})", ident(table)))
                .await;
        }

        async fn reset(&self) {
            for table in TableSet::ALL {
                self.run(&format!("DROP TABLE IF EXISTS {}", ident(table)))
                    .await;
            }
        }

        async fn break_destination(&self, table: &str) -> bool {
            self.run(&format!("DROP TABLE {}", ident(table))).await;
            true
        }

        async fn repair_destination(&self, s: &Scenario, table: &str) {
            self.create_tables(s, &[table]).await;
        }
    }
}

/// Builder pre-set for a snapshot run of `s` against `t`; tests adjust it before `build()`.
pub fn client_config(pg: &PgFixture, s: &Scenario, t: &dyn Target) -> ConfigBuilder {
    let builder = Config::builder()
        .source_connection_string(pg.url())
        .destination_type(t.dest_type())
        .destination_connection_string(t.connection_string())
        .replication_slot_name(&s.slot)
        .publication_name(&s.publication)
        .transaction_file_base_path(s.data_dir.path().to_string_lossy().into_owned())
        .initial_snapshot(true)
        .snapshot_buffer_bytes(4 * 1024 * 1024)
        .snapshot_batch_rows(200);
    match t.schema_mapping(s) {
        Some((source, destination)) => builder.schema_mapping(source, destination),
        None => builder,
    }
}

/// The client runs in its own task: `start_replication_from_lsn` blocks until cancelled, then the
/// task shuts the client down. A failure in `CdcClient::new` is reported through the same
/// channel (`wait_err`, and the `wait_*` methods) as a failure while running.
pub struct RunningClient {
    token: CancellationToken,
    handle: Option<JoinHandle<Result<(), CdcError>>>,
}

pub async fn start_client(cfg: Config, lsn_file: &Path) -> RunningClient {
    let lsn_file = lsn_file.to_str().expect("UTF-8 LSN file path");
    match CdcClient::new(cfg, Some(lsn_file)).await {
        Ok((mut client, start_lsn)) => {
            let token = client.cancellation_token();
            RunningClient::from_task(
                token,
                tokio::spawn(async move {
                    let run = client.start_replication_from_lsn(start_lsn).await;
                    let stop = client.stop().await;
                    run.and(stop)
                }),
            )
        }
        Err(e) => RunningClient::from_task(
            CancellationToken::new(),
            tokio::spawn(async move { Err(e) }),
        ),
    }
}

impl RunningClient {
    /// Wraps any task as the client; `start_client` uses it, and so can a harness self-test.
    pub fn from_task(token: CancellationToken, handle: JoinHandle<Result<(), CdcError>>) -> Self {
        Self {
            token,
            handle: Some(handle),
        }
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub fn is_finished(&self) -> bool {
        self.handle.as_ref().is_none_or(|h| h.is_finished())
    }

    /// Cancels the client, waits for its shutdown and returns its result. If `wait_err` already
    /// reported the failure, this is `Ok(())`.
    pub async fn stop(mut self) -> Result<(), CdcError> {
        self.token.cancel();
        match self.handle.take() {
            Some(handle) => handle.await.expect("client task panicked"),
            None => Ok(()),
        }
    }

    /// The error the client ended with, waiting up to `within` for it to end. `None` means no
    /// error showed up in that time: the client is still running (see `is_finished`) or it ended
    /// cleanly.
    pub async fn wait_err(&mut self, within: Duration) -> Option<CdcError> {
        let handle = self.handle.as_mut()?;
        let joined = tokio::time::timeout(within, handle).await.ok()?;
        self.handle = None;
        joined.expect("client task panicked").err()
    }

    /// Description of the client's end (consuming its outcome), or `None` while it is running.
    async fn take_outcome(&mut self) -> Option<String> {
        match &self.handle {
            None => Some("it had ended earlier and its outcome was already reported".into()),
            Some(h) if !h.is_finished() => None,
            Some(_) => Some(match self.handle.take()?.await {
                Ok(Ok(())) => "it exited cleanly while the test still needed it".into(),
                Ok(Err(e)) => format!("it failed: {e}"),
                Err(e) => format!("its task panicked: {e}"),
            }),
        }
    }

    /// `poll_until` that also fails the wait, for as long as it lasts, if the client ends first.
    pub async fn wait_until<F, Fut>(&mut self, what: &str, timeout: Duration, check: F)
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<String>>,
    {
        poll_until(Some(self), what, timeout, check).await
    }

    /// Waits until PostgreSQL and target digests of `tables` match. The digest covers ids only
    /// (see [`Digest`]). A client that ends before convergence fails the wait immediately with
    /// its outcome and the digest diff, however long `timeout` is.
    pub async fn wait_converged(
        &mut self,
        pg: &PgFixture,
        t: &dyn Target,
        tables: &[&str],
        timeout: Duration,
    ) {
        self.wait_until("convergence", timeout, || digest_diffs(pg, t, tables))
            .await
    }

    pub async fn wait_marker_state(&mut self, s: &Scenario, want: &str, timeout: Duration) {
        self.wait_until("snapshot marker state", timeout, || {
            let state = s.marker_state();
            std::future::ready(
                (state.as_deref() != Some(want))
                    .then(|| format!("marker is {state:?}, wanted {want:?}")),
            )
        })
        .await
    }

    /// Waits until `accept` approves the target's canonical row `id` of `table`; for changes the
    /// id-only digest cannot see (updates).
    pub async fn wait_for_row(
        &mut self,
        t: &dyn Target,
        table: &str,
        id: i64,
        accept: impl Fn(&[String]) -> bool,
        timeout: Duration,
    ) {
        let accept = &accept;
        self.wait_until(&format!("row {table}#{id}"), timeout, move || async move {
            match t.row(table, id).await {
                Some(row) if accept(&row) => None,
                other => Some(format!("{table} id {id} is {other:?}")),
            }
        })
        .await
    }
}

/// Calls `check` every [`POLL_INTERVAL`] until it returns `None` (condition holds; a `None` wins
/// even if the client has just ended). `Some(text)` describes what is still pending and goes
/// into the panic if `timeout` passes or the `client`, when given, ends first.
async fn poll_until<F, Fut>(
    mut client: Option<&mut RunningClient>,
    what: &str,
    timeout: Duration,
    mut check: F,
) where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<String>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        let Some(pending) = check().await else {
            return;
        };
        if let Some(client) = client.as_mut() {
            if let Some(outcome) = client.take_outcome().await {
                panic!("client ended before {what}: {outcome}\n{pending}");
            }
        }
        assert!(
            Instant::now() < deadline,
            "{what} not reached after {timeout:?}:\n{pending}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

pub async fn digest_diffs(pg: &PgFixture, t: &dyn Target, tables: &[&str]) -> Option<String> {
    let mut diffs = Vec::new();
    for table in tables {
        let (source, target) = (pg.digest(table).await, t.digest(table).await);
        if source != target {
            diffs.push(format!("{table}: postgres {source:?} != target {target:?}"));
        }
    }
    (!diffs.is_empty()).then(|| diffs.join("\n"))
}

/// Polls PostgreSQL and target digests of `tables` every 250 ms until they are equal; panics with
/// the differing digests after `timeout`. Digests cover ids only (see [`Digest`]). With a client
/// running, prefer `RunningClient::wait_converged`, which also notices the client dying.
pub async fn wait_converged(pg: &PgFixture, t: &dyn Target, tables: &[&str], timeout: Duration) {
    poll_until(None, "convergence", timeout, || digest_diffs(pg, t, tables)).await
}
