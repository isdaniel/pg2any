//! Initial-snapshot end-to-end scenarios: live PostgreSQL source, SQLite destination.
//!
//! Needs a running PostgreSQL (see `docker-compose.snapshot-test.yml`); run with
//! `make snapshot-test-sqlite`.
#![cfg(feature = "sqlite")]

#[allow(dead_code)]
mod snapshot_common;

use pg2any_lib::SnapshotTargetPolicy;
use snapshot_common::snapshot_harness::{
    client_config, start_client, PgFixture, SqliteTarget, TableSet, Target,
};
use snapshot_common::snapshot_scenarios::{self};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlite_small_snapshot_then_live_change() {
    let pg = PgFixture::connect().await;
    let scenario = pg.reset("sqlite_small").await;
    let target = SqliteTarget::new(scenario.data_dir.path());

    TableSet::create_pg(&pg, &scenario, &["snap_items"]).await;
    target.create_tables(&scenario, &["snap_items"]).await;
    TableSet::seed_items(&pg, &scenario, 1_000).await;

    let cfg = client_config(&pg, &scenario, &target)
        .build()
        .expect("valid config");
    let lsn_file = scenario.data_dir.path().join("lsn.metadata");
    let mut client = start_client(cfg, &lsn_file).await;

    // Baseline: every pre-existing row arrives through the snapshot.
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;
    assert_eq!(target.digest("snap_items").await.count, 1_000);
    client
        .wait_marker_state(&scenario, "completed", Duration::from_secs(30))
        .await;
    assert_eq!(pg.slot_count(&scenario.slot).await, 1);

    // Handoff: a row written after the snapshot arrives through the live stream.
    pg.exec(&format!(
        "INSERT INTO {}.snap_items (id, name, payload, note) \
         VALUES (1001, 'live', '\\xdeadbeef', NULL)",
        scenario.schema
    ))
    .await;
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;
    assert_eq!(target.digest("snap_items").await.count, 1_001);
    client
        .wait_for_row(
            &target,
            "snap_items",
            1_001,
            |row| row[1] == "live",
            Duration::from_secs(30),
        )
        .await;

    // Rows match column by column, including a NULL note, a live row and the extremes.
    for id in [1, 7, 500, 1_000, 1_001] {
        assert_eq!(
            target.row("snap_items", id).await,
            pg.row(&scenario, "snap_items", id).await,
            "row {id} differs between PostgreSQL and SQLite"
        );
        assert!(
            target.row("snap_items", id).await.is_some(),
            "row {id} missing"
        );
    }

    assert!(!client.is_finished(), "client stopped on its own");
    client.stop().await.expect("clean shutdown");
}

/// A failure after the slot's snapshot was taken but before the first row is written (here: the
/// destination cannot be opened) leaves no marker. `append` cannot clean up after an interrupted
/// copy and refuses to start over one, so a stale marker would block it although nothing was copied.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlite_append_restarts_after_a_pre_copy_failure() {
    let pg = PgFixture::connect().await;
    let scenario = pg.reset("sqlite_append_retry").await;
    let target = SqliteTarget::new(scenario.data_dir.path());
    TableSet::create_pg(&pg, &scenario, &["snap_items"]).await;
    target.create_tables(&scenario, &["snap_items"]).await;
    TableSet::seed_items(&pg, &scenario, 100).await;
    let lsn_file = scenario.data_dir.path().join("lsn.metadata");

    // A directory below a regular file cannot be created, so the snapshot's writer cannot connect.
    let blocker = scenario.data_dir.path().join("blocker");
    std::fs::write(&blocker, b"not a directory").expect("blocker file");
    let unreachable = client_config(&pg, &scenario, &target)
        .snapshot_target_policy(SnapshotTargetPolicy::Append)
        .destination_connection_string(blocker.join("dest.db").to_string_lossy().into_owned())
        .build()
        .expect("valid config");
    let mut client = start_client(unreachable, &lsn_file).await;
    client
        .wait_err(Duration::from_secs(60))
        .await
        .expect("an unreachable destination ends the client with an error");
    assert_eq!(
        scenario.marker_state(),
        None,
        "nothing was copied, so no marker may demand a cleanup"
    );
    assert_eq!(pg.slot_count(&scenario.slot).await, 0, "no orphaned slot");

    let reachable = client_config(&pg, &scenario, &target)
        .snapshot_target_policy(SnapshotTargetPolicy::Append)
        .build()
        .expect("valid config");
    let mut client = start_client(reachable, &lsn_file).await;
    client
        .wait_marker_state(&scenario, "completed", Duration::from_secs(60))
        .await;
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;
    assert_eq!(target.digest("snap_items").await.count, 100);
    client.stop().await.expect("clean shutdown");
}

/// A run killed right after the slot's snapshot was taken, before the first row was dispatched,
/// leaves the slot and a marker with its consistent point but no table list. Nothing was copied,
/// so even `append` (which cannot clean up after a copy) recovers: it drops the slot and starts over.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlite_append_recovers_when_the_run_stopped_before_the_copy_started() {
    let pg = PgFixture::connect().await;
    let scenario = pg.reset("sqlite_append_precopy_kill").await;
    let target = SqliteTarget::new(scenario.data_dir.path());
    TableSet::create_pg(&pg, &scenario, &["snap_items"]).await;
    target.create_tables(&scenario, &["snap_items"]).await;
    TableSet::seed_items(&pg, &scenario, 100).await;

    let consistent_point = pg.create_slot(&scenario.slot).await;
    std::fs::write(
        scenario.marker_path(),
        serde_json::json!({
            "version": 1,
            "marker": {
                "state": "in_progress",
                "slot_name": scenario.slot,
                "consistent_point": consistent_point,
                "tables": [],
                "copy_started": false,
            }
        })
        .to_string(),
    )
    .expect("marker file");

    let cfg = client_config(&pg, &scenario, &target)
        .snapshot_target_policy(SnapshotTargetPolicy::Append)
        .build()
        .expect("valid config");
    let lsn_file = scenario.data_dir.path().join("lsn.metadata");
    let mut client = start_client(cfg, &lsn_file).await;
    client
        .wait_marker_state(&scenario, "completed", Duration::from_secs(60))
        .await;
    client
        .wait_converged(&pg, &target, &["snap_items"], Duration::from_secs(60))
        .await;
    assert_eq!(target.digest("snap_items").await.count, 100);
    client.stop().await.expect("clean shutdown");
}

/// Recovery empties the tables the marker lists, so it must not run against another destination:
/// the connection string changed between the crash and the restart, and the new destination holds
/// rows of its own in tables of the same names. The start is refused and nothing is touched: no
/// row truncated, the slot kept.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlite_recovery_refuses_a_destination_other_than_the_markers_and_truncates_nothing() {
    let pg = PgFixture::connect().await;
    let scenario = pg.reset("sqlite_recover_other_dest").await;
    let target = SqliteTarget::new(scenario.data_dir.path());
    TableSet::create_pg(&pg, &scenario, &["snap_items"]).await;
    target.create_tables(&scenario, &["snap_items"]).await;
    TableSet::seed_items(&pg, &scenario, 100).await;
    for id in [7001, 7002, 7003] {
        target.insert_sentinel("snap_items", id).await;
    }

    let consistent_point = pg.create_slot(&scenario.slot).await;
    std::fs::write(
        scenario.marker_path(),
        serde_json::json!({
            "version": 1,
            "marker": {
                "state": "in_progress",
                "slot_name": scenario.slot,
                "consistent_point": consistent_point,
                "tables": [[scenario.schema, "snap_items"]],
                "copy_started": true,
                "destination": "/somewhere/else/destination.db",
            }
        })
        .to_string(),
    )
    .expect("marker file");

    let cfg = client_config(&pg, &scenario, &target)
        .build()
        .expect("valid config");
    let lsn_file = scenario.data_dir.path().join("lsn.metadata");
    let mut client = start_client(cfg, &lsn_file).await;
    let error = client
        .wait_err(Duration::from_secs(60))
        .await
        .expect("the client refuses to start");
    let message = error.to_string();
    assert!(
        message.contains("destination") && message.contains("/somewhere/else/destination.db"),
        "{message}"
    );
    assert_eq!(
        target.digest("snap_items").await.count,
        3,
        "the rows of the other destination were truncated"
    );
    assert_eq!(
        pg.slot_count(&scenario.slot).await,
        1,
        "the slot was dropped"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires live services; run via make snapshot-test-*"]
async fn sqlite_s1() {
    let dest_dir = tempfile::tempdir().expect("destination temp dir");
    let target = SqliteTarget::new(dest_dir.path());
    snapshot_scenarios::s1_consistent_handoff_under_writes(&target).await;
}

/// One `#[ignore]`d live test per shared scenario, each on its own SQLite destination file.
macro_rules! sqlite_scenario {
    ($test:ident, $scenario:ident) => {
        #[tokio::test(flavor = "multi_thread")]
        #[ignore = "requires live services; run via make snapshot-test-*"]
        async fn $test() {
            let dest_dir = tempfile::tempdir().expect("destination temp dir");
            let target = SqliteTarget::new(dest_dir.path());
            snapshot_scenarios::$scenario(&target).await;
        }
    };
}

sqlite_scenario!(sqlite_s2, s2_policy_matrix);
sqlite_scenario!(sqlite_s3, s3_existing_slot_without_marker_fails_fast);
sqlite_scenario!(sqlite_s4, s4_crash_recovery_in_progress);
sqlite_scenario!(sqlite_s5, s5_completed_marker_skips_snapshot_on_restart);
sqlite_scenario!(sqlite_s6, s6_resync_after_slot_loss);
sqlite_scenario!(sqlite_s7, s7_missing_destination_table_fails_before_copy);
sqlite_scenario!(sqlite_s8, s8_empty_tables_and_empty_publication);
sqlite_scenario!(sqlite_s9, s9_special_identifiers_and_values);
sqlite_scenario!(sqlite_s10, s10_cancel_mid_copy_then_restart_completes);
sqlite_scenario!(sqlite_s13, s13_kill_mid_stream_then_restart_converges);
sqlite_scenario!(sqlite_s14, s14_graceful_stop_mid_stream_applies_everything);
sqlite_scenario!(
    sqlite_s15,
    s15_small_transaction_backlog_converges_and_the_tail_is_not_held_back
);
// S12 needs `Target::break_destination`, which only SQLite implements (it drops the table).
sqlite_scenario!(
    sqlite_s12,
    s12_writer_failure_mid_copy_leaves_no_orphan_slot
);

/// Startup recovery against the replication stream (SQLite is the destination; the order of the
/// two does not depend on it). The destination is wrapped so that the first batch of the recovery
/// blocks until the test lets it go, which holds the client in its recovery for as long as needed.
mod recovery {
    use super::*;
    use async_trait::async_trait;
    use chrono::Utc;
    use pg2any_lib::transaction_manager::TransactionManager;
    use pg2any_lib::{
        CdcResult, Config, DestinationHandler, DestinationType, Lsn, LsnTracker, PreCommitHook,
        SqlDialect,
    };
    use pg_walstream::{ChangeEvent, ColumnValue, PgReplicationConnection, RowData};
    use snapshot_common::snapshot_harness::Scenario;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::time::Instant;

    /// Blocks every batch of the wrapped destination until `release`.
    struct Gate {
        entered: AtomicUsize,
        open: tokio::sync::watch::Sender<bool>,
    }

    impl Gate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                entered: AtomicUsize::new(0),
                open: tokio::sync::watch::channel(false).0,
            })
        }

        fn release(&self) {
            self.open.send_replace(true);
        }
    }

    struct GatedDestination {
        inner: Box<dyn DestinationHandler>,
        gate: Arc<Gate>,
    }

    #[async_trait]
    impl DestinationHandler for GatedDestination {
        async fn connect(&mut self, connection_string: &str) -> CdcResult<()> {
            self.inner.connect(connection_string).await
        }

        fn set_schema_mappings(&mut self, mappings: HashMap<String, String>) {
            self.inner.set_schema_mappings(mappings)
        }

        fn set_max_rows_per_insert(&mut self, max_rows: usize) {
            self.inner.set_max_rows_per_insert(max_rows)
        }

        fn dialect(&self) -> &'static dyn SqlDialect {
            self.inner.dialect()
        }

        async fn execute_sql_batch_with_hook(
            &mut self,
            commands: &[String],
            pre_commit_hook: Option<PreCommitHook>,
        ) -> CdcResult<()> {
            self.gate.entered.fetch_add(1, Ordering::SeqCst);
            let mut open = self.gate.open.subscribe();
            let _ = open.wait_for(|open| *open).await;
            self.inner
                .execute_sql_batch_with_hook(commands, pre_commit_hook)
                .await
        }

        async fn close(&mut self) -> CdcResult<()> {
            self.inner.close().await
        }
    }

    /// `wal_sender_timeout` of the test server for the duration of a test; restored on drop, also
    /// when the test fails, because the setting is server wide.
    struct WalSenderTimeout {
        url: String,
    }

    impl WalSenderTimeout {
        async fn set(pg: &PgFixture, value: &str) -> Self {
            pg.exec(&format!("ALTER SYSTEM SET wal_sender_timeout = '{value}'"))
                .await;
            let guard = Self {
                url: pg.url().to_string(),
            };
            pg.exec("SELECT pg_reload_conf()").await;
            let started = Instant::now();
            while pg.query("SHOW wal_sender_timeout").await[0][0].as_deref() != Some(value) {
                assert!(
                    started.elapsed() < Duration::from_secs(10),
                    "reload timed out"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            guard
        }
    }

    impl Drop for WalSenderTimeout {
        fn drop(&mut self) {
            let url = self.url.clone();
            let _ = std::thread::spawn(move || {
                if let Ok(mut conn) = PgReplicationConnection::connect(&url) {
                    let _ = conn.exec("ALTER SYSTEM RESET wal_sender_timeout");
                    let _ = conn.exec("SELECT pg_reload_conf()");
                }
            })
            .join();
        }
    }

    /// One committed transaction file per id in `sql_pending_tx/`, the way a crashed client leaves
    /// them: each inserts the row `(id, 'backlog-<id>')`, commit LSNs `first_lsn + 10, + 20, ...`.
    async fn write_backlog(dir: &Path, schema: &str, first_lsn: u64, ids: &[i64]) {
        let manager = TransactionManager::new(dir, DestinationType::SQLite, None, 64 * 1024 * 1024)
            .await
            .expect("transaction manager");
        for (i, id) in ids.iter().enumerate() {
            let tx_id = 100 + i as u32;
            let lsn = first_lsn + (i as u64 + 1) * 10;
            manager
                .begin_transaction(tx_id, Utc::now(), "normal")
                .await
                .expect("begin");
            let data = RowData::from_pairs(vec![
                ("id", ColumnValue::text(&id.to_string())),
                ("name", ColumnValue::text(&format!("backlog-{id}"))),
            ]);
            manager
                .append_event(
                    tx_id,
                    &ChangeEvent::insert(schema, "snap_items", 1, data, Lsn(lsn)),
                )
                .await
                .expect("append");
            manager.flush_all_buffers().await.expect("flush");
            manager
                .commit_transaction(tx_id, Some(Lsn(lsn)))
                .await
                .expect("commit");
        }
    }

    async fn write_position(lsn_file: &Path, lsn: u64) {
        let tracker = LsnTracker::new(Some(lsn_file.to_str().expect("path"))).await;
        tracker.commit_lsn(lsn);
        tracker.persist_async().await.expect("persist the position");
    }

    /// A source with `snap_items`, a destination with the same table and a gated destination
    /// handler in front of it.
    struct Setup {
        pg: PgFixture,
        scenario: Scenario,
        target: SqliteTarget,
        gate: Arc<Gate>,
        cfg: Config,
        lsn_file: std::path::PathBuf,
    }

    async fn setup(tag: &str) -> Setup {
        let pg = PgFixture::connect().await;
        let scenario = pg.reset(tag).await;
        let target = SqliteTarget::new(scenario.data_dir.path());
        TableSet::create_pg(&pg, &scenario, &["snap_items"]).await;
        target.create_tables(&scenario, &["snap_items"]).await;
        let lsn_file = scenario.data_dir.path().join("lsn.metadata");
        let plain = client_config(&pg, &scenario, &target)
            .initial_snapshot(false)
            .build()
            .expect("valid config");
        let gate = Gate::new();
        let cfg = client_config(&pg, &scenario, &target)
            .initial_snapshot(false)
            .custom_destination({
                let (plain, gate) = (plain.clone(), gate.clone());
                move || GatedDestination {
                    inner: plain.create_destination().expect("SQLite destination"),
                    gate: gate.clone(),
                }
            })
            .build()
            .expect("valid config");
        Setup {
            pg,
            scenario,
            target,
            gate,
            cfg,
            lsn_file,
        }
    }

    async fn walsenders(pg: &PgFixture, slot: &str) -> i64 {
        pg.query_i64(&format!(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = '{slot}' AND active"
        ))
        .await
    }

    async fn row_arrives(target: &SqliteTarget, id: i64, within: Duration) -> bool {
        let deadline = Instant::now() + within;
        loop {
            if target.row("snap_items", id).await.is_some() {
                return true;
            }
            if Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// The restart that follows a crash: the slot exists and the crashed client left committed
    /// transactions behind. While they are replayed (here held for longer than twice
    /// `wal_sender_timeout`) no walsender may serve the slot: nothing polls the stream during the
    /// recovery, so a started stream would be cut by the server (verified against PostgreSQL 15:
    /// a started, unpolled connection is terminated after the timeout and only reconnects on the
    /// first read). Once the recovery is done the stream starts and carries live changes.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_recovery_replays_pending_files_before_the_stream_starts() {
        let s = setup("sqlite_recovery_order").await;
        let confirmed = s.pg.create_slot(&s.scenario.slot).await;
        write_backlog(
            s.scenario.data_dir.path(),
            &s.scenario.schema,
            confirmed,
            &[1, 2, 3],
        )
        .await;
        write_position(&s.lsn_file, confirmed).await;
        let _timeout = WalSenderTimeout::set(&s.pg, "3s").await;

        let mut client = start_client(s.cfg.clone(), &s.lsn_file).await;
        client
            .wait_until(
                "the recovery to reach the destination",
                Duration::from_secs(60),
                || {
                    let entered = s.gate.entered.load(Ordering::SeqCst);
                    async move { (entered == 0).then(|| "no batch applied yet".to_string()) }
                },
            )
            .await;

        for second in 0..8 {
            assert_eq!(
                walsenders(&s.pg, &s.scenario.slot).await,
                0,
                "a walsender serves the slot {second}s into the recovery: the stream was started \
                 before the recovery ended"
            );
            assert!(
                !client.is_finished(),
                "the client ended during its recovery"
            );
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        s.gate.release();
        for id in 1..=3 {
            client
                .wait_for_row(
                    &s.target,
                    "snap_items",
                    id,
                    |row| row[1] == format!("backlog-{id}"),
                    Duration::from_secs(60),
                )
                .await;
        }
        s.pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name) VALUES (100, 'live')",
            s.scenario.schema
        ))
        .await;
        client
            .wait_for_row(
                &s.target,
                "snap_items",
                100,
                |row| row[1] == "live",
                Duration::from_secs(60),
            )
            .await;
        assert_eq!(walsenders(&s.pg, &s.scenario.slot).await, 1);
        client.stop().await.expect("clean shutdown");
    }

    /// The stream's connection sits idle through the recovery and dies meanwhile (a server
    /// restart, a dropped network connection). The client replaces it before it starts the stream
    /// and carries on, as the producer's reads would after a drop; it must not end with an error
    /// after a recovery that worked.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_stream_connection_lost_during_recovery_is_replaced_before_the_stream_starts() {
        let s = setup("sqlite_recovery_conn_lost").await;
        let confirmed = s.pg.create_slot(&s.scenario.slot).await;
        write_backlog(
            s.scenario.data_dir.path(),
            &s.scenario.schema,
            confirmed,
            &[1, 2, 3],
        )
        .await;
        write_position(&s.lsn_file, confirmed).await;

        let mut client = start_client(s.cfg.clone(), &s.lsn_file).await;
        client
            .wait_until(
                "the recovery to reach the destination",
                Duration::from_secs(60),
                || {
                    let entered = s.gate.entered.load(Ordering::SeqCst);
                    async move { (entered == 0).then(|| "no batch applied yet".to_string()) }
                },
            )
            .await;
        // The only walsender of the test server that is not this query's own is the stream's.
        let ended =
            s.pg.query_i64(
                "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE backend_type = 'walsender' AND pid <> pg_backend_pid()) t",
            )
            .await;
        assert_eq!(ended, 1, "the idle stream connection was ended");

        s.gate.release();
        for id in 1..=3 {
            client
                .wait_for_row(
                    &s.target,
                    "snap_items",
                    id,
                    |_| true,
                    Duration::from_secs(60),
                )
                .await;
        }
        s.pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name) VALUES (100, 'live')",
            s.scenario.schema
        ))
        .await;
        client
            .wait_for_row(
                &s.target,
                "snap_items",
                100,
                |row| row[1] == "live",
                Duration::from_secs(60),
            )
            .await;
        assert!(!client.is_finished(), "the client stopped on its own");
        client.stop().await.expect("clean shutdown");
    }

    /// A first start has no slot, so no transaction can be re-sent: a destination that already
    /// holds the row the source then inserts is a real key conflict, not a batch the destination
    /// committed before a crash. It must stop the apply (retried; nothing after it goes through)
    /// and not be skipped as a duplicate. Once the conflict is removed everything arrives.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_first_start_does_not_skip_a_real_key_conflict() {
        let pg = PgFixture::connect().await;
        let sc = pg.reset("sqlite_first_start_conflict").await;
        let target = SqliteTarget::new(sc.data_dir.path());
        TableSet::create_pg(&pg, &sc, &["snap_items"]).await;
        target.create_tables(&sc, &["snap_items"]).await;
        target.insert_sentinel("snap_items", 1).await;
        let cfg = client_config(&pg, &sc, &target)
            .initial_snapshot(false)
            .build()
            .expect("valid config");
        let lsn_file = sc.data_dir.path().join("lsn.metadata");
        let mut client = start_client(cfg, &lsn_file).await;
        let started = Instant::now();
        while pg.slot_count(&sc.slot).await != 1 {
            assert!(
                !client.is_finished(),
                "the client ended before its slot existed"
            );
            assert!(
                started.elapsed() < Duration::from_secs(60),
                "no slot in time"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for (id, name) in [(1, "source"), (2, "next")] {
            pg.exec(&format!(
                "INSERT INTO {}.snap_items (id, name) VALUES ({id}, '{name}')",
                sc.schema
            ))
            .await;
        }

        tokio::time::sleep(Duration::from_secs(8)).await;
        assert!(
            target.row("snap_items", 2).await.is_none(),
            "the conflict on id 1 was skipped and the apply went on"
        );
        assert!(!client.is_finished(), "the client ended on its own");

        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(target.connection_string())
            .busy_timeout(Duration::from_secs(30));
        let pool = sqlx::SqlitePool::connect_with(options).await.expect("pool");
        sqlx::query("DELETE FROM snap_items WHERE id = 1")
            .execute(&pool)
            .await
            .expect("remove the conflicting row");
        client
            .wait_for_row(
                &target,
                "snap_items",
                1,
                |row| row[1] == "source",
                Duration::from_secs(90),
            )
            .await;
        client
            .wait_for_row(
                &target,
                "snap_items",
                2,
                |row| row[1] == "next",
                Duration::from_secs(60),
            )
            .await;
        client.stop().await.expect("clean shutdown");
    }

    /// The slot was deleted (an operator, a failover) but the client has a position on disk and
    /// committed transactions to replay. A change committed while they are replayed must still
    /// reach the destination: the new slot has to exist from the start of the recovery, not only
    /// from the moment the stream starts, or everything committed in between is behind the slot's
    /// consistent point and lost.
    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_slot_exists_during_recovery_so_changes_made_meanwhile_are_not_lost() {
        let s = setup("sqlite_recovery_gap").await;
        write_backlog(
            s.scenario.data_dir.path(),
            &s.scenario.schema,
            100,
            &[1, 2, 3],
        )
        .await;
        write_position(&s.lsn_file, 100).await;
        assert_eq!(
            s.pg.slot_count(&s.scenario.slot).await,
            0,
            "the slot is gone"
        );

        let mut client = start_client(s.cfg.clone(), &s.lsn_file).await;
        client
            .wait_until(
                "the recovery to reach the destination",
                Duration::from_secs(60),
                || {
                    let entered = s.gate.entered.load(Ordering::SeqCst);
                    async move { (entered == 0).then(|| "no batch applied yet".to_string()) }
                },
            )
            .await;

        let slots_during_recovery = s.pg.slot_count(&s.scenario.slot).await;
        // Creating the slot early must not leave a snapshot exported by it pinning the source's
        // xmin for as long as the recovery lasts.
        let pinned =
            s.pg.query_i64(
                "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender' \
                 AND pid <> pg_backend_pid() AND backend_xmin IS NOT NULL",
            )
            .await;
        s.pg.exec(&format!(
            "INSERT INTO {}.snap_items (id, name) VALUES (9001, 'during-recovery')",
            s.scenario.schema
        ))
        .await;
        s.gate.release();

        let arrived = row_arrives(&s.target, 9001, Duration::from_secs(30)).await;
        assert!(
            slots_during_recovery == 1 && arrived,
            "the slot existed during the recovery: {}, the row committed meanwhile arrived: {arrived}",
            slots_during_recovery == 1
        );
        assert_eq!(pinned, 0, "a walsender holds an xmin during the recovery");
        for id in 1..=3 {
            assert!(
                s.target.row("snap_items", id).await.is_some(),
                "backlog row {id}"
            );
        }
        client.stop().await.expect("clean shutdown");
    }
}

#[cfg(target_os = "linux")]
mod memory {
    use super::*;
    use snapshot_scenarios::MemoryCase;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_s11c_memory_independent_of_table_size() {
        let dest_dir = tempfile::tempdir().expect("destination temp dir");
        let target = SqliteTarget::new(dest_dir.path());
        snapshot_scenarios::s11_memory_bound(&target, MemoryCase::TableSizeIndependence).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "requires live services; run via make snapshot-test-*"]
    async fn sqlite_s11b_one_huge_row() {
        let dest_dir = tempfile::tempdir().expect("destination temp dir");
        let target = SqliteTarget::new(dest_dir.path());
        snapshot_scenarios::s11_memory_bound(&target, MemoryCase::OneHugeRow).await;
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlite"]
async fn bench_snapshot_throughput_sqlite() {
    let dest_dir = tempfile::tempdir().expect("destination temp dir");
    snapshot_scenarios::bench_grid(&SqliteTarget::new(dest_dir.path())).await;
}

/// Where the live-path benchmarks keep the SQLite file: `PG2ANY_BENCH_SQLITE_DIR` (for instance a
/// tmpfs, to take the destination's fsync out of a run) or the default temp dir, which is also
/// where the client keeps its transaction files (`TMPDIR`).
#[cfg(target_os = "linux")]
fn bench_dest_dir() -> tempfile::TempDir {
    match std::env::var("PG2ANY_BENCH_SQLITE_DIR") {
        Ok(dir) => tempfile::tempdir_in(dir).expect("destination temp dir"),
        Err(_) => tempfile::tempdir().expect("destination temp dir"),
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlite"]
async fn bench_live_throughput_sqlite() {
    let dest_dir = bench_dest_dir();
    snapshot_scenarios::bench_live_grid(&SqliteTarget::new(dest_dir.path())).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlite"]
async fn bench_live_latency_sqlite() {
    let dest_dir = bench_dest_dir();
    let l = snapshot_scenarios::bench_live_latency(&SqliteTarget::new(dest_dir.path()), 100).await;
    println!(
        "live latency, idle pipeline, commit to visible: {} single-row transactions, min {:.1} ms, p50 {:.1} ms, p95 {:.1} ms, max {:.1} ms",
        l.samples, l.min_ms, l.p50_ms, l.p95_ms, l.max_ms
    );
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlite"]
async fn bench_live_drain_sqlite() {
    let dest_dir = bench_dest_dir();
    snapshot_scenarios::bench_live_drain_grid(&SqliteTarget::new(dest_dir.path())).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "benchmark: run via make snapshot-bench-sqlite"]
async fn bench_snapshot_reference_sqlite() {
    let dest_dir = tempfile::tempdir().expect("destination temp dir");
    snapshot_scenarios::bench_snapshot_reference(&SqliteTarget::new(dest_dir.path())).await;
}
