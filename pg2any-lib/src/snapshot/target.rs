//! Snapshot target-table preparation: emptiness probe and truncate, run before any row is copied.

use crate::config::SnapshotTargetPolicy;
use crate::destinations::destination_factory::DestinationHandler;
use crate::error::{CdcError, Result};
use tracing::info;

/// `tables` are (destination schema, table). Every table is probed first, under
/// every policy, so a missing or unreadable table fails before anything is
/// truncated or copied; all problems are reported in one error. Truncation only
/// starts once every probe came back clean.
pub(crate) async fn prepare_targets(
    handler: &mut dyn DestinationHandler,
    tables: &[(String, String)],
    policy: SnapshotTargetPolicy,
) -> Result<()> {
    let fail_if_not_empty = policy == SnapshotTargetPolicy::FailIfNotEmpty;
    let mut problems = Vec::new();
    for (schema, table) in tables {
        match handler.snapshot_table_is_empty(schema, table).await {
            Ok(false) if fail_if_not_empty => problems.push(format!(
                "destination table {schema}.{table} already contains rows; set CDC_SNAPSHOT_TARGET_POLICY=truncate to clear it or CDC_SNAPSHOT_TARGET_POLICY=append to keep its rows"
            )),
            Ok(_) => {}
            Err(CdcError::Unsupported(msg)) if fail_if_not_empty => {
                return Err(CdcError::unsupported(format!(
                    "{msg}; set CDC_SNAPSHOT_TARGET_POLICY=truncate or CDC_SNAPSHOT_TARGET_POLICY=append instead"
                )));
            }
            // Unknown emptiness is not an error when the policy never needs it.
            Err(CdcError::Unsupported(_)) => {}
            Err(e) => problems.push(format!(
                "destination table {schema}.{table} does not exist or is not readable: {e}"
            )),
        }
    }
    if !problems.is_empty() {
        return Err(CdcError::config(problems.join("; ")));
    }

    if policy == SnapshotTargetPolicy::Truncate {
        for (schema, table) in tables {
            info!("truncating destination table {schema}.{table} before the snapshot copy");
            handler
                .snapshot_truncate_table(schema, table)
                .await
                .map_err(|e| {
                    CdcError::generic(format!(
                        "failed to truncate destination table {schema}.{table}: {e}"
                    ))
                })?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::destination_factory::PreCommitHook;
    use crate::destinations::dialect::SqlDialect;
    use crate::destinations::dialects::{AnsiDialect, KafkaDialect};
    use crate::types::{ChangeEvent, Lsn};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use std::collections::{HashMap, HashSet};

    /// Overrides both snapshot hooks and records every call as `"probe s.t"` / `"truncate s.t"`.
    #[derive(Default)]
    struct Recording {
        calls: Vec<String>,
        non_empty: HashSet<String>,
        probe_fails: HashSet<String>,
        truncate_fails: HashSet<String>,
    }

    #[async_trait]
    impl DestinationHandler for Recording {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }
        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}
        async fn execute_sql_batch_with_hook(
            &mut self,
            _commands: &[String],
            _pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }

        async fn snapshot_table_is_empty(&mut self, schema: &str, table: &str) -> Result<bool> {
            let name = format!("{schema}.{table}");
            self.calls.push(format!("probe {name}"));
            if self.probe_fails.contains(&name) {
                return Err(CdcError::generic("boom: no such table"));
            }
            Ok(!self.non_empty.contains(&name))
        }

        async fn snapshot_truncate_table(&mut self, schema: &str, table: &str) -> Result<()> {
            let name = format!("{schema}.{table}");
            self.calls.push(format!("truncate {name}"));
            if self.truncate_fails.contains(&name) {
                return Err(CdcError::generic("boom: permission denied"));
            }
            Ok(())
        }
    }

    /// Overrides nothing snapshot-related, so the trait defaults are exercised.
    #[derive(Default)]
    struct Plain {
        kafka: bool,
        sqls: Vec<String>,
        event_batches: Vec<(u32, Option<Lsn>)>,
    }

    #[async_trait]
    impl DestinationHandler for Plain {
        async fn connect(&mut self, _connection_string: &str) -> Result<()> {
            Ok(())
        }
        fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}
        fn dialect(&self) -> &'static dyn SqlDialect {
            static ANSI: AnsiDialect = AnsiDialect;
            static KAFKA: KafkaDialect = KafkaDialect;
            if self.kafka {
                &KAFKA
            } else {
                &ANSI
            }
        }
        async fn execute_sql_batch_with_hook(
            &mut self,
            commands: &[String],
            _pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            self.sqls.extend_from_slice(commands);
            Ok(())
        }
        async fn close(&mut self) -> Result<()> {
            Ok(())
        }
        async fn execute_events_batch_with_hook(
            &mut self,
            _events: &[ChangeEvent],
            transaction_id: u32,
            _commit_timestamp: DateTime<Utc>,
            commit_lsn: Option<Lsn>,
            _pre_commit_hook: Option<PreCommitHook>,
        ) -> Result<()> {
            self.event_batches.push((transaction_id, commit_lsn));
            Ok(())
        }
    }

    fn tables(names: &[(&str, &str)]) -> Vec<(String, String)> {
        names
            .iter()
            .map(|(s, t)| (s.to_string(), t.to_string()))
            .collect()
    }

    async fn run(
        h: &mut dyn DestinationHandler,
        names: &[(&str, &str)],
        policy: SnapshotTargetPolicy,
    ) -> Result<()> {
        prepare_targets(h, &tables(names), policy).await
    }

    #[tokio::test]
    async fn fail_if_not_empty_probes_every_table_and_names_every_non_empty_one() {
        let mut h = Recording::default();
        h.non_empty.insert("b.t2".into());
        h.non_empty.insert("c.t3".into());
        let err = run(
            &mut h,
            &[("a", "t1"), ("b", "t2"), ("c", "t3")],
            SnapshotTargetPolicy::FailIfNotEmpty,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "{err:?}");
        let msg = err.to_string();
        let (b, c) = (msg.find("b.t2").unwrap(), msg.find("c.t3").unwrap());
        assert!(b < c, "{msg}");
        assert!(!msg.contains("a.t1"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=append"), "{msg}");
        assert_eq!(h.calls, ["probe a.t1", "probe b.t2", "probe c.t3"]);
    }

    #[tokio::test]
    async fn fail_if_not_empty_probes_every_table_when_all_empty() {
        let mut h = Recording::default();
        run(
            &mut h,
            &[("a", "t1"), ("b", "t2")],
            SnapshotTargetPolicy::FailIfNotEmpty,
        )
        .await
        .unwrap();
        assert_eq!(h.calls, ["probe a.t1", "probe b.t2"]);
    }

    #[tokio::test]
    async fn fail_if_not_empty_probe_error_says_table_missing_or_unreadable_and_names_it() {
        let mut h = Recording::default();
        h.probe_fails.insert("s.t".into());
        let err = run(&mut h, &[("s", "t")], SnapshotTargetPolicy::FailIfNotEmpty)
            .await
            .unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "{err:?}");
        let msg = err.to_string();
        assert!(msg.contains("s.t"), "{msg}");
        assert!(msg.contains("does not exist or is not readable"), "{msg}");
        assert!(msg.contains("boom: no such table"), "{msg}");
    }

    #[tokio::test]
    async fn missing_table_fails_before_any_truncate_under_truncate() {
        let mut h = Recording::default();
        h.probe_fails.insert("b.t2".into());
        let err = run(
            &mut h,
            &[("a", "t1"), ("b", "t2"), ("c", "t3")],
            SnapshotTargetPolicy::Truncate,
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "{err:?}");
        assert!(err.to_string().contains("b.t2"), "{err}");
        assert_eq!(h.calls, ["probe a.t1", "probe b.t2", "probe c.t3"]);
    }

    #[tokio::test]
    async fn missing_table_fails_under_append() {
        let mut h = Recording::default();
        h.probe_fails.insert("a.t1".into());
        let err = run(&mut h, &[("a", "t1")], SnapshotTargetPolicy::Append)
            .await
            .unwrap_err();
        assert!(matches!(err, CdcError::Config(_)), "{err:?}");
        assert!(err.to_string().contains("a.t1"), "{err}");
    }

    #[tokio::test]
    async fn every_problem_is_named_in_one_error_in_table_order() {
        let mut h = Recording::default();
        h.probe_fails.insert("a.t1".into());
        h.non_empty.insert("c.t3".into());
        let err = run(
            &mut h,
            &[("a", "t1"), ("b", "t2"), ("c", "t3")],
            SnapshotTargetPolicy::FailIfNotEmpty,
        )
        .await
        .unwrap_err();
        let msg = err.to_string();
        let (a, c) = (msg.find("a.t1").unwrap(), msg.find("c.t3").unwrap());
        assert!(a < c, "{msg}");
        assert!(msg.contains("does not exist or is not readable"), "{msg}");
        assert!(msg.contains("already contains rows"), "{msg}");
        assert!(!msg.contains("b.t2"), "{msg}");
    }

    #[tokio::test]
    async fn truncate_probes_all_tables_then_truncates_each_with_mapped_schema() {
        let mut h = Recording::default();
        h.non_empty.insert("cdc_db.users".into());
        run(
            &mut h,
            &[("cdc_db", "users"), ("cdc_db", "orders")],
            SnapshotTargetPolicy::Truncate,
        )
        .await
        .unwrap();
        assert_eq!(
            h.calls,
            [
                "probe cdc_db.users",
                "probe cdc_db.orders",
                "truncate cdc_db.users",
                "truncate cdc_db.orders"
            ]
        );
    }

    #[tokio::test]
    async fn truncate_error_names_the_table() {
        let mut h = Recording::default();
        h.truncate_fails.insert("a.t1".into());
        let err = run(&mut h, &[("a", "t1")], SnapshotTargetPolicy::Truncate)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to truncate destination table a.t1"),
            "{msg}"
        );
        assert!(msg.contains("boom: permission denied"), "{msg}");
    }

    #[tokio::test]
    async fn append_probes_but_ignores_non_empty_tables_and_never_truncates() {
        let mut h = Recording::default();
        h.non_empty.insert("a.t1".into());
        run(&mut h, &[("a", "t1")], SnapshotTargetPolicy::Append)
            .await
            .unwrap();
        assert_eq!(h.calls, ["probe a.t1"]);
    }

    #[tokio::test]
    async fn unsupported_probe_passes_through_unwrapped_under_fail_if_not_empty() {
        let mut h = Plain::default();
        let err = run(&mut h, &[("s", "t")], SnapshotTargetPolicy::FailIfNotEmpty)
            .await
            .unwrap_err();
        assert!(matches!(err, CdcError::Unsupported(_)), "{err:?}");
        let msg = err.to_string();
        assert!(
            msg.contains("destination does not support snapshot target checks"),
            "{msg}"
        );
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=append"), "{msg}");
    }

    #[tokio::test]
    async fn unsupported_probe_is_tolerated_under_truncate_and_append() {
        let mut h = Plain::default();
        run(
            &mut h,
            &[("s", "t1"), ("s", "t2")],
            SnapshotTargetPolicy::Truncate,
        )
        .await
        .unwrap();
        assert_eq!(
            h.sqls,
            [
                "TRUNCATE TABLE \"s\".\"t1\";",
                "TRUNCATE TABLE \"s\".\"t2\";"
            ]
        );

        let mut h = Plain::default();
        run(&mut h, &[("s", "t1")], SnapshotTargetPolicy::Append)
            .await
            .unwrap();
        assert!(h.sqls.is_empty());
    }

    #[tokio::test]
    async fn default_truncate_with_no_dialect_support_is_ok() {
        let mut h = Plain {
            kafka: true,
            ..Default::default()
        };
        h.snapshot_truncate_table("s", "t").await.unwrap();
        assert!(h.sqls.is_empty());
    }

    #[tokio::test]
    async fn default_truncate_with_sql_dialect_issues_one_statement() {
        let mut h = Plain::default();
        h.snapshot_truncate_table("s", "t").await.unwrap();
        assert_eq!(h.sqls, ["TRUNCATE TABLE \"s\".\"t\";"]);
    }

    #[tokio::test]
    async fn default_snapshot_table_is_empty_is_unsupported() {
        let mut h = Plain::default();
        let err = h.snapshot_table_is_empty("s", "t").await.unwrap_err();
        assert!(matches!(err, CdcError::Unsupported(_)), "{err:?}");
    }

    #[tokio::test]
    async fn default_snapshot_events_batch_uses_consistent_point_as_commit_lsn() {
        let mut h = Plain::default();
        h.execute_snapshot_events_batch(&[], Lsn(42)).await.unwrap();
        assert_eq!(h.event_batches, [(0, Some(Lsn(42)))]);
    }

    #[test]
    fn default_max_snapshot_writers_is_unbounded() {
        assert_eq!(Plain::default().max_snapshot_writers(), usize::MAX);
    }
}
