//! Slot-first LSN recovery.
//!
//! On startup we query `pg_replication_slots` for the slot's server-side
//! position (`confirmed_flush_lsn`) and reconcile it with the on-disk LSN.
//! For logical replication PostgreSQL resumes from `confirmed_flush_lsn`
//! regardless of the `start_lsn` hint, so the **de-dup boundary** — the LSN at
//! or below which committed transactions are skipped — is the real correctness
//! lever. We set it to `max(confirmed_flush_lsn, disk)`.

use crate::error::{CdcError, Result};
use pg_walstream::{parse_lsn, PgReplicationConnection};
use std::time::Duration;
use tracing::debug;

/// LSN positions read from a row of `pg_replication_slots`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SlotStatus {
    /// `confirmed_flush_lsn`: where logical decoding resumes. `None` if NULL.
    pub(crate) confirmed_flush_lsn: Option<u64>,
    /// `restart_lsn`: physical WAL retention floor. Diagnostic only. `None` if NULL.
    pub(crate) restart_lsn: Option<u64>,
    /// `active`: whether another connection currently holds the slot.
    pub(crate) active: bool,
}

/// Where the resume position came from (for logging).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResumeSource {
    /// Slot found; resumed from its `confirmed_flush_lsn`.
    Slot,
    /// Slot not found but an on-disk LSN exists; resumed from disk (possible gap).
    SlotDeletedFallback,
    /// Slot query failed (transient) but an on-disk LSN exists; resumed from disk (safe: slot likely still exists).
    QueryFailedFallback,
    /// No slot and no on-disk LSN; fresh start from the current WAL position.
    Fresh,
}

/// The reconciled resume decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResumeDecision {
    /// `START_REPLICATION` start hint. `Some` only when read from a live slot
    /// (its `confirmed_flush_lsn`); `None` otherwise (→ `0/0`, let PG decide).
    /// For logical replication PG resumes from `confirmed_flush_lsn` server-side
    /// regardless, so this is a hint only — `dedup_boundary` is the real lever.
    pub(crate) start_lsn: Option<u64>,
    /// Skip committed transactions with `commit_lsn <= dedup_boundary`.
    pub(crate) dedup_boundary: u64,
    pub(crate) source: ResumeSource,
}

/// Reconcile the on-disk LSN with the slot's LSNs. Pure — no I/O.
///
/// `start_lsn` is only ever `Some` when read from a live slot; every fallback
/// leaves it `None` (→ `0/0`) so PG picks the position itself. `dedup_boundary`
/// is what actually guarantees no replay.
///
/// - Slot present: `start_lsn = confirmed_flush_lsn`; boundary = `max(disk, confirmed)`.
/// - Slot absent (`Ok(None)`), disk present: boundary = disk (slot-deleted fallback, possible gap).
/// - Query failed (`Err`), disk present: boundary = disk (safe fallback, slot likely still exists).
/// - Both absent: fresh start.
pub(crate) fn reconcile_resume(
    disk: Option<u64>,
    slot: std::result::Result<Option<&SlotStatus>, ()>,
) -> ResumeDecision {
    match slot {
        Ok(Some(s)) => ResumeDecision {
            start_lsn: s.confirmed_flush_lsn,
            dedup_boundary: disk.unwrap_or(0).max(s.confirmed_flush_lsn.unwrap_or(0)),
            source: ResumeSource::Slot,
        },
        Ok(None) => match disk {
            Some(d) => ResumeDecision {
                start_lsn: None,
                dedup_boundary: d,
                source: ResumeSource::SlotDeletedFallback,
            },
            None => ResumeDecision {
                start_lsn: None,
                dedup_boundary: 0,
                source: ResumeSource::Fresh,
            },
        },
        Err(()) => match disk {
            Some(d) => ResumeDecision {
                start_lsn: None,
                dedup_boundary: d,
                source: ResumeSource::QueryFailedFallback,
            },
            None => ResumeDecision {
                start_lsn: None,
                dedup_boundary: 0,
                source: ResumeSource::Fresh,
            },
        },
    }
}

/// Validate a PostgreSQL replication slot name.
///
/// PostgreSQL restricts slot names to lower-case letters, digits, and  underscores (`[a-z0-9_]`). Validating against that set is the reusable, injection-proof way to make a slot name safe for SQL interpolation stronger than escaping, which is fragile when `standard_conforming_strings` is off. Call this at every boundary where a slot name enters a query.
pub(crate) fn validate_slot_name(slot_name: &str) -> Result<()> {
    if slot_name.is_empty() {
        return Err(CdcError::config("Replication slot name is required"));
    }
    if !slot_name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    {
        return Err(CdcError::config(format!(
            "Invalid replication slot name {slot_name:?}: only lower-case letters, digits, and underscores are allowed"
        )));
    }
    Ok(())
}

/// Build the `pg_replication_slots` lookup. The name is validated to `[a-z0-9_]`
/// by [`validate_slot_name`] before we get here, so interpolation is safe; the
/// single-quote escape stays as belt-and-suspenders.
fn build_slot_query(slot_name: &str) -> String {
    let escaped = slot_name.replace('\'', "''");
    format!(
        "SELECT restart_lsn, confirmed_flush_lsn, active \
         FROM pg_replication_slots WHERE slot_name = '{escaped}'"
    )
}

/// Parse an LSN text field from `pg_replication_slots` (e.g. `"16/B374D848"`).
/// Trims surrounding whitespace; NULL / empty / unparsable ⇒ `None`.
fn parse_lsn_field(value: Option<String>) -> Option<u64> {
    let v = value?;
    let t = v.trim();
    if t.is_empty() {
        return None;
    }
    parse_lsn(t).ok()
}

/// Parse the boolean `active` text field. PostgreSQL renders booleans as
/// `t`/`f`; we also accept `true`/`1`. NULL / anything else ⇒ `false`.
fn parse_active_field(value: Option<String>) -> bool {
    value
        .map(|s| matches!(s.trim(), "t" | "true" | "1"))
        .unwrap_or(false)
}

/// Blocking query. Returns `Ok(None)` when the slot does not exist.
/// The connection and result are dropped (RAII) before returning — no leak.
fn query_slot_blocking(conninfo: &str, slot_name: &str) -> Result<Option<SlotStatus>> {
    let mut conn = PgReplicationConnection::connect(conninfo)?;
    let result = conn.exec(&build_slot_query(slot_name))?;

    if result.ntuples() == 0 {
        return Ok(None);
    }

    Ok(Some(SlotStatus {
        restart_lsn: parse_lsn_field(result.get_value(0, 0)),
        confirmed_flush_lsn: parse_lsn_field(result.get_value(0, 1)),
        active: parse_active_field(result.get_value(0, 2)),
    }))
}

/// Query `pg_replication_slots` for `slot_name` on a short-lived connection.
///
/// Runs the blocking libpq call on a blocking thread, bounded by `timeout`.
/// `Ok(None)` = slot not found. `Err` = connect/query failed or timed out; the
/// caller should fall back to the on-disk LSN.
///
/// On timeout the blocking thread is left to finish and drop on its own — the
/// blocking `connect`/`exec` cannot be cancelled mid-call.
// ponytail: timeout stops us waiting, not the blocking call; acceptable for a one-shot startup probe.
pub(crate) async fn query_replication_slot(
    conninfo: &str,
    slot_name: &str,
    timeout: Duration,
) -> Result<Option<SlotStatus>> {
    validate_slot_name(slot_name)?;
    let conninfo = conninfo.to_string();
    let slot_name = slot_name.to_string();
    debug!("Querying pg_replication_slots for slot '{}'", slot_name);

    let handle = tokio::task::spawn_blocking(move || query_slot_blocking(&conninfo, &slot_name));

    match tokio::time::timeout(timeout, handle).await {
        Ok(Ok(res)) => res,
        Ok(Err(join_err)) => Err(CdcError::generic(format!(
            "replication slot query task failed: {join_err}"
        ))),
        Err(_elapsed) => Err(CdcError::connection(format!(
            "replication slot query timed out after {timeout:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot(confirmed: Option<u64>, restart: Option<u64>, active: bool) -> SlotStatus {
        SlotStatus {
            confirmed_flush_lsn: confirmed,
            restart_lsn: restart,
            active,
        }
    }

    #[test]
    fn slot_present_disk_behind_boundary_is_slot() {
        // confirmed > disk (rare crash window): boundary aligns up to slot.
        let d = reconcile_resume(Some(100), Ok(Some(&slot(Some(500), Some(400), false))));
        assert_eq!(d.start_lsn, Some(500));
        assert_eq!(d.dedup_boundary, 500);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_present_disk_ahead_boundary_is_disk() {
        // confirmed < disk (normal steady state): boundary stays at disk.
        let d = reconcile_resume(Some(900), Ok(Some(&slot(Some(500), Some(400), false))));
        assert_eq!(d.start_lsn, Some(500));
        assert_eq!(d.dedup_boundary, 900);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_present_disk_equal() {
        let d = reconcile_resume(Some(500), Ok(Some(&slot(Some(500), Some(400), false))));
        assert_eq!(d.dedup_boundary, 500);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_present_disk_missing() {
        let d = reconcile_resume(None, Ok(Some(&slot(Some(500), Some(400), false))));
        assert_eq!(d.start_lsn, Some(500));
        assert_eq!(d.dedup_boundary, 500);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_present_confirmed_null() {
        // confirmed_flush NULL -> start_lsn None; boundary falls back to disk.
        let d = reconcile_resume(Some(700), Ok(Some(&slot(None, Some(400), false))));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 700);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_present_confirmed_null_disk_missing() {
        // confirmed NULL and no disk -> boundary 0, start hint None (PG picks position).
        let d = reconcile_resume(None, Ok(Some(&slot(None, None, false))));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 0);
        assert_eq!(d.source, ResumeSource::Slot);
    }

    #[test]
    fn slot_absent_disk_present_is_fallback() {
        // start_lsn None: a recreated slot starts at the current WAL position,
        // so we don't hand PG the stale on-disk LSN; boundary stays at disk.
        let d = reconcile_resume(Some(700), Ok(None));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 700);
        assert_eq!(d.source, ResumeSource::SlotDeletedFallback);
    }

    #[test]
    fn query_failed_disk_present_is_fallback() {
        // Transient query failure with an on-disk LSN: safe fallback, NOT a slot-deleted gap.
        let d = reconcile_resume(Some(700), Err(()));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 700);
        assert_eq!(d.source, ResumeSource::QueryFailedFallback);
    }

    #[test]
    fn query_failed_disk_missing_is_fresh() {
        // Query failure with nothing on disk: nowhere to resume from -> fresh.
        let d = reconcile_resume(None, Err(()));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 0);
        assert_eq!(d.source, ResumeSource::Fresh);
    }

    #[test]
    fn both_absent_is_fresh() {
        let d = reconcile_resume(None, Ok(None));
        assert_eq!(d.start_lsn, None);
        assert_eq!(d.dedup_boundary, 0);
        assert_eq!(d.source, ResumeSource::Fresh);
    }

    #[test]
    fn lsn_text_parses_like_postgres() {
        // Sanity: the pg_lsn text format we will read from the slot.
        assert_eq!(pg_walstream::parse_lsn("0/0").unwrap(), 0);
        assert_eq!(
            pg_walstream::parse_lsn("16/B374D848").unwrap(),
            0x16B374D848
        );
        assert!(pg_walstream::parse_lsn("not-an-lsn").is_err());
    }

    #[test]
    fn validate_slot_name_accepts_legal_names() {
        assert!(validate_slot_name("pg2any_slot").is_ok());
        assert!(validate_slot_name("slot_123").is_ok());
        assert!(validate_slot_name("a").is_ok());
    }

    #[test]
    fn validate_slot_name_rejects_illegal_names() {
        // Empty, injection payload, and every out-of-charset class must be rejected
        // BEFORE the name ever reaches SQL interpolation.
        assert!(validate_slot_name("").is_err()); // empty
        assert!(validate_slot_name("a'b").is_err()); // single quote (injection)
        assert!(validate_slot_name("slot; DROP TABLE t").is_err()); // classic payload
        assert!(validate_slot_name("MySlot").is_err()); // uppercase
        assert!(validate_slot_name("my-slot").is_err()); // hyphen
        assert!(validate_slot_name("my slot").is_err()); // space
        assert!(validate_slot_name("slot\\").is_err()); // backslash (SCS-off vector)
    }

    #[test]
    fn build_query_contains_slot_and_columns() {
        let sql = build_slot_query("pg2any_slot");
        assert!(sql.contains("pg_replication_slots"));
        assert!(sql.contains("restart_lsn"));
        assert!(sql.contains("confirmed_flush_lsn"));
        assert!(sql.contains("slot_name = 'pg2any_slot'"));
    }

    #[test]
    fn build_query_escapes_single_quote() {
        // Defensive: PG slot names are [a-z0-9_], but never interpolate raw.
        let sql = build_slot_query("a'b");
        assert!(sql.contains("slot_name = 'a''b'"));
    }

    #[test]
    fn parse_lsn_field_valid_values() {
        assert_eq!(parse_lsn_field(Some("0/0".to_string())), Some(0));
        assert_eq!(
            parse_lsn_field(Some("16/B374D848".to_string())),
            Some(0x16B374D848)
        );
        // Surrounding whitespace is trimmed.
        assert_eq!(
            parse_lsn_field(Some("  16/B374D848  ".to_string())),
            Some(0x16B374D848)
        );
    }

    #[test]
    fn parse_lsn_field_null_empty_invalid_are_none() {
        assert_eq!(parse_lsn_field(None), None); // NULL column
        assert_eq!(parse_lsn_field(Some(String::new())), None); // empty
        assert_eq!(parse_lsn_field(Some("   ".to_string())), None); // whitespace only
        assert_eq!(parse_lsn_field(Some("garbage".to_string())), None); // no slash
        assert_eq!(parse_lsn_field(Some("16".to_string())), None); // missing '/'
        assert_eq!(parse_lsn_field(Some("zz/zz".to_string())), None); // bad hex
    }

    #[test]
    fn parse_active_field_true_values() {
        assert!(parse_active_field(Some("t".to_string())));
        assert!(parse_active_field(Some("true".to_string())));
        assert!(parse_active_field(Some("1".to_string())));
        assert!(parse_active_field(Some("  t  ".to_string()))); // trimmed
    }

    #[test]
    fn parse_active_field_false_values() {
        assert!(!parse_active_field(Some("f".to_string())));
        assert!(!parse_active_field(Some("false".to_string())));
        assert!(!parse_active_field(Some("0".to_string())));
        assert!(!parse_active_field(Some("yes".to_string()))); // not a recognized truthy token
        assert!(!parse_active_field(None)); // NULL column
        assert!(!parse_active_field(Some(String::new())));
    }

    #[tokio::test]
    async fn query_replication_slot_rejects_invalid_name() {
        // An illegal slot name must be rejected up front — before any connection
        // and before it can reach SQL interpolation. Uses an unroutable host to
        // prove we never even attempt to connect.
        let dsn = "host=192.0.2.1 port=5432 dbname=postgres user=postgres";
        let res = query_replication_slot(dsn, "bad'; DROP", Duration::from_secs(5)).await;
        assert!(
            res.is_err(),
            "invalid slot name must be rejected, got {res:?}"
        );
    }

    #[tokio::test]
    async fn query_replication_slot_connect_failure_is_err() {
        // Nothing listening on port 1 ⇒ connect refused fast ⇒ Err.
        // This is the path that makes the caller fall back to the on-disk LSN;
        // it must return Err rather than panic or hang.
        let dsn = "host=127.0.0.1 port=1 dbname=postgres user=postgres connect_timeout=1";
        let res = query_replication_slot(dsn, "any_slot", Duration::from_secs(5)).await;
        assert!(
            res.is_err(),
            "connect failure should map to Err (fallback trigger), got {res:?}"
        );
    }

    #[tokio::test]
    async fn query_replication_slot_unreachable_host_is_err() {
        let dsn = "host=192.0.2.1 port=5432 dbname=postgres user=postgres";
        let res = query_replication_slot(dsn, "any_slot", Duration::from_millis(200)).await;
        assert!(
            res.is_err(),
            "unreachable host should map to Err (fallback trigger), got {res:?}"
        );
    }

    // Live-Postgres roundtrip. Gated on `PG2ANY_TEST_SOURCE_DSN` (a
    // `replication=database` DSN); returns early (passes) when unset so the
    // suite stays green without a database. Kept in-crate so `slot` can remain
    // `pub(crate)` rather than exposing internals as public API just for a test.
    const IT_SLOT: &str = "pg2any_it_slot_lsn_recovery";

    #[tokio::test]
    async fn query_replication_slot_roundtrip_live() {
        let Ok(dsn) = std::env::var("PG2ANY_TEST_SOURCE_DSN") else {
            eprintln!("PG2ANY_TEST_SOURCE_DSN not set; skipping live slot roundtrip test");
            return;
        };

        // Setup: drop any leftover, then create a fresh logical slot.
        let d = dsn.clone();
        tokio::task::spawn_blocking(move || {
            if let Ok(mut c) = PgReplicationConnection::connect(&d) {
                let _ = c.exec(&format!("SELECT pg_drop_replication_slot('{IT_SLOT}')"));
            }
            let mut c = PgReplicationConnection::connect(&d).expect("connect for setup");
            c.exec(&format!(
                "SELECT pg_create_logical_replication_slot('{IT_SLOT}', 'pgoutput')"
            ))
            .expect("create logical slot");
        })
        .await
        .expect("setup task");

        // Slot exists: query returns Some, and all three columns map correctly.
        let status = query_replication_slot(&dsn, IT_SLOT, Duration::from_secs(10))
            .await
            .expect("query should succeed")
            .expect("slot should be found");
        assert!(
            status.confirmed_flush_lsn.is_some(),
            "confirmed_flush_lsn should be present for a fresh logical slot"
        );
        // Guard the column-index mapping of the other two columns: a fresh
        // pgoutput slot has a non-null restart_lsn and, since no one is
        // streaming it, is not active. (If restart_lsn/confirmed_flush_lsn were
        // swapped, is_some() alone would not catch it.)
        assert!(
            status.restart_lsn.is_some(),
            "restart_lsn should be present for a fresh logical slot"
        );
        assert!(
            !status.active,
            "a freshly created, unstreamed slot should not be active"
        );

        // reconcile: with the slot present, we resume from the slot.
        let decision = reconcile_resume(Some(1), Ok(Some(&status)));
        assert_eq!(decision.source, ResumeSource::Slot);
        assert_eq!(
            decision.dedup_boundary,
            status.confirmed_flush_lsn.unwrap().max(1)
        );

        // Teardown, then assert the slot-not-found path returns Ok(None).
        let d = dsn.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = PgReplicationConnection::connect(&d).expect("connect for teardown");
            c.exec(&format!("SELECT pg_drop_replication_slot('{IT_SLOT}')"))
                .expect("drop slot");
        })
        .await
        .expect("teardown task");

        let gone = query_replication_slot(&dsn, IT_SLOT, Duration::from_secs(10))
            .await
            .expect("query should succeed");
        assert!(gone.is_none(), "slot should be gone after drop");
    }
}
