//! Pure startup decision: run, skip, re-sync, recover or refuse the initial snapshot.

use super::marker::SnapshotMarker;
use crate::config::SnapshotTargetPolicy;
use crate::error::{CdcError, Result};
use crate::slot::SlotStatus;

pub(crate) struct DecisionInput<'a> {
    pub enabled: bool,
    pub policy: SnapshotTargetPolicy,
    pub slot_name: &'a str,
    /// The configured destination without its password, compared with the marker's.
    pub destination: &'a str,
    pub marker: Option<&'a SnapshotMarker>,
    /// `Err` = the slot query failed.
    pub slot: std::result::Result<Option<&'a SlotStatus>, ()>,
    /// An on-disk LSN exists, i.e. the destination already holds replicated data.
    pub has_disk_lsn: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SnapshotAction {
    Skip,
    Run,
    /// Clear the `Completed` marker, then run with truncate. `drop_slot`: a lost slot still
    /// exists on the server and must be dropped first, or `snapshot()` reports `Unavailable`.
    Resync {
        drop_slot: bool,
    },
    /// Clean our own `InProgress` leftovers (optionally dropping the slot), then run.
    Recover {
        drop_slot: bool,
    },
    Fail(String),
}

impl SnapshotAction {
    pub(crate) fn into_result(self) -> Result<Option<SnapshotAction>> {
        match self {
            SnapshotAction::Skip => Ok(None),
            SnapshotAction::Fail(msg) => Err(CdcError::config(msg)),
            action => Ok(Some(action)),
        }
    }
}

pub(crate) fn decide(input: &DecisionInput) -> SnapshotAction {
    use SnapshotAction::*;
    let name = input.slot_name;

    if !input.enabled {
        return match input.marker {
            Some(SnapshotMarker::InProgress { .. }) => Fail(
                "an incomplete initial snapshot exists but CDC_INITIAL_SNAPSHOT is not enabled; \
                 set CDC_INITIAL_SNAPSHOT=true to clean up and finish it, or clean the \
                 destination tables, drop the replication slot and remove the snapshot marker file"
                    .into(),
            ),
            _ => Skip,
        };
    }

    if let Some(
        SnapshotMarker::Completed { slot_name, .. } | SnapshotMarker::InProgress { slot_name, .. },
    ) = input.marker
    {
        if slot_name != name {
            return Fail(format!(
                "the snapshot marker belongs to slot '{slot_name}', but the configured slot is \
                 '{name}'; restore the slot name '{slot_name}', or remove the snapshot marker \
                 file after cleaning the destination tables"
            ));
        }
    }

    // Recovery empties the tables the marker lists: never in a destination it was not written for.
    if let Some(SnapshotMarker::InProgress {
        destination: Some(written_to),
        ..
    }) = input.marker
    {
        if written_to != input.destination {
            return Fail(format!(
                "the interrupted initial snapshot wrote to destination '{written_to}', but the \
                 configured destination is '{}'; recovery empties the tables the marker lists, so \
                 it is not run against another destination. Restore the destination, or clean \
                 the tables of the old one, drop the replication slot and remove the snapshot \
                 marker file",
                input.destination
            ));
        }
    }

    match (input.marker, input.slot) {
        (Some(SnapshotMarker::Completed { .. }), Err(())) => Skip,
        (Some(SnapshotMarker::Completed { .. }), Ok(Some(s))) if !s.wal_lost => Skip,
        (Some(SnapshotMarker::Completed { .. }), Ok(slot)) => {
            if input.policy == SnapshotTargetPolicy::Truncate {
                Resync {
                    drop_slot: slot.is_some(),
                }
            } else {
                Fail(format!(
                    "the snapshot marker exists but replication slot '{name}' is gone or \
                     invalidated, so WAL changes since the snapshot may be lost; set \
                     CDC_SNAPSHOT_TARGET_POLICY=truncate (with CDC_INITIAL_SNAPSHOT=true) to \
                     re-sync the destination from scratch"
                ))
            }
        }
        // Before the first row was dispatched nothing was copied, so there is nothing to clean up.
        (
            Some(SnapshotMarker::InProgress {
                copy_started: true, ..
            }),
            _,
        ) if input.policy == SnapshotTargetPolicy::Append => Fail(format!(
            "an incomplete initial snapshot exists for slot '{name}', but automatic cleanup \
                 is not done under CDC_SNAPSHOT_TARGET_POLICY=append; clean the destination \
                 tables by hand, drop the slot if it exists and remove the snapshot marker file"
        )),
        (Some(SnapshotMarker::InProgress { .. }), Err(())) => Fail(format!(
            "an incomplete initial snapshot exists for slot '{name}' but the replication slot \
             could not be queried; fix the source connection and restart"
        )),
        (Some(SnapshotMarker::InProgress { .. }), Ok(None)) => Recover { drop_slot: false },
        (Some(SnapshotMarker::InProgress { .. }), Ok(Some(s))) if s.active => Fail(format!(
            "an incomplete initial snapshot exists but slot '{name}' is active (held by another \
             connection); stop the other consumer, then restart"
        )),
        (
            Some(SnapshotMarker::InProgress {
                consistent_point, ..
            }),
            Ok(Some(s)),
        ) if consistent_point.is_some() && s.confirmed_flush_lsn != *consistent_point => {
            Fail(format!(
                "slot '{name}' exists but its confirmed_flush_lsn does not match the incomplete \
                 snapshot's consistent point, so it was not created by this snapshot; drop the \
                 slot manually, or remove the snapshot marker file after cleaning the \
                 destination tables"
            ))
        }
        (Some(SnapshotMarker::InProgress { .. }), Ok(Some(_))) => Recover { drop_slot: true },
        (None, Err(())) => Fail(format!(
            "could not query replication slot '{name}' to decide whether to run the initial \
             snapshot; fix the source connection and restart"
        )),
        (None, Ok(Some(_))) => Fail(format!(
            "a replication slot named '{name}' already exists and no completed snapshot marker \
             was found, so the destination has no baseline; drop the slot to run the snapshot, \
             or set CDC_INITIAL_SNAPSHOT=false to stream from the slot as is"
        )),
        (None, Ok(None)) if input.has_disk_lsn && input.policy == SnapshotTargetPolicy::Append => {
            Fail(format!(
                "replication slot '{name}' is gone but the destination already holds replicated \
                 data, so an append snapshot would duplicate rows; set \
                 CDC_SNAPSHOT_TARGET_POLICY=truncate to re-sync, or clear the destination data \
                 first"
            ))
        }
        (None, Ok(None)) => Run,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use SnapshotAction::*;
    use SnapshotTargetPolicy::*;

    const SLOT: &str = "pg2any_slot";
    const DEST: &str = "mysql://u:********@db-a/cdc";

    fn completed(slot: &str) -> SnapshotMarker {
        SnapshotMarker::Completed {
            slot_name: slot.into(),
        }
    }

    /// A copy that began (a recorded consistent point means it did, unless a test says otherwise).
    fn in_progress(slot: &str, cp: Option<u64>) -> SnapshotMarker {
        SnapshotMarker::InProgress {
            slot_name: slot.into(),
            consistent_point: cp,
            tables: vec![("public".into(), "t".into())],
            copy_started: cp.is_some(),
            destination: None,
        }
    }

    /// An interrupted run that recorded the destination it wrote to.
    fn in_progress_at(slot: &str, cp: u64, destination: &str) -> SnapshotMarker {
        SnapshotMarker::InProgress {
            slot_name: slot.into(),
            consistent_point: Some(cp),
            tables: vec![("public".into(), "t".into())],
            copy_started: true,
            destination: Some(destination.into()),
        }
    }

    /// The run stopped after the slot's snapshot was taken, before the first row was dispatched.
    fn in_progress_before_copy(slot: &str, cp: u64) -> SnapshotMarker {
        SnapshotMarker::InProgress {
            slot_name: slot.into(),
            consistent_point: Some(cp),
            tables: vec![],
            copy_started: false,
            destination: None,
        }
    }

    fn slot_status(active: bool, flush: Option<u64>, wal_lost: bool) -> SlotStatus {
        SlotStatus {
            confirmed_flush_lsn: flush,
            restart_lsn: flush,
            active,
            wal_lost,
        }
    }

    fn run(
        enabled: bool,
        policy: SnapshotTargetPolicy,
        marker: Option<SnapshotMarker>,
        slot: std::result::Result<Option<SlotStatus>, ()>,
    ) -> SnapshotAction {
        run_with_disk_lsn(enabled, policy, marker, slot, false)
    }

    fn run_with_disk_lsn(
        enabled: bool,
        policy: SnapshotTargetPolicy,
        marker: Option<SnapshotMarker>,
        slot: std::result::Result<Option<SlotStatus>, ()>,
        has_disk_lsn: bool,
    ) -> SnapshotAction {
        run_at(enabled, policy, marker, slot, has_disk_lsn, DEST)
    }

    fn run_at(
        enabled: bool,
        policy: SnapshotTargetPolicy,
        marker: Option<SnapshotMarker>,
        slot: std::result::Result<Option<SlotStatus>, ()>,
        has_disk_lsn: bool,
        destination: &str,
    ) -> SnapshotAction {
        decide(&DecisionInput {
            enabled,
            policy,
            slot_name: SLOT,
            destination,
            marker: marker.as_ref(),
            slot: slot.as_ref().map(|s| s.as_ref()).map_err(|_| ()),
            has_disk_lsn,
        })
    }

    fn fail_msg(action: SnapshotAction) -> String {
        match action {
            Fail(msg) => msg,
            other => panic!("expected Fail, got {other:?}"),
        }
    }

    #[test]
    fn disabled_no_marker_skips() {
        assert_eq!(run(false, FailIfNotEmpty, None, Ok(None)), Skip);
    }

    #[test]
    fn disabled_in_progress_marker_fails() {
        let msg = fail_msg(run(
            false,
            FailIfNotEmpty,
            Some(in_progress(SLOT, None)),
            Ok(None),
        ));
        assert!(msg.contains("incomplete"), "{msg}");
        assert!(msg.contains("CDC_INITIAL_SNAPSHOT=true"), "{msg}");
        assert!(msg.contains("marker file"), "{msg}");
    }

    #[test]
    fn disabled_completed_marker_skips() {
        assert_eq!(
            run(false, FailIfNotEmpty, Some(completed(SLOT)), Ok(None)),
            Skip
        );
    }

    #[test]
    fn enabled_fresh_runs() {
        assert_eq!(run(true, FailIfNotEmpty, None, Ok(None)), Run);
    }

    #[test]
    fn enabled_fresh_query_failed_fails() {
        let msg = fail_msg(run(true, FailIfNotEmpty, None, Err(())));
        assert!(msg.contains("could not query"), "{msg}");
        assert!(msg.contains("restart"), "{msg}");
    }

    #[test]
    fn enabled_slot_exists_no_marker_fails() {
        let slot = slot_status(false, Some(50), false);
        let msg = fail_msg(run(true, FailIfNotEmpty, None, Ok(Some(slot))));
        assert!(msg.contains("already exists"), "{msg}");
        assert!(msg.contains("no completed snapshot marker"), "{msg}");
        assert!(msg.contains("CDC_INITIAL_SNAPSHOT=false"), "{msg}");
    }

    const ALL_POLICIES: [SnapshotTargetPolicy; 3] = [FailIfNotEmpty, Truncate, Append];

    #[test]
    fn completed_slot_present_skips() {
        for policy in ALL_POLICIES {
            let slot = slot_status(false, Some(100), false);
            assert_eq!(
                run(true, policy, Some(completed(SLOT)), Ok(Some(slot))),
                Skip,
                "{policy:?}"
            );
        }
    }

    #[test]
    fn completed_slot_query_failed_skips() {
        for policy in ALL_POLICIES {
            assert_eq!(
                run(true, policy, Some(completed(SLOT)), Err(())),
                Skip,
                "{policy:?}"
            );
        }
    }

    #[test]
    fn completed_slot_absent_fail_if_not_empty_fails() {
        let msg = fail_msg(run(true, FailIfNotEmpty, Some(completed(SLOT)), Ok(None)));
        assert!(msg.contains("gone or invalidated"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
    }

    #[test]
    fn completed_slot_absent_truncate_resyncs() {
        assert_eq!(
            run(true, Truncate, Some(completed(SLOT)), Ok(None)),
            Resync { drop_slot: false }
        );
    }

    #[test]
    fn completed_slot_lost_behaves_like_absent() {
        let lost = || slot_status(false, Some(100), true);
        assert_eq!(
            run(true, Truncate, Some(completed(SLOT)), Ok(Some(lost()))),
            Resync { drop_slot: true }
        );
        let msg = fail_msg(run(
            true,
            FailIfNotEmpty,
            Some(completed(SLOT)),
            Ok(Some(lost())),
        ));
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
    }

    #[test]
    fn enabled_slot_absent_disk_lsn_append_fails() {
        let msg = fail_msg(run_with_disk_lsn(true, Append, None, Ok(None), true));
        assert!(msg.contains("already holds replicated data"), "{msg}");
        assert!(msg.contains("duplicate rows"), "{msg}");
        assert!(msg.contains("CDC_SNAPSHOT_TARGET_POLICY=truncate"), "{msg}");
    }

    #[test]
    fn enabled_slot_absent_disk_lsn_truncate_and_fail_if_not_empty_run() {
        for policy in [Truncate, FailIfNotEmpty] {
            assert_eq!(
                run_with_disk_lsn(true, policy, None, Ok(None), true),
                Run,
                "{policy:?}"
            );
        }
    }

    #[test]
    fn completed_marker_other_slot_name_fails() {
        let slot = slot_status(false, Some(100), false);
        let msg = fail_msg(run(
            true,
            Truncate,
            Some(completed("other_slot")),
            Ok(Some(slot)),
        ));
        assert!(msg.contains("belongs to slot 'other_slot'"), "{msg}");
        assert!(msg.contains(SLOT), "{msg}");
        assert!(msg.contains("remove"), "{msg}");
    }

    #[test]
    fn in_progress_slot_inactive_flush_equals_marker_cp_recovers_with_drop() {
        let slot = slot_status(false, Some(100), false);
        assert_eq!(
            run(
                true,
                FailIfNotEmpty,
                Some(in_progress(SLOT, Some(100))),
                Ok(Some(slot))
            ),
            Recover { drop_slot: true }
        );
    }

    #[test]
    fn in_progress_no_cp_slot_absent_recovers_without_drop() {
        assert_eq!(
            run(true, Truncate, Some(in_progress(SLOT, None)), Ok(None)),
            Recover { drop_slot: false }
        );
    }

    #[test]
    fn in_progress_no_cp_slot_present_inactive_recovers_with_drop() {
        let slot = slot_status(false, Some(70), false);
        assert_eq!(
            run(
                true,
                FailIfNotEmpty,
                Some(in_progress(SLOT, None)),
                Ok(Some(slot))
            ),
            Recover { drop_slot: true }
        );
    }

    #[test]
    fn in_progress_slot_active_fails() {
        let slot = slot_status(true, Some(100), false);
        let msg = fail_msg(run(
            true,
            FailIfNotEmpty,
            Some(in_progress(SLOT, Some(100))),
            Ok(Some(slot)),
        ));
        assert!(msg.contains("active"), "{msg}");
        assert!(msg.contains("restart"), "{msg}");
    }

    #[test]
    fn in_progress_slot_other_flush_lsn_fails() {
        let slot = slot_status(false, Some(999), false);
        let msg = fail_msg(run(
            true,
            FailIfNotEmpty,
            Some(in_progress(SLOT, Some(100))),
            Ok(Some(slot)),
        ));
        assert!(msg.contains("not created by this snapshot"), "{msg}");
        assert!(msg.contains("drop the slot"), "{msg}");
    }

    #[test]
    fn in_progress_query_failed_fails() {
        let msg = fail_msg(run(
            true,
            FailIfNotEmpty,
            Some(in_progress(SLOT, None)),
            Err(()),
        ));
        assert!(msg.contains("could not be queried"), "{msg}");
        assert!(msg.contains("restart"), "{msg}");
    }

    #[test]
    fn in_progress_append_policy_fails_with_manual_cleanup_message() {
        // The copy had begun (the consistent point is recorded), so rows that are not ours may sit
        // in the tables and nothing is cleaned up. An absent slot would otherwise recover, so this
        // proves the policy check comes first.
        let msg = fail_msg(run(
            true,
            Append,
            Some(in_progress(SLOT, Some(100))),
            Ok(None),
        ));
        assert!(msg.contains("append"), "{msg}");
        assert!(
            msg.contains("clean the destination tables by hand"),
            "{msg}"
        );
    }

    /// No consistent point yet means the run stopped before the slot's snapshot was taken, so
    /// nothing was copied: only the slot may be left over, and that is cleaned up even under
    /// `append`.
    #[test]
    fn in_progress_without_a_consistent_point_recovers_even_under_append() {
        assert_eq!(
            run(true, Append, Some(in_progress(SLOT, None)), Ok(None)),
            Recover { drop_slot: false }
        );
        let slot = slot_status(false, Some(70), false);
        assert_eq!(
            run(true, Append, Some(in_progress(SLOT, None)), Ok(Some(slot))),
            Recover { drop_slot: true }
        );
    }

    /// A consistent point alone does not mean a row was copied: the slot exists and nothing else
    /// does, so even `append` can drop the slot and start over.
    #[test]
    fn append_recovers_when_the_copy_never_started() {
        assert_eq!(
            run(
                true,
                Append,
                Some(in_progress_before_copy(SLOT, 100)),
                Ok(None)
            ),
            Recover { drop_slot: false }
        );
        let slot = slot_status(false, Some(100), false);
        assert_eq!(
            run(
                true,
                Append,
                Some(in_progress_before_copy(SLOT, 100)),
                Ok(Some(slot))
            ),
            Recover { drop_slot: true }
        );
    }

    /// Recovery empties the tables the marker lists. Another destination (the connection string
    /// was changed between the crash and the restart) may hold unrelated rows in tables of the same
    /// name: it must be refused whatever the policy, before anything is touched.
    #[test]
    fn recovery_refuses_a_destination_other_than_the_markers() {
        let slot = || slot_status(false, Some(100), false);
        for policy in ALL_POLICIES {
            let msg = fail_msg(run_at(
                true,
                policy,
                Some(in_progress_at(SLOT, 100, "mysql://u:********@db-old/cdc")),
                Ok(Some(slot())),
                false,
                "mysql://u:********@db-new/cdc",
            ));
            assert!(msg.contains("db-old") && msg.contains("db-new"), "{msg}");
            assert!(msg.contains("marker file"), "{msg}");
        }
    }

    #[test]
    fn recovery_goes_on_at_the_destination_of_the_marker() {
        let slot = slot_status(false, Some(100), false);
        assert_eq!(
            run_at(
                true,
                FailIfNotEmpty,
                Some(in_progress_at(SLOT, 100, DEST)),
                Ok(Some(slot)),
                false,
                DEST
            ),
            Recover { drop_slot: true }
        );
    }

    #[test]
    fn a_marker_without_a_destination_is_not_checked() {
        let slot = slot_status(false, Some(100), false);
        assert_eq!(
            run_at(
                true,
                FailIfNotEmpty,
                Some(in_progress(SLOT, Some(100))),
                Ok(Some(slot)),
                false,
                "mysql://u:********@anywhere/cdc"
            ),
            Recover { drop_slot: true }
        );
    }

    #[test]
    fn into_result_maps_each_action() {
        assert!(matches!(Skip.into_result(), Ok(None)));
        assert!(matches!(Run.into_result(), Ok(Some(Run))));
        assert!(matches!(
            Resync { drop_slot: true }.into_result(),
            Ok(Some(Resync { drop_slot: true }))
        ));
        assert!(matches!(
            Recover { drop_slot: true }.into_result(),
            Ok(Some(Recover { drop_slot: true }))
        ));
        assert!(matches!(
            Fail("x".into()).into_result(),
            Err(CdcError::Config(m)) if m == "x"
        ));
    }
}
