//! Initial snapshot support.

pub(crate) mod batcher;
pub(crate) mod budget;
pub(crate) mod coordinator;
pub(crate) mod decision;
pub(crate) mod marker;
pub(crate) mod pipeline;
pub(crate) mod target;

pub(crate) use coordinator::{run_snapshot_phase, SnapshotContext};
