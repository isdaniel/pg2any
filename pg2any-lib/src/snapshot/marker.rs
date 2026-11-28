//! Durable marker recording initial-snapshot progress.

use crate::error::{CdcError, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

pub(crate) const MARKER_FILE: &str = "snapshot_marker.json";
const MARKER_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum SnapshotMarker {
    InProgress {
        slot_name: String,
        consistent_point: Option<u64>,
        /// (destination schema, table) as resolved by the run that wrote them
        tables: Vec<(String, String)>,
        /// Stored just before the first row can be dispatched. Without it the run stopped before
        /// anything was copied (only the slot may be left over), which `append` can recover from.
        /// A marker written without the field counts as started: the safe reading.
        #[serde(default = "copy_started_by_default")]
        copy_started: bool,
        /// The destination this run wrote to, without its password. Recovery empties the tables
        /// listed here, so it must not run against another destination. `None` for a marker
        /// written before the field existed: not checked.
        #[serde(default)]
        destination: Option<String>,
    },
    Completed {
        slot_name: String,
    },
}

fn copy_started_by_default() -> bool {
    true
}

#[derive(Serialize, Deserialize)]
struct MarkerFile<M> {
    version: u32,
    marker: M,
}

pub(crate) struct MarkerStore {
    path: PathBuf,
}

impl MarkerStore {
    pub(crate) fn new(base_dir: &str) -> Self {
        Self {
            path: PathBuf::from(base_dir).join(MARKER_FILE),
        }
    }

    /// `Ok(None)` only when the file is absent; a corrupt file is an error so a
    /// damaged marker is never mistaken for "no snapshot yet".
    pub(crate) async fn load(&self) -> Result<Option<SnapshotMarker>> {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Check the version before decoding the marker: a newer version may change its shape.
        let corrupt = |e: serde_json::Error| {
            CdcError::Serialization(serde::de::Error::custom(format!(
                "snapshot marker {} is corrupt: {e}",
                self.path.display()
            )))
        };
        let file: MarkerFile<serde_json::Value> =
            serde_json::from_slice(&bytes).map_err(corrupt)?;
        if file.version != MARKER_VERSION {
            return Err(CdcError::config(format!(
                "unsupported snapshot marker version {} in {} (expected {})",
                file.version,
                self.path.display(),
                MARKER_VERSION
            )));
        }
        Ok(Some(serde_json::from_value(file.marker).map_err(corrupt)?))
    }

    pub(crate) async fn store(&self, m: &SnapshotMarker) -> Result<()> {
        let json = serde_json::to_vec(&MarkerFile {
            version: MARKER_VERSION,
            marker: m,
        })?;
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // Durable before the caller goes on to write the destination or the position: a host crash
        // must not bring back an older marker (or none) next to rows it never listed.
        let tmp = self.path.with_extension("json.tmp");
        let mut file = tokio::fs::File::create(&tmp).await?;
        file.write_all(&json).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, &self.path).await?;
        sync_parent(&self.path).await
    }

    pub(crate) async fn clear(&self) -> Result<()> {
        match tokio::fs::remove_file(&self.path).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
            _ => {}
        }
        sync_parent(&self.path).await
    }
}

/// Makes a rename or removal in `path`'s directory durable (a directory cannot be opened on Windows).
async fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        tokio::fs::File::open(dir).await?.sync_all().await?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn in_progress() -> SnapshotMarker {
        SnapshotMarker::InProgress {
            slot_name: "slot_a".into(),
            consistent_point: Some(0x16B3748),
            tables: vec![
                ("public".into(), "users".into()),
                ("sales".into(), "orders".into()),
            ],
            copy_started: true,
            destination: None,
        }
    }

    fn completed() -> SnapshotMarker {
        SnapshotMarker::Completed {
            slot_name: "slot_a".into(),
        }
    }

    #[tokio::test]
    async fn roundtrip_in_progress_and_completed() {
        let dir = tempfile::tempdir().unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());

        store.store(&in_progress()).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(in_progress()));

        store.store(&completed()).await.unwrap();
        assert_eq!(store.load().await.unwrap(), Some(completed()));

        let raw: serde_json::Value = serde_json::from_slice(
            &std::fs::read(dir.path().join("snapshot_marker.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(raw["version"], 1);
        assert_eq!(raw["marker"]["state"], "completed");
    }

    #[tokio::test]
    async fn a_marker_without_copy_started_counts_as_started() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("snapshot_marker.json"),
            r#"{"version":1,"marker":{"state":"in_progress","slot_name":"s","consistent_point":5,"tables":[]}}"#,
        )
        .unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        assert!(matches!(
            store.load().await.unwrap(),
            Some(SnapshotMarker::InProgress {
                copy_started: true,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn load_missing_is_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        assert_eq!(store.load().await.unwrap(), None);
    }

    #[tokio::test]
    async fn load_corrupt_json_is_error_not_none() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("snapshot_marker.json"), b"{not json").unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        assert!(matches!(
            store.load().await,
            Err(CdcError::Serialization(_))
        ));
    }

    #[tokio::test]
    async fn corrupt_marker_error_names_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snapshot_marker.json");
        let store = MarkerStore::new(dir.path().to_str().unwrap());

        std::fs::write(&path, b"{not json").unwrap();
        let msg = store.load().await.unwrap_err().to_string();
        assert!(msg.contains(path.to_str().unwrap()), "{msg}");

        // Valid JSON whose marker has an unknown shape is reported the same way.
        std::fs::write(&path, br#"{"version":1,"marker":{"state":"bogus"}}"#).unwrap();
        let err = store.load().await.unwrap_err();
        assert!(matches!(err, CdcError::Serialization(_)), "got {err:?}");
        assert!(err.to_string().contains(path.to_str().unwrap()), "{err}");
    }

    #[tokio::test]
    async fn load_unknown_version_is_config_error() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("snapshot_marker.json"),
            br#"{"version":2,"marker":{"state":"future_state"}}"#,
        )
        .unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        assert!(matches!(store.load().await, Err(CdcError::Config(_))));
    }

    #[tokio::test]
    async fn store_leaves_no_tmp_file() {
        let dir = tempfile::tempdir().unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        store.store(&completed()).await.unwrap();

        let names: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(names, vec!["snapshot_marker.json".to_string()]);
    }

    #[tokio::test]
    async fn clear_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let store = MarkerStore::new(dir.path().to_str().unwrap());
        store.clear().await.unwrap();

        store.store(&completed()).await.unwrap();
        store.clear().await.unwrap();
        assert_eq!(store.load().await.unwrap(), None);
        store.clear().await.unwrap();
    }
}
