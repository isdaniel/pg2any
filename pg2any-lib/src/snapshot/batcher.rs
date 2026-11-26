//! Groups snapshot INSERT events into per-table batches bounded by rows and bytes.

use crate::error::{CdcError, Result};
use crate::snapshot::budget::row_byte_estimate;
use crate::sql_renderer::{render_insert_as_bulk, RenderContext};
use crate::types::{ChangeEvent, EventType};

// Each rendered cell is a `String` kept in a `Vec`; its header is held on top of the text.
const CELL_OVERHEAD: usize = std::mem::size_of::<String>();

pub(crate) enum SnapshotBatch {
    Sql {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        bytes: usize,
    },
    Events {
        events: Vec<ChangeEvent>,
        bytes: usize,
    },
}

pub(crate) enum BatchMode<'a> {
    Sql(RenderContext<'a>),
    Events,
}

impl SnapshotBatch {
    fn len(&self) -> usize {
        match self {
            Self::Sql { rows, .. } => rows.len(),
            Self::Events { events, .. } => events.len(),
        }
    }

    pub(crate) fn bytes(&self) -> usize {
        match self {
            Self::Sql { bytes, .. } | Self::Events { bytes, .. } => *bytes,
        }
    }

    /// Both batches are single-table by construction, so the first event names the table.
    fn same_target(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Sql { table, columns, .. },
                Self::Sql {
                    table: t,
                    columns: c,
                    ..
                },
            ) => table == t && columns == c,
            (Self::Events { events, .. }, Self::Events { events: e, .. }) => {
                match (&events[0].event_type, &e[0].event_type) {
                    (
                        EventType::Insert { schema, table, .. },
                        EventType::Insert {
                            schema: s,
                            table: t,
                            ..
                        },
                    ) => schema == s && table == t,
                    _ => false,
                }
            }
            _ => false,
        }
    }

    fn absorb(&mut self, other: Self) {
        match (self, other) {
            (
                Self::Sql { rows, bytes, .. },
                Self::Sql {
                    rows: r, bytes: b, ..
                },
            ) => {
                rows.extend(r);
                *bytes += b;
            }
            (
                Self::Events { events, bytes },
                Self::Events {
                    events: e,
                    bytes: b,
                },
            ) => {
                events.extend(e);
                *bytes += b;
            }
            _ => unreachable!("a batcher produces one batch kind"),
        }
    }
}

pub(crate) struct Batcher<'a> {
    mode: BatchMode<'a>,
    max_rows: usize,
    max_bytes: usize,
    current: Option<SnapshotBatch>,
}

impl<'a> Batcher<'a> {
    pub(crate) fn new(mode: BatchMode<'a>, max_rows: usize, max_bytes: usize) -> Self {
        Self {
            mode,
            max_rows: max_rows.max(1),
            max_bytes,
            current: None,
        }
    }

    fn single_row(&self, event: ChangeEvent) -> Result<SnapshotBatch> {
        match &self.mode {
            BatchMode::Sql(ctx) => {
                let parts = render_insert_as_bulk(ctx, &event)?;
                let bytes = parts.row.iter().map(|c| c.len() + CELL_OVERHEAD).sum();
                Ok(SnapshotBatch::Sql {
                    table: parts.table,
                    columns: parts.columns,
                    rows: vec![parts.row],
                    bytes,
                })
            }
            BatchMode::Events => match &event.event_type {
                EventType::Insert { data, .. } => Ok(SnapshotBatch::Events {
                    bytes: row_byte_estimate(data),
                    events: vec![event],
                }),
                _ => Err(CdcError::generic(
                    "snapshot batcher accepts only INSERT events",
                )),
            },
        }
    }

    /// Returns the batches that became ready (0..=2).
    pub(crate) fn push(&mut self, event: ChangeEvent) -> Result<Vec<SnapshotBatch>> {
        let row = self.single_row(event)?;
        let mut ready = Vec::new();

        if row.bytes() > self.max_bytes {
            ready.extend(self.current.take());
            ready.push(row);
            return Ok(ready);
        }

        match self.current.as_mut() {
            Some(cur) if cur.same_target(&row) => cur.absorb(row),
            _ => ready.extend(self.current.replace(row)),
        }

        if let Some(cur) = &self.current {
            if cur.len() >= self.max_rows || cur.bytes() >= self.max_bytes {
                ready.extend(self.current.take());
            }
        }
        Ok(ready)
    }

    pub(crate) fn finish(&mut self) -> Option<SnapshotBatch> {
        self.current.take()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::dialects::MySqlDialect;
    use crate::types::{ReplicaIdentity, RowData};
    use pg_walstream::{ColumnValue, Lsn};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn insert_in(schema: &str, table: &str, id: u64, payload: &str) -> ChangeEvent {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&id.to_string())),
            ("name", ColumnValue::text(payload)),
        ]);
        ChangeEvent::insert(schema, table, 1, data, Lsn::new(id))
    }

    fn insert(table: &str, id: u64, payload: &str) -> ChangeEvent {
        insert_in("public", table, id, payload)
    }

    fn insert_with_email(table: &str, id: u64) -> ChangeEvent {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text(&id.to_string())),
            ("name", ColumnValue::text("x")),
            ("email", ColumnValue::text("x@example.com")),
        ]);
        ChangeEvent::insert("public", table, 1, data, Lsn::new(id))
    }

    fn sql_batcher<'a>(
        dialect: &'a MySqlDialect,
        mappings: &'a HashMap<String, String>,
        max_rows: usize,
        max_bytes: usize,
    ) -> Batcher<'a> {
        let ctx = RenderContext {
            dialect,
            schema_mappings: mappings,
        };
        Batcher::new(BatchMode::Sql(ctx), max_rows, max_bytes)
    }

    fn sql_rows(batch: &SnapshotBatch) -> usize {
        match batch {
            SnapshotBatch::Sql { rows, .. } => rows.len(),
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
    }

    fn event_lsns(batch: &SnapshotBatch) -> Vec<u64> {
        match batch {
            SnapshotBatch::Events { events, .. } => events.iter().map(|e| e.lsn.value()).collect(),
            SnapshotBatch::Sql { .. } => panic!("expected an Events batch"),
        }
    }

    #[test]
    fn flushes_on_row_limit() {
        let (d, m) = (MySqlDialect, HashMap::new());
        let mut b = sql_batcher(&d, &m, 3, usize::MAX);

        let mut ready = Vec::new();
        for id in 1..=7 {
            let out = b.push(insert("users", id, "x")).unwrap();
            assert_eq!(out.len(), usize::from(id % 3 == 0), "after row {id}");
            ready.extend(out);
        }
        assert_eq!(ready.iter().map(sql_rows).collect::<Vec<_>>(), [3, 3]);
        assert_eq!(sql_rows(&b.finish().unwrap()), 1);
        assert!(b.finish().is_none());
    }

    #[test]
    fn flushes_on_byte_limit() {
        let (d, m) = (MySqlDialect, HashMap::new());
        let cell_cost =
            |cells: &[String]| -> usize { cells.iter().map(|c| c.len() + CELL_OVERHEAD).sum() };
        let probe = {
            let ctx = RenderContext {
                dialect: &d,
                schema_mappings: &m,
            };
            cell_cost(
                &render_insert_as_bulk(&ctx, &insert("users", 1, "payload"))
                    .unwrap()
                    .row,
            )
        };
        // Third row reaches the limit; two rows stay below it.
        let mut b = sql_batcher(&d, &m, usize::MAX, 3 * probe - 1);

        assert!(b.push(insert("users", 1, "payload")).unwrap().is_empty());
        assert!(b.push(insert("users", 2, "payload")).unwrap().is_empty());
        let out = b.push(insert("users", 3, "payload")).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            SnapshotBatch::Sql { rows, bytes, .. } => {
                assert_eq!(rows.len(), 3);
                assert_eq!(*bytes, rows.iter().map(|r| cell_cost(r)).sum::<usize>());
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
        assert!(b.finish().is_none());
    }

    #[test]
    fn flushes_at_table_boundary() {
        let (d, m) = (MySqlDialect, HashMap::new());
        let mut b = sql_batcher(&d, &m, 100, usize::MAX);

        assert!(b.push(insert("users", 1, "x")).unwrap().is_empty());
        assert!(b.push(insert("users", 2, "x")).unwrap().is_empty());
        let out = b.push(insert("orders", 3, "x")).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            SnapshotBatch::Sql { table, rows, .. } => {
                assert_eq!(table, "`public`.`users`");
                assert_eq!(rows.len(), 2);
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
        match b.finish().unwrap() {
            SnapshotBatch::Sql { table, rows, .. } => {
                assert_eq!(table, "`public`.`orders`");
                assert_eq!(rows.len(), 1);
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
    }

    #[test]
    fn max_rows_zero_is_treated_as_one() {
        let (d, m) = (MySqlDialect, HashMap::new());
        let mut b = sql_batcher(&d, &m, 0, usize::MAX);

        for id in 1..=2 {
            let out = b.push(insert("users", id, "x")).unwrap();
            assert_eq!(
                out.iter().map(sql_rows).collect::<Vec<_>>(),
                [1],
                "row {id}"
            );
        }
        assert!(b.finish().is_none());
    }

    #[test]
    fn differing_columns_start_a_new_batch() {
        let (d, m) = (MySqlDialect, HashMap::new());
        let mut b = sql_batcher(&d, &m, 100, usize::MAX);

        assert!(b.push(insert("users", 1, "x")).unwrap().is_empty());
        let out = b.push(insert_with_email("users", 2)).unwrap();
        assert_eq!(out.len(), 1);
        match &out[0] {
            SnapshotBatch::Sql { columns, rows, .. } => {
                assert_eq!(columns, &["`id`", "`name`"]);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].len(), 2);
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }

        // Rows matching the new column set join the new batch.
        assert!(b.push(insert_with_email("users", 3)).unwrap().is_empty());
        match b.finish().unwrap() {
            SnapshotBatch::Sql { columns, rows, .. } => {
                assert_eq!(columns, &["`id`", "`name`", "`email`"]);
                assert_eq!(rows.len(), 2);
                assert!(rows.iter().all(|r| r.len() == 3));
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
    }

    #[test]
    fn events_mode_flushes_at_table_boundary() {
        let mut b = Batcher::new(BatchMode::Events, 100, usize::MAX);

        assert!(b.push(insert("users", 1, "x")).unwrap().is_empty());
        assert!(b.push(insert("users", 2, "x")).unwrap().is_empty());
        // Same table name under another schema is a different table.
        let out = b.push(insert_in("other", "users", 3, "x")).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(event_lsns(&out[0]), [1, 2]);

        let out = b.push(insert_in("other", "orders", 4, "x")).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(event_lsns(&out[0]), [3]);

        assert_eq!(event_lsns(&b.finish().unwrap()), [4]);
        assert!(b.finish().is_none());
    }

    #[test]
    fn oversize_row_becomes_its_own_batch() {
        let mut b = Batcher::new(BatchMode::Events, 100, 200);

        assert!(b.push(insert("users", 1, "x")).unwrap().is_empty());
        let out = b.push(insert("users", 2, &"y".repeat(500))).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(event_lsns(&out[0]), [1]);
        assert_eq!(event_lsns(&out[1]), [2]);
        assert!(b.finish().is_none());

        // With nothing pending, the oversize row is the only batch.
        let out = b.push(insert("users", 3, &"y".repeat(500))).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(event_lsns(&out[0]), [3]);
        assert!(b.finish().is_none());
    }

    #[test]
    fn sql_mode_applies_schema_mapping_and_dialect_quoting() {
        let d = MySqlDialect;
        let m = HashMap::from([("public".to_string(), "mydb".to_string())]);
        let mut b = sql_batcher(&d, &m, 10, usize::MAX);

        assert!(b.push(insert("users", 1, "alice")).unwrap().is_empty());
        match b.finish().unwrap() {
            SnapshotBatch::Sql {
                table,
                columns,
                rows,
                ..
            } => {
                assert_eq!(table, "`mydb`.`users`");
                assert_eq!(columns, ["`id`", "`name`"]);
                assert_eq!(rows.len(), 1);
                assert!(rows[0][1].contains("alice"));
            }
            SnapshotBatch::Events { .. } => panic!("expected an Sql batch"),
        }
    }

    #[test]
    fn events_mode_passes_events_through_unchanged() {
        let mut b = Batcher::new(BatchMode::Events, 2, usize::MAX);

        assert!(b.push(insert("users", 1, "a")).unwrap().is_empty());
        let out = b.push(insert("users", 2, "b")).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(event_lsns(&out[0]), [1, 2]);
        match &out[0] {
            SnapshotBatch::Events { events, bytes } => {
                let expected: usize = events
                    .iter()
                    .map(|e| match &e.event_type {
                        EventType::Insert { data, .. } => row_byte_estimate(data),
                        _ => unreachable!(),
                    })
                    .sum();
                assert_eq!(*bytes, expected);
                assert!(matches!(
                    &events[0].event_type,
                    EventType::Insert { table, .. } if &**table == "users"
                ));
            }
            SnapshotBatch::Sql { .. } => panic!("expected an Events batch"),
        }
    }

    #[test]
    fn non_insert_event_is_error() {
        let delete = || {
            ChangeEvent::delete(
                "public",
                "users",
                1,
                RowData::from_pairs(vec![("id", ColumnValue::text("1"))]),
                ReplicaIdentity::Default,
                vec![Arc::from("id")],
                Lsn::new(9),
            )
        };

        let (d, m) = (MySqlDialect, HashMap::new());
        assert!(sql_batcher(&d, &m, 10, 1024).push(delete()).is_err());
        assert!(Batcher::new(BatchMode::Events, 10, 1024)
            .push(delete())
            .is_err());
    }
}
