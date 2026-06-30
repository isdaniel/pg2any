//! Pure render functions: ChangeEvent → SQL string.
//!
//! All functions in this module are stateless given a [`RenderContext`].
//! They are extracted from `transaction_manager.rs` to separate the
//! read-and-apply path from batch grouping, file IO, and lifecycle code.
//!
//! See docs/superpowers/specs/2026-06-05-destination-dialect-extraction-design.md
//! for design rationale.

use crate::destinations::dialect::SqlDialect;
use crate::error::{CdcError, Result};
use crate::types::{ChangeEvent, EventType, ReplicaIdentity, RowData};
use pg_walstream::ColumnValue;
use std::collections::HashMap;
use std::sync::Arc;

/// Borrowed view of the configuration the renderer needs.
///
/// Constructed cheaply by `TransactionManager` on each render call so that
/// renderer functions remain stateless free functions instead of methods on
/// a stateful manager.
pub struct RenderContext<'a> {
    pub dialect: &'a dyn SqlDialect,
    pub schema_mappings: &'a HashMap<String, String>,
}

impl<'a> RenderContext<'a> {
    /// Map a source schema to its destination equivalent.
    /// Returns a borrow from the mapping table, the caller's input, or
    /// the `"public"` literal — no per-call allocation.
    pub fn map_schema(&self, source: Option<&'a str>) -> &'a str {
        match source {
            Some(s) => self.schema_mappings.get(s).map(String::as_str).unwrap_or(s),
            None => "public",
        }
    }
}

/// Append a quoted identifier (schema/table/column) to `out`, performing
/// destination-specific escaping of embedded quote characters.
#[inline]
pub fn append_quoted_identifier(ctx: &RenderContext, out: &mut String, name: &str) {
    ctx.dialect.quote_identifier(name, out);
}

/// Append a quoted qualified table `schema.table` (or just table for SQLite) to `out`.
#[inline]
pub fn append_qualified_table(ctx: &RenderContext, out: &mut String, schema: &str, table: &str) {
    ctx.dialect.qualify_table(schema, table, out);
}

/// Append a hex literal for raw bytes directly into `out`.
#[inline]
pub fn append_hex_literal(ctx: &RenderContext, out: &mut String, bytes: &[u8]) {
    ctx.dialect.render_hex_literal(bytes, out);
}

/// Append a `ColumnValue` literal directly into `out`.
#[inline]
pub fn append_value(ctx: &RenderContext, out: &mut String, value: &ColumnValue) {
    ctx.dialect.render_value(value, out);
}

/// Generate SQL command for a change event
pub fn generate_sql_for_event(ctx: &RenderContext, event: &ChangeEvent) -> Result<String> {
    let mut out = String::new();
    render_sql_for_event_into(ctx, event, &mut out)?;
    Ok(out)
}

/// Render the SQL for a change event into `out`. No allocation when `out`
/// already has capacity — enables buffer reuse across events.
///
/// Each per-type `render_*_into` helper clears `out` at its top (single
/// source of truth), so this dispatcher does NOT clear; the non-DML arm
/// clears explicitly so `out` ends empty for skipped events.
pub fn render_sql_for_event_into(
    ctx: &RenderContext,
    event: &ChangeEvent,
    out: &mut String,
) -> Result<()> {
    match &event.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => render_insert_into(ctx, schema, table, data, out),
        EventType::Update {
            schema,
            table,
            old_data,
            new_data,
            replica_identity,
            key_columns,
            ..
        } => render_update_into(
            ctx,
            schema,
            table,
            new_data,
            old_data.as_ref(),
            replica_identity,
            key_columns,
            out,
        ),
        EventType::Delete {
            schema,
            table,
            old_data,
            replica_identity,
            key_columns,
            ..
        } => render_delete_into(
            ctx,
            schema,
            table,
            old_data,
            replica_identity,
            key_columns,
            out,
        ),
        EventType::Truncate(tables) => render_truncate_into(ctx, tables, out),
        _ => {
            // Skip non-DML events; ensure `out` ends empty.
            out.clear();
            Ok(())
        }
    }
}

/// Generate INSERT SQL command
///
/// Accepts `&RowData` directly. Uses `iter()` for column iteration.
pub fn generate_insert_sql(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    new_data: &RowData,
) -> Result<String> {
    // Pre-size: assume ~48 bytes per (column, value) pair + overhead
    let mut sql = String::with_capacity(64 + new_data.len() * 48);
    render_insert_into(ctx, schema, table, new_data, &mut sql)?;
    Ok(sql)
}

/// Render INSERT SQL into `out` (cleared first).
fn render_insert_into(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    new_data: &RowData,
    out: &mut String,
) -> Result<()> {
    let schema = ctx.map_schema(Some(schema));

    out.clear();
    out.push_str("INSERT INTO ");
    append_qualified_table(ctx, out, schema, table);
    out.push_str(" (");
    for (i, (k, _)) in new_data.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        append_quoted_identifier(ctx, out, k);
    }
    out.push_str(") VALUES (");
    for (i, (_, v)) in new_data.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        append_value(ctx, out, v);
    }
    out.push_str(");");

    Ok(())
}

/// Generate UPDATE SQL command
pub fn generate_update_sql(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    new_data: &RowData,
    old_data: Option<&RowData>,
    replica_identity: &ReplicaIdentity,
    key_columns: &[Arc<str>],
) -> Result<String> {
    let mut sql = String::with_capacity(64 + new_data.len() * 64);
    render_update_into(
        ctx,
        schema,
        table,
        new_data,
        old_data,
        replica_identity,
        key_columns,
        &mut sql,
    )?;
    Ok(sql)
}

/// Render UPDATE SQL into `out` (cleared first).
#[allow(clippy::too_many_arguments)]
fn render_update_into(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    new_data: &RowData,
    old_data: Option<&RowData>,
    replica_identity: &ReplicaIdentity,
    key_columns: &[Arc<str>],
    out: &mut String,
) -> Result<()> {
    let schema = ctx.map_schema(Some(schema));

    out.clear();
    out.push_str("UPDATE ");
    append_qualified_table(ctx, out, schema, table);
    out.push_str(" SET ");
    for (i, (col, val)) in new_data.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        append_quoted_identifier(ctx, out, col);
        out.push_str(" = ");
        append_value(ctx, out, val);
    }
    out.push_str(" WHERE ");
    append_where_clause(ctx, out, replica_identity, key_columns, old_data, new_data)?;
    out.push(';');

    Ok(())
}

/// Generate DELETE SQL command
///
/// Accepts `&RowData` directly. For Default/Index replica identity (the most
/// common case), this avoids cloning RowData entirely — only `RowData::get()`
/// lookups are performed, with zero allocations.
pub fn generate_delete_sql(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    old_data: &RowData,
    replica_identity: &ReplicaIdentity,
    key_columns: &[Arc<str>],
) -> Result<String> {
    let mut sql = String::with_capacity(64 + key_columns.len() * 32);
    render_delete_into(
        ctx,
        schema,
        table,
        old_data,
        replica_identity,
        key_columns,
        &mut sql,
    )?;
    Ok(sql)
}

/// Render DELETE SQL into `out` (cleared first).
fn render_delete_into(
    ctx: &RenderContext,
    schema: &str,
    table: &str,
    old_data: &RowData,
    replica_identity: &ReplicaIdentity,
    key_columns: &[Arc<str>],
    out: &mut String,
) -> Result<()> {
    let schema = ctx.map_schema(Some(schema));

    out.clear();
    out.push_str("DELETE FROM ");
    append_qualified_table(ctx, out, schema, table);
    out.push_str(" WHERE ");
    append_where_clause(
        ctx,
        out,
        replica_identity,
        key_columns,
        Some(old_data),
        old_data,
    )?;
    out.push(';');

    Ok(())
}

/// Generate TRUNCATE SQL command
pub fn generate_truncate_sql(ctx: &RenderContext, tables: &[Arc<str>]) -> Result<String> {
    // Pre-size for ~48 bytes per table (keyword + identifiers + punctuation).
    let mut sql = String::with_capacity(tables.len() * 48);
    render_truncate_into(ctx, tables, &mut sql)?;
    Ok(sql)
}

/// Render TRUNCATE SQL into `out` (cleared first).
fn render_truncate_into(ctx: &RenderContext, tables: &[Arc<str>], out: &mut String) -> Result<()> {
    // Build all TRUNCATE statements into a single `String` — no intermediate Vec.
    out.clear();

    for table_spec in tables.iter() {
        let table_spec: &str = table_spec;
        let (schema, table) = match table_spec.split_once('.') {
            Some((s, t)) if !t.contains('.') => (ctx.map_schema(Some(s)), t),
            _ => (ctx.map_schema(Some("public")), table_spec),
        };

        if let Some(stmt) = ctx.dialect.truncate_table_sql(schema, table) {
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(&stmt);
        }
    }

    Ok(())
}

/// Append WHERE clause conditions directly into the provided buffer
pub fn append_where_clause(
    ctx: &RenderContext,
    out: &mut String,
    replica_identity: &ReplicaIdentity,
    key_columns: &[Arc<str>],
    old_data: Option<&RowData>,
    new_data: &RowData,
) -> Result<()> {
    match replica_identity {
        ReplicaIdentity::Default | ReplicaIdentity::Index => {
            if key_columns.is_empty() {
                return Err(CdcError::Generic(
                    "No key columns found for UPDATE/DELETE with DEFAULT/INDEX replica identity"
                        .to_string(),
                ));
            }
            let data = old_data.unwrap_or(new_data);
            for (i, col) in key_columns.iter().enumerate() {
                if i > 0 {
                    out.push_str(" AND ");
                }
                let col_str: &str = col;
                let val = data
                    .get(col_str)
                    .ok_or_else(|| CdcError::Generic(format!("Key column {col_str} not found")))?;
                append_quoted_identifier(ctx, out, col_str);
                out.push_str(" = ");
                append_value(ctx, out, val);
            }
        }
        ReplicaIdentity::Full => {
            let data = old_data.ok_or_else(|| {
                CdcError::Generic("FULL replica identity requires old data".to_string())
            })?;
            for (i, (col, val)) in data.iter().enumerate() {
                if i > 0 {
                    out.push_str(" AND ");
                }
                append_quoted_identifier(ctx, out, col);
                if val.is_null() {
                    out.push_str(" IS NULL");
                } else {
                    out.push_str(" = ");
                    append_value(ctx, out, val);
                }
            }
        }
        ReplicaIdentity::Nothing => {
            return Err(CdcError::Generic(
                "Cannot generate WHERE clause with NOTHING replica identity".to_string(),
            ));
        }
    }
    Ok(())
}

/// Output of rendering a single `ChangeEvent`.
///
/// `TransactionManager` will eventually route batches of these to the
/// appropriate `DestinationHandler` method (SQL, bulk-insert, event-mode,
/// or no-op) without re-parsing SQL strings. Currently this enum is
/// substrate for that future work; the active execute path still uses the
/// legacy string-reparse routing in
/// `transaction_manager.rs::execute_batch_with_bulk_detection`.
#[allow(dead_code)] // substrate for upcoming routing change; exercised by tests
#[derive(Debug)]
pub enum RenderedStatement {
    /// Plain SQL statement (DML or DDL) ready to send to the destination.
    Sql(String),
    /// Structured bulk-insert intent. Matches the shape of
    /// `DestinationHandler::execute_bulk_insert_with_hook`.
    BulkInsert {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
    },
    /// Raw event passthrough for event-mode destinations (Kafka).
    Event(Box<ChangeEvent>),
    /// Event produced no statement (e.g. TRUNCATE on Kafka, or filtered).
    NoOp,
}

/// Structured (table, columns, single-row) tuple for bulk insert routing.
#[allow(dead_code)]
pub(crate) struct InsertBulkParts {
    pub table: String,
    pub columns: Vec<String>,
    pub row: Vec<String>,
}

/// Render a single `ChangeEvent` into a structured statement.
///
/// `supports_bulk` should reflect the active destination handler's
/// `supports_bulk_insert()`. When true and the event is an INSERT, the
/// result is `RenderedStatement::BulkInsert`; otherwise the event is
/// rendered as `RenderedStatement::Sql`.
///
/// `event_mode` should reflect the handler's `supports_event_mode()`.
/// When true, INSERT/UPDATE/DELETE/TRUNCATE events are returned as
/// `RenderedStatement::Event(...)` (with no SQL rendering); other event
/// types become `NoOp`.
#[allow(dead_code)]
pub(crate) fn render_event(
    ctx: &RenderContext,
    event: &ChangeEvent,
    supports_bulk: bool,
    event_mode: bool,
) -> Result<RenderedStatement> {
    if event_mode {
        return match &event.event_type {
            EventType::Insert { .. }
            | EventType::Update { .. }
            | EventType::Delete { .. }
            | EventType::Truncate(_) => Ok(RenderedStatement::Event(Box::new(event.clone()))),
            _ => Ok(RenderedStatement::NoOp),
        };
    }

    if supports_bulk {
        if let EventType::Insert { .. } = &event.event_type {
            let parts = render_insert_as_bulk(ctx, event)?;
            return Ok(RenderedStatement::BulkInsert {
                table: parts.table,
                columns: parts.columns,
                rows: vec![parts.row],
            });
        }
    }

    let sql = generate_sql_for_event(ctx, event)?;
    if sql.is_empty() {
        Ok(RenderedStatement::NoOp)
    } else {
        Ok(RenderedStatement::Sql(sql))
    }
}

/// Render a single INSERT event into the structured tuple form expected by
/// `DestinationHandler::execute_bulk_insert_with_hook`.
///
/// Produces the same `(table, columns, row)` triple that the legacy
/// `destinations::bulk_insert::detect_bulk_insert_batch` extracts from
/// rendered INSERT SQL strings — but without going through SQL. The
/// `table` string is the dialect-qualified identifier (e.g.
/// `` `public`.`users` ``); each column name and value is rendered with
/// the same dialect helpers used by `generate_insert_sql`, so the strings
/// are byte-identical to what would appear in the `VALUES (...)` tuple.
#[allow(dead_code)]
pub(crate) fn render_insert_as_bulk(
    ctx: &RenderContext,
    event: &ChangeEvent,
) -> Result<InsertBulkParts> {
    let (schema, table, data) = match &event.event_type {
        EventType::Insert {
            schema,
            table,
            data,
            ..
        } => (schema.as_ref(), table.as_ref(), data),
        _ => {
            return Err(CdcError::Generic(
                "render_insert_as_bulk called with non-INSERT event".to_string(),
            ));
        }
    };

    let mapped_schema = ctx.map_schema(Some(schema));

    let mut qualified_table = String::new();
    append_qualified_table(ctx, &mut qualified_table, mapped_schema, table);

    let mut columns: Vec<String> = Vec::with_capacity(data.len());
    let mut row: Vec<String> = Vec::with_capacity(data.len());
    for (col_name, val) in data.iter() {
        let mut quoted_col = String::new();
        append_quoted_identifier(ctx, &mut quoted_col, col_name);
        columns.push(quoted_col);

        let mut rendered_val = String::new();
        append_value(ctx, &mut rendered_val, val);
        row.push(rendered_val);
    }

    Ok(InsertBulkParts {
        table: qualified_table,
        columns,
        row,
    })
}

/// Coalesce a consecutive run of `RenderedStatement::BulkInsert` items
/// targeting the same `(table, columns)` into a single BulkInsert with
/// all rows accumulated. Stops at the first item that breaks the run.
///
/// Returns the coalesced BulkInsert and the number of items consumed,
/// or `None` if the first item is not a BulkInsert.
#[allow(dead_code)]
pub(crate) fn coalesce_bulk_inserts(
    items: &[RenderedStatement],
) -> Option<(RenderedStatement, usize)> {
    let (first_table, first_cols, mut all_rows) = match items.first()? {
        RenderedStatement::BulkInsert {
            table,
            columns,
            rows,
        } => (table.clone(), columns.clone(), rows.clone()),
        _ => return None,
    };

    let mut consumed = 1;
    for item in &items[1..] {
        match item {
            RenderedStatement::BulkInsert {
                table,
                columns,
                rows,
            } if table == &first_table && columns == &first_cols => {
                all_rows.extend(rows.iter().cloned());
                consumed += 1;
            }
            _ => break,
        }
    }

    Some((
        RenderedStatement::BulkInsert {
            table: first_table,
            columns: first_cols,
            rows: all_rows,
        },
        consumed,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destinations::dialects::MySqlDialect;
    use crate::types::{ChangeEvent, EventType, ReplicaIdentity, RowData};
    use pg_walstream::{ColumnValue, Lsn};
    use std::sync::Arc;

    fn ctx() -> (MySqlDialect, HashMap<String, String>) {
        (MySqlDialect, HashMap::new())
    }

    fn make_render_ctx<'a>(
        dialect: &'a MySqlDialect,
        mappings: &'a HashMap<String, String>,
    ) -> RenderContext<'a> {
        RenderContext {
            dialect,
            schema_mappings: mappings,
        }
    }

    fn sample_insert() -> ChangeEvent {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("alice")),
        ]);
        ChangeEvent::insert("public", "users", 42, data, Lsn::new(0x100))
    }

    fn sample_update() -> ChangeEvent {
        let new_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("bob")),
        ]);
        let old_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("alice")),
        ]);
        ChangeEvent::update(
            "public",
            "users",
            42,
            Some(old_data),
            new_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(0x101),
        )
    }

    fn sample_delete() -> ChangeEvent {
        let old_data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("1")),
            ("name", ColumnValue::text("alice")),
        ]);
        ChangeEvent::delete(
            "public",
            "users",
            42,
            old_data,
            ReplicaIdentity::Default,
            vec![Arc::from("id")],
            Lsn::new(0x102),
        )
    }

    #[test]
    fn test_render_into_matches_string_variant() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        let event = sample_insert();
        let owned = generate_sql_for_event(&rc, &event).unwrap();

        let mut buf = String::from("STALE-PRELOAD"); // must be cleared by the fn
        render_sql_for_event_into(&rc, &event, &mut buf).unwrap();
        assert_eq!(buf, owned);

        // Reuse the same buffer for a second event — no leftover bytes.
        let event2 = sample_delete();
        let owned2 = generate_sql_for_event(&rc, &event2).unwrap();
        render_sql_for_event_into(&rc, &event2, &mut buf).unwrap();
        assert_eq!(buf, owned2);
    }

    #[test]
    fn render_event_insert_sql_form() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        let event = sample_insert();
        let result = render_event(&rc, &event, false, false).unwrap();
        let expected = generate_sql_for_event(&rc, &event).unwrap();
        match result {
            RenderedStatement::Sql(s) => assert_eq!(s, expected),
            other => panic!("expected Sql, got {:?}", other),
        }
    }

    #[test]
    fn render_event_insert_bulk_form_matches_legacy_parse() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        let event = sample_insert();

        let bulk_form = render_event(&rc, &event, true, false).unwrap();
        let sql_form = match &event.event_type {
            EventType::Insert {
                schema,
                table,
                data,
                ..
            } => generate_insert_sql(&rc, schema, table, data).unwrap(),
            _ => unreachable!(),
        };
        let parsed =
            crate::destinations::bulk_insert::detect_bulk_insert_batch(&[sql_form]).unwrap();

        match &bulk_form {
            RenderedStatement::BulkInsert {
                table,
                columns,
                rows,
            } => {
                assert_eq!(table, &parsed.table);
                assert_eq!(columns, &parsed.columns);
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0], parsed.rows[0]);
            }
            other => panic!("expected BulkInsert, got {:?}", other),
        }
    }

    #[test]
    fn render_event_update_event_mode_returns_event() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        let event = sample_update();
        let result = render_event(&rc, &event, false, true).unwrap();
        match result {
            RenderedStatement::Event(_) => {}
            other => panic!("expected Event, got {:?}", other),
        }
    }

    #[test]
    fn render_event_relation_event_mode_is_noop() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        // Begin is one of the non-DML event types -> NoOp in event mode.
        let event = ChangeEvent {
            event_type: EventType::Begin {
                transaction_id: 1,
                final_lsn: Lsn::new(0x200),
                commit_timestamp: chrono::Utc::now(),
            },
            lsn: Lsn::new(0x200),
            metadata: None,
        };
        let result = render_event(&rc, &event, false, true).unwrap();
        assert!(matches!(result, RenderedStatement::NoOp));
    }

    #[test]
    fn render_event_truncate_sql_form() {
        let (d, m) = ctx();
        let rc = make_render_ctx(&d, &m);
        let event = ChangeEvent {
            event_type: EventType::Truncate(vec![Arc::from("public.users")]),
            lsn: Lsn::new(0x300),
            metadata: None,
        };
        let result = render_event(&rc, &event, false, false).unwrap();
        let expected = generate_sql_for_event(&rc, &event).unwrap();
        match result {
            RenderedStatement::Sql(s) => assert_eq!(s, expected),
            other => panic!("expected Sql, got {:?}", other),
        }
    }

    fn bulk(table: &str, columns: &[&str], rows: Vec<Vec<&str>>) -> RenderedStatement {
        RenderedStatement::BulkInsert {
            table: table.to_string(),
            columns: columns.iter().map(|s| s.to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|r| r.into_iter().map(|s| s.to_string()).collect())
                .collect(),
        }
    }

    #[test]
    fn coalesce_empty_returns_none() {
        assert!(coalesce_bulk_inserts(&[]).is_none());
    }

    #[test]
    fn coalesce_first_sql_returns_none() {
        let items = vec![RenderedStatement::Sql(
            "INSERT INTO x VALUES (1);".to_string(),
        )];
        assert!(coalesce_bulk_inserts(&items).is_none());
    }

    #[test]
    fn coalesce_single_bulk() {
        let items = vec![bulk("`t`", &["`a`"], vec![vec!["1"]])];
        let (out, consumed) = coalesce_bulk_inserts(&items).unwrap();
        assert_eq!(consumed, 1);
        match out {
            RenderedStatement::BulkInsert { rows, .. } => assert_eq!(rows.len(), 1),
            _ => panic!(),
        }
    }

    #[test]
    fn coalesce_three_same_table() {
        let items = vec![
            bulk("`t`", &["`a`"], vec![vec!["1"]]),
            bulk("`t`", &["`a`"], vec![vec!["2"]]),
            bulk("`t`", &["`a`"], vec![vec!["3"]]),
        ];
        let (out, consumed) = coalesce_bulk_inserts(&items).unwrap();
        assert_eq!(consumed, 3);
        match out {
            RenderedStatement::BulkInsert { rows, .. } => assert_eq!(rows.len(), 3),
            _ => panic!(),
        }
    }

    #[test]
    fn coalesce_stops_at_different_table() {
        let items = vec![
            bulk("`t`", &["`a`"], vec![vec!["1"]]),
            bulk("`t`", &["`a`"], vec![vec!["2"]]),
            bulk("`other`", &["`a`"], vec![vec!["3"]]),
        ];
        let (out, consumed) = coalesce_bulk_inserts(&items).unwrap();
        assert_eq!(consumed, 2);
        match out {
            RenderedStatement::BulkInsert { rows, .. } => assert_eq!(rows.len(), 2),
            _ => panic!(),
        }
    }
}
