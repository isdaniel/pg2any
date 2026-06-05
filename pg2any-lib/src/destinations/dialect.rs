//! # Per-destination SQL dialect abstraction
//!
//! External destinations registered via [`crate::config::ConfigBuilder::register_destination`]
//! (or `custom_destination` / `use_destination`) can supply their own SQL
//! dialect by overriding
//! [`crate::destinations::destination_factory::DestinationHandler::dialect`].
//!
//! Three integration patterns:
//!
//! 1. **Reuse a built-in dialect** — return a reference to a static instance
//!    of one of the five built-ins ([`crate::destinations::dialects::MySqlDialect`],
//!    [`crate::destinations::dialects::SqlServerDialect`],
//!    [`crate::destinations::dialects::SqliteDialect`],
//!    [`crate::destinations::dialects::KafkaDialect`], or
//!    [`crate::destinations::dialects::AnsiDialect`]):
//!
//!    ```ignore
//!    fn dialect(&self) -> &'static dyn SqlDialect {
//!        use pg2any_lib::MySqlDialect;
//!        static D: MySqlDialect = MySqlDialect;
//!        &D
//!    }
//!    ```
//!
//! 2. **Accept the [`crate::destinations::dialects::AnsiDialect`] default** —
//!    do nothing. `DestinationHandler::dialect`'s default impl returns
//!    `&AnsiDialect`, which matches the historical `DestinationType::Custom(_)`
//!    fallback (double-quoted identifiers, `X'…'` hex, `TRUNCATE TABLE`).
//!
//! 3. **Provide a fully custom dialect** — implement [`SqlDialect`] yourself
//!    for your destination's quoting/value/truncate rules. See
//!    `examples/src/bin/custom_destination.rs` for a worked example.
//!
//! See docs/superpowers/specs/2026-06-05-destination-dialect-extraction-design.md
//! for design rationale.

use pg_walstream::ColumnValue;

/// SQL dialect rules for a destination database.
///
/// Implementations encapsulate quoting, value rendering, and DDL emission
/// differences across destination engines. All methods write into a caller-
/// supplied `String` buffer to avoid per-call allocation.
///
/// The four built-in implementations (`MySqlDialect`, `SqlServerDialect`,
/// `SqliteDialect`, `KafkaDialect`) plus the generic `AnsiDialect` cover the
/// pre-refactor branches that lived inside `TransactionManager`. External
/// destinations registered via `Config::register_destination` may either reuse
/// one of these or supply their own.
pub trait SqlDialect: Send + Sync {
    /// Append a quoted identifier (schema/table/column) into `out`.
    fn quote_identifier(&self, ident: &str, out: &mut String);

    /// Append a fully-qualified table reference into `out`.
    /// `schema` is the already-mapped destination schema name (caller is
    /// responsible for `map_schema`). Some dialects (SQLite, Kafka today)
    /// ignore the schema and emit only the table.
    fn qualify_table(&self, schema: &str, table: &str, out: &mut String);

    /// Append a hex literal for raw bytes into `out`.
    fn render_hex_literal(&self, bytes: &[u8], out: &mut String);

    /// Append a SQL literal for a `ColumnValue` into `out`.
    fn render_value(&self, value: &ColumnValue, out: &mut String);

    /// Return the TRUNCATE statement for `schema.table`, or `None` if the
    /// destination has no concept of TRUNCATE (e.g. Kafka).
    fn truncate_table_sql(&self, schema: &str, table: &str) -> Option<String>;
}

/// Append `bytes` to `out` as ASCII hex digits (no prefix/suffix).
///
/// Shared helper for dialect impls that emit hex literals.
pub(crate) fn push_hex_ascii(out: &mut String, bytes: &[u8]) {
    static HEX: &[u8; 16] = b"0123456789abcdef";
    out.reserve(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
}
