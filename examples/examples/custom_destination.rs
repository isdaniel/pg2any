//! End-to-end example: registering a custom DestinationHandler with a custom SqlDialect.
//!
//! This program is compile-checked only — it does not connect to a real destination.
//! It demonstrates the third integration pattern from `pg2any_lib::destinations::dialect`:
//! a fully custom `SqlDialect` plugged into a custom `DestinationHandler` via
//! `ConfigBuilder::custom_destination`.
//!
//! Run: `cargo run -p pg2any --bin custom_destination`

use async_trait::async_trait;
use pg2any_lib::{AnsiDialect, CdcResult, Config, DestinationHandler, PreCommitHook, SqlDialect};
use pg_walstream::ColumnValue;
use std::collections::HashMap;

// === A custom dialect mimicking ClickHouse: backtick identifiers, unhex() hex, generic TRUNCATE. ===

#[derive(Default, Debug, Clone, Copy)]
struct ClickHouseDialect;

impl SqlDialect for ClickHouseDialect {
    fn quote_identifier(&self, ident: &str, out: &mut String) {
        out.push('`');
        for ch in ident.chars() {
            if ch == '`' {
                out.push('`');
            }
            out.push(ch);
        }
        out.push('`');
    }

    fn qualify_table(&self, schema: &str, table: &str, out: &mut String) {
        self.quote_identifier(schema, out);
        out.push('.');
        self.quote_identifier(table, out);
    }

    fn render_hex_literal(&self, bytes: &[u8], out: &mut String) {
        out.push_str("unhex('");
        for b in bytes {
            out.push_str(&format!("{:02x}", b));
        }
        out.push_str("')");
    }

    fn render_value(&self, value: &ColumnValue, out: &mut String) {
        match value {
            ColumnValue::Null => out.push_str("NULL"),
            ColumnValue::Text(_) => match value.as_str() {
                Some(s) => {
                    out.push('\'');
                    for ch in s.chars() {
                        if ch == '\'' {
                            out.push_str("''");
                        } else {
                            out.push(ch);
                        }
                    }
                    out.push('\'');
                }
                None => self.render_hex_literal(value.as_bytes(), out),
            },
            ColumnValue::Binary(_) => self.render_hex_literal(value.as_bytes(), out),
        }
    }

    fn truncate_table_sql(&self, schema: &str, table: &str) -> Option<String> {
        let mut s = String::from("TRUNCATE TABLE ");
        self.qualify_table(schema, table, &mut s);
        s.push(';');
        Some(s)
    }
}

// === A custom DestinationHandler that uses our ClickHouseDialect. ===

#[derive(Default)]
struct ClickHouseDest;

#[async_trait]
impl DestinationHandler for ClickHouseDest {
    async fn connect(&mut self, _connection_string: &str) -> CdcResult<()> {
        Ok(())
    }

    fn set_schema_mappings(&mut self, _mappings: HashMap<String, String>) {}

    async fn execute_sql_batch_with_hook(
        &mut self,
        commands: &[String],
        pre_commit_hook: Option<PreCommitHook>,
    ) -> CdcResult<()> {
        for cmd in commands {
            println!("[clickhouse] {}", cmd);
        }
        if let Some(hook) = pre_commit_hook {
            hook().await?;
        }
        Ok(())
    }

    async fn close(&mut self) -> CdcResult<()> {
        Ok(())
    }

    fn dialect(&self) -> &'static dyn SqlDialect {
        static D: ClickHouseDialect = ClickHouseDialect;
        &D
    }
}

#[tokio::main]
async fn main() -> CdcResult<()> {
    // The example only compile-checks the public API — it never connects.
    // `custom_destination` switches `destination_type` to `DestinationType::Custom`
    // and registers the factory under a sentinel key.
    let _config = Config::builder()
        .source_connection_string("postgres://localhost/source_db")
        .destination_connection_string("clickhouse://localhost:9000/dest_db")
        .custom_destination(ClickHouseDest::default)
        .build()?;

    println!("Config built with custom ClickHouse destination + dialect.");

    // Pattern 2 (AnsiDialect default) — demonstrate the fallback type is reachable
    // from the crate root, even though `ClickHouseDest` overrides it above.
    let _ansi_demo: &dyn SqlDialect = &AnsiDialect;

    Ok(())
}
