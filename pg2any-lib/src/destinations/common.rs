/// Common utilities and traits for destination implementations
///
/// This module provides shared functionality for all destination implementations:
///
/// ## Schema Mapping
/// - Maps source schemas to destination databases/schemas
/// - Essential for cross-database replication
use std::collections::HashMap;

/// Map a source schema to destination schema using provided mappings
pub fn map_schema(schema_mappings: &HashMap<String, String>, source_schema: &str) -> String {
    schema_mappings
        .get(source_schema)
        .cloned()
        .unwrap_or_else(|| source_schema.to_string())
}

/// Execute a batch of SQL commands within a single sqlx transaction with optional pre-commit hook.
///
/// This is the shared implementation used by both MySQL and SQLite destinations.
/// All commands are executed atomically — if any command fails, the entire batch is rolled back.
///
/// # Arguments
/// * `pool` - sqlx connection pool for the target database
/// * `commands` - SQL commands to execute within a single transaction
/// * `pre_commit_hook` - Optional async callback invoked BEFORE COMMIT (rolled back on failure)
/// * `db_name` - Database name for error messages (e.g., "MySQL", "SQLite")
#[cfg(any(feature = "mysql", feature = "sqlite"))]
pub(crate) async fn execute_sqlx_batch_with_hook<DB>(
    pool: &sqlx::Pool<DB>,
    commands: &[String],
    pre_commit_hook: Option<super::destination_factory::PreCommitHook>,
    db_name: &str,
) -> crate::error::Result<()>
where
    DB: sqlx::Database,
    for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
    for<'q> <DB as sqlx::Database>::Arguments<'q>: sqlx::IntoArguments<'q, DB>,
{
    // Begin a transaction
    let mut tx = pool.begin().await.map_err(|e| {
        crate::error::CdcError::generic(format!("{db_name} BEGIN transaction failed: {e}"))
    })?;

    // Execute all commands in the transaction
    for (idx, sql) in commands.iter().enumerate() {
        if let Err(e) = sqlx::query(sql).execute(&mut *tx).await {
            // Rollback on error
            if let Err(rollback_err) = tx.rollback().await {
                tracing::error!(
                    "{db_name} ROLLBACK failed after execution error: {}",
                    rollback_err
                );
            }
            return Err(crate::error::CdcError::generic(format!(
                "{db_name} execute_sql_batch failed at command {}/{}: {}",
                idx + 1,
                commands.len(),
                e
            )));
        }
    }

    // Execute pre-commit hook BEFORE transaction COMMIT
    if let Some(hook) = pre_commit_hook {
        if let Err(e) = hook().await {
            // Rollback transaction if hook fails
            if let Err(rollback_err) = tx.rollback().await {
                tracing::error!(
                    "{db_name} ROLLBACK failed after pre-commit hook error: {}",
                    rollback_err
                );
            }
            return Err(crate::error::CdcError::generic(format!(
                "{db_name} pre-commit hook failed, transaction rolled back: {}",
                e
            )));
        }
    }

    // Commit the transaction
    tx.commit().await.map_err(|e| {
        crate::error::CdcError::generic(format!("{db_name} COMMIT transaction failed: {e}"))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_schema() {
        let mut mappings = HashMap::new();
        mappings.insert("public".to_string(), "cdc_db".to_string());

        assert_eq!(map_schema(&mappings, "public"), "cdc_db");
        assert_eq!(map_schema(&mappings, "other"), "other");
    }
}
