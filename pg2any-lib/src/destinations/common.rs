/// Common utilities and traits for destination implementations
use crate::error::{CdcError, Result};
use std::future::Future;

/// Shared transaction lifecycle: operation → hook → commit, with rollback on any failure.
///
/// Encapsulates the repeated BEGIN/execute/hook/COMMIT/ROLLBACK pattern used
/// by MySQL and SQL Server destinations.
pub(crate) async fn execute_with_hook_guard<OpFut, RbFut, CmFut>(
    operation: OpFut,
    pre_commit_hook: Option<super::PreCommitHook>,
    rollback: RbFut,
    commit: CmFut,
    context: &str,
) -> Result<()>
where
    OpFut: Future<Output = std::result::Result<(), CdcError>>,
    RbFut: Future<Output = ()>,
    CmFut: Future<Output = std::result::Result<(), CdcError>>,
{
    tokio::pin!(rollback);
    tokio::pin!(commit);

    if let Err(e) = operation.await {
        tracing::debug!("{context} operation failed, rolling back: {e}");
        rollback.await;
        return Err(e);
    }

    if let Some(hook) = pre_commit_hook {
        if let Err(e) = hook().await {
            tracing::debug!("{context} pre-commit hook failed, rolling back: {e}");
            rollback.await;
            return Err(CdcError::generic(format!(
                "{context} pre-commit hook failed, transaction rolled back: {e}"
            )));
        }
    }

    commit.await
}

/// Run pre-commit hook then commit. Rolls back on hook failure.
/// Call this after the main operation has already succeeded within an active transaction.
pub(crate) async fn commit_with_hook<CmFut, RbFut>(
    pre_commit_hook: Option<super::PreCommitHook>,
    commit: CmFut,
    rollback: RbFut,
    context: &str,
) -> Result<()>
where
    CmFut: Future<Output = std::result::Result<(), CdcError>>,
    RbFut: Future<Output = ()>,
{
    tokio::pin!(commit);
    tokio::pin!(rollback);

    if let Some(hook) = pre_commit_hook {
        if let Err(e) = hook().await {
            rollback.await;
            return Err(CdcError::generic(format!(
                "{context} pre-commit hook failed, rolled back: {e}"
            )));
        }
    }

    commit.await
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
    commands: &[std::borrow::Cow<'_, str>],
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
        if let Err(e) = sqlx::query(sql.as_ref()).execute(&mut *tx).await {
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_hook_guard_success_path() {
        let committed = Arc::new(AtomicBool::new(false));
        let committed_clone = committed.clone();

        let result = execute_with_hook_guard(
            async { Ok::<(), CdcError>(()) },
            None,
            async {},
            async move {
                committed_clone.store(true, Ordering::SeqCst);
                Ok::<(), CdcError>(())
            },
            "test",
        )
        .await;

        assert!(result.is_ok());
        assert!(committed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_hook_guard_operation_failure_triggers_rollback() {
        let rolled_back = Arc::new(AtomicBool::new(false));
        let rolled_back_clone = rolled_back.clone();
        let committed = Arc::new(AtomicBool::new(false));
        let committed_clone = committed.clone();

        let result = execute_with_hook_guard(
            async { Err::<(), CdcError>(CdcError::generic("op failed")) },
            None,
            async move {
                rolled_back_clone.store(true, Ordering::SeqCst);
            },
            async move {
                committed_clone.store(true, Ordering::SeqCst);
                Ok::<(), CdcError>(())
            },
            "test",
        )
        .await;

        assert!(result.is_err());
        assert!(rolled_back.load(Ordering::SeqCst));
        assert!(!committed.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_hook_guard_hook_failure_triggers_rollback() {
        let rolled_back = Arc::new(AtomicBool::new(false));
        let rolled_back_clone = rolled_back.clone();
        let committed = Arc::new(AtomicBool::new(false));
        let committed_clone = committed.clone();

        let hook: Option<crate::destinations::PreCommitHook> = Some(Box::new(|| {
            Box::pin(async { Err(CdcError::generic("hook failed")) })
        }));

        let result = execute_with_hook_guard(
            async { Ok::<(), CdcError>(()) },
            hook,
            async move {
                rolled_back_clone.store(true, Ordering::SeqCst);
            },
            async move {
                committed_clone.store(true, Ordering::SeqCst);
                Ok::<(), CdcError>(())
            },
            "test",
        )
        .await;

        assert!(result.is_err());
        assert!(rolled_back.load(Ordering::SeqCst));
        assert!(!committed.load(Ordering::SeqCst));
    }
}
