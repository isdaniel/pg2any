#[cfg(feature = "sqlite")]
pub(crate) async fn execute_sqlx_batch_with_hook(
    pool: &sqlx::SqlitePool,
    commands: &[std::borrow::Cow<'_, str>],
    pre_commit_hook: Option<super::destination_factory::PreCommitHook>,
    db_name: &str,
) -> crate::error::Result<()> {
    let mut tx = pool.begin().await.map_err(|e| {
        crate::error::CdcError::generic(format!("{db_name} BEGIN transaction failed: {e}"))
    })?;

    for (idx, sql) in commands.iter().enumerate() {
        let owned = sql.to_string();
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(owned))
            .execute(&mut *tx)
            .await
        {
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

    if let Some(hook) = pre_commit_hook {
        if let Err(e) = hook().await {
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

    tx.commit().await.map_err(|e| {
        crate::error::CdcError::generic(format!("{db_name} COMMIT transaction failed: {e}"))
    })?;

    Ok(())
}
