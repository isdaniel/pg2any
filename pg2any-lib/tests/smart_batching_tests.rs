use pg2any_lib::transaction_manager::{TransactionManager, TransactionSegment};
use pg2any_lib::types::DestinationType;
use std::path::PathBuf;
use tempfile::TempDir;

async fn create_test_manager(temp_dir: &TempDir) -> std::sync::Arc<TransactionManager> {
    std::sync::Arc::new(
        TransactionManager::new(temp_dir.path(), DestinationType::MySQL, 64 * 1024 * 1024)
            .await
            .unwrap(),
    )
}

fn write_segment(dir: &std::path::Path, name: &str, content: &str) -> TransactionSegment {
    let path = dir.join(name);
    std::fs::write(&path, content).unwrap();
    TransactionSegment {
        path,
        statement_count: content.lines().filter(|l| !l.trim().is_empty()).count(),
    }
}

#[tokio::test]
async fn test_analyze_homogeneous_inserts_single_segment() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let content = "INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (1, 'Alice');\n\
                   INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (2, 'Bob');\n";
    let segment = write_segment(temp_dir.path(), "seg1.sql", content);

    let result = manager
        .analyze_transaction_content(&[segment])
        .await
        .unwrap();
    assert!(result.is_some());
    let (table, columns) = result.unwrap();
    assert_eq!(table, "`mydb`.`users`");
    assert_eq!(columns, vec!["`id`", "`name`"]);
}

#[tokio::test]
async fn test_analyze_homogeneous_inserts_multiple_segments() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let seg1 = write_segment(
        temp_dir.path(),
        "seg1.sql",
        "INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (1, 'Alice');\n",
    );
    let seg2 = write_segment(
        temp_dir.path(),
        "seg2.sql",
        "INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (2, 'Bob');\n",
    );

    let result = manager
        .analyze_transaction_content(&[seg1, seg2])
        .await
        .unwrap();
    assert!(result.is_some());
}

#[tokio::test]
async fn test_analyze_mixed_operations_returns_none() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let content = "INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (1, 'Alice');\n\
                   DELETE FROM `mydb`.`users` WHERE `id` = 2;\n";
    let segment = write_segment(temp_dir.path(), "seg1.sql", content);

    let result = manager
        .analyze_transaction_content(&[segment])
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_analyze_different_tables_returns_none() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let content = "INSERT INTO `mydb`.`users` (`id`, `name`) VALUES (1, 'Alice');\n\
                   INSERT INTO `mydb`.`orders` (`id`, `total`) VALUES (1, 100);\n";
    let segment = write_segment(temp_dir.path(), "seg1.sql", content);

    let result = manager
        .analyze_transaction_content(&[segment])
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_analyze_empty_segments() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let segment = write_segment(temp_dir.path(), "seg1.sql", "\n\n");
    let result = manager
        .analyze_transaction_content(&[segment])
        .await
        .unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_batch_delete_files() {
    let temp_dir = TempDir::new().unwrap();
    let manager = create_test_manager(&temp_dir).await;

    let f1 = temp_dir.path().join("file1.sql");
    let f2 = temp_dir.path().join("file2.sql");
    let f3 = temp_dir.path().join("nonexistent.sql");
    std::fs::write(&f1, "data1").unwrap();
    std::fs::write(&f2, "data2").unwrap();

    let paths: Vec<PathBuf> = vec![f1.clone(), f2.clone(), f3];
    manager.batch_delete_files(&paths).await;

    assert!(!f1.exists());
    assert!(!f2.exists());
}

#[tokio::test]
async fn test_analyze_sqlserver_bracket_inserts() {
    let temp_dir = TempDir::new().unwrap();
    let manager = std::sync::Arc::new(
        TransactionManager::new(
            temp_dir.path(),
            DestinationType::SqlServer,
            64 * 1024 * 1024,
        )
        .await
        .unwrap(),
    );

    let content = "INSERT INTO [dbo].[users] ([id], [name]) VALUES (1, 'Alice');\n\
                   INSERT INTO [dbo].[users] ([id], [name]) VALUES (2, 'Bob');\n";
    let segment = write_segment(temp_dir.path(), "seg1.sql", content);

    let result = manager
        .analyze_transaction_content(&[segment])
        .await
        .unwrap();
    assert!(result.is_some());
    let (table, columns) = result.unwrap();
    assert_eq!(table, "[dbo].[users]");
    assert_eq!(columns, vec!["[id]", "[name]"]);
}
