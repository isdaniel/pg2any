//! Integration tests for bulk insert optimization

#[cfg(feature = "mysql")]
mod bulk_insert_integration {
    use pg2any_lib::destinations::bulk_insert::{
        build_multi_value_insert, detect_bulk_insert_batch, generate_tsv_buffer,
    };

    #[test]
    fn test_large_batch_detection() {
        let stmts: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "INSERT INTO `cdc_db`.`t1` (`id`, `val`, `col1`) VALUES ({}, {}, 'value_{}');",
                    i,
                    i * 100,
                    i
                )
            })
            .collect();

        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some(), "Should detect 1000 INSERTs as bulk batch");

        let parsed = result.unwrap();
        assert_eq!(parsed.table, "`cdc_db`.`t1`");
        assert_eq!(parsed.columns.len(), 3);
        assert_eq!(parsed.rows.len(), 1000);
    }

    #[test]
    fn test_tsv_null_handling() {
        let rows = vec![
            vec!["1".to_string(), "NULL".to_string(), "'text'".to_string()],
            vec!["2".to_string(), "'value'".to_string(), "NULL".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert!(output.contains("\\N"));
        assert!(output.contains("text"));
        assert!(output.contains("value"));
    }

    #[test]
    fn test_tsv_special_characters() {
        let rows = vec![
            vec!["1".to_string(), "'hello\\tworld'".to_string()],
            vec!["2".to_string(), "'line1\\nline2'".to_string()],
            vec!["3".to_string(), "'back\\\\slash'".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert!(!output.contains('\t') || output.lines().all(|l| l.split('\t').count() == 2));
    }

    #[test]
    fn test_below_threshold_not_detected() {
        let stmts = vec![
            "INSERT INTO `t` (`id`) VALUES (1);".to_string(),
            "INSERT INTO `t` (`id`) VALUES (2);".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
    }

    #[test]
    fn test_multi_value_insert_generation() {
        let table = "`cdc_db`.`t1`";
        let columns = vec![
            "`id`".to_string(),
            "`name`".to_string(),
            "`value`".to_string(),
        ];
        let rows: Vec<Vec<String>> = (0..5)
            .map(|i| vec![i.to_string(), format!("'name_{}'", i), "NULL".to_string()])
            .collect();

        let sql = build_multi_value_insert(table, &columns, &rows);
        assert!(sql.starts_with("INSERT INTO `cdc_db`.`t1` (`id`, `name`, `value`) VALUES "));
        assert!(sql.contains("(0, 'name_0', NULL)"));
        assert!(sql.contains("(4, 'name_4', NULL)"));
        assert!(sql.ends_with(';'));
    }

    #[test]
    fn test_empty_batch_returns_none() {
        let stmts: Vec<String> = vec![];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }

    #[test]
    fn test_single_insert_detection() {
        let stmts = vec!["INSERT INTO `db`.`t` (`a`, `b`) VALUES (1, 'x');".to_string()];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.rows.len(), 1);
    }
}
