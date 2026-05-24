#[cfg(feature = "sqlserver")]
mod sqlserver_bulk_insert {
    use pg2any_lib::destinations::bulk_insert::{
        build_multi_value_insert, detect_bulk_insert_batch,
    };

    #[test]
    fn test_detect_bracket_quoted_inserts() {
        let stmts = vec![
            "INSERT INTO [dbo].[users] ([id], [name]) VALUES (1, 'alice');".to_string(),
            "INSERT INTO [dbo].[users] ([id], [name]) VALUES (2, 'bob');".to_string(),
            "INSERT INTO [dbo].[users] ([id], [name]) VALUES (3, 'carol');".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some(), "Should detect bracket-quoted INSERT batch");
        let parsed = result.unwrap();
        assert_eq!(parsed.table, "[dbo].[users]");
        assert_eq!(parsed.columns, vec!["[id]", "[name]"]);
        assert_eq!(parsed.rows.len(), 3);
        assert_eq!(parsed.rows[0], vec!["1", "'alice'"]);
    }

    #[test]
    fn test_detect_bracket_quoted_large_batch() {
        let stmts: Vec<String> = (0..500)
            .map(|i| {
                format!(
                    "INSERT INTO [cdc_db].[dbo].[orders] ([id], [amount], [status]) VALUES ({}, {}.50, 'pending');",
                    i, i
                )
            })
            .collect();

        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.table, "[cdc_db].[dbo].[orders]");
        assert_eq!(parsed.columns.len(), 3);
        assert_eq!(parsed.rows.len(), 500);
    }

    #[test]
    fn test_detect_mixed_tables_returns_none() {
        let stmts = vec![
            "INSERT INTO [dbo].[t1] ([id]) VALUES (1);".to_string(),
            "INSERT INTO [dbo].[t2] ([id]) VALUES (2);".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_multi_value_insert_brackets() {
        let table = "[dbo].[users]";
        let columns = vec!["[id]".to_string(), "[name]".to_string()];
        let rows = vec![
            vec!["1".to_string(), "'alice'".to_string()],
            vec!["2".to_string(), "'bob'".to_string()],
        ];
        let sql = build_multi_value_insert(table, &columns, &rows);
        assert_eq!(
            sql,
            "INSERT INTO [dbo].[users] ([id], [name]) VALUES (1, 'alice'), (2, 'bob');"
        );
    }

    #[test]
    fn test_null_values_in_batch() {
        let stmts = vec![
            "INSERT INTO [dbo].[t] ([id], [name], [age]) VALUES (1, NULL, 25);".to_string(),
            "INSERT INTO [dbo].[t] ([id], [name], [age]) VALUES (2, 'bob', NULL);".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.rows[0], vec!["1", "NULL", "25"]);
        assert_eq!(parsed.rows[1], vec!["2", "'bob'", "NULL"]);
    }

    #[test]
    fn test_string_with_special_characters() {
        let stmts = vec![
            "INSERT INTO [dbo].[t] ([id], [val]) VALUES (1, 'it''s a test');".to_string(),
            "INSERT INTO [dbo].[t] ([id], [val]) VALUES (2, 'hello, world');".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.rows[0][1], "'it''s a test'");
        assert_eq!(parsed.rows[1][1], "'hello, world'");
    }

    #[test]
    fn test_multi_value_insert_with_nulls() {
        let table = "[dbo].[users]";
        let columns = vec![
            "[id]".to_string(),
            "[name]".to_string(),
            "[age]".to_string(),
        ];
        let rows = vec![
            vec!["1".to_string(), "'alice'".to_string(), "30".to_string()],
            vec!["2".to_string(), "NULL".to_string(), "25".to_string()],
        ];
        let sql = build_multi_value_insert(table, &columns, &rows);
        assert_eq!(
            sql,
            "INSERT INTO [dbo].[users] ([id], [name], [age]) VALUES (1, 'alice', 30), (2, NULL, 25);"
        );
    }

    #[test]
    fn test_three_part_table_name() {
        let stmts = vec![
            "INSERT INTO [mydb].[dbo].[orders] ([id], [total]) VALUES (1, 99.99);".to_string(),
            "INSERT INTO [mydb].[dbo].[orders] ([id], [total]) VALUES (2, 150.00);".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.table, "[mydb].[dbo].[orders]");
    }

    #[test]
    fn test_large_batch_performance() {
        let stmts: Vec<String> = (0..2000)
            .map(|i| {
                format!(
                    "INSERT INTO [dbo].[perf_test] ([id], [value], [label]) VALUES ({}, {}.5, 'item_{}');",
                    i, i, i
                )
            })
            .collect();

        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.rows.len(), 2000);
    }

    #[test]
    fn test_non_insert_statements_return_none() {
        let stmts = vec!["UPDATE [dbo].[t] SET [name] = 'new' WHERE [id] = 1;".to_string()];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }

    #[test]
    fn test_empty_batch() {
        let stmts: Vec<String> = vec![];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }
}
