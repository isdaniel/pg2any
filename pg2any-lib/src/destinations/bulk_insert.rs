//! Shared bulk insert utilities: INSERT batch detection and multi-value INSERT generation.
//!
//! This module is conditionally compiled when either `mysql` or `sqlserver` feature is enabled.
//! Destination-specific bulk load logic lives in each destination's own module.

#[derive(Debug, Clone)]
pub struct ParsedBulkInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

/// Parse the INSERT prefix from a single SQL statement.
/// Returns `(prefix_str, columns, table)` if the statement is a valid INSERT.
pub fn extract_insert_prefix(sql: &str) -> Option<(String, Vec<String>, String)> {
    parse_insert_prefix(sql)
}

/// Extract just the VALUES tuple from a statement whose prefix is already known.
/// `values_suffix` should be the part of the statement after the prefix (e.g., "(1, 'hello');").
pub fn extract_values_from_suffix(values_suffix: &str) -> Option<Vec<String>> {
    parse_values_tuple(values_suffix)
}

pub fn detect_bulk_insert_batch(statements: &[String]) -> Option<ParsedBulkInsert> {
    if statements.is_empty() {
        return None;
    }

    let first = parse_insert_prefix(&statements[0])?;
    let expected_prefix = &first.0;
    let columns = first.1.clone();
    let table = first.2.clone();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(statements.len());

    let first_trimmed = statements[0].trim();
    let values = parse_values_tuple(&first_trimmed[expected_prefix.len()..])?;
    rows.push(values);

    for stmt in &statements[1..] {
        let trimmed_stmt = stmt.trim();
        if !trimmed_stmt.starts_with(expected_prefix.as_str()) {
            return None;
        }
        let values = parse_values_tuple(&trimmed_stmt[expected_prefix.len()..])?;
        rows.push(values);
    }

    Some(ParsedBulkInsert {
        table,
        columns,
        rows,
    })
}

pub fn build_multi_value_insert(table: &str, columns: &[String], rows: &[Vec<String>]) -> String {
    let col_list = columns.join(", ");
    let mut sql = format!("INSERT INTO {} ({}) VALUES ", table, col_list);
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (j, val) in row.iter().enumerate() {
            if j > 0 {
                sql.push_str(", ");
            }
            sql.push_str(val);
        }
        sql.push(')');
    }
    sql.push(';');
    sql
}

/// Builds chunked multi-value INSERT statements that respect a maximum byte size per statement.
/// MySQL's default `max_allowed_packet` is 64MB but can be as low as 4MB in some configurations.
/// We use 4MB as a safe default to avoid exceeding the limit.
const DEFAULT_MAX_STATEMENT_BYTES: usize = 4 * 1024 * 1024;

pub fn build_chunked_multi_value_inserts(
    table: &str,
    columns: &[String],
    rows: &[Vec<String>],
    max_bytes: Option<usize>,
) -> Vec<String> {
    let max = max_bytes.unwrap_or(DEFAULT_MAX_STATEMENT_BYTES);
    let col_list = columns.join(", ");
    let prefix = format!("INSERT INTO {} ({}) VALUES ", table, col_list);

    let mut statements = Vec::new();
    let mut current = prefix.clone();
    let mut row_count = 0;

    for row in rows {
        let mut tuple = String::from("(");
        for (j, val) in row.iter().enumerate() {
            if j > 0 {
                tuple.push_str(", ");
            }
            tuple.push_str(val);
        }
        tuple.push(')');

        let addition_len = if row_count == 0 {
            tuple.len()
        } else {
            2 + tuple.len() // ", " + tuple
        };

        if row_count > 0 && current.len() + addition_len + 1 > max {
            current.push(';');
            statements.push(current);
            current = prefix.clone();
            row_count = 0;
        }

        if row_count > 0 {
            current.push_str(", ");
        }
        current.push_str(&tuple);
        row_count += 1;
    }

    if row_count > 0 {
        current.push(';');
        statements.push(current);
    }

    statements
}

fn parse_insert_prefix(sql: &str) -> Option<(String, Vec<String>, String)> {
    let trimmed = sql.trim();
    if !trimmed
        .get(..7)
        .map(|s| s.eq_ignore_ascii_case("INSERT "))
        .unwrap_or(false)
    {
        return None;
    }

    let values_pos = find_case_insensitive(trimmed, " VALUES ")?;
    let prefix = &trimmed[..values_pos + 8];

    let into_pos = find_case_insensitive(trimmed, "INTO ")?;
    let after_into = &trimmed[into_pos + 5..];

    let col_paren_pos = after_into.find('(')?;
    let table = after_into[..col_paren_pos].trim().to_string();

    let col_section = &after_into[col_paren_pos..];
    let close_paren = col_section.find(')')?;
    let col_list = &col_section[1..close_paren];
    let columns = split_quoted_identifiers(col_list);

    Some((prefix.to_string(), columns, table))
}

/// Split a column list respecting quoted identifiers (backticks, brackets, double-quotes).
/// Handles escaped quotes: `]]` in brackets, `` `` `` in backticks, `""` in double-quotes.
fn split_quoted_identifiers(col_list: &str) -> Vec<String> {
    let mut columns = Vec::new();
    let mut current = String::new();
    let mut in_quote = false;
    let mut quote_char = ' ';
    let mut chars = col_list.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '`' | '"' if !in_quote => {
                in_quote = true;
                quote_char = ch;
                current.push(ch);
            }
            '[' if !in_quote => {
                in_quote = true;
                quote_char = ']';
                current.push(ch);
            }
            ']' if in_quote && quote_char == ']' => {
                current.push(ch);
                if chars.peek() == Some(&']') {
                    current.push(chars.next().unwrap());
                } else {
                    in_quote = false;
                }
            }
            c if in_quote && c == quote_char => {
                current.push(c);
                if chars.peek() == Some(&quote_char) {
                    current.push(chars.next().unwrap());
                } else {
                    in_quote = false;
                }
            }
            ',' if !in_quote => {
                columns.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.trim().is_empty() {
        columns.push(current.trim().to_string());
    }
    columns
}

fn find_case_insensitive(haystack: &str, needle: &str) -> Option<usize> {
    let h = haystack.as_bytes();
    let n = needle.as_bytes();
    let n_len = n.len();
    if h.len() < n_len {
        return None;
    }
    for i in 0..=(h.len() - n_len) {
        if h[i..i + n_len].eq_ignore_ascii_case(n) {
            return Some(i);
        }
    }
    None
}

fn parse_values_tuple(values_part: &str) -> Option<Vec<String>> {
    let trimmed = values_part
        .trim()
        .strip_suffix(';')
        .unwrap_or(values_part.trim())
        .trim();
    if !trimmed.starts_with('(') || !trimmed.ends_with(')') {
        return None;
    }
    let inner = &trimmed[1..trimmed.len() - 1];

    let mut values = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = inner.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '\\' if in_quotes => {
                current.push(ch);
                if let Some(next) = chars.next() {
                    current.push(next);
                }
            }
            '\'' if !in_quotes => {
                in_quotes = true;
                current.push(ch);
            }
            '\'' if in_quotes => {
                current.push(ch);
                if chars.peek() == Some(&'\'') {
                    current.push(chars.next().unwrap());
                } else {
                    in_quotes = false;
                }
            }
            ',' if !in_quotes => {
                values.push(current.trim().to_string());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    if !current.is_empty() || !values.is_empty() {
        values.push(current.trim().to_string());
    }

    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_bulk_insert_same_table() {
        let stmts = vec![
            "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES (1, 'hello');".to_string(),
            "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES (2, 'world');".to_string(),
            "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES (3, 'foo');".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.table, "`cdc_db`.`t1`");
        assert_eq!(parsed.columns, vec!["`id`", "`name`"]);
        assert_eq!(parsed.rows.len(), 3);
        assert_eq!(parsed.rows[0], vec!["1", "'hello'"]);
    }

    #[test]
    fn test_detect_bulk_insert_backslash_escaped_quotes() {
        let stmts = vec![
            "INSERT INTO `db`.`t1` (`id`, `name`) VALUES (1, 'it\\'s here');".to_string(),
            "INSERT INTO `db`.`t1` (`id`, `name`) VALUES (2, 'she\\'s there');".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        let parsed = result.unwrap();
        assert_eq!(parsed.rows.len(), 2);
        assert_eq!(parsed.rows[0], vec!["1", "'it\\'s here'"]);
        assert_eq!(parsed.rows[1], vec!["2", "'she\\'s there'"]);
    }

    #[test]
    fn test_detect_bulk_insert_mixed_tables_returns_none() {
        let stmts = vec![
            "INSERT INTO `cdc_db`.`t1` (`id`) VALUES (1);".to_string(),
            "INSERT INTO `cdc_db`.`t2` (`id`) VALUES (2);".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }

    #[test]
    fn test_detect_bulk_insert_with_update_returns_none() {
        let stmts = vec![
            "INSERT INTO `cdc_db`.`t1` (`id`) VALUES (1);".to_string(),
            "UPDATE `cdc_db`.`t1` SET `id` = 2 WHERE `id` = 1;".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_none());
    }

    #[test]
    fn test_build_multi_value_insert() {
        let table = "`cdc_db`.`t1`";
        let columns = vec!["`id`".to_string(), "`name`".to_string()];
        let rows = vec![
            vec!["1".to_string(), "'hello'".to_string()],
            vec!["2".to_string(), "'world'".to_string()],
        ];
        let sql = build_multi_value_insert(table, &columns, &rows);
        assert_eq!(
            sql,
            "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES (1, 'hello'), (2, 'world');"
        );
    }

    #[test]
    fn test_split_quoted_identifiers_with_comma() {
        let cols = split_quoted_identifiers("`normal`, `has,comma`, `another`");
        assert_eq!(cols, vec!["`normal`", "`has,comma`", "`another`"]);
    }

    #[test]
    fn test_split_bracket_identifiers_with_comma() {
        let cols = split_quoted_identifiers("[normal], [has,comma], [another]");
        assert_eq!(cols, vec!["[normal]", "[has,comma]", "[another]"]);
    }

    #[test]
    fn test_split_bracket_identifiers_with_escaped_bracket() {
        let cols = split_quoted_identifiers("[normal], [has]]bracket], [another]");
        assert_eq!(cols, vec!["[normal]", "[has]]bracket]", "[another]"]);
    }

    #[test]
    fn test_split_backtick_identifiers_with_escaped_backtick() {
        let cols = split_quoted_identifiers("`normal`, `has``tick`, `another`");
        assert_eq!(cols, vec!["`normal`", "`has``tick`", "`another`"]);
    }

    #[test]
    fn test_split_double_quote_identifiers_with_escaped_quote() {
        let cols = split_quoted_identifiers("\"normal\", \"has\"\"quote\", \"another\"");
        assert_eq!(cols, vec!["\"normal\"", "\"has\"\"quote\"", "\"another\""]);
    }

    #[test]
    fn test_detect_bulk_insert_with_leading_whitespace() {
        let stmts = vec![
            "  INSERT INTO `db`.`t` (`id`) VALUES (1);  ".to_string(),
            "  INSERT INTO `db`.`t` (`id`) VALUES (2);  ".to_string(),
        ];
        let result = detect_bulk_insert_batch(&stmts);
        assert!(result.is_some());
        assert_eq!(result.unwrap().rows.len(), 2);
    }

    #[test]
    fn test_build_chunked_multi_value_inserts_single_chunk() {
        let table = "`db`.`t`";
        let columns = vec!["`id`".to_string(), "`name`".to_string()];
        let rows = vec![
            vec!["1".to_string(), "'a'".to_string()],
            vec!["2".to_string(), "'b'".to_string()],
        ];
        let result = build_chunked_multi_value_inserts(table, &columns, &rows, Some(1024));
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'a'), (2, 'b');"
        );
    }

    #[test]
    fn test_build_chunked_multi_value_inserts_splits_on_size() {
        let table = "`db`.`t`";
        let columns = vec!["`id`".to_string()];
        let rows = vec![
            vec!["1".to_string()],
            vec!["2".to_string()],
            vec!["3".to_string()],
        ];
        // Set max to just above prefix + one row to force splitting
        let prefix_len = "INSERT INTO `db`.`t` (`id`) VALUES ".len();
        let max = prefix_len + "(1), (2);".len(); // fits 2 rows
        let result = build_chunked_multi_value_inserts(table, &columns, &rows, Some(max));
        assert_eq!(result.len(), 2);
        assert!(result[0].contains("(1), (2)"));
        assert!(result[1].contains("(3)"));
    }
}
