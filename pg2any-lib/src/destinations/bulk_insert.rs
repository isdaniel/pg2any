#[cfg(feature = "mysql")]
#[derive(Debug, Clone)]
pub struct ParsedBulkInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub rows: Vec<Vec<String>>,
}

#[cfg(feature = "mysql")]
pub fn detect_bulk_insert_batch(statements: &[String]) -> Option<ParsedBulkInsert> {
    if statements.is_empty() {
        return None;
    }

    let first = parse_insert_prefix(&statements[0])?;
    let expected_prefix = &first.0;
    let columns = first.1.clone();
    let table = first.2.clone();

    let mut rows: Vec<Vec<String>> = Vec::with_capacity(statements.len());
    for stmt in statements {
        let parsed = parse_insert_prefix(stmt)?;
        if &parsed.0 != expected_prefix {
            return None;
        }
        let values = parse_values_tuple(&stmt[parsed.0.len()..])?;
        rows.push(values);
    }

    Some(ParsedBulkInsert {
        table,
        columns,
        rows,
    })
}

#[cfg(feature = "mysql")]
fn parse_insert_prefix(sql: &str) -> Option<(String, Vec<String>, String)> {
    let trimmed = sql.trim();
    if !trimmed
        .get(..7)
        .map(|s| s.eq_ignore_ascii_case("INSERT "))
        .unwrap_or(false)
    {
        return None;
    }

    let upper = trimmed.to_uppercase();
    let values_pos = upper.find(" VALUES ")?;
    let prefix = &trimmed[..values_pos + 8];

    let into_pos = upper.find("INTO ")?;
    let after_into = &trimmed[into_pos + 5..];

    let col_paren_pos = after_into.find('(')?;
    let table = after_into[..col_paren_pos].trim().to_string();

    let col_section = &after_into[col_paren_pos..];
    let close_paren = col_section.find(')')?;
    let col_list = &col_section[1..close_paren];
    let columns: Vec<String> = col_list.split(',').map(|c| c.trim().to_string()).collect();

    Some((prefix.to_string(), columns, table))
}

#[cfg(feature = "mysql")]
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

#[cfg(feature = "mysql")]
pub fn generate_tsv_buffer(rows: &[Vec<String>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(rows.len() * 128);

    for row in rows {
        for (col_idx, value) in row.iter().enumerate() {
            if col_idx > 0 {
                buf.push(b'\t');
            }
            let trimmed = value.trim();
            if trimmed.eq_ignore_ascii_case("NULL") {
                buf.extend_from_slice(b"\\N");
            } else if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
                let unquoted = &trimmed[1..trimmed.len() - 1];
                tsv_escape_string(unquoted, &mut buf);
            } else {
                tsv_escape_raw(trimmed.as_bytes(), &mut buf);
            }
        }
        buf.push(b'\n');
    }

    buf
}

#[cfg(feature = "mysql")]
fn tsv_escape_string(s: &str, buf: &mut Vec<u8>) {
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                if chars.clone().next() == Some('\'') {
                    chars.next();
                }
                buf.push(b'\'');
            }
            '\\' => buf.extend_from_slice(b"\\\\"),
            '\t' => buf.extend_from_slice(b"\\t"),
            '\n' => buf.extend_from_slice(b"\\n"),
            '\r' => buf.extend_from_slice(b"\\r"),
            '\0' => buf.extend_from_slice(b"\\0"),
            _ => {
                let mut bytes = [0u8; 4];
                buf.extend_from_slice(ch.encode_utf8(&mut bytes).as_bytes());
            }
        }
    }
}

#[cfg(feature = "mysql")]
fn tsv_escape_raw(data: &[u8], buf: &mut Vec<u8>) {
    for &b in data {
        match b {
            b'\\' => buf.extend_from_slice(b"\\\\"),
            b'\t' => buf.extend_from_slice(b"\\t"),
            b'\n' => buf.extend_from_slice(b"\\n"),
            b'\r' => buf.extend_from_slice(b"\\r"),
            0 => buf.extend_from_slice(b"\\0"),
            _ => buf.push(b),
        }
    }
}

#[cfg(feature = "mysql")]
pub fn build_multi_value_insert(table: &str, columns: &[String], rows: &[Vec<String>]) -> String {
    let col_list = columns.join(", ");
    let mut sql = format!("INSERT INTO {} ({}) VALUES ", table, col_list);
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        sql.push_str(&row.join(", "));
        sql.push(')');
    }
    sql.push(';');
    sql
}

#[cfg(all(test, feature = "mysql"))]
mod tests {
    use super::*;

    #[test]
    fn test_tsv_generation_basic() {
        let rows = vec![
            vec!["1".to_string(), "'hello'".to_string(), "NULL".to_string()],
            vec!["2".to_string(), "'world'".to_string(), "42".to_string()],
        ];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert_eq!(output, "1\thello\t\\N\n2\tworld\t42\n");
    }

    #[test]
    fn test_tsv_generation_escaping() {
        let rows = vec![vec!["3".to_string(), "'it''s escaped'".to_string()]];
        let tsv = generate_tsv_buffer(&rows);
        let output = String::from_utf8(tsv).unwrap();
        assert!(output.contains("it's escaped"));
    }

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
}
