// ============================================================================
// Shared DML Coalescing Module
// ============================================================================
//
// Pure string transformations that merge consecutive DML statements into
// batch operations. Used by MySQL, SQLite, and SQL Server destinations.
//
// - INSERT batching → multi-value:   `INSERT INTO t VALUES (1), (2), (3);`
// - UPDATE batching → CASE-WHEN:     `UPDATE t SET col = CASE WHEN ... END WHERE ... OR ...;`
// - DELETE batching → OR-combined:   `DELETE FROM t WHERE (...) OR (...);`

use std::borrow::Cow;

/// Identifier quoting style used by the target database.
///
/// Each database uses a different character to quote identifiers (table/column names):
/// - MySQL uses backticks: `` `identifier` ``
/// - SQLite uses double quotes: `"identifier"`
/// - SQL Server uses brackets: `[identifier]`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteStyle {
    /// MySQL: `` `identifier` `` with ` `` ` escape
    Backtick,
    /// SQLite / SQL standard: `"identifier"` with `""` escape
    DoubleQuote,
    /// SQL Server: `[identifier]` with `]]` escape
    Bracket,
}

impl QuoteStyle {
    /// The character that opens an identifier quote.
    #[inline]
    fn open_char(self) -> u8 {
        match self {
            QuoteStyle::Backtick => b'`',
            QuoteStyle::DoubleQuote => b'"',
            QuoteStyle::Bracket => b'[',
        }
    }

    /// The character that closes an identifier quote (and is doubled to escape).
    #[inline]
    fn close_char(self) -> u8 {
        match self {
            QuoteStyle::Backtick => b'`',
            QuoteStyle::DoubleQuote => b'"',
            QuoteStyle::Bracket => b']',
        }
    }
}

// ============================================================================
// Quote-Aware SQL Parsing Utilities
// ============================================================================

/// Find a keyword in SQL text that is NOT inside a quoted string or identifier.
///
/// Handles:
/// - Single-quoted strings with `''` escape: `'it''s here'`
/// - Identifier-quoted names with doubled-close escape (backtick, double-quote, or bracket)
/// - Case-insensitive keyword matching
///
/// Returns the byte offset of the first match, or `None`.
pub(crate) fn find_keyword_outside_quotes(
    sql: &str,
    keyword: &str,
    quote_style: QuoteStyle,
) -> Option<usize> {
    let bytes = sql.as_bytes();
    let keyword_len = keyword.len();
    let open = quote_style.open_char();
    let close = quote_style.close_char();
    let mut pos = 0;

    while pos < bytes.len() {
        let ch = bytes[pos];

        // Enter single-quoted string
        if ch == b'\'' {
            pos += 1;
            while pos < bytes.len() {
                if bytes[pos] == b'\'' {
                    // Check for escaped quote ''
                    if pos + 1 < bytes.len() && bytes[pos + 1] == b'\'' {
                        pos += 2;
                        continue;
                    }
                    break; // closing quote
                }
                pos += 1;
            }
            pos += 1; // skip closing quote
            continue;
        }

        // Enter identifier-quoted name
        if ch == open {
            pos += 1;
            while pos < bytes.len() {
                if bytes[pos] == close {
                    // Check for escaped close (doubled): ``, "", ]]
                    if pos + 1 < bytes.len() && bytes[pos + 1] == close {
                        pos += 2;
                        continue;
                    }
                    break;
                }
                pos += 1;
            }
            pos += 1;
            continue;
        }

        // Try keyword match at this position (byte-level to avoid UTF-8 boundary panics)
        if pos + keyword_len <= bytes.len()
            && bytes[pos..pos + keyword_len].eq_ignore_ascii_case(keyword.as_bytes())
        {
            return Some(pos);
        }

        pos += 1;
    }

    None
}

/// Parse the SET clause into (column, value) pairs.
///
/// Example (backtick): `` `col1` = val1, `col2` = 'hello, world', `col3` = NULL ``
/// Example (double-quote): `"col1" = val1, "col2" = 'hello, world'`
/// Example (bracket): `[col1] = val1, [col2] = 'hello, world'`
///
/// Handles quoted strings containing commas, identifier-quoted names, hex literals, NULL.
fn parse_set_pairs(set_clause: &str, quote_style: QuoteStyle) -> Vec<(&str, &str)> {
    let mut pairs = Vec::new();
    let bytes = set_clause.as_bytes();
    let open = quote_style.open_char();
    let close = quote_style.close_char();
    let mut pos = 0;

    while pos < bytes.len() {
        // Skip whitespace
        while pos < bytes.len() && bytes[pos].is_ascii_whitespace() {
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }

        // Expect identifier-quoted column name
        if bytes[pos] != open {
            break;
        }
        let col_start = pos;
        pos += 1; // skip opening quote
        while pos < bytes.len() {
            if bytes[pos] == close {
                // Check for escaped close (doubled)
                if pos + 1 < bytes.len() && bytes[pos + 1] == close {
                    pos += 2; // escaped
                    continue;
                }
                break;
            }
            pos += 1;
        }
        if pos >= bytes.len() {
            break;
        }
        pos += 1; // skip closing quote
        let col = &set_clause[col_start..pos];

        // Skip " = "
        while pos < bytes.len() && (bytes[pos].is_ascii_whitespace() || bytes[pos] == b'=') {
            pos += 1;
        }

        // Parse value — find end (comma outside quotes/parens, or end of string)
        let val_start = pos;
        let mut paren_depth = 0u32;
        while pos < bytes.len() {
            let ch = bytes[pos];

            if ch == b'\'' {
                // Skip single-quoted string
                pos += 1;
                while pos < bytes.len() {
                    if bytes[pos] == b'\'' {
                        if pos + 1 < bytes.len() && bytes[pos + 1] == b'\'' {
                            pos += 2; // escaped quote
                            continue;
                        }
                        break;
                    }
                    pos += 1;
                }
                if pos < bytes.len() {
                    pos += 1; // skip closing quote
                }
                continue;
            }

            if ch == b'(' {
                paren_depth += 1;
                pos += 1;
                continue;
            }

            if ch == b')' {
                paren_depth = paren_depth.saturating_sub(1);
                pos += 1;
                continue;
            }

            if ch == b',' && paren_depth == 0 {
                break; // value separator
            }

            pos += 1;
        }

        let val = set_clause[val_start..pos].trim();
        pairs.push((col, val));

        // Skip comma
        if pos < bytes.len() && bytes[pos] == b',' {
            pos += 1;
        }
    }

    pairs
}

/// Extract column signature from SET pairs for grouping.
/// Two UPDATE statements can be coalesced if they have the same table and column signature.
fn column_signature(pairs: &[(&str, &str)]) -> String {
    pairs
        .iter()
        .map(|(col, _)| *col)
        .collect::<Vec<_>>()
        .join(",")
}

// ============================================================================
// INSERT Coalescing
// ============================================================================

/// Parse an INSERT statement into its prefix and values parts.
///
/// Given: `` INSERT INTO `db`.`t` (`c1`, `c2`) VALUES (1, 'hello'); ``
/// Returns: `Some(("INSERT INTO `db`.`t` (`c1`, `c2`) VALUES ", "(1, 'hello')"))`
fn parse_insert_parts(sql: &str, quote_style: QuoteStyle) -> Option<(&str, &str)> {
    let trimmed = sql.trim();

    if !trimmed
        .get(..7)
        .map(|s| s.eq_ignore_ascii_case("INSERT "))
        .unwrap_or(false)
    {
        return None;
    }

    let values_keyword_pos = find_keyword_outside_quotes(trimmed, " VALUES ", quote_style)?;

    let prefix_end = values_keyword_pos + 8;
    let prefix = &trimmed[..prefix_end];

    let values_part = trimmed[prefix_end..].trim();
    let values_part = values_part.strip_suffix(';').unwrap_or(values_part).trim();

    if !values_part.starts_with('(') {
        return None;
    }

    Some((prefix, values_part))
}

// ============================================================================
// DELETE Coalescing
// ============================================================================

/// Parsed DELETE statement components.
struct ParsedDelete<'a> {
    /// Everything up to and including ` WHERE `: `"DELETE FROM `db`.`t` WHERE "`
    prefix: &'a str,
    /// The WHERE conditions without trailing semicolon: `` "`id` = 1 AND `type` = 'a'" ``
    where_clause: &'a str,
}

/// Parse a DELETE statement into table prefix and WHERE clause.
fn parse_delete_parts(sql: &str, quote_style: QuoteStyle) -> Option<ParsedDelete<'_>> {
    let trimmed = sql.trim();

    if !trimmed
        .get(..7)
        .map(|s| s.eq_ignore_ascii_case("DELETE "))
        .unwrap_or(false)
    {
        return None;
    }

    let where_pos = find_keyword_outside_quotes(trimmed, " WHERE ", quote_style)?;

    let prefix_end = where_pos + 7; // " WHERE " is 7 chars
    let prefix = &trimmed[..prefix_end];

    let where_clause = trimmed[prefix_end..].trim();
    let where_clause = where_clause
        .strip_suffix(';')
        .unwrap_or(where_clause)
        .trim();

    if where_clause.is_empty() {
        return None;
    }

    Some(ParsedDelete {
        prefix,
        where_clause,
    })
}

/// Build a coalesced DELETE from multiple parsed delete statements.
fn build_coalesced_delete(deletes: &[ParsedDelete<'_>]) -> String {
    debug_assert!(!deletes.is_empty());

    if deletes.len() == 1 {
        let d = &deletes[0];
        let mut out = String::with_capacity(d.prefix.len() + d.where_clause.len() + 1);
        out.push_str(d.prefix);
        out.push_str(d.where_clause);
        out.push(';');
        return out;
    }

    let where_total: usize = deletes.iter().map(|d| d.where_clause.len() + 6).sum();
    let mut out = String::with_capacity(deletes[0].prefix.len() + where_total + 1);
    out.push_str(deletes[0].prefix);
    for (i, d) in deletes.iter().enumerate() {
        if i > 0 {
            out.push_str(" OR ");
        }
        out.push('(');
        out.push_str(d.where_clause);
        out.push(')');
    }
    out.push(';');
    out
}

// ============================================================================
// UPDATE Coalescing
// ============================================================================

/// Parsed UPDATE statement components.
struct ParsedUpdate<'a> {
    /// The table reference: `` "UPDATE `db`.`t`" ``
    table: &'a str,
    /// Column-value pairs from the SET clause
    set_pairs: Vec<(&'a str, &'a str)>,
    /// The WHERE conditions: `` "`id` = 1" ``
    where_clause: &'a str,
}

/// Parse an UPDATE statement into table, SET pairs, and WHERE clause.
fn parse_update_parts(sql: &str, quote_style: QuoteStyle) -> Option<ParsedUpdate<'_>> {
    let trimmed = sql.trim();

    if !trimmed
        .get(..7)
        .map(|s| s.eq_ignore_ascii_case("UPDATE "))
        .unwrap_or(false)
    {
        return None;
    }

    // Find " SET " keyword outside quotes
    let set_pos = find_keyword_outside_quotes(trimmed, " SET ", quote_style)?;
    let table = &trimmed[..set_pos];
    let after_set = set_pos + 5; // " SET " is 5 chars

    // Find " WHERE " keyword outside quotes (searching in the remainder after SET)
    let rest = &trimmed[after_set..];
    let where_in_rest = find_keyword_outside_quotes(rest, " WHERE ", quote_style)?;

    let set_clause = &rest[..where_in_rest];
    let where_start = where_in_rest + 7; // " WHERE " is 7 chars
    let where_clause = rest[where_start..].trim();
    let where_clause = where_clause
        .strip_suffix(';')
        .unwrap_or(where_clause)
        .trim();

    if set_clause.is_empty() || where_clause.is_empty() {
        return None;
    }

    let set_pairs = parse_set_pairs(set_clause, quote_style);
    if set_pairs.is_empty() {
        return None;
    }

    Some(ParsedUpdate {
        table,
        set_pairs,
        where_clause,
    })
}

/// Build a coalesced UPDATE using CASE-WHEN from multiple parsed update statements.
///
/// Produces:
/// ```sql
/// UPDATE `db`.`t` SET
///   `col1` = CASE WHEN <w1> THEN v1_1 WHEN <w2> THEN v1_2 ELSE `col1` END,
///   `col2` = CASE WHEN <w1> THEN v2_1 WHEN <w2> THEN v2_2 ELSE `col2` END
/// WHERE (<w1>) OR (<w2>);
/// ```
fn build_coalesced_update(updates: &[ParsedUpdate<'_>]) -> String {
    debug_assert!(!updates.is_empty());

    if updates.len() == 1 {
        let u = &updates[0];
        let set_total: usize = u.set_pairs.iter().map(|(c, v)| c.len() + v.len() + 4).sum();
        let mut out =
            String::with_capacity(u.table.len() + 7 + set_total + 7 + u.where_clause.len() + 1);
        out.push_str(u.table);
        out.push_str(" SET ");
        for (i, (col, val)) in u.set_pairs.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            out.push_str(col);
            out.push_str(" = ");
            out.push_str(val);
        }
        out.push_str(" WHERE ");
        out.push_str(u.where_clause);
        out.push(';');
        return out;
    }

    let num_cols = updates[0].set_pairs.len();

    let mut case_size = 0usize;
    for col_idx in 0..num_cols {
        let col_name = updates[0].set_pairs[col_idx].0;
        case_size += col_name.len() * 2 + 16;
        for u in updates {
            if col_idx < u.set_pairs.len() {
                case_size += u.where_clause.len() + u.set_pairs[col_idx].1.len() + 10;
            }
        }
    }
    let where_size: usize = updates.iter().map(|u| u.where_clause.len() + 6).sum();
    let mut out =
        String::with_capacity(updates[0].table.len() + 7 + case_size + 7 + where_size + 1);

    out.push_str(updates[0].table);
    out.push_str(" SET ");
    for col_idx in 0..num_cols {
        if col_idx > 0 {
            out.push_str(", ");
        }
        let col_name = updates[0].set_pairs[col_idx].0;
        out.push_str(col_name);
        out.push_str(" = CASE");
        for u in updates {
            if col_idx < u.set_pairs.len() {
                out.push_str(" WHEN ");
                out.push_str(u.where_clause);
                out.push_str(" THEN ");
                out.push_str(u.set_pairs[col_idx].1);
            }
        }
        out.push_str(" ELSE ");
        out.push_str(col_name);
        out.push_str(" END");
    }
    out.push_str(" WHERE ");
    for (i, u) in updates.iter().enumerate() {
        if i > 0 {
            out.push_str(" OR ");
        }
        out.push('(');
        out.push_str(u.where_clause);
        out.push(')');
    }
    out.push(';');
    out
}

// ============================================================================
// Unified Command Coalescing
// ============================================================================

/// Coalesce consecutive DML statements targeting the same table into batch operations:
///
/// - INSERT batching → multi-value: `INSERT INTO t VALUES (1), (2), (3);`
/// - UPDATE batching → CASE-WHEN:   `UPDATE t SET col = CASE WHEN ... END WHERE ... OR ...;`
/// - DELETE batching → OR-combined: `DELETE FROM t WHERE (...) OR (...);`
///
/// Non-DML statements (TRUNCATE, etc.) pass through unchanged.
/// Coalescing only merges **consecutive** statements of the same type targeting the same table.
/// All strategies respect `max_packet_size` with an 80% safety margin.
pub(crate) fn coalesce_commands<'a>(
    commands: &'a [String],
    max_packet_size: u64,
    quote_style: QuoteStyle,
) -> Vec<Cow<'a, str>> {
    if commands.is_empty() {
        return Vec::new();
    }

    let safety_limit = if max_packet_size >= (usize::MAX as u64) {
        usize::MAX
    } else {
        ((max_packet_size as f64 * 0.8) as usize).max(1024)
    };
    let mut result: Vec<Cow<'a, str>> = Vec::with_capacity(commands.len());
    let mut i = 0;

    while i < commands.len() {
        let start_i = i;

        // ── Try INSERT coalescing ────────────────────────────────────
        if let Some((prefix, values)) = parse_insert_parts(&commands[i], quote_style) {
            let mut group_values: Vec<&str> = vec![values];
            let mut group_size = prefix.len() + values.len() + 1;
            i += 1;

            while i < commands.len() {
                if let Some((next_prefix, next_values)) =
                    parse_insert_parts(&commands[i], quote_style)
                {
                    if next_prefix == prefix {
                        let additional_size = 2 + next_values.len();
                        if group_size + additional_size <= safety_limit {
                            group_values.push(next_values);
                            group_size += additional_size;
                            i += 1;
                            continue;
                        }
                    }
                }
                break;
            }

            if group_values.len() == 1 {
                // Single-row group: the original statement is already a valid
                // INSERT — borrow it instead of rebuilding a new `String`.
                result.push(Cow::Borrowed(commands[start_i].as_str()));
            } else {
                result.push(Cow::Owned(format!(
                    "{}{};",
                    prefix,
                    group_values.join(", ")
                )));
            }
            continue;
        }

        // ── Try UPDATE coalescing (CASE-WHEN) ───────────────────────
        if let Some(first_update) = parse_update_parts(&commands[i], quote_style) {
            let col_sig = column_signature(&first_update.set_pairs);
            let table = first_update.table;
            let mut group: Vec<ParsedUpdate<'_>> = vec![first_update];
            let mut group_size = commands[i].len();
            i += 1;

            while i < commands.len() {
                if let Some(next_update) = parse_update_parts(&commands[i], quote_style) {
                    if next_update.table == table
                        && column_signature(&next_update.set_pairs) == col_sig
                    {
                        // Estimate additional size: each new row adds WHEN clauses per column + OR in WHERE
                        let num_cols = next_update.set_pairs.len();
                        let avg_val_len: usize = next_update
                            .set_pairs
                            .iter()
                            .map(|(_, v)| v.len())
                            .sum::<usize>()
                            / num_cols.max(1);
                        let additional = num_cols
                            * (6 + next_update.where_clause.len() + 6 + avg_val_len)
                            + 6
                            + next_update.where_clause.len();
                        if group_size + additional <= safety_limit {
                            group_size += additional;
                            group.push(next_update);
                            i += 1;
                            continue;
                        }
                    }
                }
                break;
            }

            if group.len() == 1 {
                result.push(Cow::Borrowed(commands[start_i].as_str()));
            } else {
                result.push(Cow::Owned(build_coalesced_update(&group)));
            }
            continue;
        }

        // ── Try DELETE coalescing (OR-combined WHERE) ────────────────
        if let Some(first_delete) = parse_delete_parts(&commands[i], quote_style) {
            let prefix = first_delete.prefix;
            let mut group: Vec<ParsedDelete<'_>> = vec![first_delete];
            let mut group_size = commands[i].len();
            i += 1;

            while i < commands.len() {
                if let Some(next_delete) = parse_delete_parts(&commands[i], quote_style) {
                    if next_delete.prefix == prefix {
                        // Additional size: " OR (where_clause)"
                        let additional = 5 + next_delete.where_clause.len() + 1; // " OR (" + where + ")"
                        if group_size + additional <= safety_limit {
                            group_size += additional;
                            group.push(next_delete);
                            i += 1;
                            continue;
                        }
                    }
                }
                break;
            }

            if group.len() == 1 {
                result.push(Cow::Borrowed(commands[start_i].as_str()));
            } else {
                result.push(Cow::Owned(build_coalesced_delete(&group)));
            }
            continue;
        }

        // ── Non-DML statement — pass through ────────────────────────
        result.push(Cow::Borrowed(commands[i].as_str()));
        i += 1;
    }

    result
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ════════════════════════════════════════════════════════════════════
    // find_keyword_outside_quotes — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_find_keyword_simple() {
        let sql = "UPDATE `t` SET `a` = 1 WHERE `id` = 1;";
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Backtick).is_some());
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Backtick).is_some());
    }

    #[test]
    fn test_find_keyword_inside_string_is_skipped() {
        let sql = "UPDATE `t` SET `query` = 'SELECT WHERE 1=1' WHERE `id` = 1;";
        let pos = find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Backtick).unwrap();
        // The real WHERE should be after the closing quote of the string value
        assert!(pos > sql.find("'SELECT").unwrap());
    }

    #[test]
    fn test_find_keyword_inside_backtick_is_skipped() {
        // When SET only appears inside a backtick-quoted identifier, it should not be found
        let sql_only_backtick = "INSERT INTO `my SET table` (`a`) VALUES (1);";
        assert!(
            find_keyword_outside_quotes(sql_only_backtick, " SET ", QuoteStyle::Backtick).is_none()
        );

        // When there's a real SET keyword AND one inside backticks, the real one is found
        let sql = "UPDATE `t` SET `my SET col` = 1 WHERE `id` = 1;";
        let set_pos = find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Backtick).unwrap();
        assert_eq!(&sql[set_pos..set_pos + 5], " SET ");
        // Real SET keyword (pos 10) comes before the backtick-quoted identifier (pos 15)
        assert!(set_pos < sql.find("`my SET col`").unwrap());
    }

    #[test]
    fn test_find_keyword_not_found() {
        let sql = "INSERT INTO `t` VALUES (1);";
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Backtick).is_none());
    }

    #[test]
    fn test_find_keyword_case_insensitive() {
        let sql = "update `t` set `a` = 1 where `id` = 1;";
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Backtick).is_some());
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Backtick).is_some());
    }

    #[test]
    fn test_find_keyword_escaped_quotes_in_string() {
        // String contains escaped quotes: 'it''s a SET test'
        let sql = "UPDATE `t` SET `a` = 'it''s a SET test' WHERE `id` = 1;";
        let set_pos = find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Backtick).unwrap();
        // Should find the keyword SET, not the one inside the string
        assert_eq!(&sql[set_pos..set_pos + 5], " SET ");
        let where_pos = find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Backtick).unwrap();
        assert!(where_pos > sql.find("test'").unwrap());
    }

    // ════════════════════════════════════════════════════════════════════
    // find_keyword_outside_quotes — DoubleQuote (SQLite)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_find_keyword_double_quote_simple() {
        let sql = r#"UPDATE "t" SET "a" = 1 WHERE "id" = 1;"#;
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::DoubleQuote).is_some());
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::DoubleQuote).is_some());
    }

    #[test]
    fn test_find_keyword_inside_double_quote_is_skipped() {
        let sql = r#"INSERT INTO "my SET table" ("a") VALUES (1);"#;
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::DoubleQuote).is_none());
    }

    #[test]
    fn test_find_keyword_double_quote_escaped() {
        // Column name with escaped double quote: "col""name"
        let sql = r#"UPDATE "t" SET "col""name" = 1 WHERE "id" = 1;"#;
        let set_pos = find_keyword_outside_quotes(sql, " SET ", QuoteStyle::DoubleQuote).unwrap();
        assert_eq!(&sql[set_pos..set_pos + 5], " SET ");
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::DoubleQuote).is_some());
    }

    // ════════════════════════════════════════════════════════════════════
    // find_keyword_outside_quotes — Bracket (SQL Server)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_find_keyword_bracket_simple() {
        let sql = "UPDATE [t] SET [a] = 1 WHERE [id] = 1;";
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Bracket).is_some());
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Bracket).is_some());
    }

    #[test]
    fn test_find_keyword_inside_bracket_is_skipped() {
        let sql = "INSERT INTO [my SET table] ([a]) VALUES (1);";
        assert!(find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Bracket).is_none());
    }

    #[test]
    fn test_find_keyword_bracket_escaped() {
        // Column name with escaped bracket: [col]]name]
        let sql = "UPDATE [t] SET [col]]name] = 1 WHERE [id] = 1;";
        let set_pos = find_keyword_outside_quotes(sql, " SET ", QuoteStyle::Bracket).unwrap();
        assert_eq!(&sql[set_pos..set_pos + 5], " SET ");
        assert!(find_keyword_outside_quotes(sql, " WHERE ", QuoteStyle::Bracket).is_some());
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_set_pairs — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_set_pairs_basic() {
        let pairs = parse_set_pairs("`id` = 1, `name` = 'hello'", QuoteStyle::Backtick);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("`id`", "1"));
        assert_eq!(pairs[1], ("`name`", "'hello'"));
    }

    #[test]
    fn test_parse_set_pairs_with_null() {
        let pairs = parse_set_pairs("`id` = 1, `name` = NULL, `age` = 30", QuoteStyle::Backtick);
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("`id`", "1"));
        assert_eq!(pairs[1], ("`name`", "NULL"));
        assert_eq!(pairs[2], ("`age`", "30"));
    }

    #[test]
    fn test_parse_set_pairs_with_quoted_string_containing_comma() {
        let pairs = parse_set_pairs(
            "`id` = 1, `addr` = 'hello, world', `age` = 25",
            QuoteStyle::Backtick,
        );
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("`id`", "1"));
        assert_eq!(pairs[1], ("`addr`", "'hello, world'"));
        assert_eq!(pairs[2], ("`age`", "25"));
    }

    #[test]
    fn test_parse_set_pairs_with_escaped_quotes() {
        let pairs = parse_set_pairs("`name` = 'it''s here', `id` = 1", QuoteStyle::Backtick);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("`name`", "'it''s here'"));
        assert_eq!(pairs[1], ("`id`", "1"));
    }

    #[test]
    fn test_parse_set_pairs_with_hex_literal() {
        let pairs = parse_set_pairs("`data` = X'deadbeef', `id` = 1", QuoteStyle::Backtick);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("`data`", "X'deadbeef'"));
        assert_eq!(pairs[1], ("`id`", "1"));
    }

    #[test]
    fn test_parse_set_pairs_with_boolean_values() {
        let pairs = parse_set_pairs("`active` = 1, `deleted` = 0", QuoteStyle::Backtick);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("`active`", "1"));
        assert_eq!(pairs[1], ("`deleted`", "0"));
    }

    #[test]
    fn test_parse_set_pairs_with_backslash_escapes() {
        let pairs = parse_set_pairs(
            r"`path` = 'C:\\Users\\test', `id` = 1",
            QuoteStyle::Backtick,
        );
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("`path`", r"'C:\\Users\\test'"));
        assert_eq!(pairs[1], ("`id`", "1"));
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_set_pairs — DoubleQuote (SQLite)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_set_pairs_double_quote_basic() {
        let pairs = parse_set_pairs(r#""id" = 1, "name" = 'hello'"#, QuoteStyle::DoubleQuote);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (r#""id""#, "1"));
        assert_eq!(pairs[1], (r#""name""#, "'hello'"));
    }

    #[test]
    fn test_parse_set_pairs_double_quote_with_null_and_comma() {
        let pairs = parse_set_pairs(
            r#""id" = 1, "addr" = 'hello, world', "bio" = NULL"#,
            QuoteStyle::DoubleQuote,
        );
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (r#""id""#, "1"));
        assert_eq!(pairs[1], (r#""addr""#, "'hello, world'"));
        assert_eq!(pairs[2], (r#""bio""#, "NULL"));
    }

    #[test]
    fn test_parse_set_pairs_double_quote_escaped_identifier() {
        // Column name: "col""name" (escaped double-quote in identifier)
        let pairs = parse_set_pairs(
            r#""col""name" = 'value', "id" = 1"#,
            QuoteStyle::DoubleQuote,
        );
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], (r#""col""name""#, "'value'"));
        assert_eq!(pairs[1], (r#""id""#, "1"));
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_set_pairs — Bracket (SQL Server)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_set_pairs_bracket_basic() {
        let pairs = parse_set_pairs("[id] = 1, [name] = 'hello'", QuoteStyle::Bracket);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("[id]", "1"));
        assert_eq!(pairs[1], ("[name]", "'hello'"));
    }

    #[test]
    fn test_parse_set_pairs_bracket_with_null_and_comma() {
        let pairs = parse_set_pairs(
            "[id] = 1, [addr] = 'hello, world', [bio] = NULL",
            QuoteStyle::Bracket,
        );
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], ("[id]", "1"));
        assert_eq!(pairs[1], ("[addr]", "'hello, world'"));
        assert_eq!(pairs[2], ("[bio]", "NULL"));
    }

    #[test]
    fn test_parse_set_pairs_bracket_escaped_identifier() {
        // Column name: [col]]name] (escaped bracket in identifier)
        let pairs = parse_set_pairs("[col]]name] = 'value', [id] = 1", QuoteStyle::Bracket);
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0], ("[col]]name]", "'value'"));
        assert_eq!(pairs[1], ("[id]", "1"));
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_insert_parts
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_insert_parts_basic() {
        let sql = "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES (1, 'hello');";
        let (prefix, values) = parse_insert_parts(sql, QuoteStyle::Backtick).expect("should parse");
        assert_eq!(prefix, "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES ");
        assert_eq!(values, "(1, 'hello')");
    }

    #[test]
    fn test_parse_insert_parts_no_semicolon() {
        let sql = "INSERT INTO `t` (`id`) VALUES (1)";
        let (prefix, values) = parse_insert_parts(sql, QuoteStyle::Backtick).expect("should parse");
        assert_eq!(prefix, "INSERT INTO `t` (`id`) VALUES ");
        assert_eq!(values, "(1)");
    }

    #[test]
    fn test_parse_insert_parts_non_insert() {
        assert!(parse_insert_parts(
            "UPDATE `t` SET `name` = 'x' WHERE `id` = 1;",
            QuoteStyle::Backtick
        )
        .is_none());
        assert!(
            parse_insert_parts("DELETE FROM `t` WHERE `id` = 1;", QuoteStyle::Backtick).is_none()
        );
        assert!(parse_insert_parts("TRUNCATE TABLE `t`;", QuoteStyle::Backtick).is_none());
        assert!(parse_insert_parts("", QuoteStyle::Backtick).is_none());
        assert!(parse_insert_parts("INSERT", QuoteStyle::Backtick).is_none());
    }

    #[test]
    fn test_parse_insert_parts_with_special_values() {
        // NULL
        let (_, v) = parse_insert_parts(
            "INSERT INTO `t` (`a`, `b`) VALUES (1, NULL);",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(v, "(1, NULL)");

        // Hex literal
        let (_, v) = parse_insert_parts(
            "INSERT INTO `t` (`a`, `b`) VALUES (1, X'deadbeef');",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(v, "(1, X'deadbeef')");

        // Escaped quotes
        let (_, v) = parse_insert_parts(
            "INSERT INTO `t` (`a`) VALUES ('it''s here');",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(v, "('it''s here')");

        // Backslash escapes
        let (_, v) = parse_insert_parts(
            r"INSERT INTO `t` (`a`) VALUES ('C:\\Users\\test');",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(v, r"('C:\\Users\\test')");
    }

    #[test]
    fn test_parse_insert_parts_case_insensitive() {
        let result = parse_insert_parts("insert into `t` (`id`) values (1);", QuoteStyle::Backtick);
        assert!(result.is_some());
    }

    #[test]
    fn test_parse_insert_parts_double_quote() {
        let sql = r#"INSERT INTO "t" ("id", "name") VALUES (1, 'hello');"#;
        let (prefix, values) =
            parse_insert_parts(sql, QuoteStyle::DoubleQuote).expect("should parse");
        assert_eq!(prefix, r#"INSERT INTO "t" ("id", "name") VALUES "#);
        assert_eq!(values, "(1, 'hello')");
    }

    #[test]
    fn test_parse_insert_parts_bracket() {
        let sql = "INSERT INTO [db].[t] ([id], [name]) VALUES (1, 'hello');";
        let (prefix, values) = parse_insert_parts(sql, QuoteStyle::Bracket).expect("should parse");
        assert_eq!(prefix, "INSERT INTO [db].[t] ([id], [name]) VALUES ");
        assert_eq!(values, "(1, 'hello')");
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_delete_parts — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_delete_parts_basic() {
        let d = parse_delete_parts("DELETE FROM `db`.`t` WHERE `id` = 1;", QuoteStyle::Backtick)
            .unwrap();
        assert_eq!(d.prefix, "DELETE FROM `db`.`t` WHERE ");
        assert_eq!(d.where_clause, "`id` = 1");
    }

    #[test]
    fn test_parse_delete_parts_composite_key() {
        let d = parse_delete_parts(
            "DELETE FROM `db`.`t` WHERE `id` = 1 AND `type` = 'a';",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(d.where_clause, "`id` = 1 AND `type` = 'a'");
    }

    #[test]
    fn test_parse_delete_parts_with_is_null() {
        let d = parse_delete_parts(
            "DELETE FROM `db`.`t` WHERE `id` = 1 AND `name` IS NULL;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(d.where_clause, "`id` = 1 AND `name` IS NULL");
    }

    #[test]
    fn test_parse_delete_parts_non_delete() {
        assert!(parse_delete_parts(
            "UPDATE `t` SET `a` = 1 WHERE `id` = 1;",
            QuoteStyle::Backtick
        )
        .is_none());
        assert!(parse_delete_parts("INSERT INTO `t` VALUES (1);", QuoteStyle::Backtick).is_none());
        assert!(parse_delete_parts("", QuoteStyle::Backtick).is_none());
    }

    #[test]
    fn test_parse_delete_parts_with_string_containing_where() {
        let d = parse_delete_parts(
            "DELETE FROM `db`.`t` WHERE `query` = 'SELECT WHERE 1' AND `id` = 1;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        // The WHERE keyword inside the string should be skipped
        assert!(d.where_clause.contains("`id` = 1"));
        assert!(d.where_clause.contains("'SELECT WHERE 1'"));
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_delete_parts — DoubleQuote (SQLite)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_delete_parts_double_quote() {
        let d = parse_delete_parts(
            r#"DELETE FROM "t" WHERE "id" = 1;"#,
            QuoteStyle::DoubleQuote,
        )
        .unwrap();
        assert_eq!(d.prefix, r#"DELETE FROM "t" WHERE "#);
        assert_eq!(d.where_clause, r#""id" = 1"#);
    }

    #[test]
    fn test_parse_delete_parts_double_quote_composite() {
        let d = parse_delete_parts(
            r#"DELETE FROM "t" WHERE "id" = 1 AND "type" = 'a';"#,
            QuoteStyle::DoubleQuote,
        )
        .unwrap();
        assert_eq!(d.where_clause, r#""id" = 1 AND "type" = 'a'"#);
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_delete_parts — Bracket (SQL Server)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_delete_parts_bracket() {
        let d = parse_delete_parts("DELETE FROM [db].[t] WHERE [id] = 1;", QuoteStyle::Bracket)
            .unwrap();
        assert_eq!(d.prefix, "DELETE FROM [db].[t] WHERE ");
        assert_eq!(d.where_clause, "[id] = 1");
    }

    #[test]
    fn test_parse_delete_parts_bracket_composite() {
        let d = parse_delete_parts(
            "DELETE FROM [db].[t] WHERE [id] = 1 AND [type] = 'a';",
            QuoteStyle::Bracket,
        )
        .unwrap();
        assert_eq!(d.where_clause, "[id] = 1 AND [type] = 'a'");
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_update_parts — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_update_parts_basic() {
        let u = parse_update_parts(
            "UPDATE `db`.`t` SET `name` = 'hello', `age` = 30 WHERE `id` = 1;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(u.table, "UPDATE `db`.`t`");
        assert_eq!(u.set_pairs.len(), 2);
        assert_eq!(u.set_pairs[0], ("`name`", "'hello'"));
        assert_eq!(u.set_pairs[1], ("`age`", "30"));
        assert_eq!(u.where_clause, "`id` = 1");
    }

    #[test]
    fn test_parse_update_parts_non_update() {
        assert!(
            parse_update_parts("DELETE FROM `t` WHERE `id` = 1;", QuoteStyle::Backtick).is_none()
        );
        assert!(parse_update_parts("INSERT INTO `t` VALUES (1);", QuoteStyle::Backtick).is_none());
        assert!(parse_update_parts("", QuoteStyle::Backtick).is_none());
    }

    #[test]
    fn test_parse_update_parts_multiple_columns() {
        let u = parse_update_parts(
            "UPDATE `db`.`t` SET `a` = 1, `b` = 'x', `c` = NULL WHERE `id` = 5;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(u.set_pairs.len(), 3);
        assert_eq!(u.set_pairs[0], ("`a`", "1"));
        assert_eq!(u.set_pairs[1], ("`b`", "'x'"));
        assert_eq!(u.set_pairs[2], ("`c`", "NULL"));
        assert_eq!(u.where_clause, "`id` = 5");
    }

    #[test]
    fn test_parse_update_parts_composite_where() {
        let u = parse_update_parts(
            "UPDATE `db`.`t` SET `val` = 'x' WHERE `k1` = 1 AND `k2` = 'a';",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(u.where_clause, "`k1` = 1 AND `k2` = 'a'");
    }

    #[test]
    fn test_parse_update_parts_with_string_containing_where() {
        let u = parse_update_parts(
            "UPDATE `db`.`t` SET `data` = 'has WHERE in it' WHERE `id` = 1;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(u.set_pairs[0], ("`data`", "'has WHERE in it'"));
        assert_eq!(u.where_clause, "`id` = 1");
    }

    #[test]
    fn test_parse_update_parts_with_string_containing_set() {
        let u = parse_update_parts(
            "UPDATE `db`.`t` SET `data` = 'has SET in it' WHERE `id` = 1;",
            QuoteStyle::Backtick,
        )
        .unwrap();
        assert_eq!(u.set_pairs[0], ("`data`", "'has SET in it'"));
        assert_eq!(u.where_clause, "`id` = 1");
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_update_parts — DoubleQuote (SQLite)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_update_parts_double_quote() {
        let u = parse_update_parts(
            r#"UPDATE "t" SET "name" = 'hello', "age" = 30 WHERE "id" = 1;"#,
            QuoteStyle::DoubleQuote,
        )
        .unwrap();
        assert_eq!(u.table, r#"UPDATE "t""#);
        assert_eq!(u.set_pairs.len(), 2);
        assert_eq!(u.set_pairs[0], (r#""name""#, "'hello'"));
        assert_eq!(u.set_pairs[1], (r#""age""#, "30"));
        assert_eq!(u.where_clause, r#""id" = 1"#);
    }

    #[test]
    fn test_parse_update_parts_double_quote_with_where_in_string() {
        let u = parse_update_parts(
            r#"UPDATE "t" SET "data" = 'has WHERE in it' WHERE "id" = 1;"#,
            QuoteStyle::DoubleQuote,
        )
        .unwrap();
        assert_eq!(u.set_pairs[0], (r#""data""#, "'has WHERE in it'"));
        assert_eq!(u.where_clause, r#""id" = 1"#);
    }

    // ════════════════════════════════════════════════════════════════════
    // parse_update_parts — Bracket (SQL Server)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_parse_update_parts_bracket() {
        let u = parse_update_parts(
            "UPDATE [db].[t] SET [name] = 'hello', [age] = 30 WHERE [id] = 1;",
            QuoteStyle::Bracket,
        )
        .unwrap();
        assert_eq!(u.table, "UPDATE [db].[t]");
        assert_eq!(u.set_pairs.len(), 2);
        assert_eq!(u.set_pairs[0], ("[name]", "'hello'"));
        assert_eq!(u.set_pairs[1], ("[age]", "30"));
        assert_eq!(u.where_clause, "[id] = 1");
    }

    #[test]
    fn test_parse_update_parts_bracket_with_where_in_string() {
        let u = parse_update_parts(
            "UPDATE [db].[t] SET [data] = 'has WHERE in it' WHERE [id] = 1;",
            QuoteStyle::Bracket,
        )
        .unwrap();
        assert_eq!(u.set_pairs[0], ("[data]", "'has WHERE in it'"));
        assert_eq!(u.where_clause, "[id] = 1");
    }

    // ════════════════════════════════════════════════════════════════════
    // INSERT coalescing (backtick — preserving existing tests)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_empty_commands() {
        let result = coalesce_commands(&[], 67108864, QuoteStyle::Backtick);
        assert!(result.is_empty());
    }

    #[test]
    fn test_coalesce_single_insert() {
        let commands = vec!["INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'hello');".to_string()];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'hello');"
        );
        // Single-row groups must be borrowed (no rebuild).
        assert!(matches!(result[0], Cow::Borrowed(_)));
    }

    #[test]
    fn test_coalesce_multiple_same_table_inserts() {
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'a');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (2, 'b');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (3, 'c');".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'a'), (2, 'b'), (3, 'c');"
        );
    }

    #[test]
    fn test_coalesce_different_tables_inserts() {
        let commands = vec![
            "INSERT INTO `db`.`t1` (`id`) VALUES (1);".to_string(),
            "INSERT INTO `db`.`t2` (`id`) VALUES (2);".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_different_columns_same_table_inserts() {
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'a');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `age`) VALUES (2, 30);".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_large_batch_inserts() {
        let commands: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "INSERT INTO `cdc_db`.`t1` (`id`, `name`) VALUES ({}, 'user_{}');",
                    i, i
                )
            })
            .collect();
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].starts_with("INSERT INTO `cdc_db`.`t1`"));
    }

    #[test]
    fn test_coalesce_insert_respects_max_packet_size() {
        let commands: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "INSERT INTO `db`.`t` (`id`, `name`) VALUES ({}, '{}');",
                    i,
                    "x".repeat(20)
                )
            })
            .collect();
        let result = coalesce_commands(&commands, 2000, QuoteStyle::Backtick);
        assert!(result.len() > 1);
        assert!(result.len() < 100);
    }

    #[test]
    fn test_coalesce_insert_max_packet_boundary() {
        let long_value = "x".repeat(400);
        let commands = vec![
            format!(
                "INSERT INTO `db`.`t` (`id`, `longcol`) VALUES (1, '{}');",
                long_value
            ),
            format!(
                "INSERT INTO `db`.`t` (`id`, `longcol`) VALUES (2, '{}');",
                long_value
            ),
            format!(
                "INSERT INTO `db`.`t` (`id`, `longcol`) VALUES (3, '{}');",
                long_value
            ),
        ];
        let result = coalesce_commands(&commands, 1375, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
        assert!(result[0].contains("), ("));
        assert!(!result[1].contains("), ("));
    }

    // ════════════════════════════════════════════════════════════════════
    // DELETE coalescing — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_single_delete() {
        let commands = vec!["DELETE FROM `db`.`t` WHERE `id` = 1;".to_string()];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "DELETE FROM `db`.`t` WHERE `id` = 1;");
    }

    #[test]
    fn test_coalesce_deletes_same_table() {
        let commands = vec![
            "DELETE FROM `db`.`t` WHERE `id` = 1;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 2;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 3;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "DELETE FROM `db`.`t` WHERE (`id` = 1) OR (`id` = 2) OR (`id` = 3);"
        );
    }

    #[test]
    fn test_coalesce_deletes_different_tables() {
        let commands = vec![
            "DELETE FROM `db`.`t1` WHERE `id` = 1;".to_string(),
            "DELETE FROM `db`.`t2` WHERE `id` = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_deletes_composite_key() {
        let commands = vec![
            "DELETE FROM `db`.`t` WHERE `k1` = 1 AND `k2` = 'a';".to_string(),
            "DELETE FROM `db`.`t` WHERE `k1` = 2 AND `k2` = 'b';".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "DELETE FROM `db`.`t` WHERE (`k1` = 1 AND `k2` = 'a') OR (`k1` = 2 AND `k2` = 'b');"
        );
    }

    #[test]
    fn test_coalesce_deletes_with_is_null() {
        let commands = vec![
            "DELETE FROM `db`.`t` WHERE `id` = 1 AND `name` IS NULL;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 2 AND `name` IS NULL;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("OR"));
    }

    #[test]
    fn test_coalesce_deletes_respects_packet_limit() {
        let commands: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "DELETE FROM `db`.`t` WHERE `id` = {} AND `name` = '{}';",
                    i,
                    "x".repeat(50)
                )
            })
            .collect();
        let result = coalesce_commands(&commands, 2000, QuoteStyle::Backtick);
        assert!(result.len() > 1, "Should split: got {}", result.len());
        assert!(result.len() < 100, "Should coalesce: got {}", result.len());
    }

    // ════════════════════════════════════════════════════════════════════
    // UPDATE coalescing — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_single_update() {
        let commands =
            vec!["UPDATE `db`.`t` SET `name` = 'hello', `age` = 30 WHERE `id` = 1;".to_string()];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "UPDATE `db`.`t` SET `name` = 'hello', `age` = 30 WHERE `id` = 1;"
        );
    }

    #[test]
    fn test_coalesce_updates_same_table() {
        let commands = vec![
            "UPDATE `db`.`t` SET `name` = 'a', `age` = 30 WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'b', `age` = 31 WHERE `id` = 2;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'c', `age` = 32 WHERE `id` = 3;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);

        // Verify CASE-WHEN structure
        assert!(result[0].starts_with("UPDATE `db`.`t` SET "));
        assert!(result[0].contains("`name` = CASE"));
        assert!(result[0].contains("`age` = CASE"));
        assert!(result[0].contains("WHEN `id` = 1 THEN 'a'"));
        assert!(result[0].contains("WHEN `id` = 2 THEN 'b'"));
        assert!(result[0].contains("WHEN `id` = 3 THEN 'c'"));
        assert!(result[0].contains("WHEN `id` = 1 THEN 30"));
        assert!(result[0].contains("WHEN `id` = 2 THEN 31"));
        assert!(result[0].contains("WHEN `id` = 3 THEN 32"));
        assert!(result[0].contains("ELSE `name` END"));
        assert!(result[0].contains("ELSE `age` END"));
        assert!(result[0].contains("(`id` = 1) OR (`id` = 2) OR (`id` = 3)"));
        assert!(result[0].ends_with(';'));
    }

    #[test]
    fn test_coalesce_updates_different_tables() {
        let commands = vec![
            "UPDATE `db`.`t1` SET `a` = 1 WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t2` SET `a` = 2 WHERE `id` = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_updates_different_columns() {
        let commands = vec![
            "UPDATE `db`.`t` SET `name` = 'a' WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t` SET `age` = 30 WHERE `id` = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        // Different SET columns → can't coalesce
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_coalesce_updates_composite_where() {
        let commands = vec![
            "UPDATE `db`.`t` SET `val` = 'x' WHERE `k1` = 1 AND `k2` = 'a';".to_string(),
            "UPDATE `db`.`t` SET `val` = 'y' WHERE `k1` = 2 AND `k2` = 'b';".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("WHEN `k1` = 1 AND `k2` = 'a' THEN 'x'"));
        assert!(result[0].contains("WHEN `k1` = 2 AND `k2` = 'b' THEN 'y'"));
        assert!(result[0].contains("(`k1` = 1 AND `k2` = 'a') OR (`k1` = 2 AND `k2` = 'b')"));
    }

    #[test]
    fn test_coalesce_updates_preserves_else_clause() {
        let commands = vec![
            "UPDATE `db`.`t` SET `name` = 'a' WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'b' WHERE `id` = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(
            result[0].contains("ELSE `name` END"),
            "Should have ELSE clause for safety: {}",
            result[0]
        );
    }

    #[test]
    fn test_coalesce_updates_respects_packet_limit() {
        let commands: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "UPDATE `db`.`t` SET `name` = '{}', `data` = '{}' WHERE `id` = {};",
                    "x".repeat(30),
                    "y".repeat(30),
                    i
                )
            })
            .collect();
        let result = coalesce_commands(&commands, 3000, QuoteStyle::Backtick);
        assert!(result.len() > 1, "Should split: got {}", result.len());
        assert!(result.len() < 100, "Should coalesce: got {}", result.len());
    }

    // ════════════════════════════════════════════════════════════════════
    // Mixed statement coalescing — Backtick (MySQL)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_mixed_insert_update_delete() {
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`) VALUES (1);".to_string(),
            "INSERT INTO `db`.`t` (`id`) VALUES (2);".to_string(),
            "UPDATE `db`.`t` SET `name` = 'x' WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'y' WHERE `id` = 2;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 99;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 100;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 3);

        // 2 INSERT statements coalesced
        assert!(result[0].starts_with("INSERT INTO"));
        assert!(result[0].contains("), ("));

        // 2 UPDATE statements coalesced into CASE-WHEN
        assert!(result[1].starts_with("UPDATE"));
        assert!(result[1].contains("CASE"));

        // 2 DELETE statements coalesced with OR
        assert!(result[2].starts_with("DELETE"));
        assert!(result[2].contains(" OR "));
    }

    #[test]
    fn test_coalesce_mixed_preserves_ordering() {
        // Interleaved types should not be merged across type boundaries
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`) VALUES (1);".to_string(),
            "UPDATE `db`.`t` SET `name` = 'x' WHERE `id` = 1;".to_string(),
            "INSERT INTO `db`.`t` (`id`) VALUES (2);".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 3;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        // No consecutive same-type statements → no coalescing
        assert_eq!(result.len(), 4);
    }

    #[test]
    fn test_coalesce_non_dml_passthrough() {
        let commands = vec![
            "TRUNCATE TABLE `db`.`t`;".to_string(),
            "INSERT INTO `db`.`t` (`id`) VALUES (1);".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], "TRUNCATE TABLE `db`.`t`;");
    }

    #[test]
    fn test_coalesce_all_types_consecutive_groups() {
        let commands = vec![
            // Group 1: 3 INSERT statements
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (1, 'a');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (2, 'b');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `name`) VALUES (3, 'c');".to_string(),
            // Group 2: 2 UPDATE statements
            "UPDATE `db`.`t` SET `name` = 'x', `age` = 1 WHERE `id` = 10;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'y', `age` = 2 WHERE `id` = 11;".to_string(),
            // Group 3: 2 DELETE statements
            "DELETE FROM `db`.`t` WHERE `id` = 99;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 100;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 3);

        // Verify INSERT coalesced
        assert!(result[0].contains("(1, 'a'), (2, 'b'), (3, 'c')"));
        // Verify UPDATE has CASE-WHEN
        assert!(result[1].contains("CASE"));
        assert!(result[1].contains("(`id` = 10) OR (`id` = 11)"));
        // Verify DELETE has OR
        assert_eq!(
            result[2],
            "DELETE FROM `db`.`t` WHERE (`id` = 99) OR (`id` = 100);"
        );
    }

    #[test]
    fn test_coalesce_preserves_non_insert_order() {
        let commands = vec![
            "UPDATE `db`.`t` SET `a` = 1 WHERE `id` = 1;".to_string(),
            "DELETE FROM `db`.`t` WHERE `id` = 2;".to_string(),
            "TRUNCATE TABLE `db`.`t`;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 3);
        // Each is a different type → no coalescing
        assert!(result[0].starts_with("UPDATE"));
        assert!(result[1].starts_with("DELETE"));
        assert!(result[2].starts_with("TRUNCATE"));
    }

    #[test]
    fn test_coalesce_alternating_tables() {
        let commands = vec![
            "INSERT INTO `db`.`t1` (`id`) VALUES (1);".to_string(),
            "INSERT INTO `db`.`t2` (`id`) VALUES (2);".to_string(),
            "INSERT INTO `db`.`t1` (`id`) VALUES (3);".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        // Alternating tables → no coalescing
        assert_eq!(result.len(), 3);
    }

    #[test]
    fn test_coalesce_values_with_special_content() {
        // String containing VALUES keyword
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`, `d`) VALUES (1, 'has VALUES inside');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `d`) VALUES (2, 'normal');".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("'has VALUES inside'"));

        // String containing parentheses
        let commands = vec![
            "INSERT INTO `db`.`t` (`id`, `d`) VALUES (1, 'foo(bar)baz');".to_string(),
            "INSERT INTO `db`.`t` (`id`, `d`) VALUES (2, 'hello');".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("'foo(bar)baz'), (2, 'hello')"));
    }

    #[test]
    fn test_coalesce_update_with_null_values() {
        let commands = vec![
            "UPDATE `db`.`t` SET `name` = 'a', `bio` = NULL WHERE `id` = 1;".to_string(),
            "UPDATE `db`.`t` SET `name` = 'b', `bio` = NULL WHERE `id` = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("WHEN `id` = 1 THEN 'a'"));
        assert!(result[0].contains("WHEN `id` = 1 THEN NULL"));
        assert!(result[0].contains("WHEN `id` = 2 THEN 'b'"));
        assert!(result[0].contains("WHEN `id` = 2 THEN NULL"));
    }

    #[test]
    fn test_coalesce_large_batch_updates() {
        let commands: Vec<String> = (0..100)
            .map(|i| {
                format!(
                    "UPDATE `db`.`t` SET `name` = 'user_{}', `age` = {} WHERE `id` = {};",
                    i,
                    20 + i,
                    i
                )
            })
            .collect();
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("CASE"));
    }

    #[test]
    fn test_coalesce_large_batch_deletes() {
        let commands: Vec<String> = (0..100)
            .map(|i| format!("DELETE FROM `db`.`t` WHERE `id` = {};", i))
            .collect();
        let result = coalesce_commands(&commands, 67108864, QuoteStyle::Backtick);
        assert_eq!(result.len(), 1);
        assert!(result[0].starts_with("DELETE FROM `db`.`t` WHERE "));
        assert!(result[0].contains(" OR "));
    }

    // ════════════════════════════════════════════════════════════════════
    // End-to-end coalescing — DoubleQuote (SQLite)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_double_quote_inserts() {
        let commands = vec![
            r#"INSERT INTO "t" ("id", "name") VALUES (1, 'a');"#.to_string(),
            r#"INSERT INTO "t" ("id", "name") VALUES (2, 'b');"#.to_string(),
            r#"INSERT INTO "t" ("id", "name") VALUES (3, 'c');"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("(1, 'a'), (2, 'b'), (3, 'c')"));
    }

    #[test]
    fn test_coalesce_double_quote_deletes() {
        let commands = vec![
            r#"DELETE FROM "t" WHERE "id" = 1;"#.to_string(),
            r#"DELETE FROM "t" WHERE "id" = 2;"#.to_string(),
            r#"DELETE FROM "t" WHERE "id" = 3;"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            r#"DELETE FROM "t" WHERE ("id" = 1) OR ("id" = 2) OR ("id" = 3);"#
        );
    }

    #[test]
    fn test_coalesce_double_quote_updates() {
        let commands = vec![
            r#"UPDATE "t" SET "name" = 'a', "age" = 30 WHERE "id" = 1;"#.to_string(),
            r#"UPDATE "t" SET "name" = 'b', "age" = 31 WHERE "id" = 2;"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains(r#""name" = CASE"#));
        assert!(result[0].contains(r#""age" = CASE"#));
        assert!(result[0].contains(r#"WHEN "id" = 1 THEN 'a'"#));
        assert!(result[0].contains(r#"WHEN "id" = 2 THEN 'b'"#));
        assert!(result[0].contains(r#"ELSE "name" END"#));
        assert!(result[0].contains(r#"ELSE "age" END"#));
        assert!(result[0].contains(r#"("id" = 1) OR ("id" = 2)"#));
    }

    #[test]
    fn test_coalesce_double_quote_mixed() {
        let commands = vec![
            r#"INSERT INTO "t" ("id") VALUES (1);"#.to_string(),
            r#"INSERT INTO "t" ("id") VALUES (2);"#.to_string(),
            r#"UPDATE "t" SET "name" = 'x' WHERE "id" = 1;"#.to_string(),
            r#"UPDATE "t" SET "name" = 'y' WHERE "id" = 2;"#.to_string(),
            r#"DELETE FROM "t" WHERE "id" = 99;"#.to_string(),
            r#"DELETE FROM "t" WHERE "id" = 100;"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 3);
        assert!(result[0].contains("), ("));
        assert!(result[1].contains("CASE"));
        assert!(result[2].contains(" OR "));
    }

    // ════════════════════════════════════════════════════════════════════
    // End-to-end coalescing — Bracket (SQL Server)
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_bracket_inserts() {
        let commands = vec![
            "INSERT INTO [db].[t] ([id], [name]) VALUES (1, 'a');".to_string(),
            "INSERT INTO [db].[t] ([id], [name]) VALUES (2, 'b');".to_string(),
            "INSERT INTO [db].[t] ([id], [name]) VALUES (3, 'c');".to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::Bracket);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("(1, 'a'), (2, 'b'), (3, 'c')"));
    }

    #[test]
    fn test_coalesce_bracket_deletes() {
        let commands = vec![
            "DELETE FROM [db].[t] WHERE [id] = 1;".to_string(),
            "DELETE FROM [db].[t] WHERE [id] = 2;".to_string(),
            "DELETE FROM [db].[t] WHERE [id] = 3;".to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::Bracket);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            "DELETE FROM [db].[t] WHERE ([id] = 1) OR ([id] = 2) OR ([id] = 3);"
        );
    }

    #[test]
    fn test_coalesce_bracket_updates() {
        let commands = vec![
            "UPDATE [db].[t] SET [name] = 'a', [age] = 30 WHERE [id] = 1;".to_string(),
            "UPDATE [db].[t] SET [name] = 'b', [age] = 31 WHERE [id] = 2;".to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::Bracket);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("[name] = CASE"));
        assert!(result[0].contains("[age] = CASE"));
        assert!(result[0].contains("WHEN [id] = 1 THEN 'a'"));
        assert!(result[0].contains("WHEN [id] = 2 THEN 'b'"));
        assert!(result[0].contains("ELSE [name] END"));
        assert!(result[0].contains("ELSE [age] END"));
        assert!(result[0].contains("([id] = 1) OR ([id] = 2)"));
    }

    #[test]
    fn test_coalesce_bracket_mixed() {
        let commands = vec![
            "INSERT INTO [db].[t] ([id]) VALUES (1);".to_string(),
            "INSERT INTO [db].[t] ([id]) VALUES (2);".to_string(),
            "UPDATE [db].[t] SET [name] = 'x' WHERE [id] = 1;".to_string(),
            "UPDATE [db].[t] SET [name] = 'y' WHERE [id] = 2;".to_string(),
            "DELETE FROM [db].[t] WHERE [id] = 99;".to_string(),
            "DELETE FROM [db].[t] WHERE [id] = 100;".to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::Bracket);
        assert_eq!(result.len(), 3);
        assert!(result[0].contains("), ("));
        assert!(result[1].contains("CASE"));
        assert!(result[2].contains(" OR "));
    }

    // ════════════════════════════════════════════════════════════════════
    // SQLite-specific: DELETE FROM without schema prefix
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_sqlite_no_schema_prefix() {
        // SQLite uses DELETE FROM "table" (no schema prefix)
        let commands = vec![
            r#"DELETE FROM "users" WHERE "id" = 1;"#.to_string(),
            r#"DELETE FROM "users" WHERE "id" = 2;"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0],
            r#"DELETE FROM "users" WHERE ("id" = 1) OR ("id" = 2);"#
        );
    }

    #[test]
    fn test_coalesce_sqlite_update_no_schema() {
        let commands = vec![
            r#"UPDATE "users" SET "name" = 'a' WHERE "id" = 1;"#.to_string(),
            r#"UPDATE "users" SET "name" = 'b' WHERE "id" = 2;"#.to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::DoubleQuote);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains(r#""name" = CASE"#));
        assert!(result[0].contains(r#"ELSE "name" END"#));
    }

    // ════════════════════════════════════════════════════════════════════
    // SQL Server hex literal (0x prefix instead of X'...')
    // ════════════════════════════════════════════════════════════════════

    #[test]
    fn test_coalesce_bracket_with_hex_literal() {
        let commands = vec![
            "INSERT INTO [db].[t] ([id], [data]) VALUES (1, 0xDEADBEEF);".to_string(),
            "INSERT INTO [db].[t] ([id], [data]) VALUES (2, 0xCAFE);".to_string(),
        ];
        let result = coalesce_commands(&commands, u64::MAX, QuoteStyle::Bracket);
        assert_eq!(result.len(), 1);
        assert!(result[0].contains("(1, 0xDEADBEEF), (2, 0xCAFE)"));
    }
}
