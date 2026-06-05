use crate::destinations::dialect::SqlDialect;
use pg_walstream::ColumnValue;

#[derive(Default, Debug, Clone, Copy)]
pub struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn quote_identifier(&self, ident: &str, out: &mut String) {
        out.reserve(ident.len() + 2);
        out.push('`');
        if ident.contains('`') {
            for ch in ident.chars() {
                if ch == '`' {
                    out.push('`');
                }
                out.push(ch);
            }
        } else {
            out.push_str(ident);
        }
        out.push('`');
    }

    fn qualify_table(&self, schema: &str, table: &str, out: &mut String) {
        self.quote_identifier(schema, out);
        out.push('.');
        self.quote_identifier(table, out);
    }

    fn render_hex_literal(&self, bytes: &[u8], out: &mut String) {
        out.push_str("X'");
        crate::destinations::dialect::push_hex_ascii(out, bytes);
        out.push('\'');
    }

    fn render_value(&self, value: &ColumnValue, out: &mut String) {
        match value {
            ColumnValue::Null => out.push_str("NULL"),
            ColumnValue::Text(_) => match value.as_str() {
                Some(s) => {
                    if s == "t" {
                        out.push('1');
                        return;
                    }
                    if s == "f" {
                        out.push('0');
                        return;
                    }
                    out.reserve(s.len() + 2);
                    out.push('\'');
                    let needs_escape = s.contains(['\'', '\\']);
                    if needs_escape {
                        for ch in s.chars() {
                            match ch {
                                '\'' => out.push_str("''"),
                                '\\' => out.push_str("\\\\"),
                                _ => out.push(ch),
                            }
                        }
                    } else {
                        out.push_str(s);
                    }
                    out.push('\'');
                }
                None => self.render_hex_literal(value.as_bytes(), out),
            },
            ColumnValue::Binary(_) => self.render_hex_literal(value.as_bytes(), out),
        }
    }

    fn truncate_table_sql(&self, schema: &str, table: &str) -> Option<String> {
        let mut sql = String::new();
        sql.push_str("TRUNCATE TABLE ");
        self.qualify_table(schema, table, &mut sql);
        sql.push(';');
        Some(sql)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(f: impl Fn(&mut String)) -> String {
        let mut o = String::new();
        f(&mut o);
        o
    }

    #[test]
    fn quote_identifier() {
        assert_eq!(s(|o| MySqlDialect.quote_identifier("users", o)), "`users`");
        assert_eq!(
            s(|o| MySqlDialect.quote_identifier("back`tick", o)),
            "`back``tick`"
        );
        assert_eq!(
            s(|o| MySqlDialect.quote_identifier("bra]cket", o)),
            "`bra]cket`"
        );
        assert_eq!(
            s(|o| MySqlDialect.quote_identifier("double\"quote", o)),
            "`double\"quote`"
        );
    }

    #[test]
    fn qualify_table() {
        assert_eq!(
            s(|o| MySqlDialect.qualify_table("public", "users", o)),
            "`public`.`users`"
        );
        assert_eq!(
            s(|o| MySqlDialect.qualify_table("custom", "items", o)),
            "`custom`.`items`"
        );
    }

    #[test]
    fn render_hex_literal() {
        assert_eq!(s(|o| MySqlDialect.render_hex_literal(&[], o)), "X''");
        assert_eq!(
            s(|o| MySqlDialect.render_hex_literal(&[0xde, 0xad, 0xbe, 0xef], o)),
            "X'deadbeef'"
        );
    }

    #[test]
    fn render_value() {
        use bytes::Bytes;
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::Null, o)),
            "NULL"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::text("t"), o)),
            "1"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::text("f"), o)),
            "0"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::text("hello"), o)),
            "'hello'"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::text("o'reilly"), o)),
            "'o''reilly'"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(&ColumnValue::text("back\\slash"), o)),
            "'back\\\\slash'"
        );
        assert_eq!(
            s(|o| MySqlDialect.render_value(
                &ColumnValue::Binary(Bytes::from_static(&[0x00, 0xff, 0xab])),
                o
            )),
            "X'00ffab'"
        );
    }

    #[test]
    fn truncate_table_sql() {
        assert_eq!(
            MySqlDialect
                .truncate_table_sql("public", "users")
                .as_deref(),
            Some("TRUNCATE TABLE `public`.`users`;")
        );
    }
}
