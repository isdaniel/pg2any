use crate::destinations::dialect::SqlDialect;
use crate::destinations::dialects::ansi::AnsiDialect;
use pg_walstream::ColumnValue;

#[derive(Default, Debug, Clone, Copy)]
pub struct KafkaDialect;

impl SqlDialect for KafkaDialect {
    fn quote_identifier(&self, ident: &str, out: &mut String) {
        AnsiDialect.quote_identifier(ident, out);
    }

    fn qualify_table(&self, _schema: &str, table: &str, out: &mut String) {
        self.quote_identifier(table, out);
    }

    fn render_hex_literal(&self, bytes: &[u8], out: &mut String) {
        AnsiDialect.render_hex_literal(bytes, out);
    }

    fn render_value(&self, value: &ColumnValue, out: &mut String) {
        AnsiDialect.render_value(value, out);
    }

    fn truncate_table_sql(&self, _schema: &str, _table: &str) -> Option<String> {
        None
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
        assert_eq!(
            s(|o| KafkaDialect.quote_identifier("users", o)),
            "\"users\""
        );
        assert_eq!(
            s(|o| KafkaDialect.quote_identifier("back`tick", o)),
            "\"back`tick\""
        );
        assert_eq!(
            s(|o| KafkaDialect.quote_identifier("bra]cket", o)),
            "\"bra]cket\""
        );
        assert_eq!(
            s(|o| KafkaDialect.quote_identifier("double\"quote", o)),
            "\"double\"\"quote\""
        );
    }

    #[test]
    fn qualify_table() {
        // Schema dropped, table-only.
        assert_eq!(
            s(|o| KafkaDialect.qualify_table("public", "users", o)),
            "\"users\""
        );
        assert_eq!(
            s(|o| KafkaDialect.qualify_table("custom", "items", o)),
            "\"items\""
        );
    }

    #[test]
    fn render_hex_literal() {
        assert_eq!(s(|o| KafkaDialect.render_hex_literal(&[], o)), "X''");
        assert_eq!(
            s(|o| KafkaDialect.render_hex_literal(&[0xde, 0xad, 0xbe, 0xef], o)),
            "X'deadbeef'"
        );
    }

    #[test]
    fn render_value() {
        use bytes::Bytes;
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::Null, o)),
            "NULL"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::text("t"), o)),
            "1"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::text("f"), o)),
            "0"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::text("hello"), o)),
            "'hello'"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::text("o'reilly"), o)),
            "'o''reilly'"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(&ColumnValue::text("back\\slash"), o)),
            "'back\\slash'"
        );
        assert_eq!(
            s(|o| KafkaDialect.render_value(
                &ColumnValue::Binary(Bytes::from_static(&[0x00, 0xff, 0xab])),
                o
            )),
            "X'00ffab'"
        );
    }

    #[test]
    fn truncate_table_sql() {
        assert_eq!(KafkaDialect.truncate_table_sql("public", "users"), None);
    }
}
