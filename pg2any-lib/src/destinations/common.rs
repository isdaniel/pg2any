/// Common utilities and traits for destination implementations
///
/// This module provides shared functionality for all destination implementations:
///
/// ## Schema Mapping
/// - Maps source schemas to destination databases/schemas
/// - Essential for cross-database replication
use std::collections::HashMap;
/// Map a source schema to destination schema using provided mappings
pub fn map_schema(schema_mappings: &HashMap<String, String>, source_schema: &str) -> String {
    schema_mappings
        .get(source_schema)
        .cloned()
        .unwrap_or_else(|| source_schema.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_schema() {
        let mut mappings = HashMap::new();
        mappings.insert("public".to_string(), "cdc_db".to_string());

        assert_eq!(map_schema(&mappings, "public"), "cdc_db");
        assert_eq!(map_schema(&mappings, "other"), "other");
    }
}
