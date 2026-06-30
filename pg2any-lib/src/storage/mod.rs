//! Storage module for transaction file operations.
//!
//! Transaction segments are persisted as binary MessagePack `ChangeEvent`
//! records (see [`binary_record`]). The producer appends records via the
//! buffered writer and the consumer decodes them with
//! [`binary_record::decode_record`]; there is no separate storage abstraction.

pub mod binary_record;
