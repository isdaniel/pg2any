//! Length-framed MessagePack encoding of a single ChangeEvent record.
//!
//! Wire format per record: `u32` little-endian byte length, then that many
//! bytes of MessagePack (rmp-serde). A self-describing binary format is
//! required because pg_walstream's ColumnValue/RowData serde impls dispatch on
//! the data stream (`deserialize_option` + `visit_str`/`visit_map`); MessagePack
//! satisfies that, is actively maintained, and is more compact than CBOR.
//! See the Phase 3 design doc.
use crate::error::{CdcError, Result};
use crate::types::ChangeEvent;
use std::io::Read;

/// Stream/record format version. Reserved for the compressed binary format
/// (Phase 3 Stage 4) which will embed it in a header.
#[allow(dead_code)]
pub const RECORD_FORMAT_VERSION: u8 = 1;

/// Upper bound on a single decoded record's payload length. A corrupt or
/// truncated length prefix could otherwise request a multi-gigabyte allocation
/// (`vec![0u8; len]`) and OOM the process. 256MB is comfortably above the 64MB
/// default segment size, so any larger length is treated as corruption.
const MAX_RECORD_BYTES: usize = 256 * 1024 * 1024;

pub fn encode_record(event: &ChangeEvent, out: &mut Vec<u8>) -> Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0u8; 4]); // length placeholder
    rmp_serde::encode::write(&mut *out, event)
        .map_err(|e| CdcError::generic(format!("MessagePack encode failed: {e}")))?;
    let len = (out.len() - start - 4) as u32;
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

pub fn decode_record<R: Read>(reader: &mut R) -> Result<Option<ChangeEvent>> {
    // Read the first length-prefix byte on its own so we can distinguish a clean EOF at a record boundary (0 bytes available) from a TRUNCATED length prefix (1-3 bytes then EOF). `read_exact` alone reports both as `UnexpectedEof`, which would silently treat a truncated stream as a normal end-of-segment.
    let mut len_buf = [0u8; 4];
    let n = loop {
        match reader.read(&mut len_buf[..1]) {
            Ok(n) => break n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(CdcError::generic(format!("record length read failed: {e}"))),
        }
    };
    if n == 0 {
        return Ok(None); // clean EOF at a record boundary
    }
    reader
        .read_exact(&mut len_buf[1..])
        .map_err(|e| CdcError::generic(format!("truncated record length prefix: {e}")))?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > MAX_RECORD_BYTES {
        return Err(CdcError::generic(format!(
            "record length {len} exceeds max {MAX_RECORD_BYTES}"
        )));
    }
    let mut payload = vec![0u8; len];
    reader
        .read_exact(&mut payload)
        .map_err(|e| CdcError::generic(format!("record payload read failed (len {len}): {e}")))?;
    let event: ChangeEvent = rmp_serde::from_slice(&payload)
        .map_err(|e| CdcError::generic(format!("MessagePack decode failed: {e}")))?;
    Ok(Some(event))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ChangeEvent, EventType};
    use pg_walstream::{ColumnValue, RowData};

    fn insert_event() -> ChangeEvent {
        let data = RowData::from_pairs(vec![
            ("id", ColumnValue::text("42")),
            ("name", ColumnValue::text("Alice")),
            (
                "payload",
                ColumnValue::Binary(vec![0u8, 159, 146, 150].into()),
            ), // non-utf8 bytes
            ("maybe", ColumnValue::Null),
        ]);
        ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "users".into(),
                relation_oid: 16384,
                data,
            },
            lsn: crate::types::Lsn(123),
            metadata: None,
        }
    }

    #[test]
    fn test_encode_decode_round_trip() {
        let ev = insert_event();
        let mut buf = Vec::new();
        encode_record(&ev, &mut buf).unwrap();
        // length prefix present
        assert!(buf.len() > 4);
        let mut cursor = std::io::Cursor::new(&buf);
        let got = decode_record(&mut cursor).unwrap().unwrap();
        // Compare via re-encode (ChangeEvent has no PartialEq across all fields):
        let mut buf2 = Vec::new();
        encode_record(&got, &mut buf2).unwrap();
        assert_eq!(buf, buf2, "round-trip must be byte-stable");
    }

    #[test]
    fn test_decode_clean_eof_is_none() {
        let mut empty = std::io::Cursor::new(Vec::<u8>::new());
        assert!(decode_record(&mut empty).unwrap().is_none());
    }

    #[test]
    fn test_decode_truncated_is_err() {
        let ev = insert_event();
        let mut buf = Vec::new();
        encode_record(&ev, &mut buf).unwrap();
        buf.truncate(buf.len() - 2); // chop the MessagePack payload
        let mut cursor = std::io::Cursor::new(&buf);
        assert!(decode_record(&mut cursor).is_err());
    }

    #[test]
    fn test_decode_truncated_length_prefix_is_err_not_eof() {
        // A torn length prefix (1-3 bytes then EOF) must be an Err, NOT a clean
        // EOF (Ok(None)) — otherwise a truncated stream is silently treated as a
        // normal end-of-segment. Only 0 bytes at the boundary is a clean EOF.
        for partial in 1..=3usize {
            let buf = vec![0u8; partial];
            let mut cursor = std::io::Cursor::new(buf);
            assert!(
                decode_record(&mut cursor).is_err(),
                "{partial}-byte length prefix must Err, not be treated as EOF"
            );
        }
    }

    #[test]
    fn test_decode_oversized_length_errs_not_panics() {
        // A corrupt length prefix claiming more than MAX_RECORD_BYTES must be
        // rejected with an Err rather than attempting a huge `vec![0u8; len]`
        // allocation (potential OOM/panic). Use u32::MAX (~4GB) as the length.
        let mut buf = Vec::new();
        buf.extend_from_slice(&u32::MAX.to_le_bytes());
        // No payload bytes follow; the guard must fire on the length alone,
        // before any allocation or read of the (absent) payload.
        let mut cursor = std::io::Cursor::new(&buf);
        let err = decode_record(&mut cursor).unwrap_err();
        assert!(
            err.to_string().contains("exceeds max"),
            "expected size-guard error, got: {err}"
        );
    }
}
