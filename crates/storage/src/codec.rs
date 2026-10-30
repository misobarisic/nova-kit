//! Versioned cache values. Keep JSON as the logical format: addon extensions
//! and old cache rows remain readable without a second metadata schema.
use std::io::{Read, Write};

use crate::{Error, ErrorKind};

pub(super) const MAX_BYTES: usize = 64 * 1024 * 1024;
const MIN_BYTES: usize = 1024;
const MAGIC: &[u8; 4] = b"NVMC";
const HEADER: usize = 10;

fn invalid(message: impl ToString) -> Error {
    Error::new(ErrorKind::Schema, message)
}

pub(super) fn encode(value: &str) -> Result<Vec<u8>, Error> {
    if value.len() > MAX_BYTES {
        return Err(invalid("metadata cache exceeds encoding limit"));
    }
    let mut payload = value.as_bytes().to_vec();
    let mut compressed = false;
    if value.len() >= MIN_BYTES {
        let mut encoder = zstd::stream::Encoder::new(Vec::new(), 3).map_err(invalid)?;
        encoder.include_checksum(true).map_err(invalid)?;
        encoder.write_all(value.as_bytes()).map_err(invalid)?;
        let candidate = encoder.finish().map_err(invalid)?;
        if candidate.len() + HEADER <= value.len() * 9 / 10 {
            payload = candidate;
            compressed = true;
        }
    }
    let mut result = Vec::with_capacity(HEADER + payload.len());
    result.extend_from_slice(MAGIC);
    result.push(1); // Envelope version, independent of JSON/cache schema.
    result.push(u8::from(compressed));
    result.extend_from_slice(&(value.len() as u32).to_le_bytes());
    result.extend_from_slice(&payload);
    Ok(result)
}

pub(super) fn decode(value: &[u8]) -> Result<String, Error> {
    if value.len() < HEADER || &value[..4] != MAGIC || value[4] != 1 {
        return Err(invalid("unrecognized metadata cache envelope"));
    }
    let length = u32::from_le_bytes(value[6..10].try_into().unwrap()) as usize;
    if length > MAX_BYTES {
        return Err(invalid("metadata cache exceeds decompression limit"));
    }
    let payload = &value[HEADER..];
    let decoded = match value[5] {
        0 => {
            if payload.len() != length {
                return Err(invalid("metadata cache length mismatch"));
            }
            payload.to_vec()
        }
        1 => {
            let mut decoder = zstd::stream::Decoder::new(payload).map_err(invalid)?;
            // Bound both the frame's advertised window and decoded output.
            decoder.window_log_max(26).map_err(invalid)?;
            let mut decoded = Vec::new();
            decoder
                .take(length as u64 + 1)
                .read_to_end(&mut decoded)
                .map_err(invalid)?;
            decoded
        }
        _ => return Err(invalid("unsupported metadata cache codec")),
    };
    if decoded.len() != length {
        return Err(invalid("metadata cache length mismatch"));
    }
    String::from_utf8(decoded).map_err(invalid)
}

/// Exact bytes are cheap; JSON object ordering is not a meaningful update.
/// Arrays, timestamps, numbers and unknown fields still participate in equality.
pub(super) fn equivalent(previous: &str, next: &str) -> bool {
    previous == next
        || matches!(
            (serde_json::from_str::<serde_json::Value>(previous), serde_json::from_str::<serde_json::Value>(next)),
            (Ok(a), Ok(b)) if a == b
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_is_adaptive_and_round_trips() {
        let small = r#"{"title":"Nova"}"#;
        let raw = encode(small).unwrap();
        assert_eq!(raw[5], 0);
        assert_eq!(decode(&raw).unwrap(), small);
        let large =
            serde_json::json!({"episodes": vec!["description and artwork URL"; 2000]}).to_string();
        let encoded = encode(&large).unwrap();
        assert_eq!(encoded[5], 1);
        assert!(encoded.len() < large.len() / 2);
        assert_eq!(decode(&encoded).unwrap(), large);
    }

    #[test]
    fn corrupt_frames_and_invalid_envelopes_are_rejected() {
        let encoded = encode(&"metadata".repeat(2000)).unwrap();
        for end in [0, 9, encoded.len() - 1] {
            assert!(decode(&encoded[..end]).is_err());
        }
        for index in [0, 4, 5, encoded.len() - 1] {
            let mut damaged = encoded.clone();
            damaged[index] ^= 0x80;
            assert!(decode(&damaged).is_err());
        }
        let mut oversized = encoded.clone();
        oversized[6..10].copy_from_slice(&((MAX_BYTES + 1) as u32).to_le_bytes());
        assert!(decode(&oversized).is_err());
        let mut wrong_length = encoded.clone();
        wrong_length[6..10].copy_from_slice(&1_u32.to_le_bytes());
        assert!(decode(&wrong_length).is_err());
        let mut invalid_utf8 = encode("a").unwrap();
        invalid_utf8[HEADER] = 255;
        assert!(decode(&invalid_utf8).is_err());
    }

    #[test]
    fn equivalence_preserves_meaningful_json_changes() {
        assert!(equivalent(
            r#"{"a":1,"b":[2,3]}"#,
            r#"{ "b": [2,3], "a": 1 }"#
        ));
        assert!(!equivalent("[2,3]", "[3,2]"));
        assert!(!equivalent(r#"{"retrieved":1}"#, r#"{"retrieved":2}"#));
        assert!(!equivalent(r#"{"a":1}"#, r#"{"a":1,"extension":true}"#));
        assert!(equivalent("not JSON", "not JSON"));
        assert!(!equivalent("not JSON", "different"));
    }
}
