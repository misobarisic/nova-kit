//! Conservative source classification for future-write compression policies.
//! This is not image admission: callers must still validate/decode the image.

fn png_depth(bytes: &[u8]) -> Option<u8> {
    if bytes.get(..8)? != b"\x89PNG\r\n\x1a\n"
        || bytes.get(8..16)? != b"\0\0\0\rIHDR"
        || bytes.len() < 33
    {
        return None;
    }
    match (bytes[24], bytes[25]) {
        (depth @ (1 | 2 | 4 | 8 | 16), 0)
        | (depth @ (8 | 16), 2 | 4 | 6)
        | (depth @ (1 | 2 | 4 | 8), 3) => Some(depth),
        _ => None,
    }
}

/// Recognize PNG and WebP's lossless VP8L bitstream. AVIF/JPEG XL may contain
/// either kind of encoding, so an extension or container signature is no proof.
/// Unknown sources follow the ordinary quality setting.
pub fn is_lossless_input(bytes: &[u8]) -> bool {
    if png_depth(bytes).is_some() {
        return true;
    }
    if bytes.get(..4) != Some(b"RIFF") || bytes.get(8..12) != Some(b"WEBP") {
        return false;
    }
    let Some(size) = bytes.get(4..8) else {
        return false;
    };
    let Some(end) = usize::try_from(u32::from_le_bytes(size.try_into().unwrap()))
        .ok()
        .and_then(|size| size.checked_add(8))
        .filter(|end| *end <= bytes.len())
    else {
        return false;
    };
    let mut position = 12usize;
    // Bound work even for a hostile container full of empty metadata chunks.
    for _ in 0..1024 {
        let Some(header) = position
            .checked_add(8)
            .and_then(|end| bytes.get(position..end))
        else {
            return false;
        };
        let length = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let Some(payload_end) = position
            .checked_add(8)
            .and_then(|p| p.checked_add(length))
            .filter(|p| *p <= end)
        else {
            return false;
        };
        match &header[..4] {
            b"VP8L" => {
                return length >= 5
                    && bytes[position + 8] == 0x2f
                    && bytes[position + 12] & 0xe0 == 0;
            }
            b"VP8 " | b"ANIM" | b"ANMF" => return false,
            _ => {}
        }
        let Some(next) = payload_end.checked_add(length % 2).filter(|p| *p < end) else {
            return false;
        };
        position = next;
    }
    false
}

/// The shared RGBA path is 8-bit. Preserve higher-depth PNG bytes instead of
/// silently reducing precision when a caller requests lossless compression.
pub fn lossless_input_requires_original(bytes: &[u8]) -> bool {
    png_depth(bytes) == Some(16)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn png(depth: u8, color: u8) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n\0\0\0\rIHDR".to_vec();
        bytes.extend([0, 0, 0, 1, 0, 0, 0, 1, depth, color, 0, 0, 0, 0, 0, 0, 0]);
        bytes
    }
    fn webp(kind: &[u8; 4]) -> Vec<u8> {
        let mut bytes = b"RIFF\x12\0\0\0WEBP".to_vec();
        bytes.extend(kind);
        bytes.extend([5, 0, 0, 0, 0x2f, 0, 0, 0, 0, 0]);
        bytes
    }
    #[test]
    fn known_lossless_sources_and_high_depth_are_distinguished() {
        for (depth, color) in [(1, 0), (4, 3), (8, 6), (16, 2)] {
            let bytes = png(depth, color);
            assert!(is_lossless_input(&bytes));
            assert_eq!(lossless_input_requires_original(&bytes), depth == 16);
        }
        assert!(!is_lossless_input(&png(16, 3)));
        assert!(is_lossless_input(&webp(b"VP8L")));
        assert!(!is_lossless_input(&webp(b"VP8 ")));
        assert!(!is_lossless_input(&webp(b"ANMF")));
    }
    #[test]
    fn malformed_unknown_and_truncated_containers_are_not_evidence() {
        for bytes in [
            b"".as_slice(),
            b"\xff\xd8\xff",
            b"\xff\x0a",
            b"\0\0\0\x18ftypavif",
        ] {
            assert!(!is_lossless_input(bytes));
        }
        let bytes = webp(b"VP8L");
        for end in 0..bytes.len() - 1 {
            assert!(!is_lossless_input(&bytes[..end]));
        }
        let mut malformed = bytes;
        malformed[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(!is_lossless_input(&malformed));
    }
}
