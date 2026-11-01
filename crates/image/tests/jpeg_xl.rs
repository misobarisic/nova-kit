use nova_image::{
    DecodeLimits, decode_jxl as decode_image, inspect_jxl as inspect_image, is_jpeg_xl,
};
const RGB: &[u8] = include_bytes!("fixtures/jxl/rgb.jxl");
const RGBA: &[u8] = include_bytes!("fixtures/jxl/rgba.jxl");

#[test]
fn jpeg_xl_preserves_native_rgb_alpha_and_admission() {
    for bytes in [RGB, RGBA] {
        assert!(is_jpeg_xl(bytes));
        let size = inspect_image(bytes, DecodeLimits::default()).unwrap();
        assert_eq!((size.width, size.height), (8, 6));
        let decoded = decode_image(bytes, DecodeLimits::default()).unwrap();
        for y in 0..6 {
            for x in 0..8 {
                let expected = if bytes == RGB {
                    [x as u8 * 30, y as u8 * 40, (x + y) as u8 * 16, 255]
                } else {
                    [40, 120, 200, (x + y) as u8 * 19]
                };
                assert_eq!(decoded.get_pixel(x, y).0, expected);
            }
        }
        assert!(
            inspect_image(
                bytes,
                DecodeLimits {
                    max_width: 7,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            decode_image(
                bytes,
                DecodeLimits {
                    max_decoded_bytes: 64,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            inspect_image(
                bytes,
                DecodeLimits {
                    max_encoded_bytes: bytes.len() - 1,
                    ..Default::default()
                }
            )
            .is_err()
        );
        for end in [2, bytes.len() / 2, bytes.len() - 1] {
            assert!(decode_image(&bytes[..end], DecodeLimits::default()).is_err());
        }
    }
    assert!(!is_jpeg_xl(b"not an image"));
}

#[test]
fn jpeg_xl_container_ignores_compressed_auxiliary_metadata() {
    let mut parser = jxl_bitstream::ContainerParser::new();
    let mut raw = Vec::new();
    for event in parser.feed_bytes(RGB) {
        if let jxl_bitstream::ParseEvent::Codestream(data) = event.unwrap() {
            raw.extend_from_slice(data);
        }
    }
    let mut boxed = b"\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl ".to_vec();
    let mut add_box = |kind: &[u8], payload: &[u8]| {
        boxed.extend_from_slice(&((payload.len() + 8) as u32).to_be_bytes());
        boxed.extend_from_slice(kind);
        boxed.extend_from_slice(payload);
    };
    // This is deliberately not a valid Brotli stream. Pixel decoding must not
    // attempt to inflate unrelated container decoration.
    add_box(b"brob", b"xml deliberately invalid compressed metadata");
    add_box(b"jxlc", &raw);
    let decoded = decode_image(&boxed, DecodeLimits::default()).unwrap();
    assert_eq!(decoded, decode_image(RGB, DecodeLimits::default()).unwrap());
}

#[test]
fn jixel_lossless_preserves_every_rgba_sample_and_rejects_invalid_input() {
    use nova_image::{Error, JxlEncoding, encode_jxl};
    for (width, height) in [(1u32, 1u32), (8, 6), (63, 17), (2, 300)] {
        let pixels = (0..width * height)
            .flat_map(|i| {
                [
                    (i * 31) as u8,
                    (i * 17) as u8,
                    (i * 7) as u8,
                    if i.is_multiple_of(5) {
                        0
                    } else {
                        (i * 19) as u8
                    },
                ]
            })
            .collect::<Vec<_>>();
        let encoded = encode_jxl(
            width,
            height,
            &pixels,
            JxlEncoding::Lossless,
            DecodeLimits::default(),
        )
        .unwrap();
        let decoded = decode_image(&encoded, DecodeLimits::default()).unwrap();
        assert_eq!(decoded.dimensions(), (width, height));
        assert_eq!(decoded.as_raw(), &pixels);
        assert!(matches!(
            encode_jxl(
                width,
                height,
                &pixels,
                JxlEncoding::Lossless,
                DecodeLimits {
                    max_encoded_bytes: 1,
                    ..Default::default()
                }
            ),
            Err(Error::Limit)
        ));
    }
    for (width, height, pixels) in [
        (0, 1, &[][..]),
        (1, 1, &[1, 2, 3][..]),
        (u32::MAX, u32::MAX, &[][..]),
    ] {
        assert!(matches!(
            encode_jxl(
                width,
                height,
                pixels,
                JxlEncoding::Lossless,
                DecodeLimits::default()
            ),
            Err(Error::Limit)
        ));
    }
}

#[test]
fn jixel_lossy_quality_changes_output_and_preserves_alpha() {
    use nova_image::{JxlEncoding, encode_jxl};
    let pixels = (0..64)
        .flat_map(|y| {
            (0..64).flat_map(move |x| {
                [
                    (x * 4) as u8,
                    (y * 4) as u8,
                    ((x + y) * 2) as u8,
                    if x < 8 {
                        0
                    } else if x < 16 {
                        96
                    } else {
                        255
                    },
                ]
            })
        })
        .collect::<Vec<_>>();
    let mut results = Vec::new();
    for quality in [20, 95] {
        let bytes = encode_jxl(
            64,
            64,
            &pixels,
            JxlEncoding::Lossy { quality },
            DecodeLimits::default(),
        )
        .unwrap();
        let decoded = decode_image(&bytes, DecodeLimits::default()).unwrap();
        let mut error = 0u64;
        for (actual, source) in decoded
            .as_raw()
            .as_chunks::<4>()
            .0
            .iter()
            .zip(pixels.as_chunks::<4>().0)
        {
            assert_eq!(actual[3], source[3]);
            if source[3] > 0 {
                error += actual[..3]
                    .iter()
                    .zip(&source[..3])
                    .map(|(a, b)| u64::from(a.abs_diff(*b)))
                    .sum::<u64>();
            }
        }
        results.push((bytes, error));
    }
    assert_ne!(results[0].0, results[1].0);
    assert!(
        results[1].1 < results[0].1,
        "higher quality should improve this smooth fixture: {:?}",
        results.iter().map(|r| r.1).collect::<Vec<_>>()
    );
}
