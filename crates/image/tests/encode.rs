use nova_image::{DecodeLimits, Error, decode_avif, encode_avif, inspect_avif};

fn legacy_pixels() -> Vec<u8> {
    (0..24)
        .flat_map(|y| {
            (0..32).flat_map(move |x| {
                [
                    (x * 7) as u8,
                    (y * 9) as u8,
                    ((x + y) * 4) as u8,
                    if (x + y) % 5 == 0 {
                        0
                    } else {
                        96 + (x % 3) as u8 * 79
                    },
                ]
            })
        })
        .collect()
}

#[test]
fn moved_encoder_is_byte_identical_to_actual_reader_encoder() {
    let bytes = encode_avif(32, 24, &legacy_pixels(), 73, DecodeLimits::default()).unwrap();
    assert_eq!(bytes, include_bytes!("fixtures/reader-legacy-rgba.avif"));
    assert_eq!(
        inspect_avif(&bytes, DecodeLimits::default()).unwrap().width,
        32
    );
    let decoded = decode_avif(&bytes, DecodeLimits::default()).unwrap();
    assert_eq!(decoded.dimensions(), (32, 24));
    assert!(
        decoded
            .as_raw()
            .as_chunks::<4>()
            .0
            .iter()
            .zip(legacy_pixels().as_chunks::<4>().0)
            .all(|(pixel, source)| pixel[3].abs_diff(source[3]) <= 1)
    );
}

#[test]
fn rgba_roundtrip_preserves_alpha_color_and_real_quality_selection() {
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
        .collect::<Vec<u8>>();
    let high = encode_avif(64, 64, &pixels, 100, DecodeLimits::default()).unwrap();
    let low = encode_avif(64, 64, &pixels, 15, DecodeLimits::default()).unwrap();
    assert_ne!(high, low);
    assert!(high.len() > low.len());
    let decoded = decode_avif(&high, DecodeLimits::default()).unwrap();
    assert_eq!(decoded.dimensions(), (64, 64));
    for (actual, source) in decoded
        .as_raw()
        .as_chunks::<4>()
        .0
        .iter()
        .zip(pixels.as_chunks::<4>().0)
    {
        assert_eq!(actual[3], source[3]);
        if source[3] > 0 {
            assert!(
                actual[..3]
                    .iter()
                    .zip(&source[..3])
                    .all(|(a, b)| a.abs_diff(*b) <= 8),
                "{actual:?} vs {source:?}"
            );
        }
    }
    assert_eq!(
        encode_avif(64, 64, &pixels, 0, DecodeLimits::default()).unwrap(),
        encode_avif(64, 64, &pixels, 1, DecodeLimits::default()).unwrap()
    );
}

#[test]
fn encoder_admission_and_output_cap_are_checked() {
    let pixels = legacy_pixels();
    let limits = DecodeLimits::default();
    for limits in [
        DecodeLimits {
            max_width: 31,
            ..limits
        },
        DecodeLimits {
            max_height: 23,
            ..limits
        },
        DecodeLimits {
            max_pixels: 767,
            ..limits
        },
        DecodeLimits {
            max_decoded_bytes: 3071,
            ..limits
        },
        DecodeLimits {
            max_encoded_bytes: 1,
            ..limits
        },
    ] {
        assert!(matches!(
            encode_avif(32, 24, &pixels, 85, limits),
            Err(Error::Limit)
        ));
    }
    assert!(encode_avif(32, 24, &pixels[..pixels.len() - 1], 85, limits).is_err());
    assert!(encode_avif(0, 0, &[], 85, limits).is_err());
}
