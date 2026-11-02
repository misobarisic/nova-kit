use nova_image::{
    DecodeLimits, Error, JxlPreviewOptions, PageDimensions, ProgressiveJxlDecoder, decode_jxl,
};
const RGB: &[u8] = include_bytes!("fixtures/jxl/rgb.jxl");
const RGBA: &[u8] = include_bytes!("fixtures/jxl/rgba.jxl");
const PROGRESSIVE: &[u8] = include_bytes!("fixtures/jxl/progressive-rgb.jxl");
fn decoder(options: JxlPreviewOptions) -> ProgressiveJxlDecoder {
    ProgressiveJxlDecoder::new(DecodeLimits::default(), options).unwrap()
}
fn raw(bytes: &[u8]) -> Vec<u8> {
    let mut parser = jxl_bitstream::ContainerParser::new();
    let mut output = Vec::new();
    for event in parser.feed_bytes(bytes) {
        if let jxl_bitstream::ParseEvent::Codestream(data) = event.unwrap() {
            output.extend_from_slice(data);
        }
    }
    output
}
fn container() -> Vec<u8> {
    b"\0\0\0\x0cJXL \r\n\x87\n\0\0\0\x14ftypjxl \0\0\0\0jxl ".to_vec()
}
fn add_box(output: &mut Vec<u8>, kind: &[u8; 4], payload: &[u8]) {
    output.extend_from_slice(&((payload.len() + 8) as u32).to_be_bytes());
    output.extend_from_slice(kind);
    output.extend_from_slice(payload);
}

#[test]
fn raw_container_and_byte_split_chunks_finish_as_the_existing_strict_pixels() {
    for fixture in [RGB, RGBA] {
        let raw = raw(fixture);
        let mut boxed = container();
        add_box(
            &mut boxed,
            b"brob",
            b"xml deliberately invalid compressed metadata",
        );
        add_box(&mut boxed, b"jxlc", &raw);
        let expected = decode_jxl(fixture, DecodeLimits::default()).unwrap();
        for encoded in [&raw, &boxed] {
            for chunk_size in [1, 2, 7, 127, 8192] {
                let mut image = decoder(Default::default());
                for chunk in encoded.chunks(chunk_size) {
                    image.feed(chunk).unwrap();
                }
                assert_eq!(
                    image.dimensions(),
                    Some(PageDimensions {
                        width: 8,
                        height: 6
                    })
                );
                image.validate_complete().unwrap();
                assert_eq!(image.finish().unwrap(), expected);
            }
        }
    }
}

#[test]
fn progressive_pixels_arrive_before_eof_and_the_final_image_remains_strict() {
    let mut image = decoder(JxlPreviewOptions {
        max_width: 128,
        max_height: 96,
        max_bytes: 128 * 96 * 4,
    });
    let mut previews = 0;
    let step = PROGRESSIVE.len().div_ceil(16);
    for (index, chunk) in PROGRESSIVE.chunks(step).enumerate() {
        image.feed(chunk).unwrap();
        assert_eq!(
            image.dimensions(),
            Some(PageDimensions {
                width: 512,
                height: 384
            })
        );
        if let Some(preview) = image.render_preview().unwrap() {
            assert_eq!(
                preview.source,
                PageDimensions {
                    width: 512,
                    height: 384
                }
            );
            assert_eq!(preview.image.dimensions(), (128, 96));
            assert!(preview.image.as_raw().len() <= 128 * 96 * 4);
            if (index + 1) * step < PROGRESSIVE.len() {
                previews += 1;
            }
        }
        assert!(
            image.render_preview().unwrap().is_none(),
            "without bytes there is no new frame"
        );
    }
    assert!(
        previews > 0,
        "authored VarDCT fixture must publish pixels before the final byte"
    );
    image.validate_complete().unwrap();
    assert_eq!(
        image.finish().unwrap(),
        decode_jxl(PROGRESSIVE, DecodeLimits::default()).unwrap()
    );
}

#[test]
fn bounded_preview_resampling_preserves_straight_alpha_and_native_geometry() {
    let mut image = decoder(JxlPreviewOptions {
        max_width: 4,
        max_height: 3,
        max_bytes: 48,
    });
    image.feed(RGBA).unwrap();
    let preview = image.render_preview().unwrap().unwrap();
    assert_eq!(
        preview.source,
        PageDimensions {
            width: 8,
            height: 6
        }
    );
    assert_eq!(preview.image.dimensions(), (4, 3));
    assert_eq!(preview.image.get_pixel(0, 0).0, [40, 120, 200, 19]);
    assert_eq!(preview.image.get_pixel(3, 2).0, [40, 120, 200, 209]);
    assert_eq!(
        image.finish().unwrap(),
        decode_jxl(RGBA, DecodeLimits::default()).unwrap()
    );
    let mut image = decoder(JxlPreviewOptions {
        max_width: 999,
        max_height: 999,
        max_bytes: 16,
    });
    image.feed(RGBA).unwrap();
    let preview = image.render_preview().unwrap().unwrap();
    assert!(preview.image.as_raw().len() <= 16);
    assert_eq!(
        preview.source,
        PageDimensions {
            width: 8,
            height: 6
        }
    );
}

#[test]
fn completed_nonprogressive_files_have_a_normal_final_path_and_no_repeated_preview() {
    let mut image = decoder(Default::default());
    image.feed(&RGB[..2]).unwrap();
    assert!(image.render_preview().unwrap().is_none());
    for byte in &RGB[2..] {
        image.feed(&[*byte]).unwrap();
    }
    let preview = image.render_preview().unwrap().unwrap();
    assert_eq!(
        preview.image,
        decode_jxl(RGB, DecodeLimits::default()).unwrap()
    );
    assert!(image.render_preview().unwrap().is_none());
    image.feed(&[]).unwrap();
    assert!(image.render_preview().unwrap().is_none());
    assert_eq!(image.finish().unwrap(), preview.image);
}

#[test]
fn truncated_pixels_and_container_eof_fail_even_after_a_preview() {
    let mut image = decoder(Default::default());
    image.feed(&PROGRESSIVE[..PROGRESSIVE.len() - 1]).unwrap();
    assert!(image.render_preview().unwrap().is_some());
    assert!(image.finish().is_err());
    let mut boxed = container();
    add_box(&mut boxed, b"jxlc", &raw(RGB));
    // Pixels are complete, but the declared unknown metadata payload is not.
    boxed.extend_from_slice(&20u32.to_be_bytes());
    boxed.extend_from_slice(b"xml ");
    boxed.extend_from_slice(b"short");
    let mut image = decoder(Default::default());
    image.feed(&boxed).unwrap();
    assert!(image.render_preview().unwrap().is_some());
    assert!(image.validate_complete().is_err());
    assert!(image.finish().is_err());
    assert!(decode_jxl(&boxed, DecodeLimits::default()).is_err());
    for end in [0, 1, 2, RGB.len() / 2, RGB.len() - 1] {
        let mut image = decoder(Default::default());
        let fed = image.feed(&RGB[..end]);
        assert!(fed.is_err() || image.finish().is_err());
    }
}

#[test]
fn jxlp_final_flag_and_every_box_header_payload_boundary_are_validated() {
    let raw = raw(RGB);
    let split = raw.len() / 2;
    let mut boxed = container();
    let mut first = 0u32.to_be_bytes().to_vec();
    first.extend_from_slice(&raw[..split]);
    add_box(&mut boxed, b"jxlp", &first);
    let mut last = 0x8000_0001u32.to_be_bytes().to_vec();
    last.extend_from_slice(&raw[split..]);
    add_box(&mut boxed, b"jxlp", &last);
    let mut image = decoder(Default::default());
    for byte in &boxed {
        image.feed(&[*byte]).unwrap();
    }
    assert_eq!(
        image.finish().unwrap(),
        decode_jxl(RGB, DecodeLimits::default()).unwrap()
    );
    // A complete codestream without the final-container-part bit is truncated.
    let flag_offset = boxed.len() - last.len();
    boxed[flag_offset] &= 0x7f;
    let mut image = decoder(Default::default());
    image.feed(&boxed).unwrap();
    assert!(image.render_preview().unwrap().is_some());
    assert!(image.finish().is_err());
    assert!(decode_jxl(&boxed, DecodeLimits::default()).is_err());
    let mut complete = container();
    add_box(&mut complete, b"jxlc", &raw);
    for suffix in [
        vec![0],
        vec![0, 0, 0, 16, b'x', b'm', b'l', b' '],
        vec![0, 0, 0, 1, b'x', b'm', b'l', b' ', 0, 0],
    ] {
        let mut invalid = complete.clone();
        invalid.extend(suffix);
        let mut image = decoder(Default::default());
        let fed = image.feed(&invalid);
        assert!(fed.is_err() || image.finish().is_err());
        assert!(decode_jxl(&invalid, DecodeLimits::default()).is_err());
    }
}

#[test]
fn limits_reject_headers_early_and_failures_cannot_resume_as_valid_images() {
    for limits in [
        DecodeLimits {
            max_width: 7,
            ..Default::default()
        },
        DecodeLimits {
            max_height: 5,
            ..Default::default()
        },
        DecodeLimits {
            max_pixels: 47,
            ..Default::default()
        },
        DecodeLimits {
            max_decoded_bytes: 64,
            ..Default::default()
        },
    ] {
        let mut image = ProgressiveJxlDecoder::new(limits, Default::default()).unwrap();
        let mut failed = false;
        for (index, byte) in RGB.iter().enumerate() {
            if image.feed(&[*byte]).is_err() {
                assert!(index < RGB.len() - 1);
                failed = true;
                break;
            }
        }
        assert!(failed);
        assert!(image.render_preview().is_err());
        assert!(image.feed(RGB).is_err());
        assert!(image.finish().is_err());
    }
    let mut image = ProgressiveJxlDecoder::new(
        DecodeLimits {
            max_encoded_bytes: RGB.len() - 1,
            ..Default::default()
        },
        Default::default(),
    )
    .unwrap();
    assert!(matches!(image.feed(RGB), Err(Error::Limit)));
    let mut image = decoder(Default::default());
    assert!(image.feed(b"not JPEG XL").is_err());
    assert!(image.finish().is_err());
    assert!(
        ProgressiveJxlDecoder::new(
            DecodeLimits::default(),
            JxlPreviewOptions {
                max_width: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    assert!(
        ProgressiveJxlDecoder::new(
            DecodeLimits::default(),
            JxlPreviewOptions {
                max_bytes: 3,
                ..Default::default()
            }
        )
        .is_err()
    );
}

#[test]
fn single_row_previews_and_native_finish_keep_alpha_without_an_extra_source_frame() {
    let pixels = (0..32)
        .flat_map(|x| [40, 120, 200, (x * 7) as u8])
        .collect::<Vec<_>>();
    let encoded = nova_image::encode_jxl(
        32,
        1,
        &pixels,
        nova_image::JxlEncoding::Lossless,
        DecodeLimits::default(),
    )
    .unwrap();
    let mut image = decoder(JxlPreviewOptions {
        max_width: 4,
        max_height: 1,
        max_bytes: 16,
    });
    image.feed(&encoded).unwrap();
    let preview = image.render_preview().unwrap().unwrap();
    assert_eq!(preview.image.dimensions(), (4, 1));
    assert_eq!(preview.image.get_pixel(0, 0).0, [40, 120, 200, 25]);
    assert_eq!(image.finish().unwrap().as_raw(), &pixels);
}

#[test]
fn orientation_is_part_of_stable_native_geometry_and_final_pixels() {
    use image::ImageEncoder;
    let pixels = (0..32 * 24)
        .flat_map(|index| [(index % 32 * 7) as u8, (index / 32 * 9) as u8, 96])
        .collect::<Vec<_>>();
    let mut jpeg = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut jpeg, 90)
        .write_image(&pixels, 32, 24, image::ExtendedColorType::Rgb8)
        .unwrap();
    let exif = b"Exif\0\0II*\0\x08\0\0\0\x01\0\x12\x01\x03\0\x01\0\0\0\x06\0\0\0\0\0\0\0";
    let mut oriented = jpeg[..2].to_vec();
    oriented.extend_from_slice(&[0xff, 0xe1]);
    oriented.extend_from_slice(&((exif.len() + 2) as u16).to_be_bytes());
    oriented.extend_from_slice(exif);
    oriented.extend_from_slice(&jpeg[2..]);
    let encoded = nova_image::transcode_jpeg_to_jxl(&oriented, DecodeLimits::default()).unwrap();
    let mut image = decoder(JxlPreviewOptions {
        max_width: 12,
        max_height: 16,
        max_bytes: 768,
    });
    for chunk in encoded.chunks(7) {
        image.feed(chunk).unwrap();
    }
    assert_eq!(
        image.dimensions(),
        Some(PageDimensions {
            width: 24,
            height: 32
        })
    );
    let preview = image.render_preview().unwrap().unwrap();
    assert_eq!(
        preview.source,
        PageDimensions {
            width: 24,
            height: 32
        }
    );
    assert_eq!(preview.image.dimensions(), (12, 16));
    assert_eq!(
        image.finish().unwrap(),
        decode_jxl(&encoded, DecodeLimits::default()).unwrap()
    );
}

#[test]
fn tiny_fixture_truncation_and_legal_extended_or_open_ended_boxes_are_explicit() {
    for fixture in [RGB, RGBA] {
        for end in 0..fixture.len() {
            let mut image = decoder(Default::default());
            let fed = image.feed(&fixture[..end]);
            assert!(
                fed.is_err() || image.finish().is_err(),
                "truncated still unexpectedly accepted at {end}/{}",
                fixture.len()
            );
        }
    }
    let raw = raw(RGB);
    let mut extended = container();
    extended.extend_from_slice(&1u32.to_be_bytes());
    extended.extend_from_slice(b"jxlc");
    extended.extend_from_slice(&((raw.len() + 16) as u64).to_be_bytes());
    extended.extend_from_slice(&raw);
    let mut open = container();
    open.extend_from_slice(&0u32.to_be_bytes());
    open.extend_from_slice(b"jxlc");
    open.extend_from_slice(&raw);
    let mut open_aux = container();
    add_box(&mut open_aux, b"jxlc", &raw);
    open_aux.extend_from_slice(&0u32.to_be_bytes());
    open_aux.extend_from_slice(b"xml ");
    open_aux.extend_from_slice(b"ignored through EOF");
    let expected = decode_jxl(RGB, DecodeLimits::default()).unwrap();
    for encoded in [extended, open, open_aux] {
        let mut image = decoder(Default::default());
        for byte in &encoded {
            image.feed(&[*byte]).unwrap();
        }
        image.validate_complete().unwrap();
        assert_eq!(image.finish().unwrap(), expected);
        assert_eq!(
            decode_jxl(&encoded, DecodeLimits::default()).unwrap(),
            expected
        );
    }
}
