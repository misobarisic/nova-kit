//! JPEG XL still pages from Kagane. Pure Rust, single-threaded decoding uses
//! the same admission as other pages, plus jxl-oxide's grid allocation tracker.
use crate::{DecodeLimits, Error, PageDimensions, Result};
use jxl_oxide::{AllocTracker, InitializeResult, JxlImage};
use jxl_oxide_common::Bundle;

pub fn is_jpeg_xl(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\xff\x0a") || bytes.starts_with(b"\0\0\0\x0cJXL \r\n\x87\n")
}
fn invalid(error: impl std::fmt::Display) -> Error {
    Error::Jxl(error.to_string())
}
fn codestream(bytes: &[u8], limits: DecodeLimits) -> Result<Vec<u8>> {
    if bytes.len() > limits.max_encoded_bytes {
        return Err(Error::Limit);
    }
    // Keep auxiliary Exif/XML/JUMBF/Brotli out of the decoder entirely. Their
    // decompression is unrelated to page pixels and is not grid-budgeted.
    let mut parser = jxl_bitstream::ContainerParser::new();
    let mut stream = Vec::new();
    for event in parser.feed_bytes(bytes) {
        if let jxl_bitstream::ParseEvent::Codestream(data) = event.map_err(invalid)? {
            if stream.len().saturating_add(data.len()) > limits.max_encoded_bytes {
                return Err(Error::Limit);
            }
            stream.extend_from_slice(data);
        }
    }
    let header = jxl_oxide::ImageHeader::parse(
        &mut jxl_bitstream::Bitstream::new(&stream[..stream.len().min(256 * 1024)]),
        (),
    )
    .map_err(invalid)?;
    if header.metadata.colour_encoding.want_icc()
        || header.metadata.preview.is_some()
        || header.metadata.animation.is_some()
        || header.metadata.ec_info.len() > 1
    {
        return Err(invalid(
            "custom ICC, preview, animation or extra JPEG XL channels are unsupported",
        ));
    }
    Ok(stream)
}
fn initialize(bytes: &[u8], limits: DecodeLimits) -> Result<(JxlImage, usize, AllocTracker)> {
    if bytes.is_empty() || bytes.len() > limits.max_encoded_bytes {
        return Err(Error::Limit);
    }
    let budget = usize::try_from(limits.max_decoded_bytes).map_err(|_| Error::Limit)?;
    let tracker = AllocTracker::with_limit(budget);
    let mut decoder = JxlImage::builder()
        .alloc_tracker(tracker.clone())
        .build_uninit();
    let mut offset = 0;
    // Bound metadata/header work independently of the allowed encoded page.
    while offset < bytes.len().min(256 * 1024) {
        let end = (offset + 1024).min(bytes.len());
        let consumed = decoder.feed_bytes(&bytes[offset..end]).map_err(invalid)?;
        if consumed == 0 {
            return Err(invalid("incomplete JPEG XL header"));
        }
        offset += consumed;
        match decoder.try_init().map_err(invalid)? {
            InitializeResult::NeedMoreData(next) => decoder = next,
            InitializeResult::Initialized(image) => {
                let width = image.width();
                let height = image.height();
                let pixels = u64::from(width) * u64::from(height);
                if width == 0
                    || height == 0
                    || width > limits.max_width
                    || height > limits.max_height
                    || pixels > limits.max_pixels
                    || pixels
                        .checked_mul(4)
                        .is_none_or(|n| n > limits.max_decoded_bytes)
                {
                    return Err(Error::Limit);
                }
                if image.image_header().metadata.animation.is_some()
                    || image.pixel_format().has_black()
                {
                    return Err(invalid("animated/CMYK JPEG XL pages are unsupported"));
                }
                return Ok((image, offset, tracker));
            }
        }
    }
    Err(invalid("incomplete or oversized JPEG XL header"))
}
pub fn inspect(bytes: &[u8], limits: DecodeLimits) -> Result<PageDimensions> {
    let stream = codestream(bytes, limits)?;
    let (image, _, _) = initialize(&stream, limits)?;
    Ok(PageDimensions {
        width: image.width(),
        height: image.height(),
    })
}
pub fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<image::RgbaImage> {
    let stream = codestream(bytes, limits)?;
    let bytes = stream.as_slice();
    let (mut image, mut offset, tracker) = initialize(bytes, limits)?;
    let width = image.width();
    let height = image.height();
    let rgba_bytes = width as usize * height as usize * 4;
    // Reserve the RGBA output and row scratch before feeding/rendering frames;
    // no full floating-point interleaved copy or native/RGBA double copy.
    let row_bytes = width as usize * 4;
    tracker
        .shrink_limit(rgba_bytes.checked_add(row_bytes).ok_or(Error::Limit)?)
        .map_err(|_| Error::Limit)?;
    while offset < bytes.len() {
        let consumed = image.feed_bytes(&bytes[offset..]).map_err(invalid)?;
        if consumed == 0 {
            break;
        }
        offset += consumed;
    }
    image.finalize().map_err(invalid)?;
    if !image.is_loading_done() || image.num_loaded_keyframes() != 1 {
        return Err(invalid("incomplete JPEG XL still page"));
    }
    image.request_color_encoding(jxl_oxide::EnumColourEncoding::srgb(
        jxl_oxide::RenderingIntent::Relative,
    ));
    let render = image.render_frame(0).map_err(invalid)?;
    let mut stream = render.stream();
    let channels = stream.channels() as usize;
    if !(1..=4).contains(&channels) || stream.width() != width || stream.height() != height {
        return Err(invalid("JPEG XL channel layout changed"));
    }
    let mut output = image::RgbaImage::new(width, height);
    let mut row = vec![0u8; width as usize * channels];
    for destination in output.as_mut().chunks_exact_mut(width as usize * 4) {
        if stream.write_to_buffer(&mut row) != row.len() {
            return Err(invalid("incomplete JPEG XL pixels"));
        }
        for (source, pixel) in row
            .chunks_exact(channels)
            .zip(destination.as_chunks_mut::<4>().0)
        {
            *pixel = match channels {
                1 => [source[0], source[0], source[0], 255],
                2 => [source[0], source[0], source[0], source[1]],
                3 => [source[0], source[1], source[2], 255],
                _ => [source[0], source[1], source[2], source[3]],
            };
        }
    }
    Ok(output)
}
