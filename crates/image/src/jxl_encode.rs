use crate::{DecodeLimits, Error, Result};

/// Lossless is explicit: lossy quality 100 still uses JPEG XL's lossy path.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum JxlEncoding {
    #[default]
    Lossless,
    Lossy {
        quality: u8,
    },
}

/// Encode eight-bit RGBA using scalar, single-threaded jixel. The caller's
/// limits admit input dimensions/bytes and the final encoded output. Encoder
/// working memory is additional, just as with `encode_avif`; this is not a
/// strict peak-memory limit. Alpha remains lossless in both modes.
pub fn encode_jxl(
    width: u32,
    height: u32,
    pixels: &[u8],
    encoding: JxlEncoding,
    limits: DecodeLimits,
) -> Result<Vec<u8>> {
    let count = u64::from(width) * u64::from(height);
    let bytes = count.checked_mul(4).ok_or(Error::Limit)?;
    if width == 0
        || height == 0
        || width > limits.max_width
        || height > limits.max_height
        || count > limits.max_pixels
        || bytes > limits.max_decoded_bytes
        || usize::try_from(bytes).ok() != Some(pixels.len())
    {
        return Err(Error::Limit);
    }
    let quality = match encoding {
        JxlEncoding::Lossless => 100,
        JxlEncoding::Lossy { quality } => quality.clamp(1, 100),
    };
    let config = jixel::EncodeConfig {
        num_threads: 1,
        speed: jixel::Speed::Fast,
        ..Default::default()
    }
    .with_quality(f32::from(quality))
    .with_lossless(encoding == JxlEncoding::Lossless)
    .with_alpha_distance(0.)
    .with_fill_invisible(false);
    let encoded = jixel::encode_image_with_alpha(pixels, width as usize, height as usize, &config)
        .map_err(|error| Error::Jxl(format!("Cannot encode JPEG XL image: {error}")))?;
    if encoded.len() > limits.max_encoded_bytes {
        return Err(Error::Limit);
    }
    Ok(encoded)
}

/// Reversibly transcode a supported JPEG without decoding/requantizing its
/// coefficients. Reconstruction data stays in the returned JXL container.
/// Like pixel encoding, these limits admit input/output, not peak working RAM.
/// Restrict output to the same still-image subset our shared decoder accepts.
pub fn transcode_jpeg_to_jxl(jpeg: &[u8], limits: DecodeLimits) -> Result<Vec<u8>> {
    if jpeg.is_empty() || jpeg.len() > limits.max_encoded_bytes || !jpeg.starts_with(&[0xff, 0xd8])
    {
        return Err(Error::Limit);
    }
    let (width, height) =
        image::ImageReader::with_format(std::io::Cursor::new(jpeg), image::ImageFormat::Jpeg)
            .into_dimensions()
            .map_err(|error| Error::Jxl(error.to_string()))?;
    let count = u64::from(width) * u64::from(height);
    if width == 0
        || height == 0
        || width > limits.max_width
        || height > limits.max_height
        || count > limits.max_pixels
        || count
            .checked_mul(4)
            .is_none_or(|n| n > limits.max_decoded_bytes)
    {
        return Err(Error::Limit);
    }
    let config = jixel::JpegTranscodeConfig::default()
        .with_jpeg_reconstruction(true)
        .with_num_threads(1)
        .with_speed(jixel::Speed::Fast);
    let encoded = jixel::encode_jpeg_lossless_with_config(jpeg, &config)
        .map_err(|error| Error::Jxl(format!("Cannot transcode JPEG: {error}")))?;
    crate::inspect_jxl(&encoded, limits)?;
    Ok(encoded)
}
