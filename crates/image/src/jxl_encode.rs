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
