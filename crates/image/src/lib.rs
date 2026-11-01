//! Shared bounded AVIF and JPEG XL still-image codecs, without native codecs.
//! See PROVENANCE.md and AVIF-NOTICE.txt for source and BSD attribution.
mod avif;
pub use avif::{decode as decode_avif, inspect as inspect_avif};
mod jxl;
pub use jxl::{decode as decode_jxl, inspect as inspect_jxl, is_jpeg_xl};
mod jxl_encode;
pub use jxl_encode::{JxlEncoding, encode_jxl, transcode_jpeg_to_jxl};
mod source_encoding;
pub use source_encoding::{is_lossless_input, lossless_input_requires_original};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("image exceeds supported limits")]
    Limit,
    #[error("AVIF: {0}")]
    Avif(String),
    #[error("JPEG XL: {0}")]
    Jxl(String),
}
pub type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DecodeLimits {
    pub max_encoded_bytes: usize,
    pub max_width: u32,
    pub max_height: u32,
    pub max_pixels: u64,
    pub max_decoded_bytes: u64,
}
impl Default for DecodeLimits {
    fn default() -> Self {
        Self {
            max_encoded_bytes: 32 * 1024 * 1024,
            max_width: 16384,
            max_height: 65536,
            max_pixels: 32_000_000,
            max_decoded_bytes: 128 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageDimensions {
    pub width: u32,
    pub height: u32,
}

/// Reader's actual 8-bit lossy encoder settings, including alpha quality 100.
/// Even quality 100 is not lossless: rav1e does not implement lossless AV1,
/// and the RGB-to-YCbCr conversion can also change samples.
/// Limits admit the input RGBA and encoded output; ravif's transient working
/// memory is additional, as in the original Reader implementation.
pub fn encode_avif(
    width: u32,
    height: u32,
    pixels: &[u8],
    quality: u8,
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
    let rgba = pixels
        .as_chunks::<4>()
        .0
        .iter()
        .map(|pixel| ravif::RGBA8::new(pixel[0], pixel[1], pixel[2], pixel[3]))
        .collect::<Vec<_>>();
    let bytes = ravif::Encoder::new()
        .with_bit_depth(ravif::BitDepth::Eight)
        .with_quality(f32::from(quality.clamp(1, 100)))
        .with_alpha_quality(100.)
        .with_alpha_color_mode(ravif::AlphaColorMode::UnassociatedDirty)
        .with_num_threads(Some(1))
        .with_speed(8)
        .encode_rgba(ravif::Img::new(&rgba, width as usize, height as usize))
        .map_err(|error| Error::Avif(format!("Cannot encode AVIF image: {error}")))?
        .avif_file;
    if bytes.len() > limits.max_encoded_bytes {
        return Err(Error::Limit);
    }
    Ok(bytes)
}
