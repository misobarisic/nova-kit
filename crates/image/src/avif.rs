//! Bounded 8-bit AVIF still-image adapter. No native codec or assembly features.
//!
//! Plane access and CICP dispatch follow Kornel Lesiński's BSD-licensed
//! avif-decode 3.0.0; see ../AVIF-NOTICE.txt for attribution and fixture license.
use super::{DecodeLimits, Error, PageDimensions, Result};
use rav1d::include::dav1d::{
    data::Dav1dData,
    dav1d::{Dav1dContext, Dav1dSettings},
    headers::*,
    picture::Dav1dPicture,
};
use rav1d::src::lib::{
    dav1d_close, dav1d_data_create, dav1d_data_unref, dav1d_default_settings, dav1d_get_picture,
    dav1d_open, dav1d_picture_unref, dav1d_send_data,
};
use std::{mem::MaybeUninit, ptr::NonNull};
use yuv::{YuvPlanarImage, YuvRange, YuvStandardMatrix};

fn invalid(message: impl std::fmt::Display) -> Error {
    Error::Avif(message.to_string())
}

#[derive(Default)]
struct MetadataBudget {
    extent_bytes: u64,
    entries: u64,
}
impl MetadataBudget {
    fn charge(&mut self, count: u64) -> Result<()> {
        self.entries = self
            .entries
            .checked_add(count)
            .filter(|entries| *entries <= 1024)
            .ok_or(Error::Limit)?;
        Ok(())
    }
}

/// Check each declared box against the actual containing bytes before passing
/// it to avif-parse, whose fallible Vec reservations trust box lengths.
fn boxes(
    bytes: &[u8],
    depth: usize,
    extent_total: &mut MetadataBudget,
    file_len: u64,
) -> Result<()> {
    if depth > 8 {
        return Err(Error::Limit);
    }
    let mut position = 0usize;
    while position < bytes.len() {
        extent_total.charge(1)?;
        let header = bytes
            .get(position..position.checked_add(8).ok_or(Error::Limit)?)
            .ok_or_else(|| invalid("truncated AVIF box"))?;
        let size = u32::from_be_bytes(header[..4].try_into().unwrap());
        let kind = &header[4..8];
        let (size, header_len) = if size == 1 {
            let large = bytes
                .get(position + 8..position + 16)
                .ok_or_else(|| invalid("truncated extended box"))?;
            (
                usize::try_from(u64::from_be_bytes(large.try_into().unwrap()))
                    .map_err(|_| Error::Limit)?,
                16,
            )
        } else if size == 0 {
            (bytes.len() - position, 8)
        } else {
            (size as usize, 8)
        };
        if size < header_len {
            return Err(invalid("invalid AVIF box size"));
        }
        let end = position
            .checked_add(size)
            .filter(|end| *end <= bytes.len())
            .ok_or(Error::Limit)?;
        let data = &bytes[position + header_len..end];
        let skip = match kind {
            b"meta" => Some(4),
            b"iref" => {
                extent_total.charge(data.len() as u64)?;
                Some(4)
            }
            b"iprp" | b"ipco" => Some(0),
            b"iinf" => {
                let mut cursor = 0;
                let version = read_integer(data, &mut cursor, 1)?;
                if version > 1 {
                    return Err(invalid("unsupported iinf version"));
                }
                read_integer(data, &mut cursor, 3)?;
                let count = read_integer(data, &mut cursor, if version == 0 { 2 } else { 4 })?;
                if count > (data.len() - cursor) as u64 / 8 {
                    return Err(Error::Limit);
                }
                extent_total.charge(count)?;
                Some(cursor)
            }
            b"ipma" => {
                extent_total.charge(data.len() as u64)?;
                None
            }
            b"pixi" => {
                let channels = *data
                    .get(4)
                    .ok_or_else(|| invalid("truncated AVIF pixel information"))?
                    as usize;
                if channels > 16 || data.len() != 5 + channels {
                    return Err(invalid("invalid AVIF pixel information"));
                }
                None
            }
            b"irot" | b"imir" | b"clap" => {
                return Err(invalid("transformed AVIF images are unsupported"));
            }
            b"moov" => return Err(invalid("animated AVIF is unsupported")),
            b"iloc" => {
                check_extents(data, extent_total, file_len)?;
                None
            }
            b"ftyp" => {
                extent_total.charge(data.len() as u64 / 4)?;
                if data.len() < 8
                    || data[..4] == *b"avis"
                    || data[8..]
                        .as_chunks::<4>()
                        .0
                        .iter()
                        .any(|brand| brand == b"avis")
                {
                    return Err(invalid("animated or invalid AVIF file type"));
                }
                None
            }
            _ => None,
        };
        if let Some(skip) = skip {
            boxes(
                data.get(skip..)
                    .ok_or_else(|| invalid("truncated AVIF container"))?,
                depth + 1,
                extent_total,
                file_len,
            )?;
        }
        position = end;
    }
    Ok(())
}

fn read_integer(bytes: &[u8], position: &mut usize, width: usize) -> Result<u64> {
    if width > 8 {
        return Err(invalid("invalid AVIF integer width"));
    }
    let end = position.checked_add(width).ok_or(Error::Limit)?;
    let slice = bytes
        .get(*position..end)
        .ok_or_else(|| invalid("truncated AVIF integer"))?;
    *position = end;
    Ok(slice
        .iter()
        .fold(0u64, |value, byte| (value << 8) | u64::from(*byte)))
}

fn check_extents(bytes: &[u8], total: &mut MetadataBudget, file_len: u64) -> Result<()> {
    let mut position = 0;
    let version = read_integer(bytes, &mut position, 1)?;
    if version > 2 {
        return Err(invalid("unsupported AVIF item location version"));
    }
    read_integer(bytes, &mut position, 3)?;
    let sizes = read_integer(bytes, &mut position, 1)? as usize;
    let base_sizes = read_integer(bytes, &mut position, 1)? as usize;
    let offset_size = sizes >> 4;
    let length_size = sizes & 15;
    let base_size = base_sizes >> 4;
    let index_size = if version == 0 { 0 } else { base_sizes & 15 };
    let count = read_integer(bytes, &mut position, if version < 2 { 2 } else { 4 })?;
    total.charge(count)?;
    for _ in 0..count {
        read_integer(bytes, &mut position, if version < 2 { 2 } else { 4 })?;
        if version > 0 {
            read_integer(bytes, &mut position, 2)?;
        }
        read_integer(bytes, &mut position, 2)?;
        read_integer(bytes, &mut position, base_size)?;
        let extents = read_integer(bytes, &mut position, 2)?;
        total.charge(extents)?;
        for _ in 0..extents {
            read_integer(bytes, &mut position, index_size)?;
            read_integer(bytes, &mut position, offset_size)?;
            let length = read_integer(bytes, &mut position, length_size)?;
            // Zero-length extents mean "to end" and could duplicate an mdat
            // many times. Our still-image encoder emits explicit lengths.
            if length == 0 {
                return Err(invalid("unbounded AVIF item extent"));
            }
            total.extent_bytes = total
                .extent_bytes
                .checked_add(length)
                .filter(|total| *total <= file_len)
                .ok_or(Error::Limit)?;
        }
    }
    Ok(())
}

/// A still item must contain one sequence header and one frame. This blocks
/// appended sequences/frames from changing admission after header inspection.
fn still_item(bytes: &[u8]) -> Result<()> {
    let mut position = 0usize;
    let mut sequences = 0;
    let mut frames = 0;
    while position < bytes.len() {
        let header = bytes[position];
        position += 1;
        if header & 0x81 != 0 || header & 2 == 0 {
            return Err(invalid("unsupported AV1 OBU header"));
        }
        let kind = (header >> 3) & 15;
        if header & 4 != 0 {
            let extension = *bytes
                .get(position)
                .ok_or_else(|| invalid("truncated AV1 extension"))?;
            if extension != 0 {
                return Err(invalid("layered AVIF is unsupported"));
            }
            position += 1;
        }
        let mut length = 0u64;
        let mut complete = false;
        for shift in (0..56).step_by(7) {
            let byte = *bytes
                .get(position)
                .ok_or_else(|| invalid("truncated AV1 length"))?;
            position += 1;
            length |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(invalid("invalid AV1 length"));
        }
        let length = usize::try_from(length).map_err(|_| Error::Limit)?;
        position = position
            .checked_add(length)
            .filter(|position| *position <= bytes.len())
            .ok_or(Error::Limit)?;
        match kind {
            1 => sequences += 1,
            3 | 6 => frames += 1,
            2 | 4 | 5 | 15 => {}
            _ => return Err(invalid("unsupported AV1 OBU type")),
        }
        if sequences > 1 || frames > 1 {
            return Err(invalid("multiframe AVIF is unsupported"));
        }
    }
    if sequences != 1 || frames != 1 {
        return Err(invalid("AVIF must contain one still frame"));
    }
    Ok(())
}

fn parsed(bytes: &[u8], limits: DecodeLimits) -> Result<(avif_parse::AvifData, PageDimensions)> {
    // avif-parse has parser-state assertions for some malformed properties.
    // This boundary covers Rust metadata parsing only, never decoder FFI.
    std::panic::catch_unwind(|| parsed_inner(bytes, limits))
        .unwrap_or_else(|_| Err(invalid("malformed AVIF metadata")))
}

fn parsed_inner(
    bytes: &[u8],
    limits: DecodeLimits,
) -> Result<(avif_parse::AvifData, PageDimensions)> {
    if bytes.len() > limits.max_encoded_bytes {
        return Err(Error::Limit);
    }
    boxes(bytes, 0, &mut MetadataBudget::default(), bytes.len() as u64)?;
    let data = avif_parse::read_avif(&mut &*bytes).map_err(invalid)?;
    still_item(&data.primary_item)?;
    let metadata = data.primary_item_metadata().map_err(invalid)?;
    if !metadata.still_picture || metadata.bit_depth != 8 {
        return Err(invalid("only 8-bit AVIF still images are supported"));
    }
    let dimensions = PageDimensions {
        width: metadata.max_frame_width.get(),
        height: metadata.max_frame_height.get(),
    };
    let pixels = u64::from(dimensions.width) * u64::from(dimensions.height);
    let stride = (u64::from(dimensions.width) + 127) & !127;
    let rows = (u64::from(dimensions.height) + 127) & !127;
    // Account conservatively for padded decoder working planes, output RGBA,
    // and parser/coded-data copies before creating any decoder context.
    let working = (stride + 64)
        .checked_mul(rows)
        .and_then(|bytes| bytes.checked_mul(12))
        .and_then(|bytes| bytes.checked_add(pixels * 4))
        .and_then(|size| size.checked_add(bytes.len() as u64 * 2))
        .ok_or(Error::Limit)?;
    if dimensions.width > limits.max_width
        || dimensions.height > limits.max_height
        || pixels > limits.max_pixels
        || working > limits.max_decoded_bytes
    {
        return Err(Error::Limit);
    }
    if let Some(alpha) = data.alpha_item.as_deref() {
        still_item(alpha)?;
        let alpha = data
            .alpha_item_metadata()
            .map_err(invalid)?
            .ok_or_else(|| invalid("missing alpha metadata"))?;
        if !alpha.still_picture
            || alpha.bit_depth != 8
            || alpha.max_frame_width.get() != dimensions.width
            || alpha.max_frame_height.get() != dimensions.height
        {
            return Err(invalid("unsupported AVIF alpha dimensions or depth"));
        }
    }
    Ok((data, dimensions))
}

pub fn inspect(bytes: &[u8], limits: DecodeLimits) -> Result<PageDimensions> {
    parsed(bytes, limits).map(|(_, dimensions)| dimensions)
}

struct Frame {
    context: Option<Dav1dContext>,
    picture: Dav1dPicture,
}
impl Drop for Frame {
    fn drop(&mut self) {
        // SAFETY: these values are owned decoder handles, released once here.
        unsafe {
            dav1d_picture_unref(Some(NonNull::from(&mut self.picture)));
            dav1d_close(Some(NonNull::from(&mut self.context)));
        }
    }
}
impl Frame {
    fn decode(bytes: &[u8], dimensions: PageDimensions) -> Result<Self> {
        let mut frame = Self {
            context: None,
            picture: Dav1dPicture::default(),
        };
        // SAFETY: the decoder initializes settings before we read them; the
        // owned context/data/picture follow rav1d's documented ownership API.
        unsafe {
            let mut settings = MaybeUninit::<Dav1dSettings>::uninit();
            dav1d_default_settings(NonNull::from(&mut settings).cast());
            let mut settings = settings.assume_init();
            settings.n_threads = 1;
            settings.max_frame_delay = 1;
            settings.frame_size_limit = dimensions
                .width
                .checked_mul(dimensions.height)
                .ok_or(Error::Limit)?;
            settings.strict_std_compliance = 1;
            settings.all_layers = 0;
            check(dav1d_open(
                Some(NonNull::from(&mut frame.context)),
                Some(NonNull::from(&mut settings)),
            ))?;
            let mut data = Dav1dData::default();
            let buffer = dav1d_data_create(Some(NonNull::from(&mut data)), bytes.len());
            if buffer.is_null() {
                return Err(Error::Limit);
            }
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), buffer, bytes.len());
            let result = dav1d_send_data(frame.context, Some(NonNull::from(&mut data)));
            dav1d_data_unref(Some(NonNull::from(&mut data)));
            check(result)?;
            check(dav1d_get_picture(
                frame.context,
                Some(NonNull::from(&mut frame.picture)),
            ))?;
        }
        if frame.picture.p.bpc != 8
            || frame.picture.p.w != dimensions.width as i32
            || frame.picture.p.h != dimensions.height as i32
        {
            return Err(invalid(
                "decoded AVIF does not match admitted dimensions/depth",
            ));
        }
        Ok(frame)
    }
    fn header(&self) -> Result<&Dav1dSequenceHeader> {
        let header = self
            .picture
            .seq_hdr
            .ok_or_else(|| invalid("missing AVIF color metadata"))?;
        // SAFETY: the picture owns its sequence header until Frame::drop.
        Ok(unsafe { header.as_ref() })
    }
    fn range(&self) -> Result<YuvRange> {
        Ok(if self.header()?.color_range != 0 {
            YuvRange::Full
        } else {
            YuvRange::Limited
        })
    }
    fn plane(&self, index: usize) -> Result<(&[u8], u32)> {
        let (horizontal, vertical) = if index == 0 {
            (0, 0)
        } else {
            match self.picture.p.layout {
                DAV1D_PIXEL_LAYOUT_I444 => (0, 0),
                DAV1D_PIXEL_LAYOUT_I422 => (1, 0),
                DAV1D_PIXEL_LAYOUT_I420 => (1, 1),
                _ => return Err(invalid("missing AVIF chroma")),
            }
        };
        let width = (self.picture.p.w as usize + horizontal) >> horizontal;
        let height = (self.picture.p.h as usize + vertical) >> vertical;
        let stride = usize::try_from(self.picture.stride[usize::from(index != 0)])
            .map_err(|_| Error::Limit)?;
        let maximum_stride = ((self.picture.p.w as usize + 127) & !127) + 64;
        if stride < width || stride > maximum_stride || width == 0 || height == 0 {
            return Err(invalid("invalid AVIF plane stride"));
        }
        let length = stride
            .checked_mul(height - 1)
            .and_then(|size| size.checked_add(width))
            .filter(|size| *size <= isize::MAX as usize)
            .ok_or(Error::Limit)?;
        let pointer = self.picture.data[index].ok_or_else(|| invalid("missing AVIF plane"))?;
        // SAFETY: rav1d owns padded full rows for this plane. Validated positive
        // strides and admitted dimensions keep this slice inside those rows;
        // its lifetime is tied to the owning Frame and never outlives it.
        Ok((
            unsafe { std::slice::from_raw_parts(pointer.as_ptr().cast(), length) },
            stride as u32,
        ))
    }
    fn rgba(&self) -> Result<image::RgbaImage> {
        let width = self.picture.p.w as u32;
        let height = self.picture.p.h as u32;
        let (y, y_stride) = self.plane(0)?;
        let range = self.range()?;
        let mut output = image::RgbaImage::new(width, height);
        if self.picture.p.layout == DAV1D_PIXEL_LAYOUT_I400 {
            for (row, pixels) in output.rows_mut().enumerate() {
                for (column, pixel) in pixels.enumerate() {
                    let value = luma(y[row * y_stride as usize + column], range);
                    *pixel = image::Rgba([value, value, value, 255]);
                }
            }
            return Ok(output);
        }
        let (u, u_stride) = self.plane(1)?;
        let (v, v_stride) = self.plane(2)?;
        let planar = YuvPlanarImage {
            y_plane: y,
            y_stride,
            u_plane: u,
            u_stride,
            v_plane: v,
            v_stride,
            width,
            height,
        };
        let destination = output.as_mut();
        let stride = width * 4;
        let layout = self.picture.p.layout;
        let matrix = match self.header()?.mtrx {
            DAV1D_MC_IDENTITY if layout == DAV1D_PIXEL_LAYOUT_I444 => {
                yuv::gbr_to_rgba(&planar, destination, stride, range).map_err(invalid)?;
                return Ok(output);
            }
            DAV1D_MC_BT709 => YuvStandardMatrix::Bt709,
            DAV1D_MC_FCC => YuvStandardMatrix::Fcc,
            DAV1D_MC_BT470BG | DAV1D_MC_BT601 | DAV1D_MC_UNKNOWN => YuvStandardMatrix::Bt601,
            DAV1D_MC_SMPTE240 => YuvStandardMatrix::Smpte240,
            DAV1D_MC_BT2020_NCL => YuvStandardMatrix::Bt2020,
            _ => return Err(invalid("unsupported AVIF color matrix")),
        };
        match layout {
            DAV1D_PIXEL_LAYOUT_I444 => {
                yuv::yuv444_to_rgba(&planar, destination, stride, range, matrix)
            }
            DAV1D_PIXEL_LAYOUT_I422 => {
                yuv::yuv422_to_rgba(&planar, destination, stride, range, matrix)
            }
            DAV1D_PIXEL_LAYOUT_I420 => {
                yuv::yuv420_to_rgba(&planar, destination, stride, range, matrix)
            }
            _ => return Err(invalid("unsupported AVIF pixel layout")),
        }
        .map_err(invalid)?;
        Ok(output)
    }
}

fn check(result: rav1d::Dav1dResult) -> Result<()> {
    if result.0 == 0 {
        Ok(())
    } else {
        Err(invalid(format!("AV1 decoder error {}", result.0)))
    }
}
fn luma(value: u8, range: YuvRange) -> u8 {
    match range {
        YuvRange::Full => value,
        YuvRange::Limited => {
            ((u32::from(value).saturating_sub(16).min(219) * 255 + 109) / 219) as u8
        }
    }
}

pub fn decode(bytes: &[u8], limits: DecodeLimits) -> Result<image::RgbaImage> {
    let (data, dimensions) = parsed(bytes, limits)?;
    let frame = Frame::decode(&data.primary_item, dimensions)?;
    let mut output = frame.rgba()?;
    drop(frame);
    if let Some(alpha) = data.alpha_item.as_deref() {
        let frame = Frame::decode(alpha, dimensions)?;
        let (plane, stride) = frame.plane(0)?;
        let range = frame.range()?;
        for (row, pixels) in output.rows_mut().enumerate() {
            for (column, pixel) in pixels.enumerate() {
                let alpha = luma(plane[row * stride as usize + column], range);
                pixel[3] = alpha;
                if data.premultiplied_alpha && alpha != 0 && alpha != 255 {
                    for color in &mut pixel.0[..3] {
                        *color = (u16::from(*color) * 255 / u16::from(alpha)).min(255) as u8;
                    }
                }
            }
        }
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    const TINY: &[u8] = include_bytes!("../tests/fixtures/tiny.avif");

    #[test]
    fn independent_avifenc_fixture_decodes_with_correct_colors() {
        let image = decode(TINY, DecodeLimits::default()).unwrap();
        assert_eq!(image.dimensions(), (16, 16));
        // Reference pixels decoded independently with Pillow/libavif. Allow a
        // few units for the YUV converter's integer rounding differences.
        for (position, expected) in [
            ((0, 0), [255u8, 0, 2, 255]),
            ((8, 8), [118, 0, 141, 255]),
            ((15, 15), [2, 2, 249, 255]),
        ] {
            let actual = image.get_pixel(position.0, position.1);
            assert!(
                actual
                    .0
                    .iter()
                    .zip(expected)
                    .all(|(actual, expected)| actual.abs_diff(expected) <= 3),
                "reference pixel {position:?}: {actual:?} != {expected:?}"
            );
        }
    }

    #[test]
    fn avif_rejects_corrupt_payload_and_truncation_without_aborting() {
        // Upstream rav1d 1.1.0 aborts on this exact malformed frame. The pinned
        // BSD fork repairs its missing-frame-header cleanup path.
        let mut corrupt = TINY.to_vec();
        corrupt[280] ^= 1;
        assert!(decode(&corrupt, DecodeLimits::default()).is_err());
        let mut metadata = TINY.to_vec();
        metadata[209] ^= 1; // pixi channel count must match its bounded payload
        assert!(decode(&metadata, DecodeLimits::default()).is_err());
        for length in [0, 8, 24, 100, TINY.len() - 1] {
            assert!(inspect(&TINY[..length], DecodeLimits::default()).is_err());
        }
    }

    #[test]
    fn avif_admission_rejects_dimensions_bytes_and_working_budget_before_decode() {
        let limits = DecodeLimits::default();
        for limits in [
            DecodeLimits {
                max_width: 15,
                ..limits
            },
            DecodeLimits {
                max_height: 15,
                ..limits
            },
            DecodeLimits {
                max_pixels: 255,
                ..limits
            },
            DecodeLimits {
                max_encoded_bytes: TINY.len() - 1,
                ..limits
            },
            DecodeLimits {
                max_decoded_bytes: 1024,
                ..limits
            },
        ] {
            assert!(matches!(inspect(TINY, limits), Err(Error::Limit)));
        }
        let oversized_box = [255, 255, 255, 255, b'm', b'd', b'a', b't'];
        assert!(matches!(
            boxes(&oversized_box, 0, &mut MetadataBudget::default(), 8),
            Err(Error::Limit)
        ));
    }

    #[test]
    fn admission_rejects_multiple_sequences_frames_and_extent_amplification() {
        let one = [0x0a, 1, 0, 0x32, 1, 0];
        assert!(still_item(&one).is_ok());
        let mut extra = one.to_vec();
        extra.extend_from_slice(&one);
        assert!(still_item(&extra).is_err());
        let mut extra = one.to_vec();
        extra.extend_from_slice(&[0x32, 1, 0]);
        assert!(still_item(&extra).is_err());
        let mut iloc = vec![0, 0, 0, 0, 0x44, 0, 0, 1, 0, 1, 0, 0, 0, 2];
        iloc.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 64]);
        iloc.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 64]);
        assert!(matches!(
            check_extents(&iloc, &mut MetadataBudget::default(), 64),
            Err(Error::Limit)
        ));
        // avif-parse preallocates iinf's declared count before parsing its
        // child boxes. Reject a tiny malicious header before that reservation.
        let iinf = [
            0, 0, 0, 16, b'i', b'i', b'n', b'f', 1, 0, 0, 0, 255, 255, 255, 255,
        ];
        assert!(matches!(
            boxes(&iinf, 0, &mut MetadataBudget::default(), 16),
            Err(Error::Limit)
        ));
        let mut budget = MetadataBudget::default();
        budget.charge(1024).unwrap();
        assert!(matches!(
            boxes(&[0, 0, 0, 8, b'f', b'r', b'e', b'e'], 0, &mut budget, 8),
            Err(Error::Limit)
        ));
    }

    #[test]
    #[ignore = "targeted process-isolated AVIF corruption sweep"]
    fn malformed_fixture_does_not_abort_decoder_process() {
        const PROBE: &str = "NOVA_READER_AVIF_CORRUPTION_PROBE";
        if let Ok(probe) = std::env::var(PROBE) {
            let probe = probe.parse::<usize>().unwrap();
            let mut bytes = TINY.to_vec();
            bytes[probe / 8] ^= 1 << (probe % 8);
            let _ = crate::decode_avif(&bytes, DecodeLimits::default());
            return;
        }
        let executable = std::env::current_exe().unwrap();
        for byte in 0..TINY.len() {
            for bit in [0, 7] {
                let output = std::process::Command::new(&executable)
                    .args([
                        "--ignored",
                        "--exact",
                        "avif::tests::malformed_fixture_does_not_abort_decoder_process",
                    ])
                    .env(PROBE, (byte * 8 + bit).to_string())
                    .output()
                    .unwrap();
                assert!(
                    output.status.success(),
                    "AVIF mutation byte {byte}, bit {bit} aborted: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        }
    }
}
