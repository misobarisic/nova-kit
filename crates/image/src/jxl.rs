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
    let mut framing = StreamFraming::default();
    framing.feed(bytes, limits.max_encoded_bytes)?;
    framing.finish()?;
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

/// Bounds for the temporary, full-image progressive preview. Native source
/// dimensions remain unchanged; previews never upscale or exceed 32 MiB RGBA.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JxlPreviewOptions {
    pub max_width: u32,
    pub max_height: u32,
    pub max_bytes: usize,
}
impl Default for JxlPreviewOptions {
    fn default() -> Self {
        Self {
            max_width: 1024,
            max_height: 2048,
            max_bytes: 8 * 1024 * 1024,
        }
    }
}

/// Provisional artwork only. A preview does not establish a complete or valid
/// file; callers must still validate EOF before publishing a final page/cache.
pub struct JxlPreview {
    pub source: PageDimensions,
    pub image: image::RgbaImage,
}

/// Incremental JPEG XL still-image decoder. `feed` only admits/feeds bytes;
/// callers choose the cadence of `render_preview` (for example every 200 ms).
/// Two source rows are converted at a time, without a full native RGBA preview.
/// The decoder retains no second encoded body and never inflates auxiliary boxes.
/// Existing strict `decode_jxl`/`inspect_jxl` behavior is unchanged.
pub struct ProgressiveJxlDecoder {
    limits: DecodeLimits,
    options: JxlPreviewOptions,
    parser: jxl_bitstream::ContainerParser,
    framing: StreamFraming,
    pending: Vec<u8>,
    header: Vec<u8>,
    header_probe: usize,
    uninitialized: Option<jxl_oxide::UninitializedJxlImage>,
    image: Option<JxlImage>,
    tracker: AllocTracker,
    source: Option<PageDimensions>,
    preview_size: Option<PageDimensions>,
    encoded_bytes: usize,
    init_bytes: usize,
    dirty: bool,
    last_preview: Option<u64>,
    failed: bool,
}

const JXL_HEADER_LIMIT: usize = 256 * 1024;
const PREVIEW_LIMIT: usize = 32 * 1024 * 1024;
impl ProgressiveJxlDecoder {
    pub fn new(limits: DecodeLimits, mut options: JxlPreviewOptions) -> Result<Self> {
        if options.max_width == 0
            || options.max_height == 0
            || options.max_bytes < 4
            || limits.max_encoded_bytes == 0
            || limits.max_decoded_bytes == 0
        {
            return Err(Error::Limit);
        }
        options.max_bytes = options.max_bytes.min(PREVIEW_LIMIT);
        let budget = usize::try_from(limits.max_decoded_bytes).map_err(|_| Error::Limit)?;
        Ok(Self {
            limits,
            options,
            parser: Default::default(),
            framing: Default::default(),
            pending: Vec::new(),
            header: Vec::new(),
            header_probe: 1,
            uninitialized: None,
            image: None,
            tracker: AllocTracker::with_limit(budget),
            source: None,
            preview_size: None,
            encoded_bytes: 0,
            init_bytes: 0,
            dirty: false,
            last_preview: None,
            failed: false,
        })
    }
    /// Oriented native dimensions, available after the bounded image header is
    /// admitted, before any progressive pixels need to be renderable.
    pub fn dimensions(&self) -> Option<PageDimensions> {
        self.source
    }

    pub fn feed(&mut self, chunk: &[u8]) -> Result<()> {
        if self.failed {
            return Err(invalid("progressive decoder already failed"));
        }
        let result = self.feed_inner(chunk);
        if result.is_err() {
            self.fail();
        }
        result
    }
    fn fail(&mut self) {
        self.failed = true;
        self.image = None;
        self.uninitialized = None;
        self.header.clear();
        self.pending.clear();
    }
    fn feed_inner(&mut self, chunk: &[u8]) -> Result<()> {
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(chunk.len())
            .filter(|bytes| *bytes <= self.limits.max_encoded_bytes)
            .ok_or(Error::Limit)?;
        // Container parsers leave at most a partial signature/header/index.
        // Bound the bridge buffer independently of a caller's chunk size.
        for part in chunk.chunks(8192) {
            self.framing.feed(part, self.limits.max_encoded_bytes)?;
            let mut pending = std::mem::take(&mut self.pending);
            pending.extend_from_slice(part);
            let mut parser = std::mem::take(&mut self.parser);
            // jxl-bitstream 1.1.0 treats an 8..15-byte extended-size header
            // as invalid instead of incomplete. Keep every partial outer
            // header until our framing gate has collected its full length.
            let ready = pending
                .len()
                .checked_sub(self.framing.header.len())
                .ok_or_else(|| invalid("invalid JPEG XL parser boundary"))?;
            for event in parser.feed_bytes(&pending[..ready]) {
                match event.map_err(invalid)? {
                    jxl_bitstream::ParseEvent::BitstreamKind(
                        jxl_bitstream::BitstreamKind::Invalid,
                    ) => return Err(invalid("invalid JPEG XL signature")),
                    jxl_bitstream::ParseEvent::Codestream(data) => self.feed_codestream(data)?,
                    _ => {} // Exif/XML/JUMBF/Brotli stay outside the image decoder.
                }
            }
            let consumed = parser.previous_consumed_bytes();
            pending.drain(..consumed);
            if pending.len() > 32 {
                return Err(invalid("oversized JPEG XL container header"));
            }
            self.pending = pending;
            self.parser = parser;
        }
        Ok(())
    }
    fn feed_codestream(&mut self, mut bytes: &[u8]) -> Result<()> {
        if !bytes.is_empty() {
            self.dirty = true;
        }
        while !bytes.is_empty() {
            if let Some(image) = &mut self.image {
                if image.feed_bytes(bytes).map_err(invalid)? != bytes.len() {
                    return Err(invalid("unconsumed JPEG XL codestream"));
                }
                return Ok(());
            }
            if self.source.is_none() {
                let count = bytes.len().min(1024);
                if self.header.len().saturating_add(count) > JXL_HEADER_LIMIT {
                    return Err(invalid("oversized JPEG XL header"));
                }
                self.header.extend_from_slice(&bytes[..count]);
                bytes = &bytes[count..];
                if self.header.len() < self.header_probe {
                    continue;
                }
                if !self.try_header()? {
                    self.header_probe = self
                        .header
                        .len()
                        .saturating_add(self.header.len().clamp(1, 1024));
                }
            } else {
                self.init_bytes = self
                    .init_bytes
                    .checked_add(bytes.len())
                    .filter(|bytes| *bytes <= JXL_HEADER_LIMIT)
                    .ok_or_else(|| invalid("oversized JPEG XL header"))?;
                let mut uninitialized = self
                    .uninitialized
                    .take()
                    .ok_or_else(|| invalid("JPEG XL initialization is unavailable"))?;
                if uninitialized.feed_bytes(bytes).map_err(invalid)? != bytes.len() {
                    return Err(invalid("incomplete JPEG XL initialization"));
                }
                bytes = &[];
                self.initialize(uninitialized)?;
            }
        }
        Ok(())
    }
    fn try_header(&mut self) -> Result<bool> {
        if self.source.is_some() {
            return Ok(true);
        }
        let header = match jxl_oxide::ImageHeader::parse(
            &mut jxl_bitstream::Bitstream::new(&self.header),
            (),
        ) {
            Ok(header) => header,
            Err(error) if error.unexpected_eof() => return Ok(false),
            Err(error) => return Err(invalid(error)),
        };
        self.admit_header(&header)?;
        let buffer = std::mem::take(&mut self.header);
        self.init_bytes = buffer.len();
        let mut uninitialized = JxlImage::builder()
            .alloc_tracker(self.tracker.clone())
            .build_uninit();
        if uninitialized.feed_bytes(&buffer).map_err(invalid)? != buffer.len() {
            return Err(invalid("incomplete JPEG XL header"));
        }
        self.initialize(uninitialized)?;
        Ok(true)
    }
    fn admit_header(&mut self, header: &jxl_oxide::ImageHeader) -> Result<()> {
        let width = header.width_with_orientation();
        let height = header.height_with_orientation();
        let pixels = u64::from(width) * u64::from(height);
        let native_bytes = pixels.checked_mul(4).ok_or(Error::Limit)?;
        if width == 0
            || height == 0
            || width > self.limits.max_width
            || height > self.limits.max_height
            || pixels > self.limits.max_pixels
            || native_bytes > self.limits.max_decoded_bytes
        {
            return Err(Error::Limit);
        }
        if header.metadata.colour_encoding.want_icc()
            || header.metadata.preview.is_some()
            || header.metadata.animation.is_some()
            || header.metadata.ec_info.len() > 1
            || header
                .metadata
                .ec_info
                .iter()
                .any(|channel| channel.is_black())
        {
            return Err(Error::JxlUnsupported(
                "custom ICC, embedded preview, animation, CMYK or extra JPEG XL channels",
            ));
        }
        let scale = (f64::from(self.options.max_width) / f64::from(width))
            .min(f64::from(self.options.max_height) / f64::from(height))
            .min((self.options.max_bytes as f64 / (pixels as f64 * 4.)).sqrt())
            .min(1.);
        let preview = PageDimensions {
            width: (f64::from(width) * scale).floor().max(1.) as u32,
            height: (f64::from(height) * scale).floor().max(1.) as u32,
        };
        let preview_bytes = u64::from(preview.width) * u64::from(preview.height) * 4;
        if preview_bytes > self.options.max_bytes as u64 {
            return Err(Error::Limit);
        }
        // Keep the established strict native-output/row reservation. A
        // bounded preview uses this otherwise unallocated output allowance;
        // consumers account for retained UI copies in their display budget.
        let reserve = native_bytes
            .checked_add(u64::from(width) * 4)
            .ok_or(Error::Limit)?;
        self.tracker
            .shrink_limit(usize::try_from(reserve).map_err(|_| Error::Limit)?)
            .map_err(|_| Error::Limit)?;
        self.source = Some(PageDimensions { width, height });
        self.preview_size = Some(preview);
        Ok(())
    }
    fn initialize(&mut self, uninitialized: jxl_oxide::UninitializedJxlImage) -> Result<()> {
        match uninitialized.try_init().map_err(invalid)? {
            InitializeResult::NeedMoreData(next) => self.uninitialized = Some(next),
            InitializeResult::Initialized(mut image) => {
                let source = self
                    .source
                    .ok_or_else(|| invalid("missing JPEG XL source geometry"))?;
                if image.width() != source.width
                    || image.height() != source.height
                    || image.pixel_format().has_black()
                    || image.image_header().metadata.animation.is_some()
                {
                    return Err(invalid("JPEG XL source geometry or channel layout changed"));
                }
                image.request_color_encoding(jxl_oxide::EnumColourEncoding::srgb(
                    jxl_oxide::RenderingIntent::Relative,
                ));
                self.image = Some(image);
            }
        }
        Ok(())
    }
    pub fn render_preview(&mut self) -> Result<Option<JxlPreview>> {
        if self.failed {
            return Err(invalid("progressive decoder already failed"));
        }
        if !self.dirty {
            return Ok(None);
        }
        // A caller-throttled preview is also an explicit header probe. Tiny
        // chunks otherwise probe at increasing intervals, bounding header work.
        self.try_header()?;
        self.dirty = false;
        let (Some(source), Some(size)) = (self.source, self.preview_size) else {
            return Ok(None);
        };
        let baseline =
            (u64::from(source.width) * u64::from(source.height) + u64::from(source.width)) * 4;
        let row_count = if size == source || source.height == 1 {
            1
        } else {
            2
        };
        let needed = (u64::from(size.width) * u64::from(size.height)
            + u64::from(source.width) * row_count)
            * 4;
        let extra = usize::try_from(needed.saturating_sub(baseline)).map_err(|_| Error::Limit)?;
        if self.tracker.shrink_limit(extra).is_err() {
            return Ok(None);
        }
        let result = (|| {
            let Some(image) = &mut self.image else {
                return Ok(None);
            };
            let render = if image.is_loading_done() {
                image.render_frame(0).map_err(invalid)?
            } else {
                // Not all files have useful partial frames. Their normal final
                // decode still works; incomplete preview rendering is optional.
                match image.render_loading_frame() {
                    Ok(render) => render,
                    Err(_) => return Ok(None),
                }
            };
            let output = convert_render(&render, source, size)?;
            use std::hash::{Hash, Hasher};
            let mut hash = std::collections::hash_map::DefaultHasher::new();
            output.as_raw().hash(&mut hash);
            let hash = hash.finish();
            if self.last_preview == Some(hash) {
                return Ok(None);
            }
            self.last_preview = Some(hash);
            Ok(Some(JxlPreview {
                source,
                image: output,
            }))
        })();
        self.tracker.expand_limit(extra);
        result
    }
    /// Consume a complete file, validating the final still frame even if earlier
    /// previews were displayed. Truncation and malformed data never become a
    /// successful final image merely because a preview could be rendered.
    pub fn finish(mut self) -> Result<image::RgbaImage> {
        self.validate_complete()?;
        let source = self
            .source
            .ok_or_else(|| invalid("incomplete JPEG XL header"))?;
        let image = self
            .image
            .as_ref()
            .ok_or_else(|| invalid("incomplete JPEG XL image"))?;
        let render = image.render_frame(0).map_err(invalid)?;
        convert_render(&render, source, source)
    }
    /// Validate EOF and final renderability without allocating native RGBA.
    /// Consumers retaining their existing strict display decoder can then drop
    /// this decoder before allocating that decoder's final output.
    pub fn validate_complete(&mut self) -> Result<()> {
        if self.failed {
            return Err(invalid("progressive decoder already failed"));
        }
        self.framing.finish()?;
        self.try_header()?;
        if !self.pending.is_empty() {
            return Err(invalid("incomplete JPEG XL container"));
        }
        let image = self
            .image
            .as_mut()
            .ok_or_else(|| invalid("incomplete JPEG XL header"))?;
        image.finalize().map_err(invalid)?;
        if !image.is_loading_done() || image.num_loaded_keyframes() != 1 {
            return Err(invalid("incomplete JPEG XL still page"));
        }
        let render = image.render_frame(0).map_err(invalid)?;
        let stream = render.stream();
        let source = self
            .source
            .ok_or_else(|| invalid("missing JPEG XL source geometry"))?;
        if stream.width() != source.width
            || stream.height() != source.height
            || !(1..=4).contains(&stream.channels())
        {
            return Err(invalid("JPEG XL channel layout changed"));
        }
        Ok(())
    }
}

fn convert_render(
    render: &jxl_oxide::Render,
    source: PageDimensions,
    target: PageDimensions,
) -> Result<image::RgbaImage> {
    let mut stream = render.stream();
    let channels = stream.channels() as usize;
    if !(1..=4).contains(&channels)
        || stream.width() != source.width
        || stream.height() != source.height
    {
        return Err(invalid("JPEG XL channel layout changed"));
    }
    let row_len = (source.width as usize)
        .checked_mul(channels)
        .ok_or(Error::Limit)?;
    if source == target {
        let mut output = image::RgbaImage::new(source.width, source.height);
        let mut row = vec![0u8; row_len];
        for destination in output.as_mut().chunks_exact_mut(source.width as usize * 4) {
            if stream.write_to_buffer(&mut row) != row_len {
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
        return Ok(output);
    }
    let mut previous = if source.height == 1 {
        Vec::new()
    } else {
        vec![0u8; row_len]
    };
    let mut current = vec![0u8; row_len];
    let mut next_row = 0u32;
    let mut output = image::RgbaImage::new(target.width, target.height);
    let sample = |row: &[u8], x: u32| -> [u8; 4] {
        let pixel = &row[x as usize * channels..][..channels];
        match channels {
            1 => [pixel[0], pixel[0], pixel[0], 255],
            2 => [pixel[0], pixel[0], pixel[0], pixel[1]],
            3 => [pixel[0], pixel[1], pixel[2], 255],
            _ => [pixel[0], pixel[1], pixel[2], pixel[3]],
        }
    };
    for y in 0..target.height {
        let sy = ((f64::from(y) + 0.5) * f64::from(source.height) / f64::from(target.height) - 0.5)
            .clamp(0., f64::from(source.height - 1));
        let y0 = sy.floor() as u32;
        let y1 = (y0 + 1).min(source.height - 1);
        while next_row <= y1 {
            if source.height != 1 {
                std::mem::swap(&mut previous, &mut current);
            }
            if stream.write_to_buffer(&mut current) != row_len {
                return Err(invalid("incomplete JPEG XL pixels"));
            }
            next_row += 1;
        }
        let first = if y0 == y1 { &current } else { &previous };
        for x in 0..target.width {
            let sx = ((f64::from(x) + 0.5) * f64::from(source.width) / f64::from(target.width)
                - 0.5)
                .clamp(0., f64::from(source.width - 1));
            let x0 = sx.floor() as u32;
            let x1 = (x0 + 1).min(source.width - 1);
            let fx = sx - f64::from(x0);
            let fy = sy - f64::from(y0);
            let pixel = if fx == 0. && fy == 0. {
                sample(first, x0)
            } else {
                let mut rgb = [0.; 3];
                let mut alpha = 0.;
                for (pixel, weight) in [
                    (sample(first, x0), (1. - fx) * (1. - fy)),
                    (sample(first, x1), fx * (1. - fy)),
                    (sample(&current, x0), (1. - fx) * fy),
                    (sample(&current, x1), fx * fy),
                ] {
                    let coverage = f64::from(pixel[3]) * weight;
                    alpha += coverage;
                    for channel in 0..3 {
                        rgb[channel] += f64::from(pixel[channel]) * coverage;
                    }
                }
                let mut result = [0; 4];
                if alpha > 0. {
                    for channel in 0..3 {
                        result[channel] = (rgb[channel] / alpha).round().clamp(0., 255.) as u8;
                    }
                }
                result[3] = alpha.round().clamp(0., 255.) as u8;
                result
            };
            output.put_pixel(x, y, image::Rgba(pixel));
        }
    }
    Ok(output)
}

/// ContainerParser intentionally has no EOF operation. Track only box framing
/// (not auxiliary payloads) so a complete codestream in a truncated container
/// cannot be promoted to a successful final file.
#[derive(Default)]
struct StreamFraming {
    kind: u8, // 0 signature, 1 raw, 2 boxes
    header: Vec<u8>,
    remaining: Option<u64>,
    in_body: bool,
    partial_index: Vec<u8>,
    partial_box: bool,
    saw_partial: bool,
    last_partial: bool,
}
impl StreamFraming {
    fn feed(&mut self, mut bytes: &[u8], limit: usize) -> Result<()> {
        const SIGNATURE: &[u8] = b"\0\0\0\x0cJXL \r\n\x87\n";
        while !bytes.is_empty() {
            if self.kind == 1 {
                return Ok(());
            }
            if self.kind == 0 {
                self.header.push(bytes[0]);
                bytes = &bytes[1..];
                if self.header == b"\xff\x0a" {
                    self.kind = 1;
                    self.header.clear();
                    continue;
                }
                if !b"\xff\x0a".starts_with(&self.header) && !SIGNATURE.starts_with(&self.header) {
                    return Err(invalid("invalid JPEG XL signature"));
                }
                if self.header == SIGNATURE {
                    self.kind = 2;
                    self.header.clear();
                }
                continue;
            }
            if self.in_body {
                let count = self
                    .remaining
                    .map_or(bytes.len(), |left| bytes.len().min(left as usize));
                if self.partial_box && self.partial_index.len() < 4 {
                    let index_bytes = count.min(4 - self.partial_index.len());
                    self.partial_index.extend_from_slice(&bytes[..index_bytes]);
                    if self.partial_index.len() == 4 {
                        self.last_partial =
                            u32::from_be_bytes(self.partial_index[..].try_into().unwrap())
                                & 0x80000000
                                != 0;
                    }
                }
                bytes = &bytes[count..];
                if let Some(remaining) = &mut self.remaining {
                    *remaining -= count as u64;
                    if *remaining == 0 {
                        self.in_body = false;
                    }
                }
                continue;
            }
            let needed = if self.header.len() >= 8 && self.header[..4] == 1u32.to_be_bytes() {
                16
            } else {
                8
            };
            let count = (needed - self.header.len()).min(bytes.len());
            self.header.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.header.len() < 8 {
                continue;
            }
            let size = u32::from_be_bytes(self.header[..4].try_into().unwrap());
            if size == 1 && self.header.len() < 16 {
                continue;
            }
            let (size, header_size) = if size == 1 {
                (
                    u64::from_be_bytes(self.header[8..16].try_into().unwrap()),
                    16,
                )
            } else {
                (u64::from(size), 8)
            };
            let payload = if size == 0 {
                None
            } else {
                Some(
                    size.checked_sub(header_size)
                        .ok_or_else(|| invalid("invalid JPEG XL box length"))?,
                )
            };
            if size > limit as u64 {
                return Err(Error::Limit);
            }
            self.partial_box = &self.header[4..8] == b"jxlp";
            if self.partial_box {
                self.saw_partial = true;
                self.partial_index.clear();
                if payload.is_some_and(|bytes| bytes < 4) {
                    return Err(invalid("incomplete JPEG XL partial box"));
                }
            }
            self.remaining = payload;
            self.in_body = payload != Some(0);
            self.header.clear();
        }
        Ok(())
    }
    fn finish(&self) -> Result<()> {
        if self.kind == 0
            || !self.header.is_empty()
            || (self.in_body && self.remaining.is_some_and(|remaining| remaining != 0))
            || (self.partial_box && self.partial_index.len() < 4)
            || (self.saw_partial && !self.last_partial)
        {
            return Err(invalid("incomplete JPEG XL container"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod progressive_admission_tests {
    use super::*;
    fn parsed_header() -> jxl_oxide::ImageHeader {
        let bytes = codestream(
            include_bytes!("../tests/fixtures/jxl/rgb.jxl"),
            DecodeLimits::default(),
        )
        .unwrap();
        jxl_oxide::ImageHeader::parse(&mut jxl_bitstream::Bitstream::new(&bytes), ()).unwrap()
    }
    #[test]
    fn unsupported_headers_are_distinct_and_rejected_before_native_decoder_allocation() {
        for kind in 0..4 {
            let mut header = parsed_header();
            match kind {
                0 => {
                    header.metadata.animation = Some(jxl_oxide::image::AnimationHeader {
                        tps_numerator: 1,
                        tps_denominator: 1,
                        num_loops: 0,
                        have_timecodes: false,
                    })
                }
                1 => header
                    .metadata
                    .ec_info
                    .push(jxl_oxide::image::ExtraChannelInfo {
                        ty: jxl_oxide::ExtraChannelType::Black,
                        ..Default::default()
                    }),
                2 => {
                    header.metadata.colour_encoding = jxl_oxide::color::ColourEncoding::IccProfile(
                        jxl_oxide::color::ColourSpace::Rgb,
                    )
                }
                _ => header.metadata.ec_info = vec![Default::default(), Default::default()],
            }
            let mut decoder =
                ProgressiveJxlDecoder::new(DecodeLimits::default(), Default::default()).unwrap();
            assert!(matches!(
                decoder.admit_header(&header),
                Err(Error::JxlUnsupported(_))
            ));
            assert!(
                decoder.source.is_none()
                    && decoder.image.is_none()
                    && decoder.uninitialized.is_none()
            );
        }
    }
    #[test]
    fn preview_hard_cap_and_oriented_header_admission_do_not_allocate_rgba() {
        fn require_send<T: Send>() {}
        require_send::<ProgressiveJxlDecoder>();
        let mut header = parsed_header();
        header.size.width = 4096;
        header.size.height = 4096;
        let mut decoder = ProgressiveJxlDecoder::new(
            DecodeLimits::default(),
            JxlPreviewOptions {
                max_width: u32::MAX,
                max_height: u32::MAX,
                max_bytes: usize::MAX,
            },
        )
        .unwrap();
        decoder.admit_header(&header).unwrap();
        let size = decoder.preview_size.unwrap();
        assert!(u64::from(size.width) * u64::from(size.height) * 4 <= PREVIEW_LIMIT as u64);
        assert!(decoder.image.is_none());
        let mut header = parsed_header();
        header.metadata.orientation = 6;
        let mut decoder = ProgressiveJxlDecoder::new(
            DecodeLimits {
                max_width: 6,
                max_height: 8,
                ..Default::default()
            },
            Default::default(),
        )
        .unwrap();
        decoder.admit_header(&header).unwrap();
        assert_eq!(
            decoder.source,
            Some(PageDimensions {
                width: 6,
                height: 8
            })
        );
    }
}
