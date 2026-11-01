//! One recipe-keyed decoded LRU and processed disk-variant pipeline on all platforms.
//! Downloads and source decodes are transient; only bounded, processed pixels enter
//! the cache. Transport is supplied by the application, independent of this core.
//! Relocated from Nova b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda; see PROVENANCE.md.
use crate::config as nova_config;
pub use config::{CacheImageFormat, CacheSettings};
use config::{active_cache_settings, fnv1a, image_settings_revision, poster_cache_dir};
use serde::{Deserialize, Serialize};
use slint::{Rgba8Pixel, SharedPixelBuffer};
use std::{
    collections::{HashMap, HashSet},
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, LazyLock, Mutex, RwLock, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

#[path = "cache_gc.rs"]
pub mod gc;

pub mod blob;
pub mod config;

pub type Pixels = SharedPixelBuffer<Rgba8Pixel>;
pub const DISPLAY_POSTER_SIDE: u32 = 480;
pub const EPISODE_THUMB_SIDE: u32 = 360;
pub const DETAIL_BACKDROP_SIDE: u32 = 960;
pub const UNLOAD_EXTRA_CARDS: usize = 48;

/// Semantic uses resolve preferences at the start of each load; fixed uses keep
/// their existing display sizes. Uses with identical recipes share pixels/files.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ImageUse {
    DetailPoster,
    DetailBackdrop,
    EpisodeThumbnail,
    PlayerEpisodeBackdrop,
    Fixed(Option<u32>),
}
impl From<Option<u32>> for ImageUse {
    fn from(value: Option<u32>) -> Self {
        Self::Fixed(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ImageRecipe {
    pub max_side: Option<u32>,
    pub compress: bool,
    pub format: Option<CacheImageFormat>,
    pub quality: u8,
}
impl ImageRecipe {
    pub fn resolve(image_use: impl Into<ImageUse>, settings: impl Into<CacheSettings>) -> Self {
        let settings = settings.into();
        let max_side = match image_use.into() {
            ImageUse::DetailPoster => settings.detail_poster_max_side,
            ImageUse::DetailBackdrop => settings.detail_backdrop_max_side,
            ImageUse::EpisodeThumbnail => settings.episode_thumbnail_max_side,
            ImageUse::PlayerEpisodeBackdrop => settings.player_episode_backdrop_max_side,
            ImageUse::Fixed(side) => side,
        };
        Self {
            max_side,
            compress: settings.enabled,
            format: settings.enabled.then_some(settings.format),
            quality: if settings.enabled {
                if settings.format == CacheImageFormat::Avif {
                    settings.avif_quality.clamp(1, 100)
                } else if settings.format == CacheImageFormat::JxlLossless {
                    0
                } else if settings.format == CacheImageFormat::JxlLossy {
                    settings.jxl_quality.clamp(1, 100)
                } else {
                    settings.quality.clamp(1, 100)
                }
            } else {
                0
            },
        }
    }
    fn key(self) -> String {
        serde_json::to_string(&self).expect("image recipe is serializable")
    }
}
fn image_recipe_cache_key(url: &str, recipe: ImageRecipe) -> String {
    format!("{url}\u{1f}nova-image-v2:{}", recipe.key())
}
pub fn image_cache_key(url: &str, image_use: impl Into<ImageUse>) -> String {
    image_recipe_cache_key(
        url,
        ImageRecipe::resolve(image_use, active_cache_settings()),
    )
}
pub fn sized_cache_key(url: &str, max_side: Option<u32>) -> String {
    image_cache_key(url, ImageUse::Fixed(max_side))
}
pub fn backdrop_pixels_cached(url: &str) -> Option<Pixels> {
    decoded_cache_get(&image_cache_key(url, ImageUse::DetailBackdrop))
}

struct ImageCacheEntry {
    pixels: Pixels,
    bytes: usize,
    last_use: u64,
}
pub struct DecodedCache {
    map: HashMap<String, ImageCacheEntry>,
    total_bytes: usize,
    max_bytes: usize,
    clock: u64,
    protected: HashSet<String>,
}
impl DecodedCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            map: HashMap::new(),
            total_bytes: 0,
            max_bytes,
            clock: 0,
            protected: HashSet::new(),
        }
    }
    pub fn get(&mut self, key: &str) -> Option<Pixels> {
        self.touch_get(key)
    }
    pub fn peek(&self, key: &str) -> Option<Pixels> {
        self.map.get(key).map(|e| e.pixels.clone())
    }
    pub fn contains(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }
    pub fn stats(&self) -> (usize, usize) {
        (self.map.len(), self.total_bytes)
    }
    /// Oldest first. Useful for retaining a bounded UI window without a second LRU.
    pub fn keys_by_recency(&self) -> Vec<String> {
        let mut entries = self.map.iter().collect::<Vec<_>>();
        entries.sort_by_key(|(_, e)| e.last_use);
        entries.into_iter().map(|(k, _)| k.clone()).collect()
    }
    pub fn remove(&mut self, key: &str) -> Option<Pixels> {
        let old = self.map.remove(key)?;
        self.total_bytes = self.total_bytes.saturating_sub(old.bytes);
        Some(old.pixels)
    }
    pub fn set_protected(&mut self, keys: HashSet<String>) -> Vec<String> {
        self.protected = keys;
        self.set_budget(self.max_bytes)
    }
    pub fn set_budget(&mut self, max_bytes: usize) -> Vec<String> {
        let before = self.map.keys().cloned().collect::<Vec<_>>();
        self.max_bytes = max_bytes;
        self.evict();
        before
            .into_iter()
            .filter(|k| !self.map.contains_key(k))
            .collect()
    }
    pub fn insert(&mut self, key: String, pixels: Pixels) -> Vec<String> {
        let before = self.map.keys().cloned().collect::<Vec<_>>();
        self.store_insert(key.clone(), pixels);
        let mut evicted = before
            .into_iter()
            .filter(|k| !self.map.contains_key(k))
            .collect::<Vec<_>>();
        if !self.map.contains_key(&key) && !evicted.contains(&key) {
            evicted.push(key);
        }
        evicted
    }
    fn touch_get(&mut self, key: &str) -> Option<Pixels> {
        self.clock = self.clock.wrapping_add(1);
        let entry = self.map.get_mut(key)?;
        entry.last_use = self.clock;
        Some(entry.pixels.clone())
    }
    fn store_insert(&mut self, key: String, pixels: Pixels) {
        if let Some(old) = self.map.remove(&key) {
            self.total_bytes = self.total_bytes.saturating_sub(old.bytes);
        }
        let bytes = pixels.as_bytes().len();
        if bytes > self.max_bytes {
            return;
        }
        self.clock = self.clock.wrapping_add(1);
        self.map.insert(
            key,
            ImageCacheEntry {
                pixels,
                bytes,
                last_use: self.clock,
            },
        );
        self.total_bytes = self.total_bytes.saturating_add(bytes);
        self.evict();
    }
    fn evict(&mut self) {
        while self.total_bytes > self.max_bytes {
            let Some(key) = self
                .map
                .iter()
                .filter(|(key, _)| !self.protected.contains(*key))
                .min_by_key(|(_, entry)| entry.last_use)
                .map(|(key, _)| key.clone())
                .or_else(|| {
                    self.map
                        .iter()
                        .min_by_key(|(_, entry)| entry.last_use)
                        .map(|(key, _)| key.clone())
                })
            else {
                break;
            };
            if let Some(old) = self.map.remove(&key) {
                self.total_bytes = self.total_bytes.saturating_sub(old.bytes);
            }
        }
    }
    pub fn clear(&mut self) {
        self.map.clear();
        self.protected.clear();
        self.total_bytes = 0;
    }
}
static DECODED_POSTERS: LazyLock<Mutex<DecodedCache>> = LazyLock::new(|| {
    Mutex::new(DecodedCache {
        map: HashMap::new(),
        total_bytes: 0,
        max_bytes: 128 * 1024 * 1024,
        clock: 0,
        protected: HashSet::new(),
    })
});
pub fn set_decoded_cache_budget(mb: u32) {
    let mut cache = DECODED_POSTERS.lock().unwrap();
    cache.max_bytes = (mb as usize).saturating_mul(1024 * 1024);
    cache.evict();
}
pub fn decoded_cache_stats() -> (usize, usize) {
    let cache = DECODED_POSTERS.lock().unwrap();
    (cache.map.len(), cache.total_bytes)
}
pub fn decoded_cache_get(key: &str) -> Option<Pixels> {
    let pixels = DECODED_POSTERS.lock().unwrap().touch_get(key);
    if pixels.is_some() {
        gc::touch_decoded(key);
    }
    pixels
}
pub fn decoded_cache_contains(key: &str) -> bool {
    decoded_cache_get(key).is_some()
}
pub fn decoded_cache_insert(key: &str, pixels: Pixels) {
    DECODED_POSTERS
        .lock()
        .unwrap()
        .insert(key.to_owned(), pixels);
}
pub fn decoded_cache_clear() {
    DECODED_POSTERS.lock().unwrap().clear();
}
pub fn image_buffers_equal(a: &Pixels, b: &Pixels) -> bool {
    a.width() == b.width() && a.height() == b.height() && a.as_bytes() == b.as_bytes()
}
pub fn decode_image_bytes(
    bytes: &[u8],
) -> Result<Pixels, Box<dyn std::error::Error + Send + Sync>> {
    let rgba = decode_source(bytes)?.into_rgba8();
    Ok(SharedPixelBuffer::clone_from_slice(
        rgba.as_raw(),
        rgba.width(),
        rgba.height(),
    ))
}
fn decode_source(
    bytes: &[u8],
) -> Result<image::DynamicImage, Box<dyn std::error::Error + Send + Sync>> {
    if nova_image::is_jpeg_xl(bytes) {
        Ok(image::DynamicImage::ImageRgba8(nova_image::decode_jxl(
            bytes,
            nova_image::DecodeLimits::default(),
        )?))
    } else if image::guess_format(bytes).is_ok_and(|format| format == image::ImageFormat::Avif) {
        Ok(image::DynamicImage::ImageRgba8(nova_image::decode_avif(
            bytes,
            nova_image::DecodeLimits::default(),
        )?))
    } else {
        Ok(image::load_from_memory(bytes)?)
    }
}
pub fn downscale_for_display(pixels: &Pixels, max_side: u32) -> Pixels {
    if max_side == 0 || pixels.width().max(pixels.height()) <= max_side {
        return pixels.clone();
    }
    let Some(rgba) =
        image::RgbaImage::from_raw(pixels.width(), pixels.height(), pixels.as_bytes().to_vec())
    else {
        return pixels.clone();
    };
    let resized = image::DynamicImage::ImageRgba8(rgba)
        .resize(max_side, max_side, image::imageops::FilterType::Triangle)
        .into_rgba8();
    SharedPixelBuffer::clone_from_slice(resized.as_raw(), resized.width(), resized.height())
}
#[cfg(target_os = "linux")]
pub fn trim_heap() {
    unsafe {
        libc::malloc_trim(0);
    }
}
#[cfg(not(target_os = "linux"))]
pub fn trim_heap() {}

static ENTRY_LOCKS: LazyLock<Mutex<HashMap<PathBuf, Weak<Mutex<()>>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
fn entry_lock(path: &Path) -> Arc<Mutex<()>> {
    let mut locks = ENTRY_LOCKS.lock().unwrap();
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_owned(), Arc::downgrade(&lock));
    lock
}
fn url_lock(dir: &Path, url: &str) -> Arc<Mutex<()>> {
    entry_lock(&dir.join(format!("{:016x}.lock", fnv1a(url.as_bytes()))))
}
static CACHE_MUTATION: LazyLock<RwLock<()>> = LazyLock::new(|| RwLock::new(()));
static CACHE_GENERATION: AtomicU64 = AtomicU64::new(0);
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static ENCODINGS: (Mutex<usize>, Condvar) = (Mutex::new(0), Condvar::new());
struct EncodingPermit;
impl EncodingPermit {
    fn acquire() -> Self {
        let mut count = ENCODINGS.0.lock().unwrap();
        while *count >= 2 {
            count = ENCODINGS.1.wait(count).unwrap();
        }
        *count += 1;
        Self
    }
}
impl Drop for EncodingPermit {
    fn drop(&mut self) {
        *ENCODINGS.0.lock().unwrap() -= 1;
        ENCODINGS.1.notify_one();
    }
}

struct Processed {
    pixels: Pixels,
    encoded: Option<Vec<u8>>,
}
fn encode_rgba(rgba: &image::RgbaImage, recipe: ImageRecipe) -> Option<Vec<u8>> {
    let _permit = EncodingPermit::acquire();
    let mut bytes = Vec::new();
    match recipe.format {
        Some(CacheImageFormat::Avif) => {
            bytes = nova_image::encode_avif(
                rgba.width(),
                rgba.height(),
                rgba.as_raw(),
                recipe.quality,
                nova_image::DecodeLimits::default(),
            )
            .ok()?;
        }
        Some(CacheImageFormat::JxlLossless | CacheImageFormat::JxlLossy) => {
            let mode = if recipe.format == Some(CacheImageFormat::JxlLossless) {
                nova_image::JxlEncoding::Lossless
            } else {
                nova_image::JxlEncoding::Lossy {
                    quality: recipe.quality,
                }
            };
            bytes = nova_image::encode_jxl(
                rgba.width(),
                rgba.height(),
                rgba.as_raw(),
                mode,
                nova_image::DecodeLimits::default(),
            )
            .ok()?;
        }
        Some(CacheImageFormat::Webp) => {
            bytes = webp::Encoder::from_rgba(rgba.as_raw(), rgba.width(), rgba.height())
                .encode_simple(false, recipe.quality as f32)
                .ok()?
                .to_vec();
        }
        Some(CacheImageFormat::Jpeg) if rgba.pixels().all(|p| p.0[3] == 255) => {
            let rgb = image::DynamicImage::ImageRgba8(rgba.clone()).into_rgb8();
            image::ImageEncoder::write_image(
                image::codecs::jpeg::JpegEncoder::new_with_quality(&mut bytes, recipe.quality),
                rgb.as_raw(),
                rgb.width(),
                rgb.height(),
                image::ExtendedColorType::Rgb8,
            )
            .ok()?;
        }
        _ => {
            image::ImageEncoder::write_image(
                image::codecs::png::PngEncoder::new(&mut bytes),
                rgba.as_raw(),
                rgba.width(),
                rgba.height(),
                image::ExtendedColorType::Rgba8,
            )
            .ok()?;
        }
    }
    Some(bytes)
}
fn process_source(bytes: Vec<u8>, recipe: ImageRecipe) -> Option<Processed> {
    process_source_with(bytes, recipe, encode_rgba)
}
fn process_source_with(
    bytes: Vec<u8>,
    recipe: ImageRecipe,
    encoder: impl FnOnce(&image::RgbaImage, ImageRecipe) -> Option<Vec<u8>>,
) -> Option<Processed> {
    let source = decode_source(&bytes).ok()?;
    let resized = fit_source(source, recipe.max_side)?;
    let rgba = resized.into_rgba8();
    let encoded = if !recipe.compress && recipe.max_side.is_none() {
        Some(bytes)
    } else {
        drop(bytes);
        encoder(&rgba, recipe)
    };
    // Construct only processed-size Slint buffers, and prefer the decoded encoded
    // representation so cold, memory and disk hits have exactly the same quality.
    if let Some(encoded) = encoded
        && let Ok(pixels) = decode_image_bytes(&encoded)
    {
        return Some(Processed {
            pixels,
            encoded: Some(encoded),
        });
    }
    Some(Processed {
        pixels: SharedPixelBuffer::clone_from_slice(rgba.as_raw(), rgba.width(), rgba.height()),
        encoded: None,
    })
}

/// Nova's original owned Triangle resize, for app-supplied bounded codecs.
/// None preserves the input, no upscaling occurs, and zero is invalid.
pub fn fit_rgba(source: image::RgbaImage, max_side: Option<u32>) -> Option<image::RgbaImage> {
    fit_source(image::DynamicImage::ImageRgba8(source), max_side)
        .map(image::DynamicImage::into_rgba8)
}

fn fit_source(source: image::DynamicImage, max_side: Option<u32>) -> Option<image::DynamicImage> {
    Some(match max_side {
        Some(0) => return None,
        Some(side) if source.width().max(source.height()) > side => {
            let fitted = source.resize(side, side, image::imageops::FilterType::Triangle);
            // The full-size decode is only a resize input, never a second
            // retained buffer while encoding or constructing Slint pixels.
            drop(source);
            fitted
        }
        _ => source,
    })
}

const MAGIC: &[u8; 8] = b"NVIMAGE2";
#[derive(Clone, Debug, Serialize, Deserialize)]
struct VariantMetadata {
    version: u32,
    url: String,
    uses: Vec<ImageUse>,
    width: u32,
    height: u32,
    recipe: ImageRecipe,
    checksum: u64,
}
struct DiskVariant {
    metadata: VariantMetadata,
    encoded: Vec<u8>,
}
fn recipe_disk_path(dir: &Path, url: &str, recipe: ImageRecipe) -> PathBuf {
    dir.join(format!(
        "{:016x}-{:016x}.variant",
        fnv1a(url.as_bytes()),
        fnv1a(recipe.key().as_bytes())
    ))
}
fn read_variant(path: &Path) -> Option<DiskVariant> {
    let result = (|| {
        let data = fs::read(path).ok()?;
        if data.get(..8)? != MAGIC {
            return None;
        }
        let length = u32::from_le_bytes(data.get(8..12)?.try_into().ok()?) as usize;
        if length > 64 * 1024 {
            return None;
        }
        let end = 12usize.checked_add(length)?;
        let metadata: VariantMetadata = serde_json::from_slice(data.get(12..end)?).ok()?;
        let encoded = data.get(end..)?.to_vec();
        if metadata.version != 2 || metadata.uses.is_empty() || metadata.checksum != fnv1a(&encoded)
        {
            return None;
        }
        Some(DiskVariant { metadata, encoded })
    })();
    if result.is_none() {
        gc::mark_invalid(path);
    }
    result
}
fn variant_pixels(variant: &DiskVariant, url: &str, recipe: ImageRecipe) -> Option<Pixels> {
    if variant.metadata.url != url || variant.metadata.recipe != recipe {
        return None;
    }
    let pixels = decode_image_bytes(&variant.encoded).ok()?;
    if pixels.width() != variant.metadata.width
        || pixels.height() != variant.metadata.height
        || recipe
            .max_side
            .is_some_and(|side| pixels.width().max(pixels.height()) > side)
    {
        return None;
    }
    Some(pixels)
}
fn atomic_write(path: &Path, data: &[u8]) -> std::io::Result<()> {
    atomic_write_if(path, data, || true).map(|_| ())
}
fn atomic_write_if(
    path: &Path,
    data: &[u8],
    still_current: impl FnOnce() -> bool,
) -> std::io::Result<bool> {
    let name = path
        .file_name()
        .ok_or_else(|| std::io::Error::other("missing cache filename"))?
        .to_string_lossy();
    let temp = path.with_file_name(format!(
        "{name}.tmp-{}-{}",
        std::process::id(),
        TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ));
    let result = (|| {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        if !still_current() {
            return Ok(false);
        }
        fs::rename(&temp, path)?;
        Ok(true)
    })();
    if !matches!(result, Ok(true)) {
        let _ = fs::remove_file(&temp);
    }
    result
}
fn write_variant(path: &Path, variant: &DiskVariant) -> std::io::Result<()> {
    write_variant_if(path, variant, || true).map(|_| ())
}
fn write_variant_if(
    path: &Path,
    variant: &DiskVariant,
    still_current: impl FnOnce() -> bool,
) -> std::io::Result<bool> {
    let metadata = serde_json::to_vec(&variant.metadata)?;
    let mut data = Vec::with_capacity(12 + metadata.len() + variant.encoded.len());
    data.extend_from_slice(MAGIC);
    data.extend_from_slice(&(metadata.len() as u32).to_le_bytes());
    data.extend_from_slice(&metadata);
    data.extend_from_slice(&variant.encoded);
    let written = atomic_write_if(path, &data, still_current)?;
    if written {
        gc::record_variant(path, &variant.metadata, data.len() as u64);
    }
    Ok(written)
}
fn variant_paths(dir: &Path) -> impl Iterator<Item = PathBuf> {
    fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|e| e == "variant"))
}
fn directory_variants(dir: &Path) -> impl Iterator<Item = (PathBuf, DiskVariant)> {
    // Stream files rather than retaining the entire compressed disk cache while
    // collecting metadata for maintenance.
    variant_paths(dir).filter_map(|p| read_variant(&p).map(|v| (p, v)))
}
/// Move a use only after its replacement succeeded. Other consumers of an old
/// recipe keep that file until their own successful load migrates them.
fn retire_use_history(dir: &Path, url: &str, uses: &[ImageUse], keep: &Path) {
    let prefix = format!("{:016x}", fnv1a(url.as_bytes()));
    // Most loads replace one URL's variants. Do not read every other URL's
    // payload just to retire that URL's older processing settings.
    let history = variant_paths(dir)
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        })
        .filter_map(|path| read_variant(&path).map(|variant| (path, variant)));
    for (path, mut variant) in history {
        if path == keep || variant.metadata.url != url {
            continue;
        }
        let previous = variant.metadata.uses.len();
        variant
            .metadata
            .uses
            .retain(|image_use| !uses.contains(image_use));
        if previous == variant.metadata.uses.len() {
            continue;
        }
        if variant.metadata.uses.is_empty() {
            if fs::remove_file(&path).is_ok() {
                gc::removed(&path);
            }
        } else {
            let _ = write_variant(&path, &variant);
        }
    }
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == format!("{prefix}.img")
                || name == format!("{prefix}.cfg")
                || name.starts_with(&format!("{prefix}.d"))
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
}
// Maintenance owns these watches only for the lifetime of its registration
// snapshot. Marking successful registrations also catches settings ABA: the
// same recipe/bytes can recur after ownership moved to a newer load.
type RegistrationWatchKey = (PathBuf, String, ImageUse);
type RegistrationWatchers = HashMap<RegistrationWatchKey, Vec<Weak<AtomicBool>>>;
static REGISTRATION_WATCHERS: LazyLock<Mutex<RegistrationWatchers>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
struct RegistrationWatch {
    key: RegistrationWatchKey,
    stale: Arc<AtomicBool>,
}
impl RegistrationWatch {
    fn capture(dir: &Path, url: &str, image_use: ImageUse) -> Self {
        let key = (dir.to_owned(), url.to_owned(), image_use);
        let stale = Arc::new(AtomicBool::new(false));
        REGISTRATION_WATCHERS
            .lock()
            .unwrap()
            .entry(key.clone())
            .or_default()
            .push(Arc::downgrade(&stale));
        Self { key, stale }
    }
}
impl Drop for RegistrationWatch {
    fn drop(&mut self) {
        let own = Arc::downgrade(&self.stale);
        let mut watchers = REGISTRATION_WATCHERS.lock().unwrap();
        if let Some(registered) = watchers.get_mut(&self.key) {
            registered.retain(|watch| !watch.ptr_eq(&own) && watch.strong_count() > 0);
            if registered.is_empty() {
                watchers.remove(&self.key);
            }
        }
    }
}
fn invalidate_registration_snapshots(dir: &Path, url: &str, uses: &[ImageUse]) {
    let watchers = REGISTRATION_WATCHERS.lock().unwrap();
    if watchers.is_empty() {
        return;
    }
    for image_use in uses {
        let key = (dir.to_owned(), url.to_owned(), *image_use);
        if let Some(registered) = watchers.get(&key) {
            for watch in registered.iter().filter_map(Weak::upgrade) {
                watch.store(true, Ordering::Release);
            }
        }
    }
}

fn persist_processed(
    dir: &Path,
    url: &str,
    uses: &[ImageUse],
    recipe: ImageRecipe,
    processed: &Processed,
) -> std::io::Result<()> {
    let encoded = processed
        .encoded
        .as_ref()
        .ok_or_else(|| std::io::Error::other("image encoding failed"))?;
    fs::create_dir_all(dir)?;
    let path = recipe_disk_path(dir, url, recipe);
    let mut registered = read_variant(&path)
        .filter(|v| v.metadata.url == url && v.metadata.recipe == recipe)
        .map(|v| v.metadata.uses)
        .unwrap_or_default();
    for image_use in uses {
        if !registered.contains(image_use) {
            registered.push(*image_use);
        }
    }
    let variant = DiskVariant {
        metadata: VariantMetadata {
            version: 2,
            url: url.into(),
            uses: registered,
            width: processed.pixels.width(),
            height: processed.pixels.height(),
            recipe,
            checksum: fnv1a(encoded),
        },
        encoded: encoded.clone(),
    };
    write_variant(&path, &variant)?;
    invalidate_registration_snapshots(dir, url, uses);
    retire_use_history(dir, url, uses, &path);
    Ok(())
}
fn register_cached_use_locked(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    recipe: ImageRecipe,
) -> bool {
    let path = recipe_disk_path(dir, url, recipe);
    let Some(mut variant) =
        read_variant(&path).filter(|v| v.metadata.url == url && v.metadata.recipe == recipe)
    else {
        return false;
    };
    if !variant.metadata.uses.contains(&image_use) {
        variant.metadata.uses.push(image_use);
        if write_variant(&path, &variant).is_ok() {
            invalidate_registration_snapshots(dir, url, &[image_use]);
            retire_use_history(dir, url, &[image_use], &path);
        }
    }
    gc::record_existing(&path, &variant.metadata);
    true
}

fn register_refreshed_use(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    recipe: ImageRecipe,
    pixels: &Pixels,
    epoch: LoadEpoch,
) {
    let lock = url_lock(dir, url);
    let _guard = lock.lock().unwrap();
    if !epoch.current() {
        return;
    }
    register_refreshed_use_locked(dir, url, image_use, recipe, pixels);
}
fn register_refreshed_use_locked(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    recipe: ImageRecipe,
    pixels: &Pixels,
) {
    let path = recipe_disk_path(dir, url, recipe);
    let Some(mut variant) =
        read_variant(&path).filter(|v| v.metadata.url == url && v.metadata.recipe == recipe)
    else {
        return;
    };
    // A failed atomic replacement leaves the previous valid variant in place.
    // Do not attach a new caller role to that older generation.
    if !variant_pixels(&variant, url, recipe)
        .is_some_and(|stored| image_buffers_equal(&stored, pixels))
    {
        return;
    }
    if !variant.metadata.uses.contains(&image_use) {
        variant.metadata.uses.push(image_use);
        if write_variant(&path, &variant).is_ok() {
            invalidate_registration_snapshots(dir, url, &[image_use]);
            retire_use_history(dir, url, &[image_use], &path);
        }
    }
}

#[derive(Clone, Copy)]
struct LoadEpoch {
    clear: u64,
    settings: u64,
}
impl LoadEpoch {
    fn capture() -> Self {
        Self {
            clear: CACHE_GENERATION.load(Ordering::Acquire),
            settings: image_settings_revision(),
        }
    }
    fn current(self) -> bool {
        self.clear == CACHE_GENERATION.load(Ordering::Acquire)
            && self.settings == image_settings_revision()
    }
}
type Flight = Arc<(Mutex<Option<Option<Pixels>>>, Condvar)>;
static IN_FLIGHT: LazyLock<Mutex<HashMap<String, Flight>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
// Refreshes must always consult the transport, so they share completion slots
// with other refreshes but never join ordinary cache loads.
static REFRESH_IN_FLIGHT: LazyLock<Mutex<HashMap<String, Flight>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Tickets order successful source publications across ordinary loads, forced
// refreshes and maintenance. Maintenance keeps only sequence evidence. Pixels
// live here only while an older load/refresh still needs its completion handed
// off; queued maintenance must never become another retained pixel cache.
struct PublishedSource {
    sequence: u64,
    pixels: Option<Pixels>,
}
struct PublicationState {
    active: usize,
    readers: HashSet<u64>,
    newest: Option<PublishedSource>,
}
static PUBLICATIONS: LazyLock<Mutex<HashMap<String, PublicationState>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));
static PUBLICATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);
struct PublicationTicket {
    key: String,
    sequence: u64,
    recipe: ImageRecipe,
    delivery: bool,
}
impl PublicationTicket {
    fn acquire(dir: &Path, url: &str, recipe: ImageRecipe, clear: u64) -> Self {
        Self::acquire_for(dir, url, recipe, clear, true)
    }
    fn acquire_maintenance(dir: &Path, url: &str, recipe: ImageRecipe, clear: u64) -> Self {
        Self::acquire_for(dir, url, recipe, clear, false)
    }
    fn acquire_for(dir: &Path, url: &str, recipe: ImageRecipe, clear: u64, delivery: bool) -> Self {
        let key = format!(
            "{}:{clear}:{}",
            dir.display(),
            image_recipe_cache_key(url, recipe)
        );
        let mut publications = PUBLICATIONS.lock().unwrap();
        let sequence = PUBLICATION_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let state = publications.entry(key.clone()).or_insert(PublicationState {
            active: 0,
            readers: HashSet::new(),
            newest: None,
        });
        state.active += 1;
        if delivery {
            state.readers.insert(sequence);
        }
        Self {
            key,
            sequence,
            recipe,
            delivery,
        }
    }
    // Call under the URL lock, together with the disk/LRU mutation below.
    fn newer_pixels(&self) -> Option<Pixels> {
        PUBLICATIONS
            .lock()
            .unwrap()
            .get(&self.key)?
            .newest
            .as_ref()
            .filter(|source| source.sequence > self.sequence)
            .and_then(|source| source.pixels.clone())
    }
    fn newer_publication(&self) -> bool {
        PUBLICATIONS
            .lock()
            .unwrap()
            .get(&self.key)
            .and_then(|state| state.newest.as_ref())
            .is_some_and(|source| source.sequence > self.sequence)
    }
    fn published(&self, pixels: &Pixels) {
        let mut publications = PUBLICATIONS.lock().unwrap();
        let state = publications.get_mut(&self.key).unwrap();
        let needs_handoff = state
            .readers
            .iter()
            .any(|sequence| *sequence < self.sequence);
        state.newest = Some(PublishedSource {
            sequence: self.sequence,
            pixels: needs_handoff.then(|| pixels.clone()),
        });
    }
}

impl Drop for PublicationTicket {
    fn drop(&mut self) {
        let mut publications = PUBLICATIONS.lock().unwrap();
        let state = publications.get_mut(&self.key).unwrap();
        state.active -= 1;
        if self.delivery {
            state.readers.remove(&self.sequence);
        }
        if let Some(source) = state.newest.as_mut()
            && !state
                .readers
                .iter()
                .any(|sequence| *sequence < source.sequence)
        {
            source.pixels = None;
        }
        if state.active == 0 {
            publications.remove(&self.key);
        }
    }
}
fn publish_processed_locked(
    dir: &Path,
    url: &str,
    uses: &[ImageUse],
    disk: bool,
    ticket: &PublicationTicket,
    processed: Option<Processed>,
) -> Option<Pixels> {
    if let Some(newer) = ticket.newer_pixels() {
        if disk {
            for image_use in uses {
                register_refreshed_use_locked(dir, url, *image_use, ticket.recipe, &newer);
            }
        }
        return Some(newer);
    }
    let processed = processed?;
    let recipe = ticket.recipe;
    if processed.encoded.is_some() {
        if disk {
            let _ = persist_processed(dir, url, uses, recipe, &processed);
        }
        decoded_cache_insert(
            &image_recipe_cache_key(url, recipe),
            processed.pixels.clone(),
        );
        ticket.published(&processed.pixels);
    }
    Some(processed.pixels)
}

/// All transports call this core. The injected source fetch makes behavior tests
/// independent of HTTP. Neither old variants nor originals can satisfy a new recipe.
pub fn cached_pixels_with(
    dir: &Path,
    url: &str,
    image_use: impl Into<ImageUse>,
    settings: &CacheSettings,
    download: impl FnOnce() -> Option<Vec<u8>>,
) -> Option<Pixels> {
    cached_pixels_at(
        dir,
        url,
        image_use.into(),
        settings,
        LoadEpoch::capture(),
        download,
    )
}
fn cached_pixels_at(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    settings: &CacheSettings,
    epoch: LoadEpoch,
    download: impl FnOnce() -> Option<Vec<u8>>,
) -> Option<Pixels> {
    let recipe = ImageRecipe::resolve(image_use, settings);
    let key = image_recipe_cache_key(url, recipe);
    let flight_key = format!(
        "{}:{}:{}:{}",
        dir.display(),
        epoch.clear,
        epoch.settings,
        key
    );
    let (flight, ticket, joined_refresh) = {
        // A forced refresh already underway owns the next source generation.
        // Ordinary callers must wait for it even if an older cache hit exists.
        let refreshes = REFRESH_IN_FLIGHT.lock().unwrap();
        let mut flights = IN_FLIGHT.lock().unwrap();
        if let Some(flight) = refreshes.get(&flight_key) {
            (flight.clone(), None, true)
        } else if let Some(flight) = flights.get(&flight_key) {
            (flight.clone(), None, false)
        } else {
            let flight = Arc::new((Mutex::new(None), Condvar::new()));
            let ticket = PublicationTicket::acquire(dir, url, recipe, epoch.clear);
            flights.insert(flight_key.clone(), flight.clone());
            (flight, Some(ticket), false)
        }
    };
    let Some(ticket) = ticket else {
        let mut result = flight.0.lock().unwrap();
        while result.is_none() {
            result = flight.1.wait(result).unwrap();
        }
        let pixels = result.as_ref().unwrap().clone();
        drop(result);
        if pixels.is_none() && joined_refresh && epoch.current() {
            // A failed refresh leaves valid old entries usable by ordinary loads.
            return cached_pixels_at(dir, url, image_use, settings, epoch, download);
        }
        if let Some(pixels) = pixels.as_ref()
            && settings.cache_images
        {
            let _mutation = CACHE_MUTATION.read().unwrap();
            if epoch.current() {
                register_refreshed_use(dir, url, image_use, recipe, pixels, epoch);
            }
        }
        return if epoch.current() { pixels } else { None };
    };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        load_recipe(dir, url, image_use, settings, epoch, &ticket, download)
    }))
    .ok()
    .flatten();
    {
        let _mutation = CACHE_MUTATION.read().unwrap();
        let lock = url_lock(dir, url);
        let _guard = lock.lock().unwrap();
        // A refresh can publish between the source commit and completion-slot
        // delivery. Complete the flight under the same publication lock.
        let result = if epoch.current() {
            if let Some(newer) = ticket.newer_pixels() {
                if settings.cache_images {
                    register_refreshed_use_locked(dir, url, image_use, recipe, &newer);
                }
                Some(newer)
            } else {
                result
            }
        } else {
            None
        };
        let mut flights = IN_FLIGHT.lock().unwrap();
        *flight.0.lock().unwrap() = Some(result.clone());
        flights.remove(&flight_key);
        flight.1.notify_all();
        result
    }
}
fn load_recipe(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    settings: &CacheSettings,
    epoch: LoadEpoch,
    ticket: &PublicationTicket,
    download: impl FnOnce() -> Option<Vec<u8>>,
) -> Option<Pixels> {
    let recipe = ImageRecipe::resolve(image_use, settings);
    let disk = settings.cache_images;
    {
        let _mutation = CACHE_MUTATION.read().unwrap();
        let lock = url_lock(dir, url);
        let _guard = lock.lock().unwrap();
        if !epoch.current() {
            return None;
        }
        if let Some(newer) = ticket.newer_pixels() {
            if disk {
                register_refreshed_use_locked(dir, url, image_use, recipe, &newer);
            }
            return Some(newer);
        }
        if let Some(pixels) = cached_recipe_locked(dir, url, image_use, recipe, disk) {
            return Some(pixels);
        }
    }
    let processed = download().and_then(|bytes| process_source(bytes, recipe));
    let _mutation = CACHE_MUTATION.read().unwrap();
    if !epoch.current() {
        return None;
    }
    let lock = url_lock(dir, url);
    let _guard = lock.lock().unwrap();
    // Settings can change while this source commit waits behind another use.
    // A stale recipe must not retire that newer use's successful registration.
    if !epoch.current() {
        return None;
    }
    publish_processed_locked(dir, url, &[image_use], disk, ticket, processed)
}

fn cached_recipe_locked(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    recipe: ImageRecipe,
    disk: bool,
) -> Option<Pixels> {
    let key = image_recipe_cache_key(url, recipe);
    if let Some(pixels) = decoded_cache_get(&key)
        && (!disk || register_cached_use_locked(dir, url, image_use, recipe))
    {
        return Some(pixels);
    }
    // A memory entry from a disk-disabled load has no source bytes to save.
    // Fetch its source rather than recompressing the decoded lossy image.
    if disk {
        let path = recipe_disk_path(dir, url, recipe);
        if let Some(mut variant) = read_variant(&path)
            && let Some(pixels) = variant_pixels(&variant, url, recipe)
        {
            if !variant.metadata.uses.contains(&image_use) {
                variant.metadata.uses.push(image_use);
                if write_variant(&path, &variant).is_ok() {
                    invalidate_registration_snapshots(dir, url, &[image_use]);
                    retire_use_history(dir, url, &[image_use], &path);
                }
            }
            gc::record_existing(&path, &variant.metadata);
            decoded_cache_insert(&key, pixels.clone());
            return Some(pixels);
        }
    }
    None
}

/// Restore local pixels without joining or creating a network flight. A miss
/// is not a failed download and must never be published to HTTP subscribers.
pub fn cached_display_pixels(url: &str, image_use: impl Into<ImageUse>) -> Option<Pixels> {
    let epoch = LoadEpoch::capture();
    let (settings, revision) = nova_config::image_settings_snapshot();
    if revision != epoch.settings {
        return None;
    }
    let dir = poster_cache_dir();
    cached_display_pixels_at(&dir, url, image_use.into(), &settings, epoch)
}

fn cached_display_pixels_at(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    settings: &CacheSettings,
    epoch: LoadEpoch,
) -> Option<Pixels> {
    let recipe = ImageRecipe::resolve(image_use, settings);
    let _mutation = CACHE_MUTATION.read().unwrap();
    let lock = url_lock(dir, url);
    let _guard = lock.lock().unwrap();
    if !epoch.current() {
        return None;
    }
    let pixels = cached_recipe_locked(dir, url, image_use, recipe, settings.cache_images);
    if epoch.current() { pixels } else { None }
}

pub fn display_pixels(url: &str, image_use: impl Into<ImageUse>) -> Option<Pixels> {
    let clear = CACHE_GENERATION.load(Ordering::Acquire);
    let (settings, revision) = nova_config::image_settings_snapshot();
    cached_pixels_at(
        &poster_cache_dir(),
        url,
        image_use.into(),
        &settings,
        LoadEpoch {
            clear,
            settings: revision,
        },
        || config::fetch_image_bytes(url),
    )
}

/// Refresh the exact same processing recipe. Comparison happens after encoding;
/// unchanged lossy pixels do not cause a visible swap on every refresh.
pub fn refresh_image_if_changed(
    url: String,
    image_use: impl Into<ImageUse>,
    then: impl FnOnce(Option<Pixels>) + Send + 'static,
) {
    let image_use = image_use.into();
    let clear = CACHE_GENERATION.load(Ordering::Acquire);
    let (settings, revision) = nova_config::image_settings_snapshot();
    let epoch = LoadEpoch {
        clear,
        settings: revision,
    };
    std::thread::Builder::new()
        .name("image-refresh".into())
        .spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                refresh_image_if_changed_with(
                    &poster_cache_dir(),
                    &url,
                    image_use,
                    &settings,
                    epoch,
                    || config::fetch_image_bytes(&url),
                )
            }))
            .ok()
            .flatten();
            then(result);
        })
        .expect("spawn image refresh worker");
}

fn refresh_image_if_changed_with(
    dir: &Path,
    url: &str,
    image_use: ImageUse,
    settings: &CacheSettings,
    epoch: LoadEpoch,
    download: impl FnOnce() -> Option<Vec<u8>>,
) -> Option<Pixels> {
    let recipe = ImageRecipe::resolve(image_use, settings);
    let key = image_recipe_cache_key(url, recipe);
    let previous = decoded_cache_get(&key);
    let flight_key = format!(
        "{}:{}:{}:{}",
        dir.display(),
        epoch.clear,
        epoch.settings,
        key
    );
    let (flight, ticket) = {
        let mut flights = REFRESH_IN_FLIGHT.lock().unwrap();
        if let Some(flight) = flights.get(&flight_key) {
            (flight.clone(), None)
        } else {
            let flight = Arc::new((Mutex::new(None), Condvar::new()));
            let ticket = PublicationTicket::acquire(dir, url, recipe, epoch.clear);
            flights.insert(flight_key.clone(), flight.clone());
            (flight, Some(ticket))
        }
    };

    let refreshed = if let Some(ticket) = ticket {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let processed = download().and_then(|bytes| process_source(bytes, recipe));
            let _mutation = CACHE_MUTATION.read().unwrap();
            if !epoch.current() {
                return None;
            }
            let lock = url_lock(dir, url);
            let _guard = lock.lock().unwrap();
            if !epoch.current() {
                return None;
            }
            publish_processed_locked(
                dir,
                url,
                &[image_use],
                settings.cache_images,
                &ticket,
                processed,
            )
        }))
        .ok()
        .flatten();
        {
            let _mutation = CACHE_MUTATION.read().unwrap();
            let lock = url_lock(dir, url);
            let _guard = lock.lock().unwrap();
            let result = if epoch.current() {
                if let Some(newer) = ticket.newer_pixels() {
                    if settings.cache_images {
                        register_refreshed_use_locked(dir, url, image_use, recipe, &newer);
                    }
                    Some(newer)
                } else {
                    result
                }
            } else {
                None
            };
            let mut flights = REFRESH_IN_FLIGHT.lock().unwrap();
            *flight.0.lock().unwrap() = Some(result.clone());
            flights.remove(&flight_key);
            flight.1.notify_all();
            result
        }
    } else {
        let mut result = flight.0.lock().unwrap();
        while result.is_none() {
            result = flight.1.wait(result).unwrap();
        }
        result.as_ref().unwrap().clone()
    };

    let refreshed = refreshed?;
    if !epoch.current() {
        return None;
    }
    // A refresh may be requested by another semantic use that resolves to the
    // same recipe. Record each caller's use after the shared generation ends.
    if settings.cache_images {
        let _mutation = CACHE_MUTATION.read().unwrap();
        if !epoch.current() {
            return None;
        }
        register_refreshed_use(dir, url, image_use, recipe, &refreshed, epoch);
    }
    previous
        .as_ref()
        .is_none_or(|old| !image_buffers_equal(old, &refreshed))
        .then_some(refreshed)
}

static MAINTENANCE: (Mutex<bool>, Condvar) = (Mutex::new(false), Condvar::new());
pub struct MaintenanceGuard;
impl MaintenanceGuard {
    pub fn try_acquire() -> Option<Self> {
        let mut busy = MAINTENANCE.0.lock().unwrap();
        if *busy {
            return None;
        }
        *busy = true;
        Some(Self)
    }
    fn acquire() -> Self {
        let mut busy = MAINTENANCE.0.lock().unwrap();
        while *busy {
            busy = MAINTENANCE.1.wait(busy).unwrap();
        }
        *busy = true;
        Self
    }
    pub fn clear(&self, dir: &Path) -> (u64, usize) {
        clear_cache_entries(dir)
    }
    pub fn rewrite(
        &self,
        dir: &Path,
        settings: &CacheSettings,
        cancel: &AtomicBool,
        progress: impl FnMut(RewriteProgress),
    ) -> RewriteProgress {
        rewrite_entries_with(dir, settings, cancel, progress, config::fetch_image_bytes)
    }
}
impl Drop for MaintenanceGuard {
    fn drop(&mut self) {
        *MAINTENANCE.0.lock().unwrap() = false;
        MAINTENANCE.1.notify_all();
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RewriteProgress {
    pub processed: usize,
    pub total: usize,
    pub converted: usize,
    pub skipped: usize,
    pub failed: usize,
    pub cancelled: bool,
}
#[derive(Clone, Copy, PartialEq)]
struct UseRegistration {
    recipe: ImageRecipe,
    checksum: u64,
}
type UseRegistrations = HashMap<ImageUse, Vec<UseRegistration>>;
fn use_registrations(dir: &Path, url: &str) -> UseRegistrations {
    let prefix = format!("{:016x}", fnv1a(url.as_bytes()));
    let mut registrations = HashMap::<ImageUse, Vec<UseRegistration>>::new();
    for variant in variant_paths(dir)
        .filter(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with(&prefix))
        })
        .filter_map(|path| read_variant(&path))
        .filter(|variant| variant.metadata.url == url)
    {
        for image_use in variant.metadata.uses {
            registrations
                .entry(image_use)
                .or_default()
                .push(UseRegistration {
                    recipe: variant.metadata.recipe,
                    checksum: variant.metadata.checksum,
                });
        }
    }
    registrations
}
fn registration_current(
    captured: &UseRegistrations,
    current: &UseRegistrations,
    image_use: ImageUse,
) -> bool {
    let Some(captured) = captured.get(&image_use) else {
        return false;
    };
    current.get(&image_use).is_some_and(|current| {
        current.len() == captured.len() && current.iter().all(|entry| captured.contains(entry))
    })
}

fn rewrite_entries_with(
    dir: &Path,
    settings: &CacheSettings,
    cancel: &AtomicBool,
    mut progress: impl FnMut(RewriteProgress),
    mut download: impl FnMut(&str) -> Option<Vec<u8>>,
) -> RewriteProgress {
    let mut result = RewriteProgress::default();
    let generation = CACHE_GENERATION.load(Ordering::Acquire);
    let urls: HashSet<String> = directory_variants(dir)
        .map(|(_, variant)| variant.metadata.url)
        .collect();
    let mut snapshots = HashMap::new();
    {
        let _mutation = CACHE_MUTATION.read().unwrap();
        for url in urls {
            let lock = url_lock(dir, &url);
            let _guard = lock.lock().unwrap();
            let registrations = use_registrations(dir, &url);
            let watches: HashMap<_, _> = registrations
                .keys()
                .copied()
                .map(|image_use| (image_use, RegistrationWatch::capture(dir, &url, image_use)))
                .collect();
            let mut recipes: HashMap<String, (ImageRecipe, Vec<ImageUse>)> = HashMap::new();
            for image_use in registrations.keys().copied() {
                let recipe = ImageRecipe::resolve(image_use, settings);
                recipes
                    .entry(recipe.key())
                    .or_insert_with(|| (recipe, Vec::new()))
                    .1
                    .push(image_use);
            }
            // Reserve every snapshot recipe now, including URLs processed later.
            // A newer source published while another URL is downloading must
            // also displace this snapshot. These tickets retain only sequence
            // evidence, even if newer pixels cannot enter the LRU.
            let recipes: Vec<_> = recipes
                .into_values()
                .map(|(recipe, uses)| {
                    let ticket =
                        PublicationTicket::acquire_maintenance(dir, &url, recipe, generation);
                    (recipe, uses, ticket)
                })
                .collect();
            snapshots.insert(url, (registrations, watches, recipes));
        }
    }
    result.total = snapshots.len();
    progress(result);
    // Settings snapshots can finish after a settings change, but a use rebuilt
    // since the registration snapshot belongs to that newer successful load.
    for (url, (registrations, watches, recipes)) in snapshots {
        if cancel.load(Ordering::Acquire) {
            result.cancelled = true;
            break;
        }
        if !settings.cache_images {
            result.skipped += 1;
            result.processed += 1;
            progress(result);
            continue;
        }
        let Some(bytes) = download(&url) else {
            result.failed += 1;
            result.processed += 1;
            progress(result);
            continue;
        };
        // Decode the source once per URL; no recipe starts from a cached derivative.
        let Ok(source) = decode_source(&bytes) else {
            result.failed += 1;
            result.processed += 1;
            progress(result);
            continue;
        };
        for (recipe, uses, ticket) in recipes {
            if cancel.load(Ordering::Acquire) {
                result.cancelled = true;
                break;
            }
            let fitted = match recipe.max_side {
                Some(side) if source.width().max(source.height()) > side => {
                    source.resize(side, side, image::imageops::FilterType::Triangle)
                }
                _ => source.clone(),
            };
            let rgba = fitted.into_rgba8();
            let encoded = if !recipe.compress && recipe.max_side.is_none() {
                Some(bytes.clone())
            } else {
                encode_rgba(&rgba, recipe)
            };
            let Some(encoded) = encoded else {
                result.failed += 1;
                continue;
            };
            let Ok(pixels) = decode_image_bytes(&encoded) else {
                result.failed += 1;
                continue;
            };
            let processed = Processed {
                pixels,
                encoded: Some(encoded),
            };
            let _mutation = CACHE_MUTATION.read().unwrap();
            if CACHE_GENERATION.load(Ordering::Acquire) != generation {
                result.cancelled = true;
                break;
            }
            let lock = url_lock(dir, &url);
            let _guard = lock.lock().unwrap();
            let current = use_registrations(dir, &url);
            let uses: Vec<_> = uses
                .into_iter()
                .filter(|image_use| {
                    watches
                        .get(image_use)
                        .is_some_and(|watch| !watch.stale.load(Ordering::Acquire))
                        && registration_current(&registrations, &current, *image_use)
                })
                .collect();
            if uses.is_empty() || ticket.newer_publication() {
                result.skipped += 1;
            } else if persist_processed(dir, &url, &uses, recipe, &processed).is_ok() {
                decoded_cache_insert(
                    &image_recipe_cache_key(&url, recipe),
                    processed.pixels.clone(),
                );
                ticket.published(&processed.pixels);
                result.converted += 1;
            } else {
                result.failed += 1;
            }
        }
        result.processed += 1;
        progress(result);
        if result.cancelled {
            break;
        }
    }
    progress(result);
    result
}
pub fn rewrite_cache_dir_with_progress(
    dir: &Path,
    settings: &CacheSettings,
    cancel: &AtomicBool,
    progress: impl FnMut(RewriteProgress),
) -> RewriteProgress {
    MaintenanceGuard::acquire().rewrite(dir, settings, cancel, progress)
}
pub fn rewrite_cache_dir_to_format(dir: &Path, settings: &CacheSettings) -> usize {
    rewrite_cache_dir_with_progress(dir, settings, &AtomicBool::new(false), |_| {}).converted
}
pub fn clear_poster_cache_dir(dir: &Path) -> (u64, usize) {
    MaintenanceGuard::acquire().clear(dir)
}
fn clear_cache_entries(dir: &Path) -> (u64, usize) {
    let _mutation = CACHE_MUTATION.write().unwrap();
    CACHE_GENERATION.fetch_add(1, Ordering::AcqRel);
    gc::cleared(dir);
    decoded_cache_clear();
    let mut total = (0u64, 0);
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Ok(metadata) = entry.metadata()
                && metadata.is_file()
                && fs::remove_file(path).is_ok()
            {
                total.0 = total.0.saturating_add(metadata.len());
                total.1 += 1;
            }
        }
    }
    total
}
pub fn poster_cache_disk_usage(dir: &Path) -> (u64, usize) {
    fs::read_dir(dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .fold((0u64, 0), |(bytes, count), metadata| {
            (bytes.saturating_add(metadata.len()), count + 1)
        })
}

/// Compatibility byte helpers use processed Original variants, never a backup.
pub fn poster_cache_path_in(dir: &Path, url: &str) -> PathBuf {
    recipe_disk_path(
        dir,
        url,
        ImageRecipe::resolve(ImageUse::Fixed(None), active_cache_settings()),
    )
}
pub fn read_poster_bytes(dir: &Path, url: &str) -> Option<Vec<u8>> {
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), active_cache_settings());
    let _mutation = CACHE_MUTATION.read().unwrap();
    let lock = url_lock(dir, url);
    let _guard = lock.lock().unwrap();
    read_variant(&recipe_disk_path(dir, url, recipe))
        .filter(|v| v.metadata.url == url && v.metadata.recipe == recipe)
        .map(|v| v.encoded)
}
pub fn write_poster_bytes(dir: &Path, url: &str, bytes: &[u8]) {
    let recipe = ImageRecipe::resolve(ImageUse::Fixed(None), active_cache_settings());
    if let Some(processed) = process_source(bytes.to_vec(), recipe) {
        let _mutation = CACHE_MUTATION.read().unwrap();
        let lock = url_lock(dir, url);
        let _guard = lock.lock().unwrap();
        let _ = persist_processed(dir, url, &[ImageUse::Fixed(None)], recipe, &processed);
    }
}
#[cfg(test)]
#[path = "cache_tests.rs"]
mod tests;
