//! Image-only settings/runtime adapters extracted from Nova b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda.
use serde::{Deserialize, Serialize};
use std::{
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex, RwLock},
};

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheImageFormat {
    Jpeg,
    Webp,
    Avif,
    #[serde(rename = "jxl_lossless")]
    JxlLossless,
    #[serde(rename = "jxl_lossy")]
    JxlLossy,
}

impl CacheImageFormat {
    pub fn label(&self) -> &'static str {
        match self {
            CacheImageFormat::Jpeg => "JPEG",
            CacheImageFormat::Webp => "WebP",
            CacheImageFormat::Avif => "AVIF",
            CacheImageFormat::JxlLossless => "JPEG XL (lossless)",
            CacheImageFormat::JxlLossy => "JPEG XL (lossy)",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct CacheSettings {
    pub cache_images: bool,
    pub image_disk_cache_limit_mb: Option<u32>,
    pub enabled: bool,
    pub format: CacheImageFormat,
    pub quality: u8,
    pub avif_quality: u8,
    pub jxl_quality: u8,
    pub detail_poster_max_side: Option<u32>,
    pub detail_backdrop_max_side: Option<u32>,
    pub episode_thumbnail_max_side: Option<u32>,
    pub player_episode_backdrop_max_side: Option<u32>,
    pub lru_cache_mb: u32,
}
pub type ImageCacheSettings = CacheSettings;
impl From<&CacheSettings> for CacheSettings {
    fn from(settings: &CacheSettings) -> Self {
        settings.clone()
    }
}
pub const fn default_image_disk_cache_limit() -> Option<u32> {
    Some(1024)
}
pub const fn default_cache_image_format() -> CacheImageFormat {
    CacheImageFormat::Webp
}
pub const fn default_avif_quality() -> u8 {
    85
}
pub const fn default_jxl_quality() -> u8 {
    85
}
pub const fn default_cache_image_quality() -> u8 {
    50
}
impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            cache_images: true,
            image_disk_cache_limit_mb: default_image_disk_cache_limit(),
            enabled: true,
            format: default_cache_image_format(),
            quality: default_cache_image_quality(),
            avif_quality: default_avif_quality(),
            jxl_quality: default_jxl_quality(),
            detail_poster_max_side: Some(480),
            detail_backdrop_max_side: Some(960),
            episode_thumbnail_max_side: Some(360),
            player_episode_backdrop_max_side: None,
            lru_cache_mb: 128,
        }
    }
}
impl CacheSettings {
    /// Encoding summary for compatibility with callers displaying the configuration.
    pub fn config_key(&self) -> String {
        let format = match self.format {
            CacheImageFormat::Jpeg => "jpeg",
            CacheImageFormat::Webp => "webp",
            CacheImageFormat::Avif => "avif",
            CacheImageFormat::JxlLossless => return "jxl_lossless".into(),
            CacheImageFormat::JxlLossy => "jxl_lossy",
        };
        format!(
            "{format}:q{}",
            if self.format == CacheImageFormat::Avif {
                self.avif_quality
            } else if self.format == CacheImageFormat::JxlLossy {
                self.jxl_quality
            } else {
                self.quality
            }
        )
    }
}

impl<'de> Deserialize<'de> for CacheSettings {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let mut value = serde_json::Value::deserialize(deserializer)?;
        let object = value
            .as_object_mut()
            .ok_or_else(|| serde::de::Error::custom("settings must be an object"))?;
        normalize_image_settings(object).map_err(serde::de::Error::custom)?;
        let mut defaults =
            serde_json::to_value(Self::default()).map_err(serde::de::Error::custom)?;
        defaults.as_object_mut().unwrap().extend(object.clone());
        #[derive(Deserialize)]
        struct Wire {
            cache_images: bool,
            image_disk_cache_limit_mb: Option<u32>,
            enabled: bool,
            format: CacheImageFormat,
            quality: u8,
            avif_quality: u8,
            jxl_quality: u8,
            detail_poster_max_side: Option<u32>,
            detail_backdrop_max_side: Option<u32>,
            episode_thumbnail_max_side: Option<u32>,
            player_episode_backdrop_max_side: Option<u32>,
            lru_cache_mb: u32,
        }
        let w: Wire = serde_json::from_value(defaults).map_err(serde::de::Error::custom)?;
        Ok(Self {
            cache_images: w.cache_images,
            image_disk_cache_limit_mb: w.image_disk_cache_limit_mb,
            enabled: w.enabled,
            format: w.format,
            quality: w.quality,
            avif_quality: w.avif_quality,
            jxl_quality: w.jxl_quality,
            detail_poster_max_side: w.detail_poster_max_side,
            detail_backdrop_max_side: w.detail_backdrop_max_side,
            episode_thumbnail_max_side: w.episode_thumbnail_max_side,
            player_episode_backdrop_max_side: w.player_episode_backdrop_max_side,
            lru_cache_mb: w.lru_cache_mb,
        })
    }
}
pub const IMAGE_RESOLUTION_PRESETS: &[u32] = &[240, 360, 480, 720, 960, 1024, 1280, 1920, 2560];
/// The actual Nova settings choices; None means unlimited disk cache.
pub const DISK_CACHE_LIMITS: [Option<u32>; 7] = [
    None,
    Some(256),
    Some(512),
    Some(1024),
    Some(2048),
    Some(4096),
    Some(8192),
];
pub fn normalize_image_settings(
    object: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<(), String> {
    let legacy_player =
        if object.get("downscale").and_then(serde_json::Value::as_bool) == Some(true) {
            serde_json::json!(1024)
        } else {
            serde_json::Value::Null
        };
    for (key, fallback) in [
        ("detail_poster_max_side", serde_json::json!(480)),
        ("detail_backdrop_max_side", serde_json::json!(960)),
        ("episode_thumbnail_max_side", serde_json::json!(360)),
        ("player_episode_backdrop_max_side", legacy_player),
    ] {
        let setting = object.entry(key).or_insert(fallback);
        if !setting.is_null()
            && !setting.as_u64().is_some_and(|n| {
                IMAGE_RESOLUTION_PRESETS
                    .iter()
                    .any(|preset| u64::from(*preset) == n)
            })
        {
            return Err(format!("invalid image resolution for {key}"));
        }
    }
    if let Some(value) = object.get("jxl_quality")
        && !value
            .as_u64()
            .is_some_and(|quality| (1..=100).contains(&quality))
    {
        return Err("invalid JPEG XL quality".into());
    }
    Ok(())
}
static CURRENT_CACHE_SETTINGS: LazyLock<Mutex<CacheSettings>> =
    LazyLock::new(|| Mutex::new(CacheSettings::default()));

/// Cheap hit-path policy check without cloning the full settings snapshot.
pub fn disk_image_cache_enabled() -> bool {
    CURRENT_CACHE_SETTINGS.lock().unwrap().cache_images
}

/// The active image-cache settings.
pub fn active_cache_settings() -> CacheSettings {
    image_settings_snapshot().0
}

pub fn image_settings_snapshot() -> (CacheSettings, u64) {
    let settings = CURRENT_CACHE_SETTINGS.lock().unwrap();
    (settings.clone(), image_settings_revision())
}

/// Store the active image-cache settings (the decoded LRU budget is a
/// separate concern — see `nova-media`).
static IMAGE_SETTINGS_REVISION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Revision used to discard image completions started before a recipe change.
pub fn image_settings_revision() -> u64 {
    IMAGE_SETTINGS_REVISION.load(std::sync::atomic::Ordering::Acquire)
}

pub fn set_cache_settings(settings: CacheSettings) {
    let mut current = CURRENT_CACHE_SETTINGS.lock().unwrap();
    if current.cache_images != settings.cache_images
        || current.enabled != settings.enabled
        || current.format != settings.format
        || current.quality != settings.quality
        || current.avif_quality != settings.avif_quality
        || current.jxl_quality != settings.jxl_quality
        || current.detail_poster_max_side != settings.detail_poster_max_side
        || current.detail_backdrop_max_side != settings.detail_backdrop_max_side
        || current.episode_thumbnail_max_side != settings.episode_thumbnail_max_side
        || current.player_episode_backdrop_max_side != settings.player_episode_backdrop_max_side
    {
        IMAGE_SETTINGS_REVISION.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    *current = settings;
}

/// FNV-1a 64-bit hash, used for cache file names and stable IDs.
pub fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// Seconds since the Unix epoch.
pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

type Fetch = dyn Fn(&str) -> Option<Vec<u8>> + Send + Sync;
struct Runtime {
    directory: PathBuf,
    fetch: Arc<Fetch>,
}
static RUNTIME: LazyLock<RwLock<Runtime>> = LazyLock::new(|| {
    RwLock::new(Runtime {
        directory: PathBuf::new(),
        fetch: Arc::new(|_| None),
    })
});
/// Install app-owned paths/transport. Configure before the first cache load.
pub fn configure(
    directory: PathBuf,
    fetch: impl Fn(&str) -> Option<Vec<u8>> + Send + Sync + 'static,
) {
    *RUNTIME.write().unwrap() = Runtime {
        directory,
        fetch: Arc::new(fetch),
    };
}
pub fn poster_cache_dir() -> PathBuf {
    RUNTIME.read().unwrap().directory.clone()
}
pub fn fetch_image_bytes(url: &str) -> Option<Vec<u8>> {
    let fetch = RUNTIME.read().unwrap().fetch.clone();
    fetch(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nova_image_defaults_and_legacy_resolution_migration_are_preserved() {
        let defaults: CacheSettings = serde_json::from_str("{}").unwrap();
        assert_eq!(defaults, CacheSettings::default());
        assert_eq!(defaults.config_key(), "webp:q50");
        assert_eq!(defaults.image_disk_cache_limit_mb, Some(1024));
        assert_eq!(defaults.lru_cache_mb, 128);
        let legacy: CacheSettings =
            serde_json::from_str(r#"{"downscale":true,"image_disk_cache_limit_mb":null}"#).unwrap();
        assert_eq!(legacy.player_episode_backdrop_max_side, Some(1024));
        assert_eq!(legacy.image_disk_cache_limit_mb, None);
        let explicit:CacheSettings=serde_json::from_str(r#"{"downscale":true,"player_episode_backdrop_max_side":null,"detail_poster_max_side":1920}"#).unwrap();
        assert_eq!(explicit.player_episode_backdrop_max_side, None);
        assert_eq!(explicit.detail_poster_max_side, Some(1920));
        for invalid in [0, 1, 479, 65536] {
            assert!(
                serde_json::from_value::<CacheSettings>(
                    serde_json::json!({"detail_poster_max_side":invalid})
                )
                .is_err()
            );
        }
    }
}
