use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use reqwest::Url;
use serde::{Deserialize, Serialize};
use tokio::sync::Notify;

mod capacity;
pub mod offline;
pub use capacity::{available_bytes, filesystem_capacity};
mod hls;
mod http;

pub use http::{
    download_http, download_http_for_job, download_http_for_job_blocking, download_http_resume,
    download_http_with_options, download_http_with_request, download_http_with_request_blocking,
    is_manifest_content_type, is_manifest_url, new_http_client, probe_http_size,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DownloadSource {
    #[serde(alias = "Http")]
    Http {
        url: String,
        #[serde(default)]
        headers: Vec<(String, String)>,
    },
    #[serde(alias = "Torrent")]
    Torrent {
        info_hash: String,
        #[serde(default)]
        file_idx: Option<u32>,
    },
}

impl Default for DownloadSource {
    fn default() -> Self {
        Self::Http {
            url: String::new(),
            headers: Vec::new(),
        }
    }
}

impl DownloadSource {
    pub fn http(url: impl Into<String>) -> Self {
        Self::Http {
            url: url.into(),
            headers: Vec::new(),
        }
    }

    pub fn http_with_headers(url: impl Into<String>, headers: Vec<(String, String)>) -> Self {
        Self::Http {
            url: url.into(),
            headers,
        }
    }

    pub fn torrent(info_hash: impl Into<String>, file_idx: Option<u32>) -> Self {
        Self::Torrent {
            info_hash: info_hash.into(),
            file_idx,
        }
    }

    pub fn url(&self) -> Option<&str> {
        match self {
            Self::Http { url, .. } => Some(url),
            Self::Torrent { .. } => None,
        }
    }

    pub fn headers(&self) -> Option<&[(String, String)]> {
        match self {
            Self::Http { headers, .. } => Some(headers),
            Self::Torrent { .. } => None,
        }
    }

    pub fn is_http(&self) -> bool {
        matches!(self, Self::Http { .. })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DownloadPhase {
    #[default]
    #[serde(alias = "Queued")]
    Queued,
    #[serde(alias = "Resolving")]
    Resolving,
    #[serde(alias = "Downloading")]
    Downloading,
    #[serde(alias = "Paused")]
    Paused,
    #[serde(alias = "Completed")]
    Completed,
    #[serde(alias = "Failed")]
    Failed,
}

impl DownloadPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed)
    }
}

/// Confirmed provider coordinates captured when a download is requested.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DownloadIntroRequest {
    IntroDb {
        key: String,
        id: String,
        season: u32,
        episode: u32,
    },
    AniSkip {
        mal: u32,
        episode: u32,
    },
}

impl DownloadIntroRequest {
    pub fn coordinates(&self) -> (u32, u32) {
        match self {
            Self::IntroDb {
                season, episode, ..
            } => (*season, *episode),
            Self::AniSkip { episode, .. } => (1, *episode),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadIntroSegment {
    pub kind: i32,
    pub start_ms: u64,
    pub end_ms: u64,
    // Older manifests only retained timings that passed the automatic threshold.
    #[serde(default)]
    pub prompt_only: bool,
    /// None means the provider does not score timings. Missing legacy scores
    /// retain the old 80% lower bound rather than being promoted to certainty.
    #[serde(default = "legacy_intro_confidence")]
    pub confidence_percent: Option<u8>,
}

fn legacy_intro_confidence() -> Option<u8> {
    Some(80)
}

/// Local timing evidence belongs to this exact completed artifact, never to a
/// different encode that happens to reuse a download ID or filename.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadIntroTimings {
    pub duration_ms: u64,
    pub segments: Vec<DownloadIntroSegment>,
    pub artifact_size: u64,
    pub artifact_modified_ns: u128,
}

impl DownloadIntroTimings {
    pub fn capture(
        path: &Path,
        duration_ms: u64,
        segments: Vec<DownloadIntroSegment>,
    ) -> Option<Self> {
        let metadata = std::fs::metadata(path).ok()?;
        if !metadata.is_file() {
            return None;
        }
        Some(Self {
            duration_ms,
            segments,
            artifact_size: metadata.len(),
            artifact_modified_ns: metadata
                .modified()
                .ok()?
                .duration_since(UNIX_EPOCH)
                .ok()?
                .as_nanos(),
        })
    }

    pub fn matches_path(&self, path: &Path) -> bool {
        let Ok(metadata) = std::fs::metadata(path) else {
            return false;
        };
        metadata.is_file()
            && metadata.len() == self.artifact_size
            && metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .is_some_and(|time| time.as_nanos() == self.artifact_modified_ns)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadJob {
    pub id: String,
    pub media_type: String,
    pub media_id: String,
    pub request_id: String,
    pub title: String,
    pub year: Option<String>,
    pub display: String,
    pub addon: String,
    pub source: DownloadSource,
    pub created_at: u64,
    pub updated_at: u64,
    pub phase: DownloadPhase,
    pub bytes_downloaded: u64,
    pub total_bytes: Option<u64>,
    pub bytes_per_second: u64,
    /// Transient HLS resource counts, never an estimated byte total. Rebuilt
    /// from committed bundle assets when a paused download resumes.
    #[serde(skip)]
    pub resource_progress: Option<(u64, u64)>,
    /// Chosen HTTP destination (including queued jobs), or the completed
    /// artifact path. UI labels stay independent from the on-disk basename.
    pub artifact_path: Option<PathBuf>,
    /// Actual shared torrent bundle and resolved file, saved before completion.
    pub torrent_directory: Option<PathBuf>,
    pub torrent_file_idx: Option<usize>,
    pub file_name: Option<String>,
    pub error: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    #[serde(default)]
    pub intro_request: Option<DownloadIntroRequest>,
    #[serde(default)]
    pub intro_timings: Option<DownloadIntroTimings>,
    /// Device-local ownership and reservation; legacy jobs remain manually kept.
    #[serde(default)]
    pub offline: offline::JobOwnership,
}

impl Default for DownloadJob {
    fn default() -> Self {
        Self {
            id: String::new(),
            media_type: String::new(),
            media_id: String::new(),
            request_id: String::new(),
            title: String::new(),
            year: None,
            display: String::new(),
            addon: String::new(),
            source: DownloadSource::default(),
            created_at: 0,
            updated_at: 0,
            phase: DownloadPhase::Queued,
            bytes_downloaded: 0,
            total_bytes: None,
            bytes_per_second: 0,
            resource_progress: None,
            artifact_path: None,
            torrent_directory: None,
            torrent_file_idx: None,
            file_name: None,
            error: None,
            etag: None,
            last_modified: None,
            intro_request: None,
            intro_timings: None,
            offline: offline::JobOwnership::default(),
        }
    }
}

impl DownloadJob {
    pub fn new(id: impl Into<String>, source: DownloadSource) -> Self {
        let now = unix_timestamp();
        Self {
            id: id.into(),
            created_at: now,
            updated_at: now,
            source,
            ..Self::default()
        }
    }

    pub fn http_request(
        &self,
        destination: impl Into<PathBuf>,
    ) -> Result<HttpDownloadRequest, DownloadError> {
        let url = self.source.url().ok_or_else(|| {
            DownloadError::InvalidSource("torrent downloads are not HTTP downloads".into())
        })?;
        let resume = DownloadResume {
            bytes_downloaded: self.bytes_downloaded,
            etag: self.etag.clone(),
            last_modified: self.last_modified.clone(),
        };
        Ok(HttpDownloadRequest {
            url: url.to_string(),
            destination: destination.into(),
            resume: (!resume.is_empty()).then_some(resume),
            headers: self.source.headers().unwrap_or_default().to_vec(),
            private_source: false,
        })
    }

    pub fn resume_state(&self) -> DownloadResume {
        DownloadResume {
            bytes_downloaded: self.bytes_downloaded,
            etag: self.etag.clone(),
            last_modified: self.last_modified.clone(),
        }
    }

    pub fn display_name(&self) -> &str {
        &self.display
    }

    pub fn addon_name(&self) -> &str {
        &self.addon
    }

    pub fn apply_progress(&mut self, progress: &DownloadProgress) {
        self.bytes_downloaded = progress.bytes_downloaded;
        self.total_bytes = progress.total_bytes;
        self.bytes_per_second = progress.bytes_per_second;
        self.resource_progress = progress.resource_progress;
        self.phase = DownloadPhase::Downloading;
        self.updated_at = unix_timestamp();
    }

    pub fn apply_outcome(&mut self, outcome: &DownloadOutcome) {
        self.bytes_downloaded = outcome.bytes_downloaded;
        self.total_bytes = outcome.total_bytes;
        self.bytes_per_second = outcome.bytes_per_second;
        self.resource_progress = None;
        self.artifact_path = Some(outcome.path.clone());
        self.file_name = Some(outcome.file_name.clone());
        self.etag = outcome.etag.clone();
        self.last_modified = outcome.last_modified.clone();
        self.error = None;
        self.phase = DownloadPhase::Completed;
        self.updated_at = unix_timestamp();
    }

    pub fn apply_error(&mut self, error: &DownloadError) {
        self.error = Some(error.to_string());
        self.phase = if error.is_cancelled() {
            DownloadPhase::Paused
        } else {
            DownloadPhase::Failed
        };
        self.updated_at = unix_timestamp();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadManifest {
    pub version: u32,
    pub jobs: Vec<DownloadJob>,
}

impl Default for DownloadManifest {
    fn default() -> Self {
        Self {
            version: 1,
            jobs: Vec::new(),
        }
    }
}

impl DownloadManifest {
    pub fn new(jobs: Vec<DownloadJob>) -> Self {
        Self { version: 1, jobs }
    }

    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DownloadResume {
    pub bytes_downloaded: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

impl DownloadResume {
    pub fn new(bytes_downloaded: u64, etag: Option<String>, last_modified: Option<String>) -> Self {
        Self {
            bytes_downloaded,
            etag,
            last_modified,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bytes_downloaded == 0 && self.etag.is_none() && self.last_modified.is_none()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpDownloadRequest {
    pub url: String,
    pub destination: PathBuf,
    pub resume: Option<DownloadResume>,
    pub headers: Vec<(String, String)>,
    /// Signed resolver URLs must never reach resume sidecars or HLS bundles.
    /// Private transfers restart their partial file on retry.
    pub private_source: bool,
}

impl Default for HttpDownloadRequest {
    fn default() -> Self {
        Self {
            url: String::new(),
            destination: PathBuf::new(),
            resume: None,
            headers: Vec::new(),
            private_source: false,
        }
    }
}

impl HttpDownloadRequest {
    pub fn new(url: impl Into<String>, destination: impl Into<PathBuf>) -> Self {
        Self {
            url: url.into(),
            destination: destination.into(),
            resume: None,
            headers: Vec::new(),
            private_source: false,
        }
    }

    pub fn with_resume(mut self, resume: DownloadResume) -> Self {
        self.resume = Some(resume);
        self
    }

    pub fn with_headers(mut self, headers: Vec<(String, String)>) -> Self {
        self.headers = headers;
        self
    }

    pub fn from_job(
        job: &DownloadJob,
        destination: impl Into<PathBuf>,
    ) -> Result<Self, DownloadError> {
        job.http_request(destination)
    }
}

impl From<(String, PathBuf)> for HttpDownloadRequest {
    fn from((url, destination): (String, PathBuf)) -> Self {
        Self::new(url, destination)
    }
}

impl From<(&str, PathBuf)> for HttpDownloadRequest {
    fn from((url, destination): (&str, PathBuf)) -> Self {
        Self::new(url, destination)
    }
}

pub type DownloadHttpRequest = HttpDownloadRequest;
pub type DownloadHttpOptions = HttpDownloadRequest;
pub type DownloadOptions = HttpDownloadRequest;
pub type DownloadResult = DownloadOutcome;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DownloadProgress {
    pub bytes_downloaded: u64,
    pub total_bytes: Option<u64>,
    pub bytes_per_second: u64,
    #[serde(skip)]
    pub resource_progress: Option<(u64, u64)>,
}

impl DownloadProgress {
    pub fn new(bytes_downloaded: u64, total_bytes: Option<u64>, bytes_per_second: u64) -> Self {
        Self {
            bytes_downloaded,
            total_bytes,
            bytes_per_second,
            resource_progress: None,
        }
    }

    pub fn fraction(&self) -> Option<f64> {
        if let Some((completed, total)) = self.resource_progress {
            return Some(if total == 0 {
                1.0
            } else {
                (completed as f64 / total as f64).clamp(0.0, 1.0)
            });
        }
        self.total_bytes.map(|total| {
            if total == 0 {
                1.0
            } else {
                (self.bytes_downloaded as f64 / total as f64).clamp(0.0, 1.0)
            }
        })
    }

    pub fn with_resources(mut self, completed: u64, total: u64) -> Self {
        self.resource_progress = Some((completed, total));
        self
    }

    pub fn percent(&self) -> Option<f64> {
        self.fraction().map(|fraction| fraction * 100.0)
    }

    pub fn is_complete(&self) -> bool {
        self.total_bytes
            .is_some_and(|total| self.bytes_downloaded >= total)
    }

    pub fn formatted_bytes(&self) -> String {
        format_bytes(self.bytes_downloaded)
    }

    pub fn formatted_speed(&self) -> String {
        format_speed(self.bytes_per_second)
    }

    pub fn formatted_percent(&self) -> String {
        if let Some((completed, total)) = self.resource_progress {
            return format_percent(completed, Some(total));
        }
        format_percent(self.bytes_downloaded, self.total_bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownloadOutcome {
    pub path: PathBuf,
    pub part_path: PathBuf,
    pub file_name: String,
    pub bytes_downloaded: u64,
    pub total_bytes: Option<u64>,
    pub bytes_per_second: u64,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
}

pub type ByteAdmission = Arc<dyn Fn(u64) -> bool + Send + Sync>;
pub type WriteAdmission = Arc<dyn Fn(u64, u64) -> bool + Send + Sync>;

#[derive(Clone, Default)]
pub struct CancellationToken {
    flag: Arc<AtomicBool>,
    notify: Arc<Notify>,
    admission: Arc<std::sync::Mutex<Option<WriteAdmission>>>,
}

impl fmt::Debug for CancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    /// Called before each HTTP/HLS body write, independently of UI progress.
    pub fn set_byte_admission(&self, check: Arc<dyn Fn(u64) -> bool + Send + Sync>) {
        self.set_write_admission(Arc::new(move |next, _| check(next)));
    }

    /// Receives logical progress and the incoming body-write length separately,
    /// so a caller can charge old HLS assets retained during replacement.
    pub fn set_write_admission(&self, check: WriteAdmission) {
        *self.admission.lock().unwrap() = Some(check);
    }

    pub fn admit_bytes(&self, next_total: u64) -> bool {
        self.admit_write(next_total, 0)
    }

    pub fn admit_write(&self, next_total: u64, incoming: u64) -> bool {
        let check = self.admission.lock().unwrap().clone();
        let allowed = !self.is_cancelled() && check.is_none_or(|check| check(next_total, incoming));
        if !allowed {
            self.cancel();
        }
        allowed
    }

    pub fn cancel(&self) {
        if !self.flag.swap(true, Ordering::AcqRel) {
            self.notify.notify_waiters();
            self.notify.notify_one();
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.flag.load(Ordering::Acquire)
    }

    pub fn cancelled(&self) -> bool {
        self.is_cancelled()
    }

    pub async fn wait_cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        self.notify.notified().await;
    }
}

pub type CancellationControl = CancellationToken;
pub type DownloadCancellation = CancellationToken;
pub type CancelToken = CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DownloadError {
    Cancelled,
    InvalidUrl(String),
    InvalidSource(String),
    UnsupportedUrl(String),
    UnsupportedContentType(String),
    HttpStatus { status: u16, message: String },
    InvalidResponse(String),
    InvalidResume(String),
    Io(String),
}

impl DownloadError {
    pub fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }

    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for DownloadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => formatter.write_str("download cancelled"),
            Self::InvalidUrl(url) => write!(formatter, "invalid download URL: {url}"),
            Self::InvalidSource(message) => formatter.write_str(message),
            Self::UnsupportedUrl(url) => write!(formatter, "unsupported download URL: {url}"),
            Self::UnsupportedContentType(content_type) => {
                write!(
                    formatter,
                    "unsupported download content type: {content_type}"
                )
            }
            Self::HttpStatus { status, message } => {
                if message.is_empty() {
                    write!(formatter, "HTTP {status}")
                } else {
                    write!(formatter, "HTTP {status}: {message}")
                }
            }
            Self::InvalidResponse(message) => {
                write!(formatter, "invalid download response: {message}")
            }
            Self::InvalidResume(message) => {
                write!(formatter, "invalid download resume state: {message}")
            }
            Self::Io(message) => write!(formatter, "download I/O error: {message}"),
        }
    }
}

impl std::error::Error for DownloadError {}

impl From<std::io::Error> for DownloadError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

pub fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

pub fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn format_speed(bytes_per_second: u64) -> String {
    format!("{}/s", format_bytes(bytes_per_second))
}

pub fn format_percent(downloaded: u64, total: Option<u64>) -> String {
    let Some(total) = total else {
        return "—".into();
    };
    if total == 0 {
        return "100%".into();
    }
    let percent = (downloaded as f64 / total as f64 * 100.0).clamp(0.0, 100.0);
    format!("{percent:.0}%")
}

pub fn format_progress(downloaded: u64, total: Option<u64>) -> String {
    match total {
        Some(total) => format!(
            "{} / {} ({})",
            format_bytes(downloaded),
            format_bytes(total),
            format_percent(downloaded, Some(total))
        ),
        None => format_bytes(downloaded),
    }
}

/// Disk names are independent of unbounded provider labels. A full stable hash
/// keeps distinct download identities separate, while reserving room for both
/// `.part` and `.part.resume.json` on filesystems with 255-byte components.
pub fn artifact_filename(download_key: &str, label: &str) -> String {
    let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, download_key.as_bytes());
    let hash: String = digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let extension = label
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or(label)
        .rsplit_once('.')
        .map(|(_, extension)| extension)
        .filter(|extension| {
            !extension.is_empty()
                && extension.len() <= 16
                && extension.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
        .map(str::to_ascii_lowercase)
        .filter(|extension| {
            matches!(
                extension.as_str(),
                "mp4"
                    | "mkv"
                    | "webm"
                    | "avi"
                    | "mov"
                    | "m4v"
                    | "ts"
                    | "m2ts"
                    | "mpeg"
                    | "mpg"
                    | "m3u8"
                    | "bin"
            )
        })
        .unwrap_or_else(|| "bin".into());
    format!("download-{hash}.{extension}")
}

pub fn sanitize_filename(input: &str) -> String {
    let basename = input.rsplit(['/', '\\']).next().unwrap_or(input);
    let mut cleaned = String::with_capacity(basename.len());
    for character in basename.chars() {
        if character.is_control()
            || matches!(
                character,
                '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*'
            )
        {
            cleaned.push('_');
        } else {
            cleaned.push(character);
        }
    }
    let trimmed = cleaned
        .trim_matches(|character: char| character == ' ' || character == '.')
        .to_string();
    if trimmed.is_empty() {
        return "download".into();
    }
    let device_stem = trimmed.split('.').next().unwrap_or(trimmed.as_str());
    if is_reserved_name(device_stem) {
        let suffix = trimmed
            .find('.')
            .map(|index| &trimmed[index..])
            .unwrap_or("");
        return limit_filename(&format!("{device_stem}_{suffix}"));
    }
    limit_filename(&trimmed)
}

pub fn sanitize_file_name(input: &str) -> String {
    sanitize_filename(input)
}

pub fn safe_filename(input: &str) -> String {
    sanitize_filename(input)
}

fn split_extension(name: &str) -> (&str, &str) {
    let Some((stem, extension)) = name.rsplit_once('.') else {
        return (name, "");
    };
    if stem.is_empty() || extension.is_empty() || extension.len() > 20 {
        return (name, "");
    }
    (stem, extension)
}

fn is_reserved_name(stem: &str) -> bool {
    let upper = stem.to_ascii_uppercase();
    matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        || (upper.len() == 4
            && (upper.starts_with("COM") || upper.starts_with("LPT"))
            && upper.as_bytes()[3].is_ascii_digit())
}

fn limit_filename(name: &str) -> String {
    const LIMIT: usize = 240;
    if name.len() <= LIMIT {
        return name.to_string();
    }
    let (stem, extension) = split_extension(name);
    let suffix = if extension.is_empty() {
        String::new()
    } else {
        format!(".{extension}")
    };
    let available = LIMIT.saturating_sub(suffix.len());
    if available == 0 {
        return "download".into();
    }
    let mut end = available.min(stem.len());
    while end > 0 && !stem.is_char_boundary(end) {
        end -= 1;
    }
    let mut limited = format!("{}{}", &stem[..end], suffix);
    limited = limited.trim_end_matches([' ', '.']).to_string();
    if limited.is_empty() {
        "download".into()
    } else {
        limited
    }
}

pub fn is_reserved_filename(input: &str) -> bool {
    let basename = input.rsplit(['/', '\\']).next().unwrap_or(input);
    let stem = basename.split('.').next().unwrap_or(basename);
    is_reserved_name(stem)
}

pub fn validate_download_url(url: &str) -> Result<Url, DownloadError> {
    let trimmed = url.trim();
    let parsed =
        Url::parse(trimmed).map_err(|error| DownloadError::InvalidUrl(error.to_string()))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(DownloadError::UnsupportedUrl(trimmed.to_string()));
    }
    if http::is_unsupported_download_url(trimmed) {
        return Err(DownloadError::UnsupportedUrl(trimmed.to_string()));
    }
    Ok(parsed)
}

pub fn part_path(destination: impl AsRef<Path>) -> PathBuf {
    let destination = destination.as_ref();
    let mut part = destination.as_os_str().to_os_string();
    part.push(".part");
    PathBuf::from(part)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn intro_metadata_preserves_legacy_json_and_round_trips_provider_coordinates() {
        let legacy: DownloadJob =
            serde_json::from_str(r#"{"id":"legacy","phase":"completed"}"#).unwrap();
        assert!(legacy.intro_request.is_none());
        assert!(legacy.intro_timings.is_none());
        let legacy_segment: DownloadIntroSegment =
            serde_json::from_str(r#"{"kind":1,"start_ms":30000,"end_ms":120000}"#).unwrap();
        assert!(!legacy_segment.prompt_only);
        for request in [
            DownloadIntroRequest::IntroDb {
                key: "imdb".into(),
                id: "tt123".into(),
                season: 2,
                episode: 3,
            },
            DownloadIntroRequest::AniSkip {
                mal: 123,
                episode: 3,
            },
        ] {
            let coordinates = request.coordinates();
            let mut job = legacy.clone();
            job.intro_request = Some(request);
            job.intro_timings = Some(DownloadIntroTimings {
                duration_ms: 1_440_000,
                segments: vec![DownloadIntroSegment {
                    kind: 0,
                    start_ms: 30_000,
                    end_ms: 120_000,
                    prompt_only: true,
                    confidence_percent: Some(56),
                }],
                artifact_size: 123,
                artifact_modified_ns: 1_777_777_777_000_000_000,
            });
            let decoded: DownloadJob =
                serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
            assert_eq!(decoded, job);
            assert_eq!(decoded.intro_request.unwrap().coordinates(), coordinates);
        }
    }

    #[test]
    fn intro_fingerprint_rejects_changed_missing_and_non_file_artifacts() {
        let root = std::env::temp_dir().join(format!(
            "nova-intro-fingerprint-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("episode.mkv");
        std::fs::write(&path, b"same size").unwrap();
        let timings = DownloadIntroTimings::capture(&path, 1000, vec![]).unwrap();
        assert!(timings.matches_path(&path));
        let modified = std::fs::metadata(&path).unwrap().modified().unwrap()
            + std::time::Duration::from_secs(1);
        std::fs::File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert!(
            !timings.matches_path(&path),
            "same-length replacement is invalidated by mtime"
        );
        let timings = DownloadIntroTimings::capture(&path, 1000, vec![]).unwrap();
        std::fs::write(&path, b"new longer encode").unwrap();
        assert!(!timings.matches_path(&path));
        std::fs::remove_file(&path).unwrap();
        assert!(!timings.matches_path(&path));
        assert!(DownloadIntroTimings::capture(&root, 1000, vec![]).is_none());
        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn legacy_http_jobs_default_headers_and_new_jobs_preserve_them() {
        let source: DownloadSource =
            serde_json::from_str(r#"{"http":{"url":"https://example.com/video.mp4"}}"#).unwrap();
        assert_eq!(source.headers(), Some(&[][..]));
        let headers = vec![("Referer".into(), "https://example.com/watch".into())];
        let job = DownloadJob::new(
            "headers",
            DownloadSource::http_with_headers("https://example.com/video.mp4", headers.clone()),
        );
        let saved: DownloadJob =
            serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
        assert_eq!(
            saved.http_request("/tmp/video.mp4").unwrap().headers,
            headers
        );
    }

    #[test]
    fn models_round_trip_and_defaults_are_stable() {
        let source = DownloadSource::torrent("abc", Some(2));
        let json = serde_json::to_string(&source).unwrap();
        assert_eq!(json, "{\"torrent\":{\"info_hash\":\"abc\",\"file_idx\":2}}");
        assert_eq!(
            serde_json::from_str::<DownloadSource>(&json).unwrap(),
            source
        );

        let manifest: DownloadManifest = serde_json::from_str("{}").unwrap();
        assert_eq!(manifest.version, 1);
        assert!(manifest.jobs.is_empty());

        let job: DownloadJob = serde_json::from_str("{}").unwrap();
        assert_eq!(job.phase, DownloadPhase::Queued);
        assert_eq!(job.bytes_downloaded, 0);
        assert_eq!(job.total_bytes, None);
        assert!(job.etag.is_none());
    }

    #[test]
    fn artifact_names_hash_identity_preserve_media_extensions_and_bound_sidecars() {
        let long = format!("{}.MKV", "ć東京".repeat(300));
        let name = artifact_filename("abc", &long);
        assert_eq!(
            name,
            "download-ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad.mkv"
        );
        assert_eq!(artifact_filename("abc", &long), name);
        assert_ne!(artifact_filename("other-key", &long), name);
        assert_ne!(
            artifact_filename("episode-1", "same.mp4"),
            artifact_filename("episode-2", "same.mp4")
        );
        let destination = Path::new("http").join(&name);
        let part = part_path(&destination);
        let sidecar = format!(
            "{}.resume.json",
            part.file_name().unwrap().to_str().unwrap()
        );
        assert!(name.len() < 100);
        assert!(sidecar.len() <= 255);
        for (label, extension) in [
            ("../../movie.MP4", "mp4"),
            ("C:\\movie.mkv", "mkv"),
            ("movie.mp4?token=unsafe", "bin"),
            ("movie.part", "bin"),
            ("movie...", "bin"),
            ("NUL", "bin"),
            ("movie.m3u8", "m3u8"),
        ] {
            let name = artifact_filename("key", label);
            assert_eq!(Path::new(&name).components().count(), 1);
            assert!(name.ends_with(&format!(".{extension}")));
            assert!(!name.contains(['/', '\\']));
        }
    }

    #[test]
    fn sanitizer_handles_paths_controls_and_reserved_names() {
        assert_eq!(sanitize_filename("../../bad/name?.mp4"), "name_.mp4");
        assert_eq!(sanitize_filename("CON.txt"), "CON_.txt");
        assert_eq!(sanitize_filename("..."), "download");
        assert_eq!(sanitize_filename("a\nb"), "a_b");
        assert!(sanitize_filename(&"x".repeat(400)).len() <= 240);
    }

    #[test]
    fn progress_helpers_are_bounded_and_formatted() {
        let progress = DownloadProgress::new(512, Some(1024), 2048);
        assert_eq!(progress.fraction(), Some(0.5));
        assert_eq!(progress.percent(), Some(50.0));
        assert_eq!(progress.formatted_bytes(), "512 B");
        assert_eq!(progress.formatted_speed(), "2.0 KiB/s");
        assert_eq!(progress.formatted_percent(), "50%");
        assert_eq!(format_percent(2, Some(1)), "100%");
        assert_eq!(format_bytes(0), "0 B");
    }

    #[test]
    fn hls_progress_keeps_real_bytes_and_transient_resource_counts_separate() {
        let progress = DownloadProgress::new(124 * 1024 * 1024, None, 1024).with_resources(65, 100);
        assert_eq!(progress.fraction(), Some(0.65));
        assert_eq!(progress.formatted_percent(), "65%");
        let mut job = DownloadJob::new(
            "hls",
            DownloadSource::http("https://example.com/master.m3u8"),
        );
        job.apply_progress(&progress);
        let json = serde_json::to_string(&job).unwrap();
        assert!(!json.contains("resource_progress"));
        let loaded: DownloadJob = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.total_bytes, None);
        assert_eq!(loaded.bytes_downloaded, 124 * 1024 * 1024);
        assert_eq!(loaded.resource_progress, None);
    }

    #[test]
    fn cancellation_is_shared_by_clones() {
        let token = CancellationToken::new();
        let clone = token.clone();
        assert!(!clone.is_cancelled());
        token.cancel();
        assert!(clone.cancelled());
    }
}
