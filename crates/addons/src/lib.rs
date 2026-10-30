//! Minimal client for the [Stremio addon protocol](https://github.com/Stremio/stremio-addon-sdk).
//!
//! An *addon* is just an HTTP server exposing JSON endpoints:
//!
//! * `GET /manifest.json` — what the addon offers (catalogs, types, resources)
//! * `GET /catalog/{type}/{id}.json[?extra=…]` — lists of media
//! * `GET /meta/{type}/{id}.json` — details about one piece of media
//! * `GET /stream/{type}/{id}.json` — stream sources for one piece of media
//! * `GET /subtitles/{type}/{id}.json` — external subtitle choices
//!
//! This crate is transport-agnostic: [`Addon`] knows how to normalise install
//! URLs, build endpoint URLs ([`Addon::manifest_url`], …) and parse response
//! *bytes* ([`Addon::parse_manifest`], …). The actual fetching is done by the
//! caller — blocking HTTP on native (`client` feature, [`http_get_blocking`])
//! or a platform-specific client.
//!
//! Unknown fields in the JSON responses are preserved via `#[serde(flatten)]`,
//! so new protocol fields keep working without a crate update.

pub mod types;

pub use types::{
    Catalog, CatalogExtra, Manifest, MetaDetail, MetaItem, MetaPreview, Resource, Stream, Subtitle,
    Video,
};

use std::fmt;
use url::Url;

/// Errors produced while talking to an addon.
#[derive(Debug)]
pub enum Error {
    /// The install URL could not be parsed into a usable base URL.
    InvalidUrl(String),
    /// The addon returned a non-success HTTP status. `retries` is how many
    /// automatic retries already ran for transient (502/503/504) statuses
    /// before this one was returned.
    Status {
        status: u16,
        url: String,
        retries: usize,
    },
    /// The addon replied with a body that is not valid JSON.
    Json {
        url: String,
        source: serde_json::Error,
    },
    /// Transport-level failure (DNS, connect, timeout, TLS, …). Only present
    /// with the `client` feature (platforms without it report errors as messages).
    #[cfg(feature = "client")]
    Http(reqwest::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidUrl(u) => write!(f, "invalid addon URL: {u}"),
            Error::Status {
                status,
                url,
                retries,
            } => {
                if *retries > 0 {
                    write!(
                        f,
                        "addon returned HTTP {status} for {url} (auto-retried {retries}×)"
                    )
                } else {
                    write!(f, "addon returned HTTP {status} for {url}")
                }
            }
            Error::Json { url, source } => {
                write!(f, "invalid JSON from addon at {url}: {source}")
            }
            #[cfg(feature = "client")]
            Error::Http(e) => write!(f, "request failed: {e}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Json { source, .. } => Some(source),
            #[cfg(feature = "client")]
            Error::Http(e) => Some(e),
            _ => None,
        }
    }
}

#[cfg(feature = "client")]
impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        Error::Http(e)
    }
}

/// One Stremio addon, reachable at a base URL such as `https://v3-cinemeta.strem.io`.
#[derive(Clone)]
pub struct Addon {
    base: Url,
}

impl fmt::Debug for Addon {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Addon").field("base", &self.base).finish()
    }
}

/// Per-request transport limits. Generous but bounded so the UI never hangs.
#[cfg(feature = "client")]
const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

/// Statuses meaning "the addon's gateway is up but its upstream timed out or
/// is briefly overloaded". Safe to retry: the requests are idempotent GETs,
/// and a retry often hits a freshly warmed cache. (Cinemeta's search backend
/// regularly exceeds its own gateway's ~10 s limit on a cold lookup and then
/// answers the immediate retry in well under a second.)
pub const TRANSIENT_STATUSES: [u16; 3] = [502, 503, 504];

/// Maximum number of automatic retries after a transient failure.
pub const MAX_RETRIES: usize = 2;

/// Backoff before retry `retry` (1-based): 250 ms, then 500 ms.
#[cfg(feature = "client")]
fn retry_delay(retry: usize) -> std::time::Duration {
    std::time::Duration::from_millis(250 * (1u64 << (retry - 1)))
}

impl Addon {
    /// Build an addon client from an *install URL*.
    ///
    /// Accepts any of these forms and normalises them to the base URL the
    /// `manifest.json` is served from:
    ///
    /// * `https://host` or `https://host/path`
    /// * `https://host/manifest.json` / `https://host/path/manifest.json`
    /// * `https://host/configure` / `https://host/path/configure`
    ///
    /// A bare host (no scheme) is assumed to be `https`.
    pub fn new(install_url: &str) -> Result<Self, Error> {
        let trimmed = install_url.trim();
        if trimmed.is_empty() {
            return Err(Error::InvalidUrl(install_url.into()));
        }

        let with_scheme = if trimmed.contains("://") {
            trimmed.to_string()
        } else {
            format!("https://{trimmed}")
        };

        let mut url = Url::parse(&with_scheme).map_err(|_| Error::InvalidUrl(trimmed.into()))?;

        // Strip well-known suffixes so the remaining path is the base.
        let mut path = url.path().to_string();
        for suffix in ["/manifest.json", "/configure"] {
            if path.ends_with(suffix) {
                path.truncate(path.len() - suffix.len());
                break;
            }
        }
        if !path.ends_with('/') {
            path.push('/');
        }
        url.set_path(&path);
        url.set_query(None);
        url.set_fragment(None);

        Ok(Addon { base: url })
    }

    /// The normalised base URL (where `manifest.json` lives).
    pub fn base_url(&self) -> &str {
        self.base.as_str().trim_end_matches('/')
    }

    // ---- Endpoint URLs (transport-agnostic) --------------------------------

    /// URL of `GET /manifest.json`.
    pub fn manifest_url(&self) -> String {
        self.join("manifest.json").to_string()
    }

    /// URL of `GET /catalog/{type}/{id}.json` with optional extra parameters
    /// (e.g. `search` or `genre`).
    ///
    /// Extras are encoded as `key=value` pairs in one `&`-separated path segment, the form Stremio
    /// addons (e.g. Cinemeta) actually route on:
    /// `…/catalog/movie/top/search=batman.json`.
    pub fn catalog_url(&self, type_: &str, id: &str, extra: &[(&str, &str)]) -> String {
        let path = if extra.is_empty() {
            format!("catalog/{type_}/{id}.json")
        } else {
            let segments: Vec<String> = extra
                .iter()
                .map(|(k, v)| format!("{k}={}", Self::encode_segment(v)))
                .collect();
            format!("catalog/{type_}/{id}/{}.json", segments.join("&"))
        };
        self.join(&path).to_string()
    }

    /// URL of `GET /meta/{type}/{id}.json`.
    pub fn meta_url(&self, type_: &str, id: &str) -> String {
        self.join(&format!("meta/{type_}/{id}.json")).to_string()
    }

    /// URL of `GET /stream/{type}/{id}.json`.
    pub fn stream_url(&self, type_: &str, id: &str) -> String {
        self.join(&format!("stream/{type_}/{id}.json")).to_string()
    }

    /// Subtitle extras identify the selected release, not just the title.
    pub fn subtitles_url(&self, type_: &str, id: &str, extra: &[(&str, &str)]) -> String {
        let extras = extra
            .iter()
            .map(|(key, value)| format!("{key}={}", Self::encode_segment(value)))
            .collect::<Vec<_>>()
            .join("&");
        let suffix = if extras.is_empty() {
            String::new()
        } else {
            format!("/{extras}")
        };
        self.join(&format!(
            "subtitles/{}/{id}{suffix}.json",
            Self::encode_segment(type_),
            id = Self::encode_segment(id)
        ))
        .to_string()
    }

    pub fn parse_subtitles(bytes: &[u8]) -> Result<Vec<Subtitle>, serde_json::Error> {
        #[derive(serde::Deserialize)]
        struct Response {
            #[serde(default)]
            subtitles: Vec<Subtitle>,
        }
        serde_json::from_slice::<Response>(bytes).map(|response| response.subtitles)
    }

    // ---- Response parsers (transport-agnostic) -----------------------------

    /// Decode a `/manifest.json` body.
    pub fn parse_manifest(bytes: &[u8]) -> Result<Manifest, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Decode a `/catalog/...` body into its list of previews.
    pub fn parse_catalog(bytes: &[u8]) -> Result<Vec<MetaPreview>, serde_json::Error> {
        serde_json::from_slice::<MetaDetail>(bytes).map(|d| d.metas)
    }

    /// Decode a `/meta/...` body. Returns `None` for addons that answer
    /// unknown ids with an empty stub object (a transport-level 404 is the
    /// caller's concern).
    pub fn parse_meta(bytes: &[u8]) -> Result<Option<MetaItem>, serde_json::Error> {
        let resp: MetaResponse = serde_json::from_slice(bytes)?;
        Ok(resp
            .meta
            .filter(|m| !m.preview.id.is_empty() || !m.preview.title().is_empty()))
    }

    /// Decode a `/stream/...` body into its list of streams.
    pub fn parse_streams(bytes: &[u8]) -> Result<Vec<Stream>, serde_json::Error> {
        serde_json::from_slice::<StreamsResponse>(bytes).map(|r| r.streams)
    }

    // ---- Blocking convenience client (native + `client` feature only) ------

    /// `GET /manifest.json`
    #[cfg(feature = "client")]
    pub fn manifest(&self) -> Result<Manifest, Error> {
        let url = self.manifest_url();
        http_get_blocking(&url).and_then(|b| {
            Self::parse_manifest(&b).map_err(|source| Error::Json {
                url: url.clone(),
                source,
            })
        })
    }

    /// `GET /catalog/{type}/{id}.json` with optional extra parameters.
    #[cfg(feature = "client")]
    pub fn catalog(
        &self,
        type_: &str,
        id: &str,
        extra: &[(&str, &str)],
    ) -> Result<Vec<MetaPreview>, Error> {
        let url = self.catalog_url(type_, id, extra);
        http_get_blocking(&url).and_then(|b| {
            Self::parse_catalog(&b).map_err(|source| Error::Json {
                url: url.clone(),
                source,
            })
        })
    }

    /// `GET /meta/{type}/{id}.json`. Returns `Ok(None)` when the addon does
    /// not know this item (HTTP 404, or an empty stub object).
    #[cfg(feature = "client")]
    pub fn meta(&self, type_: &str, id: &str) -> Result<Option<MetaItem>, Error> {
        let url = self.meta_url(type_, id);
        match http_get_blocking(&url) {
            Ok(bytes) => Self::parse_meta(&bytes).map_err(|source| Error::Json { url, source }),
            Err(Error::Status { status: 404, .. }) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `GET /stream/{type}/{id}.json`. Streams from all addons that provide a
    /// stream resource are combined by the caller.
    #[cfg(feature = "client")]
    pub fn streams(&self, type_: &str, id: &str) -> Result<Vec<Stream>, Error> {
        let url = self.stream_url(type_, id);
        http_get_blocking(&url).and_then(|b| {
            Self::parse_streams(&b).map_err(|source| Error::Json {
                url: url.clone(),
                source,
            })
        })
    }

    fn join(&self, path: &str) -> Url {
        // `self.base` always ends with '/', so `join` appends relative to it.
        self.base.join(path).expect("static relative paths join")
    }

    /// Percent-encode a value for use inside a URL path segment.
    fn encode_segment(value: &str) -> String {
        use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
        utf8_percent_encode(value, NON_ALPHANUMERIC).to_string()
    }
}

// ---------------------------------------------------------------------------
// Blocking HTTP (native, `client` feature)
// ---------------------------------------------------------------------------

#[cfg(feature = "client")]
static SHARED_CLIENT: std::sync::OnceLock<reqwest::blocking::Client> = std::sync::OnceLock::new();

#[cfg(feature = "client")]
fn shared_client() -> reqwest::blocking::Client {
    SHARED_CLIENT
        .get_or_init(|| {
            reqwest::blocking::Client::builder()
                .timeout(TIMEOUT)
                .build()
                .unwrap_or_default()
        })
        .clone()
}

/// Blocking GET returning the raw body, with the same transparent retry
/// policy as the addon methods (transient 502/503/504 + timeouts/connect
/// errors, short backoff). Shared by the desktop app's fetch layer.
#[cfg(feature = "client")]
pub fn http_get_blocking(url: &str) -> Result<Vec<u8>, Error> {
    let mut retries = 0usize;
    loop {
        let result = (|| {
            let response = shared_client().get(url).send()?;
            let status = response.status();
            if !status.is_success() {
                return Err(Error::Status {
                    status: status.as_u16(),
                    url: url.to_string(),
                    retries: 0,
                });
            }
            Ok(response.bytes()?.to_vec())
        })();

        match result {
            Ok(v) => return Ok(v),
            Err(e) => {
                let transient = match &e {
                    Error::Status { status, .. } => TRANSIENT_STATUSES.contains(status),
                    Error::Http(he) => he.is_timeout() || he.is_connect(),
                    _ => false,
                };
                if transient && retries < MAX_RETRIES {
                    retries += 1;
                    std::thread::sleep(retry_delay(retries));
                    continue;
                }
                // Surface how many retries ran when a transient status
                // ultimately failed, so callers can report it.
                return Err(match (retries, e) {
                    (0, e) => e,
                    (_, Error::Status { status, url, .. }) => Error::Status {
                        status,
                        url,
                        retries,
                    },
                    (_, e) => e,
                });
            }
        }
    }
}

#[derive(serde::Deserialize)]
struct MetaResponse {
    #[serde(default)]
    meta: Option<MetaItem>,
}

#[derive(serde::Deserialize)]
struct StreamsResponse {
    #[serde(default)]
    streams: Vec<Stream>,
}

#[cfg(all(test, feature = "client"))]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    /// Serve canned JSON responses on one local socket and hand back the
    /// resulting install URL. Lets us exercise the client fully offline.
    /// Paths absent from `routes` answer 404.
    fn serve(routes: Vec<(&'static str, &'static str)>) -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for _ in 0..16 {
                let (mut stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let request = {
                    let mut buf = [0u8; 2048];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    String::from_utf8_lossy(&buf[..n]).to_string()
                };
                let requested = request
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("/");
                let body = routes
                    .iter()
                    .find(|(p, _)| *p == requested)
                    .map(|(_, body)| {
                        format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    })
                    .unwrap_or_else(|| {
                        "HTTP/1.1 404 Not Found\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
                            .to_string()
                    });
                let _ = stream.write_all(body.as_bytes());
            }
        });
        format!("http://{addr}")
    }

    const MANIFEST: &str = r#"{
        "id": "community.cinemeta",
        "version": "3.6.0",
        "name": "Cinemeta",
        "resources": ["catalog", "meta", "stream"],
        "types": ["movie", "series"],
        "catalogs": [
            {"type": "movie", "id": "top", "name": "Top Movies",
             "extra": [{"name": "genre", "options": ["Action", "Drama"]},
                       {"name": "search"}]}
        ]
    }"#;

    #[test]
    fn url_normalisation() {
        let cases = [
            (
                "https://v3-cinemeta.strem.io",
                "https://v3-cinemeta.strem.io/",
            ),
            (
                "https://v3-cinemeta.strem.io/manifest.json",
                "https://v3-cinemeta.strem.io/",
            ),
            (
                "https://example.com/stremio/torrentio/configure",
                "https://example.com/stremio/torrentio/",
            ),
            ("example.com/foo", "https://example.com/foo/"),
        ];
        for (input, expected) in cases {
            let addon = Addon::new(input).unwrap();
            assert_eq!(addon.base_url(), expected.trim_end_matches('/'));
        }
        assert!(Addon::new("").is_err());
    }

    #[test]
    fn endpoint_urls() {
        let addon = Addon::new("https://v3-cinemeta.strem.io").unwrap();
        assert_eq!(
            addon.manifest_url(),
            "https://v3-cinemeta.strem.io/manifest.json"
        );
        assert_eq!(
            addon.catalog_url("movie", "top", &[]),
            "https://v3-cinemeta.strem.io/catalog/movie/top.json"
        );
        assert_eq!(
            addon.catalog_url("movie", "top", &[("search", "batman")]),
            "https://v3-cinemeta.strem.io/catalog/movie/top/search=batman.json"
        );
        assert_eq!(
            addon.catalog_url("movie", "top", &[("genre", "Action & Adventure")]),
            "https://v3-cinemeta.strem.io/catalog/movie/top/genre=Action%20%26%20Adventure.json"
        );
        assert_eq!(
            addon.meta_url("series", "tt0944947"),
            "https://v3-cinemeta.strem.io/meta/series/tt0944947.json"
        );
        assert_eq!(
            addon.stream_url("movie", "tt0111161"),
            "https://v3-cinemeta.strem.io/stream/movie/tt0111161.json"
        );
    }

    #[test]
    fn fetch_manifest_and_catalog() {
        let catalog = r#"{"metas": [
            {"id": "tt0111161", "type": "movie", "name": "The Shawshank Redemption",
             "releaseInfo": "1994", "poster": "https://img/x.jpg"}
        ]}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/catalog/movie/top.json", catalog),
        ]);
        let addon = Addon::new(&base).unwrap();

        let m = addon.manifest().unwrap();
        assert_eq!(m.id, "community.cinemeta");
        assert!(m.has_resource("stream"));
        assert!(m.has_catalogs() && m.has_meta());
        let cat = m.catalog_for("movie", "top").expect("catalog present");
        assert_eq!(cat.name, "Top Movies");
        assert!(cat.supports_extra("genre"));
        assert!(cat.supports_extra("search"));

        let metas = addon.catalog("movie", "top", &[]).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].name, "The Shawshank Redemption");
        assert_eq!(metas[0].year_str().as_deref(), Some("1994"));
        assert_eq!(metas[0].poster.as_deref(), Some("https://img/x.jpg"));
    }

    #[test]
    fn cinemeta_style_year_and_release_info_do_not_collide() {
        // Real Cinemeta catalog items carry BOTH a numeric "year" and a
        // "releaseInfo" string; parsing must not treat that as a duplicate
        // and must expose the release info.
        let catalog = r#"{"metas": [
            {"id": "tt0111161", "type": "movie", "name": "The Shawshank Redemption",
             "year": 1994, "releaseInfo": "1994", "imdbRating": 9.3}
        ]}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/catalog/movie/top.json", catalog),
        ]);
        let addon = Addon::new(&base).unwrap();
        let _ = addon.manifest().unwrap();
        let metas = addon.catalog("movie", "top", &[]).unwrap();
        assert_eq!(metas[0].year_str().as_deref(), Some("1994"));
        assert_eq!(metas[0].rating_str().as_deref(), Some("9.3"));

        // An addon that only sends a numeric "year" still resolves it.
        let only_year = r#"{"metas": [{"id": "tt1", "type": "movie", "name": "X", "year": 2020}]}"#;
        let base2 = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/catalog/movie/top.json", only_year),
        ]);
        let addon2 = Addon::new(&base2).unwrap();
        let _ = addon2.manifest().unwrap();
        let metas2 = addon2.catalog("movie", "top", &[]).unwrap();
        assert_eq!(metas2[0].year_str().as_deref(), Some("2020"));
    }

    #[test]
    fn legacy_title_keys_do_not_collide_with_name() {
        // Catalog + stream payloads sometimes carry BOTH "name" and "title";
        // parsing must succeed and prefer name for display.
        let catalog = r#"{"metas": [
            {"id": "tt1", "type": "movie", "name": "Real Name", "title": "Legacy Title"}
        ]}"#;
        let streams = r#"{"streams": [
            {"name": "1080p", "title": "Legacy Label", "url": "https://cdn/x.mp4"},
            {"title": "Only Title", "infoHash": "abc"}
        ]}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/catalog/movie/top.json", catalog),
            ("/stream/movie/tt1.json", streams),
        ]);
        let addon = Addon::new(&base).unwrap();
        let _ = addon.manifest().unwrap();

        let metas = addon.catalog("movie", "top", &[]).unwrap();
        assert_eq!(metas[0].title(), "Real Name");

        let got = addon.streams("movie", "tt1").unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].label(), "1080p");
        assert_eq!(got[1].label(), "Only Title");
    }

    #[test]
    fn catalog_combines_extras_in_one_segment() {
        let addon = Addon::new("https://example.com/manifest.json").unwrap();
        assert_eq!(
            addon.catalog_url(
                "movie",
                "top",
                &[("genre", "Action & Adventure"), ("skip", "100")]
            ),
            "https://example.com/catalog/movie/top/genre=Action%20%26%20Adventure&skip=100.json"
        );
    }

    #[test]
    fn extras_are_sent_as_path_segments() {
        let search = r#"{"metas": [{"id":"tt0468569","type":"movie","name":"The Dark Knight"}]}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            // Path-encoded extra (Cinemeta style).
            ("/catalog/movie/top/search=batman.json", search),
        ]);
        let addon = Addon::new(&base).unwrap();
        let _ = addon.manifest().unwrap();

        let metas = addon
            .catalog("movie", "top", &[("search", "batman")])
            .unwrap();
        assert_eq!(metas[0].id, "tt0468569");

        // Values needing escaping survive the path encoding.
        let search2 = r#"{"metas": [{"id":"tt1","type":"movie","name":"A"}]}"#;
        let base2 = serve(vec![
            ("/manifest.json", MANIFEST),
            (
                "/catalog/movie/top/search=shawshank%20redemption.json",
                search2,
            ),
        ]);
        let addon2 = Addon::new(&base2).unwrap();
        let _ = addon2.manifest().unwrap();
        let metas2 = addon2
            .catalog("movie", "top", &[("search", "shawshank redemption")])
            .unwrap();
        assert_eq!(metas2.len(), 1);
    }

    #[test]
    fn meta_404_is_none_and_streams_decode() {
        let streams = r#"{"streams": [
            {"name": "1080p", "description": "web", "url": "https://cdn/x.mp4"},
            {"name": "Torrent", "infoHash": "abc123", "fileIdx": 0}
        ]}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/stream/movie/tt0111161.json", streams),
        ]);
        let addon = Addon::new(&base).unwrap();

        // Not routed => 404 => treated as "addon does not know this item".
        assert!(matches!(addon.meta("movie", "tt0000000"), Ok(None)));

        let got = addon.streams("movie", "tt0111161").unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].url.as_deref(), Some("https://cdn/x.mp4"));
        assert_eq!(got[0].label(), "1080p");
        assert_eq!(got[1].info_hash.as_deref(), Some("abc123"));
        assert_eq!(got[1].file_idx, Some(0));
        assert_eq!(got[1].label(), "Torrent");
        assert!(got[0].web_url().is_some());
        assert!(got[1].web_url().is_none());
    }

    /// Serve on one local socket where a closure picks the HTTP status +
    /// body for each accepted request (1-based request number). Returns the
    /// base URL and a counter of accepted requests.
    fn serve_answering(
        answer: impl Fn(usize) -> (u16, &'static str) + std::marker::Send + 'static,
    ) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits2 = hits.clone();
        std::thread::spawn(move || {
            for _ in 0..16 {
                let (mut stream, _) = match listener.accept() {
                    Ok(s) => s,
                    Err(_) => break,
                };
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).unwrap_or(0);
                let n = hits2.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let (code, body) = answer(n);
                let phrase = match code {
                    200 => "OK",
                    404 => "Not Found",
                    504 => "Gateway Timeout",
                    _ => "Error",
                };
                let head = format!(
                    "HTTP/1.1 {code} {phrase}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(head.as_bytes());
                let _ = stream.write_all(body.as_bytes());
            }
        });
        (format!("http://{addr}"), hits)
    }

    #[test]
    fn gateway_504_is_retried_until_success() {
        // A cold Cinemeta-style lookup: the first two attempts trip the
        // gateway timeout, the retry hits the now-warm backend cache.
        let body = r#"{"metas": [{"id":"tt0816692","type":"movie","name":"Interstellar","releaseInfo":"2014"}]}"#;
        let (base, hits) = serve_answering(|n| if n <= 2 { (504, "") } else { (200, body) });
        let addon = Addon::new(&base).unwrap();
        let metas = addon
            .catalog("movie", "top", &[("search", "interstellar 2014")])
            .unwrap();
        assert_eq!(metas[0].id, "tt0816692");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn persistent_504_fails_after_max_retries_with_count() {
        let (base, hits) = serve_answering(|_| (504, ""));
        let addon = Addon::new(&base).unwrap();
        let err = addon.catalog("movie", "top", &[]).err().unwrap();
        let msg = err.to_string();
        assert!(msg.contains("504"), "{msg}");
        assert!(msg.contains("auto-retried 2×"), "{msg}");
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            1 + MAX_RETRIES
        );
    }

    #[test]
    fn non_transient_404_is_not_retried() {
        let (base, hits) = serve_answering(|_| (404, ""));
        let addon = Addon::new(&base).unwrap();
        assert!(matches!(addon.meta("movie", "tt0000000"), Ok(None)));
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn cinemeta_style_videos_use_name_and_number() {
        // Cinemeta sends episode titles under `name` and positions under
        // `number` (plus `episode`); other addons send `title`.
        let v: Video = serde_json::from_str(
            r#"{"id":"tt0944947:1:1","name":"Winter Is Coming","season":1,"episode":1,"number":1}"#,
        )
        .unwrap();
        assert_eq!(v.id, "tt0944947:1:1");
        assert_eq!(v.label(), "Winter Is Coming");
        assert_eq!(v.episode_number(), Some(1));

        let legacy: Video =
            serde_json::from_str(r#"{"id":"x","title":"Only Title","season":2}"#).unwrap();
        assert_eq!(legacy.label(), "Only Title");
        assert_eq!(legacy.episode_number(), None);
    }

    #[test]
    fn meta_response_with_episodes_decodes() {
        // A Cinemeta-style /meta/series payload: preview fields on the item
        // plus a videos array of episodes with season/episode/name.
        let body = r#"{"meta": {
            "id": "tt0944947", "type": "series", "name": "Game of Thrones",
            "videos": [
                {"id": "tt0944947:1:1", "name": "Winter Is Coming", "season": 1, "episode": 1, "number": 1},
                {"id": "tt0944947:1:2", "name": "The Kingsroad", "season": 1, "episode": 2, "number": 2},
                {"id": "tt0944947:0:1", "name": "Inside Game of Thrones", "season": 0, "number": 1}
            ]
        }}"#;
        let base = serve(vec![
            ("/manifest.json", MANIFEST),
            ("/meta/series/tt0944947.json", body),
        ]);
        let addon = Addon::new(&base).unwrap();
        let item = addon.meta("series", "tt0944947").unwrap().unwrap();
        assert_eq!(item.preview.title(), "Game of Thrones");
        assert_eq!(item.videos.len(), 3);
        assert_eq!(item.videos[0].id, "tt0944947:1:1");
        assert_eq!(item.videos[0].label(), "Winter Is Coming");
        assert_eq!(item.videos[1].episode_number(), Some(2));
        assert_eq!(item.videos[2].season, Some(0));
        assert_eq!(item.videos[2].episode_number(), Some(1)); // via `number`
    }
}

#[cfg(test)]
mod subtitle_tests {
    use super::*;

    #[test]
    fn opensubtitles_manifest_matches_movies_and_episode_ids() {
        let manifest = Addon::parse_manifest(br#"{"id":"org.stremio.opensubtitlesv3","version":"1.0.0","name":"OpenSubtitles v3","catalogs":[],"resources":["subtitles"],"types":["movie","series"],"idPrefixes":["tt"]}"#).unwrap();
        assert!(manifest.has_subtitles());
        assert!(manifest.accepts("subtitles", "movie", "tt1160419"));
        assert!(manifest.accepts("subtitles", "series", "tt0944947:1:1"));
        assert!(!manifest.accepts("subtitles", "series", "mal:1"));
        assert!(!manifest.accepts("stream", "movie", "tt1160419"));
    }

    #[test]
    fn subtitle_extras_cannot_change_request_path_or_parameter_boundaries() {
        let addon = Addon::new("https://example.com/configured/manifest.json").unwrap();
        assert_eq!(
            addon.subtitles_url("series", "tt0944947:1:1", &[]),
            "https://example.com/configured/subtitles/series/tt0944947%3A1%3A1.json"
        );
        let url = addon.subtitles_url(
            "movie",
            "tt1160419",
            &[
                ("filename", "Movie /x&videoSize=0.mkv"),
                ("videoSize", "1234"),
            ],
        );
        assert!(
            url.ends_with("/filename=Movie%20%2Fx%26videoSize%3D0%2Emkv&videoSize=1234.json"),
            "{url}"
        );
        let rows = Addon::parse_subtitles(
            br#"{"subtitles":[{"id":"english","url":"https://example.com/sub.srt","lang":"eng"}]}"#,
        )
        .unwrap();
        assert_eq!(rows[0].lang.as_deref(), Some("eng"));
        assert!(Addon::parse_subtitles(br#"{"subtitles":"invalid"}"#).is_err());
    }
}
