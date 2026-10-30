//! Typed models for the Stremio addon protocol responses.
//!
//! Field names follow the JSON payloads (`camelCase` in the wire format is
//! mapped to `snake_case`). Unknown keys are preserved through
//! `#[serde(flatten)]` so that addons may send additional metadata without
//! breaking deserialisation.

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

fn default_false() -> bool {
    false
}

/// Optional metadata collections are sometimes explicitly null (Cinemeta's
/// `director`, for example). Treat that as absent without relaxing IDs or
/// accepting malformed non-null values.
fn null_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + Default,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

/// The `manifest.json` document describing an addon.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub id: String,
    pub version: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub resources: Vec<Resource>,
    #[serde(default)]
    pub types: Vec<String>,
    #[serde(default)]
    pub catalogs: Vec<Catalog>,
    #[serde(default)]
    pub id_prefixes: Vec<String>,
    #[serde(default)]
    pub logo: Option<String>,
    #[serde(default)]
    pub background: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl Manifest {
    /// Whether this addon implements a resource whose name starts with
    /// `resource` (handles `"stream"` and `{"name": "stream/…"}` alike).
    pub fn has_resource(&self, resource: &str) -> bool {
        self.resources.iter().any(|r| {
            let name = r.name();
            name == resource || name.starts_with(&format!("{resource}/"))
        })
    }

    /// Convenience checks for the three well-known resource kinds.
    pub fn has_catalogs(&self) -> bool {
        self.has_resource("catalog")
    }

    pub fn has_meta(&self) -> bool {
        self.has_resource("meta")
    }

    pub fn has_streams(&self) -> bool {
        self.has_resource("stream")
    }

    pub fn has_subtitles(&self) -> bool {
        self.has_resource("subtitles")
    }

    /// Match the resource's media types and opaque ID prefixes. Detailed
    /// resource declarations have their own restrictions; plain declarations
    /// inherit the manifest's restrictions, as specified by Stremio.
    pub fn accepts(&self, resource: &str, type_: &str, id: &str) -> bool {
        self.resources.iter().any(|entry| {
            let name = entry.name();
            if name != resource && !name.starts_with(&format!("{resource}/")) {
                return false;
            }
            let (types, prefixes) = match entry {
                Resource::Plain(_) => (self.types.clone(), &self.id_prefixes),
                Resource::Detailed(detail) => (
                    detail
                        .extra
                        .get("types")
                        .and_then(Value::as_array)
                        .map(|types| {
                            types
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        })
                        .unwrap_or_default(),
                    &detail.id_prefixes,
                ),
            };
            (types.is_empty() || types.iter().any(|kind| kind == type_))
                && (resource == "catalog"
                    || prefixes.is_empty()
                    || prefixes.iter().any(|prefix| id.starts_with(prefix)))
        })
    }

    /// The catalogs this addon declares for `type_` (e.g. `"movie"`).
    pub fn catalogs_for_type(&self, type_: &str) -> impl Iterator<Item = &Catalog> {
        self.catalogs.iter().filter(move |c| c.type_ == type_)
    }

    /// Search capability shared by Discover and metadata enrichment.
    pub fn search_catalogs<'a>(&'a self, type_: &'a str) -> impl Iterator<Item = &'a Catalog> {
        self.catalogs_for_type(type_).filter(|catalog| {
            (self.resources.is_empty() || self.accepts("catalog", type_, &catalog.id))
                && catalog.supports_extra("search")
        })
    }

    /// Find the catalog with the given media type and catalog id.
    pub fn catalog_for(&self, type_: &str, id: &str) -> Option<&Catalog> {
        self.catalogs
            .iter()
            .find(|c| c.type_ == type_ && c.id == id)
    }
}

/// An entry in `manifest.resources` — either a plain name string
/// (`"catalog"`, `"stream"`) or a detailed object with extra properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Resource {
    Plain(String),
    Detailed(ResourceDetail),
}

impl Resource {
    pub fn name(&self) -> &str {
        match self {
            Resource::Plain(name) => name,
            Resource::Detailed(detail) => &detail.name,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceDetail {
    pub name: String,
    #[serde(default)]
    pub id_prefixes: Vec<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// One catalog entry from `manifest.catalogs`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    #[serde(rename = "type")]
    pub type_: String,
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub extra: Vec<CatalogExtra>,
    #[serde(default)]
    pub genres: Vec<String>,
    #[serde(flatten)]
    pub extra_fields: HashMap<String, Value>,
}

impl Catalog {
    /// Does this catalog accept the extra parameter `name` (e.g. `"search"`
    /// or `"genre"`)?
    pub fn supports_extra(&self, name: &str) -> bool {
        self.extra.iter().any(|e| e.name == name)
    }
}

/// A declared extra parameter of a catalog (search, genre, skip, …).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct CatalogExtra {
    pub name: String,
    #[serde(default)]
    pub options: Vec<String>,
    #[serde(default = "default_false")]
    pub is_required: bool,
    #[serde(default = "default_false")]
    pub options_required: bool,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// Top-level shape of a catalog response: `{"metas": […]}`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MetaDetail {
    #[serde(default, deserialize_with = "null_default")]
    pub metas: Vec<MetaPreview>,
}

/// A catalog entry / light metadata card, as returned by
/// `GET /catalog/{type}/{id}.json`.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetaPreview {
    pub id: String,
    #[serde(rename = "type", alias = "type_")]
    pub type_: String,
    /// Display title. Some addons emit `title` (and some emit both, so the
    /// two must not share a serde alias — see [`MetaPreview::title`]).
    #[serde(default)]
    pub name: String,
    #[serde(default, rename = "title")]
    pub title_legacy: Option<String>,
    #[serde(default)]
    pub poster: Option<String>,
    #[serde(default)]
    pub background: Option<String>,
    #[serde(default)]
    pub logo: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub genres: Vec<String>,
    /// Human-readable release info such as `"1994"` or `"2017-2018"`.
    /// Some addons send a number, so keep the raw value. (Some addons, e.g.
    /// Cinemeta, also send a separate `year` key — that one lands in `extra`
    /// and is consulted by [`MetaPreview::year_str`] as a fallback.)
    #[serde(default)]
    pub release_info: Option<Value>,
    #[serde(default)]
    pub runtime: Option<String>,
    #[serde(default)]
    pub imdb_rating: Option<Value>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl MetaPreview {
    /// The display title: the SDK `name`, or the legacy `title` key when an
    /// addon only sends that.
    pub fn title(&self) -> String {
        if !self.name.is_empty() {
            self.name.clone()
        } else {
            self.title_legacy.clone().unwrap_or_default()
        }
    }

    /// Release info as a plain string, if present. Prefers `releaseInfo`,
    /// falls back to the plain `year` key some addons send instead.
    pub fn year_str(&self) -> Option<String> {
        if let Some(info) = self.release_info.as_ref().and_then(stringify) {
            return Some(info);
        }
        self.extra.get("year").and_then(stringify)
    }

    /// IMDb rating as a plain string, if present.
    pub fn rating_str(&self) -> Option<String> {
        self.imdb_rating.as_ref().and_then(stringify)
    }
}

/// A piece of media — catalog entry with optional extra detail
/// (`videos` for series episodes, cast, etc.).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct MetaItem {
    #[serde(flatten)]
    pub preview: MetaPreview,
    #[serde(default, deserialize_with = "null_default")]
    pub videos: Vec<Video>,
    #[serde(default, deserialize_with = "null_default")]
    pub cast: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub director: Vec<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub writer: Vec<String>,
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default, deserialize_with = "null_default")]
    pub trailer_streams: Vec<Stream>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl MetaItem {
    /// Season artwork is an optional addon extension, not the series-wide
    /// `background`. Read it tolerantly so absent/malformed season details
    /// never prevent an otherwise valid episode list from loading.
    pub fn season_backdrops(&self) -> HashMap<u32, String> {
        fn image(value: &Value) -> Option<&str> {
            value
                .as_str()
                .or_else(|| {
                    ["background", "backdrop"]
                        .iter()
                        .filter_map(|key| value.get(key).and_then(Value::as_str))
                        .find(|url| !url.trim().is_empty())
                })
                .map(str::trim)
                .filter(|url| !url.is_empty())
        }
        fn season(value: &Value) -> Option<u32> {
            value
                .as_u64()
                .and_then(|n| u32::try_from(n).ok())
                .or_else(|| value.as_str()?.parse().ok())
        }
        let mut backdrops = HashMap::new();
        // Flattened extension fields can be held by either metadata layer.
        for extra in [&self.extra, &self.preview.extra] {
            if let Some(Value::Object(seasons)) = extra.get("seasonBackdrops") {
                for (number, value) in seasons {
                    if let (Ok(number), Some(url)) = (number.parse::<u32>(), image(value)) {
                        backdrops.entry(number).or_insert_with(|| url.to_owned());
                    }
                }
            }
            if let Some(Value::Array(seasons)) = extra.get("seasons") {
                for value in seasons {
                    let number = ["season", "seasonNumber", "season_number", "number"]
                        .iter()
                        .find_map(|key| value.get(key).and_then(season));
                    if let (Some(number), Some(url)) = (number, image(value)) {
                        backdrops.entry(number).or_insert_with(|| url.to_owned());
                    }
                }
            }
        }
        backdrops
    }
}

/// An episode/entry inside `meta.videos`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Video {
    pub id: String,
    /// Display title. Cinemeta and many addons send `name`; others send
    /// `title`. Prefer `name` when both are present (see [`Video::label`]).
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub title: String,
    #[serde(default)]
    pub released: Option<String>,
    #[serde(default)]
    pub season: Option<u32>,
    #[serde(default)]
    pub episode: Option<u32>,
    /// Position within the season; some addons send `number` instead of
    /// (or in addition to) `episode`.
    #[serde(default)]
    pub number: Option<u32>,
    #[serde(default)]
    pub overview: Option<String>,
    #[serde(default)]
    pub thumbnail: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl Video {
    /// The display title: the `name` key (Cinemeta-style), or the `title`
    /// key when an addon only sends that.
    pub fn label(&self) -> String {
        if !self.name.is_empty() {
            self.name.clone()
        } else {
            self.title.clone()
        }
    }

    /// The episode position within its season, whatever the addon called it.
    pub fn episode_number(&self) -> Option<u32> {
        self.episode.or(self.number)
    }
}

/// One item of `GET /stream/{type}/{id}.json` — a possible way to watch.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Stream {
    /// Display name. Some addons emit `title` (and some emit both, so the two
    /// must not share a serde alias — see [`Stream::label`]).
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default, rename = "title")]
    pub title_legacy: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Direct playable URL (HLS/MP4/…).
    #[serde(default)]
    pub url: Option<String>,
    /// YouTube video id.
    #[serde(default)]
    pub yt_id: Option<String>,
    /// BitTorrent magnet-less infohash; combine with a tracker or a torrent
    /// client to play.
    #[serde(default)]
    pub info_hash: Option<String>,
    #[serde(default)]
    pub file_idx: Option<u32>,
    #[serde(default)]
    pub subtitles: Vec<Subtitle>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl Stream {
    /// A label suitable for a UI row (falls back to a placeholder).
    pub fn label(&self) -> String {
        self.name
            .clone()
            .filter(|s| !s.trim().is_empty())
            .or_else(|| self.title_legacy.clone().filter(|s| !s.trim().is_empty()))
            .unwrap_or_else(|| {
                if self.url.is_some() {
                    "Stream".into()
                } else if self.info_hash.is_some() {
                    "Torrent".into()
                } else if self.yt_id.is_some() {
                    "YouTube".into()
                } else {
                    "Stream".into()
                }
            })
    }

    /// A URL that can be opened directly in a browser/player, if the addon
    /// provides one (direct URL or YouTube).
    pub fn web_url(&self) -> Option<String> {
        if let Some(url) = &self.url {
            return Some(url.clone());
        }
        self.yt_id
            .as_ref()
            .map(|id| format!("https://www.youtube.com/watch?v={id}"))
    }
}

/// A subtitle track attached to a [`Stream`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Subtitle {
    pub id: String,
    pub url: String,
    #[serde(default)]
    pub lang: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl Subtitle {
    /// Subtitle addons supply remote media, never local paths or mpv pseudo-URLs.
    pub fn is_remote(&self) -> bool {
        self.url.len() <= 4096
            && !self.url.chars().any(char::is_control)
            && url::Url::parse(&self.url).is_ok_and(|url| {
                matches!(url.scheme(), "http" | "https") && url.host_str().is_some()
            })
    }
}

fn stringify(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod resource_tests {
    use super::*;

    #[test]
    fn season_backdrops_read_addon_extensions_without_using_series_art() {
        let meta = crate::Addon::parse_meta(
            br#"{"meta":{
            "id":"series", "type":"series", "background":"https://img/series",
            "seasonBackdrops":{"2":"https://img/season-two", "bad":"ignored", "3":" "},
            "seasons":[
                {"season":1,"background":" https://img/season-one "},
                {"seasonNumber":"2","backdrop":"https://img/other-two"},
                {"season_number":0,"background":"", "backdrop":"https://img/specials"},
                {"number":3,"background":null},
                {"season":-1,"background":"ignored"},
                {"season":4294967296,"background":"ignored"}
            ],
            "videos":[{"id":"s1e1", "season":1, "episode":1}]
        }}"#,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            meta.season_backdrops(),
            HashMap::from([
                (0, "https://img/specials".into()),
                (1, "https://img/season-one".into()),
                (2, "https://img/season-two".into()),
            ])
        );
        assert_eq!(meta.videos.len(), 1);

        for seasons in [
            Value::Null,
            Value::String("invalid".into()),
            Value::Number(2.into()),
        ] {
            let meta = crate::Addon::parse_meta(&serde_json::to_vec(&serde_json::json!({
                "meta":{"id":"series", "type":"series", "background":"https://img/series", "seasons":seasons}
            })).unwrap()).unwrap().unwrap();
            assert!(meta.season_backdrops().is_empty());
        }
    }

    #[test]
    fn nullable_optional_metadata_does_not_discard_valid_episodes() {
        let meta = crate::Addon::parse_meta(br#"{"meta":{
            "id":"tt0994314", "type":"series", "name":"Code Geass",
            "genres":null, "cast":null, "director":null, "writer":null,
            "trailerStreams":null,
            "videos":[{"id":"tt0994314:1:1", "name":"The Day a New Demon Was Born", "season":1, "episode":1}]
        }}"#).unwrap().unwrap();
        assert!(meta.director.is_empty());
        assert!(meta.preview.genres.is_empty());
        assert_eq!(meta.videos[0].id, "tt0994314:1:1");
        let catalog = crate::Addon::parse_catalog(
            br#"{"metas":[{
            "id":"tt0994314", "type":"series", "name":"Code Geass", "genres":null
        }]}"#,
        )
        .unwrap();
        assert_eq!(catalog[0].title(), "Code Geass");
        assert!(
            crate::Addon::parse_catalog(br#"{"metas":null}"#)
                .unwrap()
                .is_empty()
        );
        assert!(
            crate::Addon::parse_meta(br#"{"meta":{"id":"tt1", "type":"series", "videos":null}}"#)
                .unwrap()
                .unwrap()
                .videos
                .is_empty()
        );
        assert!(
            crate::Addon::parse_meta(
                br#"{"meta":{"id":"tt1", "type":"series", "director":"invalid"}}"#
            )
            .is_err()
        );
        assert!(crate::Addon::parse_meta(br#"{"meta":{"id":null, "type":"series"}}"#).is_err());
    }

    #[test]
    fn plain_resources_inherit_prefixes_and_detailed_resources_have_their_own() {
        let manifest: Manifest = serde_json::from_value(serde_json::json!({
            "id":"fixture", "name":"Fixture", "version":"1", "types":["series"], "idPrefixes":["anikoto:"],
            "resources":["catalog", "meta", {"name":"stream", "types":["movie"], "idPrefixes":["tt"]}]
        })).unwrap();
        assert!(manifest.accepts("catalog", "series", "popular"));
        assert!(manifest.accepts("meta", "series", "anikoto:opaque"));
        assert!(!manifest.accepts("meta", "series", "tt123"));
        assert!(!manifest.accepts("meta", "movie", "anikoto:opaque"));
        assert!(manifest.accepts("stream", "movie", "tt123"));
        assert!(!manifest.accepts("stream", "series", "tt123"));
        assert!(!manifest.accepts("stream", "movie", "anikoto:opaque"));
    }
}
