//! Store finite HLS presentations as self-contained local playlists. Keeping
//! their original segment boundaries also preserves encryption IVs, byte
//! ranges, discontinuities and separate audio without a platform remuxer.
use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::time::{Duration, Instant};

use m3u8_rs::{AlternativeMediaType, Key, KeyMethod, Playlist};
use reqwest::{Client, Response, Url};
use tokio::fs;
use tokio::io::AsyncWriteExt;

use crate::{
    CancellationToken, DownloadError, DownloadOutcome, DownloadProgress, DownloadResume, part_path,
};

const TIMEOUT: Duration = Duration::from_secs(60);
const MAX_PLAYLIST_BYTES: usize = 8 * 1024 * 1024;
const MAX_PLAYLISTS: usize = 32;
const MAX_ASSETS: usize = 50_000;

fn invalid(message: impl Into<String>) -> DownloadError {
    DownloadError::InvalidResponse(message.into())
}

struct Asset {
    url: Url,
    name: String,
    key: bool,
}

#[derive(Default)]
struct Plan {
    pending: VecDeque<(Url, String, usize)>,
    playlists: Vec<(String, Vec<u8>)>,
    assets: Vec<Asset>,
    asset_ids: HashMap<String, usize>,
    playlist_ids: HashMap<String, String>,
}

impl Plan {
    fn playlist(&mut self, base: &Url, uri: &str, depth: usize) -> Result<String, DownloadError> {
        let url = resolve(base, uri)?;
        if let Some(name) = self.playlist_ids.get(url.as_str()) {
            // A repeated playlist can form a cycle. Reject rather than write
            // an offline master that recursively refers back to itself.
            return Err(invalid(format!(
                "HLS playlist cycle or repeated playlist: {name}"
            )));
        }
        if depth > 4 || self.playlist_ids.len() >= MAX_PLAYLISTS {
            return Err(invalid("HLS playlist nesting is too large"));
        }
        let name = format!("playlist-{}.m3u8", self.playlist_ids.len());
        self.playlist_ids.insert(url.to_string(), name.clone());
        self.pending.push_back((url, name.clone(), depth));
        Ok(name)
    }

    fn asset(&mut self, base: &Url, uri: &str, key: bool) -> Result<String, DownloadError> {
        let url = resolve(base, uri)?;
        if let Some(index) = self.asset_ids.get(url.as_str()) {
            if self.assets[*index].key != key {
                return Err(invalid("HLS key and media share a URL"));
            }
            return Ok(self.assets[*index].name.clone());
        }
        if self.assets.len() >= MAX_ASSETS {
            return Err(invalid("HLS presentation has too many segments"));
        }
        // Names never come from remote paths. Preserve only media extensions
        // accepted by the local HLS demuxer, including fragmented MP4 and VTT.
        let extension = if key {
            // FFmpeg applies its media-extension allowlist to local keys too.
            // A descriptive media suffix works without relaxing that allowlist.
            "key.mp4"
        } else {
            url.path()
                .rsplit_once('.')
                .map(|(_, ext)| ext)
                .filter(|ext| matches!(*ext, "ts" | "m4s" | "mp4" | "aac" | "vtt" | "mp3"))
                .unwrap_or("ts")
        };
        let name = format!("asset-{}.{}", self.assets.len(), extension);
        self.asset_ids.insert(url.to_string(), self.assets.len());
        self.assets.push(Asset {
            url,
            name: name.clone(),
            key,
        });
        Ok(name)
    }

    fn key(&mut self, base: &Url, key: &mut Key) -> Result<(), DownloadError> {
        if key.method == KeyMethod::None {
            key.iv = None;
            key.uri = None;
            return Ok(());
        }
        if key.method != KeyMethod::AES128
            || key
                .keyformat
                .as_deref()
                .is_some_and(|format| format != "identity")
        {
            return Err(invalid(
                "This HLS encryption is not supported for offline downloads",
            ));
        }
        let uri = key
            .uri
            .as_deref()
            .ok_or_else(|| invalid("HLS key has no URI"))?;
        key.uri = Some(self.asset(base, uri, true)?);
        Ok(())
    }

    fn rewrite(
        &mut self,
        base: &Url,
        bytes: &[u8],
        depth: usize,
    ) -> Result<Vec<u8>, DownloadError> {
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("HLS playlist is not UTF-8"))?;
        if !text.trim_start_matches('\u{feff}').starts_with("#EXTM3U") {
            return Err(invalid("Response is not an HLS playlist"));
        }
        // m3u8-rs 6 requires an IV even for METHOD=NONE. Supply one to its
        // parser and remove it from the normalized unencrypted key above.
        let normalized = text
            .lines()
            .map(|line| {
                if line.trim() == "#EXT-X-KEY:METHOD=NONE" {
                    "#EXT-X-KEY:METHOD=NONE,IV=0x0"
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        let (_, mut playlist) = m3u8_rs::parse_playlist(normalized.as_bytes())
            .map_err(|_| invalid("Invalid HLS playlist"))?;
        match &mut playlist {
            Playlist::MasterPlaylist(master) => {
                let mut variant = master
                    .variants
                    .iter()
                    .filter(|variant| !variant.is_i_frame)
                    .max_by_key(|variant| variant.bandwidth)
                    .cloned()
                    .ok_or_else(|| invalid("HLS master has no playable variant"))?;
                // Keep the chosen quality and its audio/subtitle groups only.
                // Downloading every quality would multiply the episode size.
                master
                    .alternatives
                    .retain(|alternative| match alternative.media_type {
                        AlternativeMediaType::Audio => {
                            variant.audio.as_ref() == Some(&alternative.group_id)
                        }
                        AlternativeMediaType::Video => {
                            variant.video.as_ref() == Some(&alternative.group_id)
                        }
                        AlternativeMediaType::Subtitles => {
                            variant.subtitles.as_ref() == Some(&alternative.group_id)
                        }
                        AlternativeMediaType::ClosedCaptions => true,
                        _ => false,
                    });
                variant.uri = self.playlist(base, &variant.uri, depth + 1)?;
                for alternative in &mut master.alternatives {
                    if let Some(uri) = &mut alternative.uri {
                        *uri = self.playlist(base, uri, depth + 1)?;
                    }
                }
                master.variants = vec![variant];
                for key in &mut master.session_key {
                    self.key(base, &mut key.0)?;
                }
                master.session_data.clear();
                master.unknown_tags.clear();
            }
            Playlist::MediaPlaylist(media) => {
                if !media.end_list || media.segments.is_empty() {
                    return Err(invalid(
                        "Only complete, finite HLS episodes can be downloaded",
                    ));
                }
                media.unknown_tags.clear();
                for segment in &mut media.segments {
                    segment.uri = self.asset(base, &segment.uri, false)?;
                    if let Some(key) = &mut segment.key {
                        self.key(base, key)?;
                    }
                    if let Some(map) = &mut segment.map {
                        map.uri = self.asset(base, &map.uri, false)?;
                    }
                    segment.unknown_tags.clear();
                    segment.daterange = None;
                }
            }
        }
        let mut output = Vec::new();
        playlist
            .write_to(&mut output)
            .map_err(DownloadError::from)?;
        Ok(output)
    }
}

fn resolve(base: &Url, uri: &str) -> Result<Url, DownloadError> {
    let url = base.join(uri).map_err(|error| invalid(error.to_string()))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(DownloadError::UnsupportedUrl(url.to_string()));
    }
    Ok(url)
}

async fn chunk(
    response: &mut Response,
    cancel: &CancellationToken,
) -> Result<Option<Vec<u8>>, DownloadError> {
    tokio::select! {
        biased;
        _ = cancel.wait_cancelled() => Err(DownloadError::Cancelled),
        result = tokio::time::timeout(TIMEOUT, response.chunk()) => {
            result.map_err(|_| invalid("HLS response stalled"))?
                .map(|chunk| chunk.map(|bytes| bytes.to_vec()))
                .map_err(|error| invalid(error.to_string()))
        }
    }
}

async fn get(
    client: &Client,
    url: &Url,
    headers: &[(String, String)],
    cancel: &CancellationToken,
) -> Result<Response, DownloadError> {
    let response = super::http::send_request_retrying_rate_limits(
        client,
        url,
        0,
        &DownloadResume::default(),
        headers,
        cancel,
        TIMEOUT,
    )
    .await?;
    if response.status() != reqwest::StatusCode::OK {
        return Err(DownloadError::HttpStatus {
            status: response.status().as_u16(),
            message: "HLS request failed".into(),
        });
    }
    Ok(response)
}

async fn playlist_bytes(
    mut response: Response,
    cancel: &CancellationToken,
) -> Result<(Url, Vec<u8>), DownloadError> {
    if response.status() != reqwest::StatusCode::OK {
        return Err(invalid(
            "HLS playlist request did not return a complete response",
        ));
    }
    let url = response.url().clone();
    let mut bytes = Vec::new();
    while let Some(chunk) = chunk(&mut response, cancel).await? {
        if bytes.len() + chunk.len() > MAX_PLAYLIST_BYTES {
            return Err(invalid("HLS playlist is too large"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok((url, bytes))
}

pub(super) async fn download_hls<F>(
    client: &Client,
    response: Response,
    headers: &[(String, String)],
    destination: &Path,
    cancel: CancellationToken,
    mut on_progress: F,
) -> Result<DownloadOutcome, DownloadError>
where
    F: FnMut(DownloadProgress) + Send,
{
    let (base, bytes) = playlist_bytes(response, &cancel).await?;
    let mut plan = Plan::default();
    plan.playlist_ids
        .insert(base.to_string(), "index.m3u8".into());
    let root = plan.rewrite(&base, &bytes, 0)?;
    while let Some((url, name, depth)) = plan.pending.pop_front() {
        let (base, bytes) =
            playlist_bytes(get(client, &url, headers, &cancel).await?, &cancel).await?;
        let bytes = plan.rewrite(&base, &bytes, depth)?;
        plan.playlists.push((name, bytes));
    }
    let destination = destination.with_extension("m3u8");
    let parent = destination
        .parent()
        .ok_or_else(|| invalid("HLS destination has no parent"))?;
    let bundle = parent.join("hls");
    fs::create_dir_all(&bundle).await?;
    let start = Instant::now();
    let mut downloaded = 0;
    let mut transferred = 0;
    for (index, asset) in plan.assets.iter().enumerate() {
        let path = bundle.join(&asset.name);
        let marker = path.with_extension(format!(
            "{}.done",
            path.extension().unwrap().to_string_lossy()
        ));
        // Check both URL identity and committed length before reusing a whole
        // segment. Partial segments restart; completed segments survive pause
        // or process death. A refreshed signed URL is never mistaken for the
        // previous source at the same ordinal.
        let cached = match (fs::read_to_string(&marker).await, fs::metadata(&path).await) {
            (Ok(marker), Ok(meta)) => {
                (!asset.key || meta.len() == 16)
                    && marker == format!("{}\n{}", asset.url, meta.len())
            }
            _ => false,
        };
        if !cached {
            let mut response = get(client, &asset.url, headers, &cancel).await?;
            let expected = response.content_length();
            let part = part_path(&path);
            let mut file = fs::File::create(&part).await?;
            let mut bytes = 0u64;
            while let Some(chunk) = chunk(&mut response, &cancel).await? {
                bytes += chunk.len() as u64;
                if asset.key && bytes > 16 {
                    return Err(invalid("Invalid AES-128 key length"));
                }
                // Tokio files can retain the previous write in their worker
                // buffer. Physical admission must see that allocation first.
                file.flush().await?;
                if !cancel.admit_write(downloaded + bytes, chunk.len() as u64) {
                    file.flush().await?;
                    return Err(DownloadError::Cancelled);
                }
                file.write_all(&chunk).await?;
                transferred += chunk.len() as u64;
                on_progress(
                    DownloadProgress::new(
                        downloaded + bytes,
                        None,
                        (transferred as f64 / start.elapsed().as_secs_f64().max(0.001)) as u64,
                    )
                    .with_resources(index as u64, plan.assets.len() as u64),
                );
            }
            if bytes == 0
                || expected.is_some_and(|expected| expected != bytes)
                || (asset.key && bytes != 16)
            {
                return Err(invalid("Incomplete HLS segment or key"));
            }
            file.sync_all().await?;
            drop(file);
            fs::rename(&part, &path).await?;
            fs::write(&marker, format!("{}\n{bytes}", asset.url)).await?;
        }
        if cancel.is_cancelled() {
            return Err(DownloadError::Cancelled);
        }
        downloaded += fs::metadata(&path).await?.len();
        on_progress(
            DownloadProgress::new(
                downloaded,
                None,
                (transferred as f64 / start.elapsed().as_secs_f64().max(0.001)) as u64,
            )
            .with_resources((index + 1) as u64, plan.assets.len() as u64),
        );
    }
    // Publish the entry playlist last. A missing manifest can re-adopt this
    // single root artifact, but can never adopt partial segments as episodes.
    for (name, bytes) in plan.playlists {
        fs::write(bundle.join(name), bytes).await?;
    }
    let root = String::from_utf8(root).map_err(|_| invalid("Invalid rewritten HLS"))?;
    // Only the entry playlist lives outside hls/; prefix its local references.
    let root = root
        .lines()
        .map(|line| {
            if line.starts_with('#') {
                line.replace("URI=\"", "URI=\"hls/")
            } else if !line.is_empty() {
                format!("hls/{line}")
            } else {
                String::new()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
        + "\n";
    let part = part_path(&destination);
    fs::write(&part, root).await?;
    if cancel.is_cancelled() {
        return Err(DownloadError::Cancelled);
    }
    fs::rename(&part, &destination).await?;
    Ok(DownloadOutcome {
        file_name: destination
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned(),
        path: destination,
        part_path: part,
        bytes_downloaded: downloaded,
        total_bytes: Some(downloaded),
        bytes_per_second: 0,
        etag: None,
        last_modified: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    type Hits = Arc<Mutex<Vec<String>>>;

    async fn server(
        routes: Vec<(&str, &str, &[u8])>,
    ) -> (String, Hits, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let routes = routes
            .into_iter()
            .map(|(path, kind, body)| (path.to_string(), (kind.to_string(), body.to_vec())))
            .collect::<HashMap<_, _>>();
        let hits = Hits::default();
        let captured = hits.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                while !bytes.ends_with(b"\r\n\r\n") {
                    let mut chunk = [0; 2048];
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let request = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
                assert!(request.contains("referer: https://provider.example/watch\r\n"));
                // Provider headers cannot override transfer-owned range headers.
                assert!(!request.contains("range: bytes=999-"));
                let path = request.split_whitespace().nth(1).unwrap();
                captured.lock().unwrap().push(path.to_string());
                let (kind, body) = routes
                    .get(path)
                    .unwrap_or_else(|| panic!("unexpected path {path}"));
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(body).await.unwrap();
            }
        });
        (base, hits, task)
    }

    fn destination(name: &str) -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir()
            .join(format!(
                "nova-hls-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .join("episode.mp4")
    }

    async fn download<F>(
        url: String,
        path: &Path,
        token: CancellationToken,
        progress: F,
    ) -> Result<DownloadOutcome, DownloadError>
    where
        F: FnMut(DownloadProgress) + Send,
    {
        let mut request = crate::HttpDownloadRequest::new(url, path);
        request.headers = vec![
            ("Referer".into(), "https://provider.example/watch".into()),
            ("Range".into(), "bytes=999-".into()),
        ];
        crate::download_http_with_request(&Client::new(), request, token, progress).await
    }

    #[tokio::test]
    async fn downloads_one_quality_with_audio_keys_maps_ranges_and_subtitles() {
        let master = b"#EXTM3U\n#EXT-X-MEDIA:TYPE=AUDIO,GROUP-ID=\"audio\",NAME=\"English\",DEFAULT=YES,URI=\"audio.m3u8\"\n#EXT-X-MEDIA:TYPE=SUBTITLES,GROUP-ID=\"subs\",NAME=\"English\",URI=\"subs.m3u8\"\n#EXT-X-STREAM-INF:BANDWIDTH=100\nlow.m3u8\n#EXT-X-STREAM-INF:BANDWIDTH=200,AUDIO=\"audio\",SUBTITLES=\"subs\"\nhigh.m3u8\n";
        let media = b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-MEDIA-SEQUENCE:10\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\n#EXT-X-MAP:URI=\"init.mp4\",BYTERANGE=\"2@0\"\n#EXTINF:4,\n#EXT-X-BYTERANGE:2@0\nvideo.ts\n#EXT-X-KEY:METHOD=NONE\n#EXT-X-DISCONTINUITY\n#EXTINF:4,\n#EXT-X-BYTERANGE:2\nvideo.ts\n#EXT-X-ENDLIST\n";
        let audio = b"#EXTM3U\n#EXT-X-TARGETDURATION:8\n#EXTINF:8,\naudio.aac\n#EXT-X-ENDLIST\n";
        let subtitles = b"#EXTM3U\n#EXT-X-TARGETDURATION:8\n#EXTINF:8,\nsub.vtt\n#EXT-X-ENDLIST\n";
        let (base, hits, task) = server(vec![
            ("/master.m3u8", "application/vnd.apple.mpegurl", master),
            ("/high.m3u8", "application/vnd.apple.mpegurl", media),
            ("/audio.m3u8", "application/vnd.apple.mpegurl", audio),
            ("/subs.m3u8", "application/vnd.apple.mpegurl", subtitles),
            ("/video.ts", "video/mp2t", b"abcd"),
            ("/key", "application/octet-stream", b"0123456789abcdef"),
            ("/init.mp4", "video/mp4", b"init"),
            ("/audio.aac", "audio/aac", b"audio"),
            ("/sub.vtt", "text/vtt", b"WEBVTT\n\n"),
        ])
        .await;
        let path = destination("master");
        let mut updates = Vec::new();
        let result = download(
            format!("{base}/master.m3u8"),
            &path,
            CancellationToken::new(),
            |progress| updates.push(progress),
        )
        .await
        .unwrap();
        task.abort();
        assert!(!updates.is_empty());
        assert!(
            updates
                .iter()
                .all(|progress| progress.total_bytes.is_none()
                    && progress.resource_progress.is_some())
        );
        assert!(
            updates
                .windows(2)
                .all(|pair| pair[0].fraction() <= pair[1].fraction())
        );
        assert_eq!(result.path.extension().unwrap(), "m3u8");
        let root = fs::read_to_string(&result.path).await.unwrap();
        assert!(root.contains("hls/playlist-1.m3u8"));
        assert!(!root.contains("BANDWIDTH=100\n"));
        let video = fs::read_to_string(path.parent().unwrap().join("hls/playlist-1.m3u8"))
            .await
            .unwrap();
        assert!(video.contains("METHOD=AES-128,URI=\"asset-1.key.mp4\""));
        assert!(video.contains("METHOD=NONE"));
        assert!(video.contains("#EXT-X-BYTERANGE:2@0"));
        assert!(video.contains("#EXT-X-DISCONTINUITY"));
        assert!(video.contains("MEDIA-SEQUENCE:10"));
        assert!(!video.contains("http"));
        {
            let hits = hits.lock().unwrap();
            assert!(!hits.iter().any(|path| path == "/low.m3u8"));
            assert_eq!(hits.iter().filter(|path| *path == "/video.ts").count(), 1);
        }
        assert_eq!(
            fs::read(path.parent().unwrap().join("hls/asset-1.key.mp4"))
                .await
                .unwrap(),
            b"0123456789abcdef"
        );
        fs::remove_dir_all(path.parent().unwrap()).await.unwrap();
    }

    #[tokio::test]
    async fn pause_and_resume_reuses_only_committed_segments() {
        for after_segment in [false, true] {
            let media = b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\none.ts\n#EXTINF:4,\ntwo.ts\n#EXT-X-ENDLIST\n";
            let (base, hits, task) = server(vec![
                ("/video", "application/vnd.apple.mpegurl", media),
                ("/one.ts", "video/mp2t", b"one"),
                ("/two.ts", "video/mp2t", b"two"),
            ])
            .await;
            let path = destination("resume");
            let token = CancellationToken::new();
            let cancel = token.clone();
            let result = download(format!("{base}/video"), &path, token, move |progress| {
                if !after_segment
                    || progress
                        .resource_progress
                        .is_some_and(|(completed, _)| completed > 0)
                {
                    cancel.cancel();
                }
            })
            .await;
            assert!(matches!(result, Err(DownloadError::Cancelled)));
            assert!(!path.with_extension("m3u8").exists());
            let result = download(
                format!("{base}/video"),
                &path,
                CancellationToken::new(),
                |_| {},
            )
            .await
            .unwrap();
            task.abort();
            assert_eq!(result.bytes_downloaded, 6);
            assert_eq!(
                hits.lock()
                    .unwrap()
                    .iter()
                    .filter(|path| *path == "/one.ts")
                    .count(),
                if after_segment { 1 } else { 2 }
            );
            fs::remove_dir_all(path.parent().unwrap()).await.unwrap();
        }
    }

    #[test]
    fn rejects_live_unsupported_encryption_and_playlist_cycles() {
        let base = Url::parse("https://provider.example/master.m3u8").unwrap();
        let mut plan = Plan::default();
        assert!(
            plan.rewrite(
                &base,
                b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\nlive.ts\n",
                0
            )
            .is_err()
        );
        assert!(plan.rewrite(&base, b"#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=SAMPLE-AES,URI=\"key\"\n#EXTINF:4,\nvideo.ts\n#EXT-X-ENDLIST\n", 0).is_err());
        plan.playlist_ids
            .insert(base.to_string(), "index.m3u8".into());
        assert!(
            plan.rewrite(
                &base,
                b"#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=1\nmaster.m3u8\n",
                0
            )
            .is_err()
        );
        assert!(resolve(&base, "file:///private/media.ts").is_err());
    }
}
