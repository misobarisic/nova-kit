use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nova_download::{CancellationToken, DownloadError, HttpDownloadRequest};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const MEDIA: &str =
    "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\none.ts\n#EXTINF:4,\ntwo.ts\n#EXT-X-ENDLIST\n";

struct Reply {
    status: u16,
    body: &'static str,
    retry_after: Option<&'static str>,
}

fn ok(body: &'static str) -> Reply {
    Reply {
        status: 200,
        body,
        retry_after: None,
    }
}

fn throttled(delay: &'static str) -> Reply {
    Reply {
        status: 429,
        body: "rate limited",
        retry_after: Some(delay),
    }
}

struct Fixture {
    base: String,
    destination: PathBuf,
    requests: Arc<Mutex<Vec<String>>>,
    throttled: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn new(routes: Vec<(&str, Vec<Reply>)>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut routes: HashMap<String, VecDeque<Reply>> = routes
            .into_iter()
            .map(|(path, replies)| (path.into(), replies.into()))
            .collect();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let throttled = Arc::new(Notify::new());
        let signal = throttled.clone();
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
                let request = String::from_utf8(bytes).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_string();
                captured.lock().unwrap().push(request);
                let replies = routes
                    .get_mut(&path)
                    .unwrap_or_else(|| panic!("unexpected path {path}"));
                // Keep the final response so a terminal-status test can detect retries.
                let reply = if replies.len() > 1 {
                    replies.pop_front().unwrap()
                } else {
                    let reply = replies.front().unwrap();
                    Reply {
                        status: reply.status,
                        body: reply.body,
                        retry_after: reply.retry_after,
                    }
                };
                let retry = reply
                    .retry_after
                    .map(|delay| format!("Retry-After: {delay}\r\n"))
                    .unwrap_or_default();
                let kind = if path.ends_with(".m3u8") {
                    "application/vnd.apple.mpegurl"
                } else {
                    "application/octet-stream"
                };
                let header = format!(
                    "HTTP/1.1 {} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\n{retry}Connection: close\r\n\r\n",
                    reply.status,
                    reply.body.len()
                );
                socket.write_all(header.as_bytes()).await.unwrap();
                socket.write_all(reply.body.as_bytes()).await.unwrap();
                if reply.status == 429 {
                    signal.notify_one();
                }
            }
        });
        let destination = std::env::temp_dir()
            .join(format!(
                "nova-hls-rate-limit-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ))
            .join("episode.mp4");
        Self {
            base,
            destination,
            requests,
            throttled,
            task,
        }
    }

    fn request(&self) -> HttpDownloadRequest {
        HttpDownloadRequest::new(format!("{}/master.m3u8", self.base), &self.destination)
            .with_headers(vec![
                ("Referer".into(), "https://provider.example/watch".into()),
                ("Origin".into(), "https://provider.example".into()),
                ("Range".into(), "bytes=999-".into()),
            ])
    }

    fn count(&self, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.split_whitespace().nth(1) == Some(path))
            .count()
    }

    async fn download(
        &self,
        token: CancellationToken,
    ) -> Result<nova_download::DownloadOutcome, DownloadError> {
        tokio::time::timeout(
            Duration::from_secs(5),
            nova_download::download_http_with_request(
                &reqwest::Client::new(),
                self.request(),
                token,
                |_| {},
            ),
        )
        .await
        .expect("download must finish promptly")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(self.destination.parent().unwrap());
    }
}

#[tokio::test]
async fn late_segment_rate_limit_recovers_without_refetching_committed_segments() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        ("/two.ts", vec![throttled("0"), ok("two")]),
    ])
    .await;
    let outcome = fixture.download(CancellationToken::new()).await.unwrap();
    assert_eq!(outcome.bytes_downloaded, 6);
    assert!(outcome.path.exists());
    assert_eq!(fixture.count("/one.ts"), 1);
    assert_eq!(fixture.count("/two.ts"), 2);
    for request in fixture.requests.lock().unwrap().iter() {
        let request = request.to_ascii_lowercase();
        assert!(request.contains("referer: https://provider.example/watch\r\n"));
        assert!(request.contains("origin: https://provider.example\r\n"));
        assert!(!request.contains("range: bytes=999-"));
    }
}

#[tokio::test]
async fn initial_manifest_rate_limit_recovers() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![throttled("0"), ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        ("/two.ts", vec![ok("two")]),
    ])
    .await;
    assert_eq!(
        fixture
            .download(CancellationToken::new())
            .await
            .unwrap()
            .bytes_downloaded,
        6
    );
    assert_eq!(fixture.count("/master.m3u8"), 2);
}

#[tokio::test]
async fn forbidden_and_missing_segments_are_not_retried() {
    for status in [403, 404] {
        let fixture = Fixture::new(vec![
            ("/master.m3u8", vec![ok(MEDIA)]),
            ("/one.ts", vec![ok("one")]),
            (
                "/two.ts",
                vec![Reply {
                    status,
                    body: "unavailable",
                    retry_after: Some("0"),
                }],
            ),
        ])
        .await;
        assert!(
            matches!(fixture.download(CancellationToken::new()).await, Err(DownloadError::HttpStatus { status: actual, .. }) if actual == status)
        );
        assert_eq!(fixture.count("/two.ts"), 1);
        assert!(!fixture.destination.with_extension("m3u8").exists());
    }
}

#[tokio::test]
async fn cancellation_interrupts_rate_limit_cooldown_and_keeps_committed_segments() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        ("/two.ts", vec![throttled("60"), ok("two")]),
    ])
    .await;
    let token = CancellationToken::new();
    let cancel = token.clone();
    let download = fixture.download(token);
    let cancel_when_throttled = async {
        fixture.throttled.notified().await;
        // Let the response enter the cooldown before exercising cancellation.
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancel.cancel();
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(download, cancel_when_throttled)
    })
    .await
    .expect("throttled request and cancellation must finish promptly");
    assert!(matches!(result, Err(DownloadError::Cancelled)));
    assert_eq!(fixture.count("/two.ts"), 1);
    assert!(!fixture.destination.with_extension("m3u8").exists());
    let bundle = fixture.destination.parent().unwrap().join("hls");
    let committed: Vec<_> = std::fs::read_dir(&bundle)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "done")
        })
        .collect();
    assert_eq!(committed.len(), 1);
    let marker = std::fs::read_to_string(&committed[0]).unwrap();
    assert_eq!(marker, format!("{}/one.ts\n3", fixture.base));
    assert_eq!(
        fixture
            .download(CancellationToken::new())
            .await
            .unwrap()
            .bytes_downloaded,
        6
    );
    assert_eq!(fixture.count("/one.ts"), 1);
}

#[tokio::test]
async fn child_playlist_map_and_key_rate_limits_recover() {
    let master = "#EXTM3U\n#EXT-X-STREAM-INF:BANDWIDTH=100\nchild.m3u8\n";
    let child = "#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXT-X-KEY:METHOD=AES-128,URI=\"key\"\n#EXT-X-MAP:URI=\"init.mp4\"\n#EXTINF:4,\none.ts\n#EXT-X-ENDLIST\n";
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(master)]),
        ("/child.m3u8", vec![throttled("0"), ok(child)]),
        ("/key", vec![throttled("0"), ok("0123456789abcdef")]),
        ("/init.mp4", vec![throttled("0"), ok("init")]),
        ("/one.ts", vec![ok("one")]),
    ])
    .await;
    assert_eq!(
        fixture
            .download(CancellationToken::new())
            .await
            .unwrap()
            .bytes_downloaded,
        23
    );
    for path in ["/child.m3u8", "/key", "/init.mp4"] {
        assert_eq!(fixture.count(path), 2);
    }
}

#[tokio::test]
async fn persistent_rate_limit_stops_after_bounded_attempts() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        ("/two.ts", vec![throttled("0")]),
    ])
    .await;
    assert!(matches!(
        fixture.download(CancellationToken::new()).await,
        Err(DownloadError::HttpStatus { status: 429, .. })
    ));
    assert_eq!(fixture.count("/two.ts"), 7);
    assert_eq!(fixture.count("/one.ts"), 1);
    assert!(!fixture.destination.with_extension("m3u8").exists());
}

#[tokio::test]
async fn excessive_server_cooldown_fails_without_retrying_early() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        ("/two.ts", vec![throttled("3600"), ok("two")]),
    ])
    .await;
    assert!(matches!(
        fixture.download(CancellationToken::new()).await,
        Err(DownloadError::HttpStatus { status: 429, .. })
    ));
    assert_eq!(fixture.count("/two.ts"), 1);
    assert!(!fixture.destination.with_extension("m3u8").exists());
}

#[tokio::test]
async fn missing_retry_after_uses_backoff_without_refetching_committed_segments() {
    let fixture = Fixture::new(vec![
        ("/master.m3u8", vec![ok(MEDIA)]),
        ("/one.ts", vec![ok("one")]),
        (
            "/two.ts",
            vec![
                Reply {
                    status: 429,
                    body: "rate limited",
                    retry_after: None,
                },
                ok("two"),
            ],
        ),
    ])
    .await;
    let started = std::time::Instant::now();
    assert_eq!(
        fixture
            .download(CancellationToken::new())
            .await
            .unwrap()
            .bytes_downloaded,
        6
    );
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(fixture.count("/one.ts"), 1);
    assert_eq!(fixture.count("/two.ts"), 2);
}
