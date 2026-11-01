//! Bounded raw addon package transfer, independent of the record wire schema.
//! Hashes identify registered blobs only; the application verifies SHA-256 and
//! package validity before registration and again before installing a receipt.
//! Each connection carries one stream: a 64-byte lowercase hash and FIN, then
//! a status byte (0 = missing, 1 = found), an 8-byte big-endian length for a
//! found blob, its raw bytes and FIN. There are no compressed record frames.
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, EndpointId};

pub const PACKAGE_ALPN: &[u8] = b"nova/addon-package/1";
pub const MAX_PACKAGE_BYTES: usize = 8 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;
const PEER_TIMEOUT: Duration = Duration::from_secs(30);
const TOTAL_TIMEOUT: Duration = Duration::from_secs(90);

static OUTGOING: AtomicUsize = AtomicUsize::new(0);

/// Transient UI accounting only; package and record wire formats are unchanged.
pub fn outgoing_package_transfers() -> usize {
    OUTGOING.load(Ordering::Relaxed)
}

struct ActiveSend<'a>(&'a AtomicUsize);
impl<'a> ActiveSend<'a> {
    fn new(counter: &'a AtomicUsize) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}
impl Drop for ActiveSend<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Only registered, hash-verified addon packages should be returned. Never
/// interpret a requested hash as an arbitrary file path.
pub trait PackageBlobStore: Send + Sync {
    fn load(&self, hash: &str) -> Result<Option<Vec<u8>>>;
}
static STORE: LazyLock<Mutex<Option<Arc<dyn PackageBlobStore>>>> =
    LazyLock::new(|| Mutex::new(None));

pub fn configure_package_store(store: Arc<dyn PackageBlobStore>) {
    *STORE.lock().unwrap_or_else(|e| e.into_inner()) = Some(store);
}

fn validate_hash(hash: &str) -> Result<()> {
    ensure!(
        hash.len() == 64
            && hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "package hash must be 64 lowercase hexadecimal characters"
    );
    Ok(())
}

#[derive(Clone)]
pub(crate) struct PackageHandler {
    peers: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
}
impl std::fmt::Debug for PackageHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PackageHandler").finish_non_exhaustive()
    }
}
impl PackageHandler {
    pub(crate) fn new(peers: Arc<Mutex<Vec<String>>>, stop: Arc<AtomicBool>) -> Self {
        Self { peers, stop }
    }
    fn authorized(&self, peer: &str) -> bool {
        !self.stop.load(Ordering::Acquire)
            && self
                .peers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .iter()
                .any(|p| p == peer)
    }
    async fn guard<T>(
        &self,
        peer: &str,
        cancel: Option<&AtomicBool>,
        work: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let revoked = async {
            loop {
                ensure!(self.authorized(peer), "package peer authorization revoked");
                ensure!(
                    !cancel.is_some_and(|c| c.load(Ordering::Acquire)),
                    "package transfer cancelled"
                );
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            #[allow(unreachable_code)]
            Ok::<(), anyhow::Error>(())
        };
        tokio::select! {
            biased;
            result = revoked => { result?; bail!("package transfer interrupted") },
            result = work => {
                ensure!(self.authorized(peer), "package peer authorization revoked");
                ensure!(!cancel.is_some_and(|c| c.load(Ordering::Acquire)), "package transfer cancelled");
                result
            }
        }
    }
    async fn serve(&self, conn: &Connection) -> Result<()> {
        let (mut send, mut recv) = conn.accept_bi().await?;
        let mut request = [0; 64];
        recv.read_exact(&mut request).await?;
        ensure!(
            recv.read_chunk(1).await?.is_none(),
            "extra package request bytes"
        );
        let hash = std::str::from_utf8(&request)?;
        validate_hash(hash)?;
        let store = STORE.lock().unwrap_or_else(|e| e.into_inner()).clone();
        // Disk access cannot stall iroh's runtime workers. Cancellation drops
        // this wait; the provider's read may finish in the blocking pool.
        let hash = hash.to_owned();
        let blob = tokio::task::spawn_blocking(move || match store {
            Some(store) => store.load(&hash),
            None => Ok(None),
        })
        .await??;
        ensure!(
            self.authorized(&conn.remote_id().to_string()),
            "package peer authorization revoked"
        );
        match blob {
            Some(bytes) => {
                ensure!(
                    bytes.len() <= MAX_PACKAGE_BYTES,
                    "registered package exceeds size limit"
                );
                // The guard also releases on timeout, peer revocation or an
                // interrupted future, so the UI cannot retain a stuck transfer.
                let _sending = ActiveSend::new(&OUTGOING);
                send.write_all(&[1]).await?;
                send.write_all(&(bytes.len() as u64).to_be_bytes()).await?;
                for chunk in bytes.chunks(CHUNK) {
                    ensure!(
                        self.authorized(&conn.remote_id().to_string()),
                        "package peer authorization revoked"
                    );
                    send.write_all(chunk).await?;
                }
                send.finish()?;
                send.stopped().await?;
                return Ok(());
            }
            None => send.write_all(&[0]).await?,
        }
        send.finish()?;
        // Keep the connection alive until the client has received its FIN.
        send.stopped().await?;
        Ok(())
    }
}
impl ProtocolHandler for PackageHandler {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let peer = conn.remote_id().to_string();
        let result =
            tokio::time::timeout(PEER_TIMEOUT, self.guard(&peer, None, self.serve(&conn))).await;
        if !matches!(result, Ok(Ok(()))) {
            conn.close(
                VarInt::from_u32(crate::protocol::REVOKED_CODE),
                b"package transfer rejected",
            );
        }
        Ok(())
    }
}

async fn receive(conn: &Connection, hash: &str) -> Result<Vec<u8>> {
    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(hash.as_bytes()).await?;
    send.finish()?;
    let mut status = [0];
    recv.read_exact(&mut status).await?;
    ensure!(status == [1], "package unavailable from peer");
    let mut length = [0; 8];
    recv.read_exact(&mut length).await?;
    let length = u64::from_be_bytes(length);
    ensure!(
        length <= MAX_PACKAGE_BYTES as u64,
        "package exceeds size limit"
    );
    let mut bytes = Vec::with_capacity(length as usize);
    while let Some(chunk) = recv.read_chunk(CHUNK).await? {
        ensure!(
            bytes.len() + chunk.len() <= length as usize,
            "package exceeds advertised length"
        );
        bytes.extend_from_slice(&chunk);
    }
    ensure!(bytes.len() == length as usize, "truncated package");
    Ok(bytes)
}

pub(crate) async fn fetch(
    endpoint: Endpoint,
    peers: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    hash: String,
    cancel: Arc<AtomicBool>,
) -> Result<Vec<u8>> {
    validate_hash(&hash)?;
    ensure!(
        !cancel.load(Ordering::Acquire),
        "package transfer cancelled"
    );
    let handler = PackageHandler::new(peers.clone(), stop);
    let ids = peers.lock().unwrap_or_else(|e| e.into_inner()).clone();
    let work = async {
        let mut last_error = anyhow::anyhow!("no paired peer has this package");
        for peer in ids {
            ensure!(
                !cancel.load(Ordering::Acquire),
                "package transfer cancelled"
            );
            if !handler.authorized(&peer) {
                continue;
            }
            let id: EndpointId = match peer.parse() {
                Ok(id) => id,
                Err(_) => continue,
            };
            let attempt = async {
                let conn = endpoint
                    .connect(
                        EndpointAddr::from(id),
                        crate::configuration::protocol().package,
                    )
                    .await?;
                let result = handler
                    .guard(&peer, Some(&cancel), receive(&conn, &hash))
                    .await;
                conn.close(VarInt::from_u32(0), b"package request complete");
                result
            };
            match tokio::time::timeout(PEER_TIMEOUT, handler.guard(&peer, Some(&cancel), attempt))
                .await
            {
                Ok(Ok(bytes)) => return Ok(bytes),
                Ok(Err(error)) => last_error = error,
                Err(_) => last_error = anyhow::anyhow!("package peer timed out"),
            }
        }
        Err(last_error)
    };
    tokio::time::timeout(TOTAL_TIMEOUT, work)
        .await
        .context("package transfer timed out")?
}

/// Blocking worker API. Never creates an endpoint while sync is disabled.
/// The application must verify the returned bytes against `hash`.
pub fn fetch_package(hash: &str, cancel: Arc<AtomicBool>) -> Result<Vec<u8>> {
    validate_hash(hash)?;
    ensure!(crate::read_settings().enabled, "sync is disabled");
    crate::engine()
        .context("sync engine is not installed")?
        .fetch_package(hash, cancel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn send_accounting_clears_when_a_transfer_future_is_dropped() {
        let counter = AtomicUsize::new(0);
        let mut transfer = Box::pin(async {
            let _sending = ActiveSend::new(&counter);
            std::future::pending::<()>().await;
        });
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(transfer.as_mut().poll(&mut context).is_pending());
        assert_eq!(counter.load(Ordering::Relaxed), 1);
        drop(transfer);
        assert_eq!(counter.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn hashes_are_strict_content_identifiers() {
        assert!(validate_hash(&"a".repeat(64)).is_ok());
        for hash in [
            "A".repeat(64),
            "g".repeat(64),
            "a".repeat(63),
            format!("../{}", "a".repeat(61)),
        ] {
            assert!(validate_hash(&hash).is_err());
        }
    }

    async fn endpoint() -> Endpoint {
        Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![PACKAGE_ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    }

    struct MemoryStore;
    impl PackageBlobStore for MemoryStore {
        fn load(&self, hash: &str) -> Result<Option<Vec<u8>>> {
            Ok(match hash.as_bytes()[0] {
                b'a' => Some(vec![42; CHUNK * 3 + 7]),
                b'b' => Some(vec![0; MAX_PACKAGE_BYTES + 1]),
                _ => None,
            })
        }
    }

    #[tokio::test]
    async fn registered_transfer_missing_oversized_and_membership() {
        configure_package_store(Arc::new(MemoryStore));
        let a = endpoint().await;
        let b = endpoint().await;
        let peers = Arc::new(Mutex::new(vec![a.id().to_string()]));
        let handler = PackageHandler::new(peers.clone(), Arc::new(AtomicBool::new(false)));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(PACKAGE_ALPN, handler)
            .spawn();
        for (hash, expected) in [
            ('a', Some(vec![42; CHUNK * 3 + 7])),
            ('b', None),
            ('c', None),
        ] {
            let conn = a.connect(b.addr(), PACKAGE_ALPN).await.unwrap();
            let got = tokio::time::timeout(
                Duration::from_secs(5),
                receive(&conn, &hash.to_string().repeat(64)),
            )
            .await
            .unwrap();
            if let Some(expected) = expected {
                assert_eq!(got.unwrap(), expected);
            } else {
                assert!(got.is_err());
            }
            conn.close(VarInt::from_u32(0), b"done");
        }
        // An existing connection cannot outlive its membership.
        let conn = a.connect(b.addr(), PACKAGE_ALPN).await.unwrap();
        peers.lock().unwrap().clear();
        assert!(
            tokio::time::timeout(Duration::from_secs(5), receive(&conn, &"a".repeat(64)))
                .await
                .unwrap()
                .is_err()
        );
        let outsider = endpoint().await;
        if let Ok(conn) = outsider.connect(b.addr(), PACKAGE_ALPN).await {
            assert!(
                tokio::time::timeout(Duration::from_secs(5), receive(&conn, &"a".repeat(64)))
                    .await
                    .unwrap()
                    .is_err()
            );
        }
        router.shutdown().await.unwrap();
        assert_eq!(outgoing_package_transfers(), 0);
        a.close().await;
        b.close().await;
        outsider.close().await;
    }

    #[tokio::test]
    async fn blocked_transfer_is_cancelled_or_revoked() {
        let peers = Arc::new(Mutex::new(vec!["peer".into()]));
        let handler = PackageHandler::new(peers.clone(), Arc::new(AtomicBool::new(false)));
        let cancel = Arc::new(AtomicBool::new(false));
        for revoke in [false, true] {
            let waiter = handler.guard("peer", Some(&cancel), std::future::pending::<Result<()>>());
            let change = async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                if revoke {
                    peers.lock().unwrap().clear();
                } else {
                    cancel.store(true, Ordering::Release);
                }
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(waiter, change)
            })
            .await
            .unwrap();
            assert!(result.is_err());
            cancel.store(false, Ordering::Release);
        }
    }

    #[derive(Debug)]
    struct InvalidReply {
        advertised: u64,
        bytes: usize,
    }
    impl ProtocolHandler for InvalidReply {
        async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
            let (mut send, mut recv) = conn.accept_bi().await.unwrap();
            let mut request = [0; 64];
            recv.read_exact(&mut request).await.unwrap();
            send.write_all(&[1]).await.unwrap();
            send.write_all(&self.advertised.to_be_bytes())
                .await
                .unwrap();
            send.write_all(&vec![1; self.bytes]).await.unwrap();
            send.finish().unwrap();
            let _ = send.stopped().await;
            Ok(())
        }
    }
    #[tokio::test]
    async fn receive_rejects_truncation_and_oversized_header() {
        let a = endpoint().await;
        for advertised in [10, MAX_PACKAGE_BYTES as u64 + 1] {
            let b = endpoint().await;
            let router = iroh::protocol::Router::builder(b.clone())
                .accept(
                    PACKAGE_ALPN,
                    InvalidReply {
                        advertised,
                        bytes: 3,
                    },
                )
                .spawn();
            let conn = a.connect(b.addr(), PACKAGE_ALPN).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_secs(5), receive(&conn, &"a".repeat(64)))
                    .await
                    .unwrap()
                    .is_err()
            );
            conn.close(VarInt::from_u32(0), b"done");
            router.shutdown().await.unwrap();
            b.close().await;
        }
        a.close().await;
    }
}
