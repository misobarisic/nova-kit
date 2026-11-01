//! A separate, ephemeral control protocol on the existing iroh endpoint.
//! Never put live playback state or source locations into the record store.
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use iroh::endpoint::{Connection, ReadExactError, RecvStream, SendStream};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr, EndpointId};
use nova_playback_api::{
    PeerContext, PlaybackError, PlaybackHandler, PlaybackSnapshot, Request, Response,
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{Semaphore, watch};

// /5 adds receiver-owned queue execution and sleep-timer commands/status.
pub const ALPN: &[u8] = b"nova/playback/5";
const MAX_FRAME: usize = 256 * 1024;
const TIMEOUT: Duration = Duration::from_secs(15);
const MEMBERSHIP_POLL: Duration = Duration::from_millis(100);
pub(crate) fn request_timeout(request: &Request) -> Duration {
    if matches!(
        request,
        Request::Lookup(_)
            | Request::CurrentOffers { .. }
            | Request::QueueStart { .. }
            | Request::Command {
                action: nova_playback_api::PlaybackAction::QueueSkip,
                ..
            }
    ) {
        Duration::from_secs(90)
    } else {
        TIMEOUT
    }
}
const HISTORY_SIZE: usize = 256;
const HISTORY_TTL: Duration = Duration::from_secs(300);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WireRequest {
    epoch: u64,
    id: u64,
    request: Request,
}
#[derive(Serialize, Deserialize)]
struct WireResponse {
    epoch: u64,
    id: u64,
    response: Response,
}

struct Entry {
    request: Request,
    created: Instant,
    response: watch::Sender<Option<Response>>,
}
#[derive(Default)]
struct History {
    epoch: u64,
    highest: u64,
    retired_through: u64,
    entries: HashMap<u64, Entry>,
}

struct ClientConnection {
    connection: tokio::sync::Mutex<Option<Connection>>,
    retired: AtomicBool,
}
impl ClientConnection {
    fn new(connection: Option<Connection>) -> Self {
        Self {
            connection: tokio::sync::Mutex::new(connection),
            retired: AtomicBool::new(false),
        }
    }
}
type ClientSlot = Arc<ClientConnection>;

/// A controller request bound to the connection cache generation captured on
/// the UI thread. Disconnect retires its slot before a worker can begin dialing.
pub struct PreparedPlaybackRequest {
    peer: EndpointId,
    request: Request,
    slot: ClientSlot,
    service_epoch: u64,
}
impl PreparedPlaybackRequest {
    pub(crate) fn timeout(&self) -> Duration {
        request_timeout(&self.request)
    }
}

/// Cancellation (including a timeout or revoked membership) must close the
/// controller connection so the receiver releases a held preview promptly.
struct RequestLease(Arc<AtomicBool>);
impl Drop for RequestLease {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

struct RequestConnection {
    connection: Connection,
    completed: bool,
}
impl Drop for RequestConnection {
    fn drop(&mut self) {
        if !self.completed {
            self.connection
                .close(0u32.into(), b"playback request cancelled");
        }
    }
}

#[derive(Clone)]
pub(crate) struct PlaybackService {
    peers: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    adapter: Arc<Mutex<Option<PlaybackHandler>>>,
    adapter_generation: Arc<AtomicU64>,
    history: Arc<Mutex<HashMap<String, History>>>,
    clients: Arc<Mutex<HashMap<String, ClientSlot>>>,
    inbound: Arc<Mutex<HashMap<u64, (String, Connection)>>>,
    streams: Arc<Semaphore>,
    epoch: u64,
    next_id: Arc<AtomicU64>,
    next_connection_id: Arc<AtomicU64>,
}
impl std::fmt::Debug for PlaybackService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlaybackService").finish_non_exhaustive()
    }
}
impl PlaybackService {
    pub(crate) fn new(peers: Arc<Mutex<Vec<String>>>, stop: Arc<AtomicBool>) -> Result<Self> {
        // Reserve before advertising the service. A clock rollback must not
        // make a restarted controller older than its previous process. The
        // upper half also supersedes legacy wall-clock epochs before 2262.
        let epoch = nova_storage::try_allocate_local_sequence("playback:boot_epoch:v1", 1 << 63)
            .context("reserve durable playback boot epoch")?;
        Ok(Self::with_epoch(peers, stop, epoch))
    }

    fn with_epoch(peers: Arc<Mutex<Vec<String>>>, stop: Arc<AtomicBool>, epoch: u64) -> Self {
        Self {
            peers,
            stop,
            adapter: Arc::new(Mutex::new(None)),
            adapter_generation: Arc::new(AtomicU64::new(0)),
            history: Arc::new(Mutex::new(HashMap::new())),
            clients: Arc::new(Mutex::new(HashMap::new())),
            inbound: Arc::new(Mutex::new(HashMap::new())),
            streams: Arc::new(Semaphore::new(32)),
            epoch,
            next_id: Arc::new(AtomicU64::new(1)),
            next_connection_id: Arc::new(AtomicU64::new(1)),
        }
    }
    pub(crate) fn set_handler(&self, handler: Option<PlaybackHandler>) {
        let mut adapter = self.adapter.lock().unwrap_or_else(|e| e.into_inner());
        let disabled = handler.is_none();
        *adapter = handler;
        self.adapter_generation.fetch_add(1, Ordering::AcqRel);
        // Retire replies as well as queued UI authority: a retry after opting
        // out must not reveal an old title or replay a formerly accepted start.
        // Keep the high-water marks so re-enabling cannot execute old IDs again.
        for history in self
            .history
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values_mut()
        {
            history.retired_through = history.retired_through.max(history.highest);
            history.entries.clear();
        }
        drop(adapter);
        if disabled {
            let connections =
                std::mem::take(&mut *self.inbound.lock().unwrap_or_else(|e| e.into_inner()));
            for (_, (_, connection)) in connections {
                connection.close(0u32.into(), b"playback unavailable");
            }
        }
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
    fn context(&self, peer: String, id: u64, epoch: u64, live: Arc<AtomicBool>) -> PeerContext {
        let service = self.clone();
        let checked_peer = peer.clone();
        let generation = self.adapter_generation.load(Ordering::Acquire);
        PeerContext::new(
            peer,
            id,
            epoch,
            Arc::new(move || {
                live.load(Ordering::Acquire)
                    && generation == service.adapter_generation.load(Ordering::Acquire)
                    && service.authorized(&checked_peer)
                    && (epoch == 0
                        || service
                            .history
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .get(&checked_peer)
                            .is_some_and(|h| h.epoch == epoch))
            }),
        )
    }
    fn cleanup(&self, peer: &str, epoch: u64, connection_id: u64) {
        // Cleanup intentionally reaches the adapter after revocation. It only
        // releases the preview owned by this peer, never changes a new session.
        if let Some(adapter) = self
            .adapter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
        {
            adapter(
                self.context(peer.to_owned(), 0, epoch, Arc::new(AtomicBool::new(true)))
                    .with_connection_id(connection_id),
                Request::ReleasePreview,
                Box::new(|_| {}),
            );
        }
    }
    pub(crate) fn revoke(&self, peer: &str) {
        let revoked = {
            let mut inbound = self.inbound.lock().unwrap_or_else(|e| e.into_inner());
            let ids = inbound
                .iter()
                .filter(|(_, (owner, _))| owner == peer)
                .map(|(id, _)| *id)
                .collect::<Vec<_>>();
            ids.into_iter()
                .filter_map(|id| inbound.remove(&id).map(|(_, connection)| connection))
                .collect::<Vec<_>>()
        };
        for connection in revoked {
            connection.close(crate::protocol::REVOKED_CODE.into(), b"removed peer");
        }

        self.retire(peer);
        self.cleanup(peer, 0, 0);
    }
    /// Retire synchronously before a new controller can acquire the cache slot.
    /// A dial already in progress observes the flag and closes before sending.
    pub(crate) fn retire(&self, peer: &str) {
        let Some(slot) = self
            .clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(peer)
        else {
            return;
        };
        slot.retired.store(true, Ordering::Release);
        if let Ok(mut connection) = slot.connection.try_lock()
            && let Some(connection) = connection.take()
        {
            connection.close(0u32.into(), b"controller detached");
        }
    }

    async fn dispatch_connected(
        &self,
        peer: &str,
        wire: WireRequest,
        connected: Arc<AtomicBool>,
        request_alive: Arc<AtomicBool>,
        connection_id: u64,
    ) -> Response {
        if !self.authorized(peer) {
            return Response::Error(PlaybackError::Unauthorized);
        }
        if wire.id == 0 || wire.epoch == 0 {
            return Response::Error(PlaybackError::InvalidRequest);
        }
        if !wire.request.valid_payload() {
            return Response::Error(PlaybackError::InvalidRequest);
        }
        let mut receiver;
        let mut owner = false;
        let (adapter, generation) = {
            // Bind queued work to the policy that accepted it, before spawning.
            // Take locks in the same order as set_handler so an off/on cycle
            // cannot give an old request the replacement adapter's authority.
            let adapter = self.adapter.lock().unwrap_or_else(|e| e.into_inner());
            let generation = self.adapter_generation.load(Ordering::Acquire);
            let mut histories = self.history.lock().unwrap_or_else(|e| e.into_inner());
            let history = histories.entry(peer.to_owned()).or_default();
            if wire.epoch < history.epoch {
                return Response::Error(PlaybackError::InvalidRequest);
            }
            if wire.epoch > history.epoch {
                *history = History {
                    epoch: wire.epoch,
                    ..History::default()
                };
            }
            let retired = history
                .entries
                .iter()
                .filter(|(_, e)| {
                    e.created.elapsed() >= HISTORY_TTL && e.response.borrow().is_some()
                })
                .map(|(id, _)| *id)
                .max()
                .unwrap_or(0);
            history.retired_through = history.retired_through.max(retired);
            history
                .entries
                .retain(|_, e| e.created.elapsed() < HISTORY_TTL || e.response.borrow().is_none());
            if let Some(entry) = history.entries.get(&wire.id) {
                if entry.request != wire.request {
                    return Response::Error(PlaybackError::InvalidRequest);
                }
                receiver = entry.response.subscribe();
            } else {
                // A reply evicted from the bounded cache must never execute again.
                if wire.id <= history.retired_through {
                    return Response::Error(PlaybackError::InvalidRequest);
                }
                if history.entries.len() >= HISTORY_SIZE {
                    let oldest = history
                        .entries
                        .iter()
                        .filter(|(_, e)| e.response.borrow().is_some())
                        .min_by_key(|(id, _)| **id)
                        .map(|(id, _)| *id);
                    if let Some(id) = oldest {
                        history.entries.remove(&id);
                        history.retired_through = history.retired_through.max(id);
                        if wire.id <= history.retired_through {
                            return Response::Error(PlaybackError::InvalidRequest);
                        }
                    } else {
                        return Response::Error(PlaybackError::Busy);
                    }
                }
                let (tx, rx) = watch::channel(None);
                receiver = rx;
                history.highest = history.highest.max(wire.id);
                history.retired_through = history
                    .retired_through
                    .max(history.highest.saturating_sub(HISTORY_SIZE as u64));
                history.entries.insert(
                    wire.id,
                    Entry {
                        request: wire.request.clone(),
                        created: Instant::now(),
                        response: tx,
                    },
                );
                owner = true;
            }
            (adapter.clone(), generation)
        };
        if owner {
            let service = self.clone();
            let peer = peer.to_owned();
            tokio::spawn(async move {
                let live = Arc::new(AtomicBool::new(true));
                let base = service.context(peer.clone(), wire.id, wire.epoch, live.clone());
                let authorization_service = service.clone();
                let authorization_peer = peer.clone();
                let request_id = wire.id;
                let request_epoch = wire.epoch;
                let context = PeerContext::new(
                    peer.clone(),
                    wire.id,
                    wire.epoch,
                    Arc::new(move || {
                        connected.load(Ordering::Acquire)
                            && request_alive.load(Ordering::Acquire)
                            && base.is_authorized()
                            && generation
                                == authorization_service
                                    .adapter_generation
                                    .load(Ordering::Acquire)
                            && authorization_service
                                .history
                                .lock()
                                .unwrap_or_else(|e| e.into_inner())
                                .get(&authorization_peer)
                                .is_some_and(|history| {
                                    history.epoch == request_epoch
                                        && history.entries.contains_key(&request_id)
                                })
                            && (connection_id == 0
                                || authorization_service
                                    .inbound
                                    .lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .get(&connection_id)
                                    .is_some_and(|(peer, _)| peer == &authorization_peer))
                    }),
                )
                .with_connection_id(connection_id);
                let response = if !context.is_authorized() {
                    Response::Error(PlaybackError::Unauthorized)
                } else if let Some(adapter) = adapter {
                    let timeout = request_timeout(&wire.request);
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    adapter(
                        context,
                        wire.request,
                        Box::new(move |reply| {
                            let _ = tx.send(reply);
                        }),
                    );
                    tokio::select! {
                        response = tokio::time::timeout(timeout, rx) => match response {
                            Ok(Ok(response)) => response,
                            Ok(Err(_)) => Response::Error(PlaybackError::Unavailable),
                            Err(_) => Response::Error(PlaybackError::Timeout),
                        },
                        _ = service.wait_revoked(&peer) => Response::Error(PlaybackError::Unauthorized),
                    }
                } else if matches!(wire.request, Request::Snapshot) {
                    Response::Snapshot(PlaybackSnapshot::default())
                } else {
                    Response::Error(PlaybackError::Unavailable)
                };
                live.store(false, Ordering::Release);
                let mut histories = service.history.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(history) = histories.get_mut(&peer)
                    && history.epoch == wire.epoch
                    && let Some(entry) = history.entries.get_mut(&wire.id)
                {
                    entry.response.send_replace(Some(response));
                }
            });
        }
        loop {
            if !self.authorized(peer) {
                return Response::Error(PlaybackError::Unauthorized);
            }
            if let Some(response) = receiver.borrow().clone() {
                return response;
            }
            tokio::select! {
                changed = receiver.changed() => if changed.is_err() { return Response::Error(PlaybackError::Unavailable); },
                _ = self.wait_revoked(peer) => return Response::Error(PlaybackError::Unauthorized),
            }
        }
    }
    #[cfg(test)]
    async fn dispatch(&self, peer: &str, wire: WireRequest) -> Response {
        self.dispatch_connected(
            peer,
            wire,
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(true)),
            0,
        )
        .await
    }
    async fn wait_revoked(&self, peer: &str) {
        while self.authorized(peer) {
            tokio::time::sleep(MEMBERSHIP_POLL).await;
        }
    }
    async fn serve(
        &self,
        peer: &str,
        mut send: SendStream,
        mut recv: RecvStream,
        connected: Arc<AtomicBool>,
        controller_epoch: Arc<AtomicU64>,
        connection_id: u64,
    ) -> Result<()> {
        let wire: WireRequest = read_frame(&mut recv)
            .await?
            .context("missing playback request")?;
        let epoch = wire.epoch;
        controller_epoch.store(epoch, Ordering::Release);
        let id = wire.id;
        let lease = RequestLease(Arc::new(AtomicBool::new(true)));
        let response = self
            .dispatch_connected(peer, wire, connected, lease.0.clone(), connection_id)
            .await;
        ensure!(self.authorized(peer), "playback peer revoked");
        write_frame(
            &mut send,
            &WireResponse {
                epoch,
                id,
                response,
            },
        )
        .await?;
        send.finish()?;
        Ok(())
    }
    pub(crate) fn prepare(
        &self,
        peer: EndpointId,
        request: Request,
    ) -> Result<PreparedPlaybackRequest> {
        let peer_text = peer.to_string();
        ensure!(self.authorized(&peer_text), "playback peer is not paired");
        let slot = self
            .clients
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(peer_text)
            .or_insert_with(|| Arc::new(ClientConnection::new(None)))
            .clone();
        Ok(PreparedPlaybackRequest {
            peer,
            request,
            slot,
            service_epoch: self.epoch,
        })
    }
    #[cfg(test)]
    pub(crate) async fn request(
        &self,
        endpoint: &Endpoint,
        peer: EndpointId,
        request: Request,
    ) -> Result<Response> {
        self.execute(endpoint, self.prepare(peer, request)?).await
    }
    pub(crate) async fn execute(
        &self,
        endpoint: &Endpoint,
        prepared: PreparedPlaybackRequest,
    ) -> Result<Response> {
        ensure!(
            prepared.service_epoch == self.epoch,
            "playback engine replaced"
        );
        let PreparedPlaybackRequest {
            peer,
            request,
            slot,
            ..
        } = prepared;
        let peer_text = peer.to_string();
        ensure!(self.authorized(&peer_text), "playback peer is not paired");
        ensure!(
            !slot.retired.load(Ordering::Acquire),
            "playback controller detached"
        );
        tokio::select! {
            _ = self.wait_revoked(&peer_text) => bail!("playback peer revoked"),
            result = tokio::time::timeout(request_timeout(&request) + Duration::from_secs(2), async {
                let conn = {
                let mut cached = slot.connection.lock().await;
                ensure!(!slot.retired.load(Ordering::Acquire), "playback controller detached");
                if cached.as_ref().is_some_and(|c| c.close_reason().is_some()) { *cached = None; }
                if cached.is_none() { *cached = Some(endpoint.connect(EndpointAddr::from(peer), crate::configuration::protocol().playback).await.context("connect playback peer")?); }
                if slot.retired.load(Ordering::Acquire) {
                    if let Some(connection) = cached.take() { connection.close(0u32.into(), b"controller detached during dial"); }
                    bail!("playback controller detached");
                }
                cached.as_ref().unwrap().clone()
                };
                let mut guard = RequestConnection { connection: conn.clone(), completed: false };
                let wire = WireRequest { epoch: self.epoch, id: self.next_id.fetch_add(1, Ordering::Relaxed), request };
                let exchange: Result<Response> = async {
                    ensure!(!slot.retired.load(Ordering::Acquire), "playback controller detached");
                    let (mut send, mut recv) = conn.open_bi().await?;
                    write_frame(&mut send, &wire).await?;
                    send.finish()?;
                    let reply: WireResponse = read_frame(&mut recv).await?.context("missing playback reply")?;
                    ensure!(reply.epoch == wire.epoch && reply.id == wire.id, "mismatched playback request ID");
                    ensure!(self.authorized(&peer_text), "playback peer revoked");
                    Ok(reply.response)
                }.await;
                if exchange.is_err() {
                    conn.close(0u32.into(), b"playback request failed");
                    let mut cached = slot.connection.lock().await;
                    if cached.as_ref().is_some_and(|cached| cached.stable_id() == conn.stable_id()) { *cached = None; }
                }
                guard.completed = exchange.is_ok();
                exchange
            }) => result.context("playback request timed out")?,
        }
    }
}
impl ProtocolHandler for PlaybackService {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let peer = conn.remote_id().to_string();
        if !self.authorized(&peer) {
            conn.close(crate::protocol::REVOKED_CODE.into(), b"not paired");
            return Ok(());
        }
        let connected = Arc::new(AtomicBool::new(true));
        let controller_epoch = Arc::new(AtomicU64::new(0));
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        {
            let mut inbound = self.inbound.lock().unwrap_or_else(|e| e.into_inner());
            if inbound.len() >= 64 {
                conn.close(0u32.into(), b"too many playback connections");
                return Ok(());
            }
            inbound.insert(connection_id, (peer.clone(), conn.clone()));
        }
        loop {
            let streams = tokio::select! {
                _ = self.wait_revoked(&peer) => { conn.close(crate::protocol::REVOKED_CODE.into(), b"removed peer"); break; },
                result = conn.accept_bi() => match result { Ok(streams) => streams, Err(_) => break },
            };
            let Ok(permit) = self.streams.clone().try_acquire_owned() else {
                conn.close(0u32.into(), b"too many playback requests");
                break;
            };
            let service = self.clone();
            let peer = peer.clone();
            let connected = connected.clone();
            let controller_epoch = controller_epoch.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = tokio::time::timeout(
                    Duration::from_secs(91),
                    service.serve(
                        &peer,
                        streams.0,
                        streams.1,
                        connected,
                        controller_epoch,
                        connection_id,
                    ),
                )
                .await;
            });
        }
        connected.store(false, Ordering::Release);
        self.inbound
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&connection_id);
        // A connection without a request owns no preview; never emit wildcard
        // cleanup for a stale handshake or malformed empty connection.
        let epoch = controller_epoch.load(Ordering::Acquire);
        if epoch != 0 {
            self.cleanup(&peer, epoch, connection_id);
        }
        Ok(())
    }
}

fn valid_length(n: usize) -> Result<()> {
    ensure!(n > 0 && n <= MAX_FRAME, "bad playback frame length {n}");
    Ok(())
}
async fn write_frame<T: Serialize>(send: &mut SendStream, message: &T) -> Result<()> {
    let bytes = postcard::to_allocvec(message)?;
    valid_length(bytes.len())?;
    send.write_all(&(bytes.len() as u32).to_be_bytes()).await?;
    send.write_all(&bytes).await?;
    Ok(())
}
async fn read_frame<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<Option<T>> {
    let mut header = [0; 4];
    match recv.read_exact(&mut header).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let length = u32::from_be_bytes(header) as usize;
    valid_length(length)?;
    let mut bytes = vec![0; length];
    recv.read_exact(&mut bytes).await?;
    let (message, remaining) = postcard::take_from_bytes(&bytes)?;
    ensure!(remaining.is_empty(), "trailing playback frame bytes");
    Ok(Some(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn a_full_bounded_offer_page_fits_the_frame_budget() {
        let offers = (0..256)
            .map(|i| nova_playback_api::PlaybackOffer {
                id: format!("{}:{}", u64::MAX, i),
                name: "n".repeat(128),
                description: "d".repeat(512),
                size: "s".repeat(32),
            })
            .collect();
        let response = WireResponse {
            epoch: u64::MAX,
            id: u64::MAX,
            response: Response::Offers(offers),
        };
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert!(bytes.len() <= MAX_FRAME);
        valid_length(bytes.len()).unwrap();
        let decoded: WireResponse = postcard::from_bytes(&bytes).unwrap();
        let Response::Offers(offers) = decoded.response else {
            panic!("expected offer page");
        };
        assert_eq!(offers.len(), 256);
        assert!(offers.iter().all(|offer| offer.size == "s".repeat(32)));
    }

    #[test]
    fn expanded_snapshot_is_bounded_and_semantic_commands_roundtrip() {
        use nova_playback_api::*;
        let track = PlaybackTrack {
            id: i32::MAX,
            token: "t".repeat(MAX_REMOTE_ID_BYTES),
            label: "l".repeat(MAX_REMOTE_LABEL_BYTES),
        };
        let snapshot = PlaybackSnapshot {
            title: "t".repeat(512),
            episode: "e".repeat(512),
            duration: 100.0,
            audio_tracks: vec![track.clone(); MAX_REMOTE_TRACKS],
            subtitle_tracks: vec![track.clone(); MAX_REMOTE_TRACKS],
            secondary_subtitle_tracks: vec![track; MAX_REMOTE_TRACKS],
            chapters: vec![
                PlaybackChapter {
                    id: "c".repeat(MAX_REMOTE_ID_BYTES),
                    label: "l".repeat(MAX_REMOTE_LABEL_BYTES),
                    time: 50.0
                };
                MAX_REMOTE_CHAPTERS
            ],
            bookmarks: vec![
                PlaybackBookmark {
                    id: "b".repeat(MAX_REMOTE_ID_BYTES),
                    note: "n".repeat(MAX_REMOTE_NOTE_BYTES),
                    time: 50.0
                };
                MAX_REMOTE_BOOKMARKS
            ],
            bookmark_notice: "n".repeat(512),
            next_episode_token: "n".repeat(4096),
            next_episode_media: Some(MediaIdentity {
                media_type: "series".into(),
                title_id: "i".repeat(1024),
                episode_id: "i".repeat(1024),
                title: "t".repeat(512),
                episode: "e".repeat(512),
            }),
            ..Default::default()
        };
        assert!(snapshot.valid_payload());
        let response = WireResponse {
            epoch: u64::MAX,
            id: u64::MAX,
            response: Response::Snapshot(snapshot.clone()),
        };
        let bytes = postcard::to_allocvec(&response).unwrap();
        assert!(bytes.len() <= MAX_FRAME, "{} bytes", bytes.len());
        let decoded: WireResponse = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.response, Response::Snapshot(snapshot.clone()));
        let mut oversized = snapshot;
        oversized.bookmarks.push(PlaybackBookmark::default());
        assert!(!oversized.valid_payload());
        let session = SessionId {
            epoch: 42,
            sequence: 9,
        };
        for action in [
            PlaybackAction::SeekChapter { id: "c123".into() },
            PlaybackAction::SetSubtitleStyle {
                control: 0,
                value: -1.7,
            },
            PlaybackAction::SetSubtitleStyle {
                control: 7,
                value: 45.0,
            },
            PlaybackAction::SelectSecondarySubtitleTrackToken("t123".into()),
            PlaybackAction::EditBookmark {
                id: "b123".into(),
                note: "A scene".into(),
            },
            PlaybackAction::SkipSegment {
                token: "s123".into(),
            },
            PlaybackAction::CancelSubtitleDownload { secondary: true },
        ] {
            assert!(action.valid_payload());
            let request = wire(7, Request::Command { session, action });
            let decoded: WireRequest =
                postcard::from_bytes(&postcard::to_allocvec(&request).unwrap()).unwrap();
            assert_eq!(decoded.request, request.request);
        }
        assert!(
            !PlaybackAction::SetSubtitleStyle {
                control: 8,
                value: 1.0
            }
            .valid_payload()
        );
        assert!(
            !PlaybackAction::SetSubtitleStyle {
                control: 0,
                value: f64::NAN
            }
            .valid_payload()
        );
        for value in [-1.0, 101.0, f64::NAN, f64::INFINITY] {
            assert!(!PlaybackAction::SetSubtitleStyle { control: 7, value }.valid_payload());
        }
        assert!(
            !PlaybackAction::AddBookmark {
                note: "x".repeat(281)
            }
            .valid_payload()
        );
        assert!(!PlaybackAction::SetSpeed(3.0).valid_payload());
        assert_eq!(ALPN, b"nova/playback/5");
        assert_eq!(
            request_timeout(&Request::CurrentOffers { session }),
            Duration::from_secs(90)
        );
    }

    fn test_service(peers: Arc<Mutex<Vec<String>>>, stop: Arc<AtomicBool>) -> PlaybackService {
        static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1 << 63);
        PlaybackService::with_epoch(peers, stop, NEXT_EPOCH.fetch_add(1, Ordering::Relaxed))
    }

    fn service() -> PlaybackService {
        test_service(
            Arc::new(Mutex::new(vec!["paired".into()])),
            Arc::new(AtomicBool::new(false)),
        )
    }
    fn wire(id: u64, request: Request) -> WireRequest {
        WireRequest {
            epoch: 42,
            id,
            request,
        }
    }
    #[test]
    fn playback_frames_have_a_small_strict_bound() {
        assert!(valid_length(0).is_err());
        assert!(valid_length(MAX_FRAME).is_ok());
        assert!(valid_length(MAX_FRAME + 1).is_err());
        let wire = wire(1, Request::Snapshot);
        let bytes = postcard::to_allocvec(&wire).unwrap();
        let decoded: WireRequest = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.request, Request::Snapshot);
    }
    #[tokio::test]
    async fn headless_readiness_and_membership_are_explicit() {
        let service = service();
        assert_eq!(
            service
                .dispatch("stranger", wire(1, Request::Snapshot))
                .await,
            Response::Error(PlaybackError::Unauthorized)
        );
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Snapshot(PlaybackSnapshot::default())
        );
        assert_eq!(
            service
                .dispatch("paired", wire(2, Request::ReleasePreview))
                .await,
            Response::Error(PlaybackError::Unavailable)
        );
    }
    #[tokio::test]
    async fn duplicate_ids_are_bound_to_authenticated_peer_and_content() {
        let service = service();
        let executions = Arc::new(AtomicUsize::new(0));
        let count = executions.clone();
        service.set_handler(Some(Arc::new(move |_, _, completion| {
            count.fetch_add(1, Ordering::Relaxed);
            completion(Response::Accepted);
        })));
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Accepted
        );
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Accepted
        );
        assert_eq!(executions.load(Ordering::Relaxed), 1);
        assert_eq!(
            service
                .dispatch("paired", wire(1, Request::ReleasePreview))
                .await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(
            service
                .dispatch("stranger", wire(1, Request::Snapshot))
                .await,
            Response::Error(PlaybackError::Unauthorized)
        );
        service
            .history
            .lock()
            .unwrap()
            .get_mut("paired")
            .unwrap()
            .entries
            .clear();
        service
            .history
            .lock()
            .unwrap()
            .get_mut("paired")
            .unwrap()
            .retired_through = 1;
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(executions.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn queue_start_replay_executes_once_and_reused_id_cannot_retarget() {
        let service = service();
        let starts = Arc::new(AtomicUsize::new(0));
        let executions = starts.clone();
        service.set_handler(Some(Arc::new(move |context, request, done| {
            assert!(context.is_authorized());
            assert!(matches!(request, Request::QueueStart { .. }));
            executions.fetch_add(1, Ordering::Relaxed);
            done(Response::Accepted);
        })));
        let request = Request::QueueStart {
            profile_id: "default".into(),
            item_key: Some(r#"["series","show","one"]"#.into()),
            expected_session: None,
        };
        assert_eq!(request_timeout(&request), Duration::from_secs(90));
        assert_eq!(
            service.dispatch("paired", wire(1, request.clone())).await,
            Response::Accepted
        );
        assert_eq!(
            service.dispatch("paired", wire(1, request)).await,
            Response::Accepted
        );
        assert_eq!(starts.load(Ordering::Relaxed), 1);
        let changed = Request::QueueStart {
            profile_id: "other".into(),
            item_key: None,
            expected_session: None,
        };
        assert_eq!(
            service.dispatch("paired", wire(1, changed)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(starts.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn pending_queue_stop_roundtrips_without_native_session_and_is_bounded() {
        let request = Request::QueueStop {
            profile_id: "default".into(),
            run_id: 4,
            expected_session: None,
        };
        assert!(request.valid_payload());
        assert_eq!(request_timeout(&request), TIMEOUT);
        let wire = wire(7, request.clone());
        let decoded: WireRequest =
            postcard::from_bytes(&postcard::to_allocvec(&wire).unwrap()).unwrap();
        assert_eq!(decoded.request, request);
        assert!(
            !Request::QueueStop {
                profile_id: "default".into(),
                run_id: 0,
                expected_session: None
            }
            .valid_payload()
        );
        assert!(
            !Request::QueueStop {
                profile_id: "\n".into(),
                run_id: 4,
                expected_session: None
            }
            .valid_payload()
        );
    }
    #[tokio::test]
    async fn a_live_quic_connection_is_revoked_and_releases_preview() {
        let a = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let b = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let peers = Arc::new(Mutex::new(vec![a.id().to_string()]));
        let service = test_service(peers.clone(), Arc::new(AtomicBool::new(false)));
        let releases = Arc::new(AtomicUsize::new(0));
        let released = releases.clone();
        service.set_handler(Some(Arc::new(move |context, request, done| {
            if matches!(request, Request::ReleasePreview) {
                released.fetch_add(1, Ordering::Relaxed);
                done(Response::Accepted);
            } else {
                assert!(context.is_authorized());
                done(Response::Snapshot(PlaybackSnapshot {
                    ready: true,
                    ..PlaybackSnapshot::default()
                }));
            }
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, service)
            .spawn();
        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        let (mut send, mut recv) = conn.open_bi().await.unwrap();
        write_frame(&mut send, &wire(1, Request::Snapshot))
            .await
            .unwrap();
        send.finish().unwrap();
        let reply: WireResponse = read_frame(&mut recv).await.unwrap().unwrap();
        assert!(matches!(
            reply.response,
            Response::Snapshot(PlaybackSnapshot { ready: true, .. })
        ));
        peers.lock().unwrap().clear();
        tokio::time::timeout(Duration::from_secs(2), conn.closed())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while releases.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn reordered_new_ids_execute_once_and_retired_ids_never_execute_again() {
        let service = service();
        let executions = Arc::new(AtomicUsize::new(0));
        let count = executions.clone();
        service.set_handler(Some(Arc::new(move |_, _, done| {
            count.fetch_add(1, Ordering::Relaxed);
            done(Response::Accepted);
        })));
        for id in [3, 1, 2, 3, 2, 1] {
            assert_eq!(
                service
                    .dispatch("paired", wire(id, Request::Snapshot))
                    .await,
                Response::Accepted
            );
        }
        assert_eq!(executions.load(Ordering::Relaxed), 3);
        service
            .history
            .lock()
            .unwrap()
            .get_mut("paired")
            .unwrap()
            .entries
            .get_mut(&2)
            .unwrap()
            .created = Instant::now() - HISTORY_TTL;
        assert_eq!(
            service.dispatch("paired", wire(2, Request::Snapshot)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(executions.load(Ordering::Relaxed), 3);
    }

    async fn endpoint() -> Endpoint {
        Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    }

    async fn rpc(connection: &Connection, request: &WireRequest) -> Result<Response> {
        tokio::time::timeout(Duration::from_secs(3), async {
            let (mut send, mut recv) = connection.open_bi().await?;
            write_frame(&mut send, request).await?;
            send.finish()?;
            let response: WireResponse =
                read_frame(&mut recv).await?.context("missing response")?;
            ensure!(
                response.id == request.id && response.epoch == request.epoch,
                "wrong response ID"
            );
            Ok(response.response)
        })
        .await
        .context("test RPC timeout")?
    }

    #[tokio::test]
    async fn explicit_revoke_and_immediate_readd_cannot_restore_queued_authority() {
        let a = endpoint().await;
        let b = endpoint().await;
        let peer_id = a.id().to_string();
        let peers = Arc::new(Mutex::new(vec![peer_id.clone()]));
        let service = test_service(peers.clone(), Arc::new(AtomicBool::new(false)));
        let pending = Arc::new(Mutex::new(None));
        let handler_pending = pending.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = started.clone();
        service.set_handler(Some(Arc::new(move |context, request, done| {
            if matches!(request, Request::Snapshot) {
                *handler_pending.lock().unwrap() = Some((context, done));
                handler_started.notify_one();
            } else {
                done(Response::Accepted);
            }
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, service.clone())
            .spawn();
        let connection = a.connect(b.addr(), ALPN).await.unwrap();
        let query_connection = connection.clone();
        let query =
            tokio::spawn(async move { rpc(&query_connection, &wire(1, Request::Snapshot)).await });
        tokio::time::timeout(Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        assert!(pending.lock().unwrap().as_ref().unwrap().0.is_authorized());
        peers.lock().unwrap().clear();
        service.revoke(&peer_id);
        peers.lock().unwrap().push(peer_id);
        assert!(!pending.lock().unwrap().as_ref().unwrap().0.is_authorized());
        tokio::time::timeout(Duration::from_secs(3), connection.closed())
            .await
            .unwrap();
        let (_, completion) = pending.lock().unwrap().take().unwrap();
        completion(Response::Accepted);
        assert!(query.await.unwrap().is_err());
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn slow_lookup_does_not_block_commands_on_the_cached_connection() {
        use nova_playback_api::{MediaIdentity, PlaybackAction, SessionId};
        let a = endpoint().await;
        let b = endpoint().await;
        let receiver = test_service(
            Arc::new(Mutex::new(vec![a.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let pending: Arc<Mutex<Option<nova_playback_api::Completion>>> = Arc::new(Mutex::new(None));
        let lookup_started = Arc::new(tokio::sync::Notify::new());
        let handler_pending = pending.clone();
        let started = lookup_started.clone();
        receiver.set_handler(Some(Arc::new(move |_, request, done| {
            if matches!(request, Request::Lookup(_)) {
                *handler_pending.lock().unwrap() = Some(done);
                started.notify_one();
            } else {
                done(Response::Accepted);
            }
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, receiver)
            .spawn();
        let client = test_service(
            Arc::new(Mutex::new(vec![b.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let connection = a.connect(b.addr(), ALPN).await.unwrap();
        client.clients.lock().unwrap().insert(
            b.id().to_string(),
            Arc::new(ClientConnection::new(Some(connection))),
        );
        let lookup_client = client.clone();
        let lookup_endpoint = a.clone();
        let receiver_id = b.id();
        let lookup = tokio::spawn(async move {
            lookup_client
                .request(
                    &lookup_endpoint,
                    receiver_id,
                    Request::Lookup(MediaIdentity::default()),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), lookup_started.notified())
            .await
            .unwrap();
        let response = tokio::time::timeout(
            Duration::from_secs(3),
            client.request(
                &a,
                b.id(),
                Request::Command {
                    session: SessionId {
                        epoch: 99,
                        sequence: 1,
                    },
                    action: PlaybackAction::Stop,
                },
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response, Response::Accepted);
        let completion = pending.lock().unwrap().take().unwrap();
        completion(Response::Offers(Vec::new()));
        assert_eq!(lookup.await.unwrap().unwrap(), Response::Offers(Vec::new()));
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn cleanup_is_bound_to_the_connection_that_owned_the_preview() {
        use nova_playback_api::{PlaybackAction, SessionId};
        let a = endpoint().await;
        let b = endpoint().await;
        let receiver = test_service(
            Arc::new(Mutex::new(vec![a.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let owner = Arc::new(Mutex::new(None));
        let handler_owner = owner.clone();
        let cleanup = Arc::new(tokio::sync::Notify::new());
        let cleanup_notify = cleanup.clone();
        receiver.set_handler(Some(Arc::new(move |context, request, done| {
            match request {
                Request::Command {
                    action: PlaybackAction::BeginSpeedPreview,
                    ..
                } => {
                    *handler_owner.lock().unwrap() = Some(context.connection_id);
                }
                Request::ReleasePreview => {
                    let mut owner = handler_owner.lock().unwrap();
                    if *owner == Some(context.connection_id) {
                        *owner = None;
                    }
                    cleanup_notify.notify_one();
                }
                _ => {}
            }
            done(Response::Accepted);
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, receiver)
            .spawn();
        let old = a.connect(b.addr(), ALPN).await.unwrap();
        let preview = |id| {
            wire(
                id,
                Request::Command {
                    session: SessionId {
                        epoch: 99,
                        sequence: 1,
                    },
                    action: PlaybackAction::BeginSpeedPreview,
                },
            )
        };
        assert_eq!(rpc(&old, &preview(1)).await.unwrap(), Response::Accepted);
        let old_owner = owner.lock().unwrap().unwrap();
        let new = a.connect(b.addr(), ALPN).await.unwrap();
        assert_eq!(rpc(&new, &preview(2)).await.unwrap(), Response::Accepted);
        let new_owner = owner.lock().unwrap().unwrap();
        assert_ne!(old_owner, new_owner);
        old.close(0u32.into(), b"old controller connection");
        tokio::time::timeout(Duration::from_secs(3), cleanup.notified())
            .await
            .unwrap();
        assert_eq!(*owner.lock().unwrap(), Some(new_owner));
        new.close(0u32.into(), b"new controller connection");
        tokio::time::timeout(Duration::from_secs(3), cleanup.notified())
            .await
            .unwrap();
        assert_eq!(*owner.lock().unwrap(), None);
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn retiring_a_controller_does_not_release_the_reopened_preview() {
        use nova_playback_api::{PlaybackAction, SessionId};
        let a = endpoint().await;
        let b = endpoint().await;
        let receiver = test_service(
            Arc::new(Mutex::new(vec![a.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let owner = Arc::new(Mutex::new(None));
        let handler_owner = owner.clone();
        let cleanup_ids = Arc::new(Mutex::new(Vec::new()));
        let recorded_cleanup = cleanup_ids.clone();
        let cleanup = Arc::new(tokio::sync::Notify::new());
        let cleanup_notify = cleanup.clone();
        receiver.set_handler(Some(Arc::new(move |context, request, done| {
            match request {
                Request::Command {
                    action: PlaybackAction::BeginSpeedPreview,
                    ..
                } => {
                    *handler_owner.lock().unwrap() = Some(context.connection_id);
                }
                Request::ReleasePreview => {
                    // Delay applying cleanup like a queued UI event; new controls
                    // can acquire their own lease before this callback runs.
                    recorded_cleanup.lock().unwrap().push(context.connection_id);
                    cleanup_notify.notify_one();
                }
                _ => {}
            }
            done(Response::Accepted);
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, receiver)
            .spawn();
        let client = test_service(
            Arc::new(Mutex::new(vec![b.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let first = a.connect(b.addr(), ALPN).await.unwrap();
        let first_slot = Arc::new(ClientConnection::new(Some(first)));
        client
            .clients
            .lock()
            .unwrap()
            .insert(b.id().to_string(), first_slot.clone());
        let preview = || Request::Command {
            session: SessionId {
                epoch: 99,
                sequence: 1,
            },
            action: PlaybackAction::BeginSpeedPreview,
        };
        assert_eq!(
            client.request(&a, b.id(), preview()).await.unwrap(),
            Response::Accepted
        );
        let old_owner = owner.lock().unwrap().unwrap();
        let stale_prepared = client.prepare(b.id(), preview()).unwrap();
        client.retire(&b.id().to_string());
        assert!(first_slot.retired.load(Ordering::Acquire));
        assert!(
            !client
                .clients
                .lock()
                .unwrap()
                .contains_key(&b.id().to_string())
        );
        let second = a.connect(b.addr(), ALPN).await.unwrap();
        client.clients.lock().unwrap().insert(
            b.id().to_string(),
            Arc::new(ClientConnection::new(Some(second))),
        );
        assert_eq!(
            client.request(&a, b.id(), preview()).await.unwrap(),
            Response::Accepted
        );
        let new_owner = owner.lock().unwrap().unwrap();
        assert_ne!(old_owner, new_owner);
        assert!(
            client
                .execute(&a, stale_prepared)
                .await
                .unwrap_err()
                .to_string()
                .contains("detached")
        );
        assert_eq!(*owner.lock().unwrap(), Some(new_owner));
        tokio::time::timeout(Duration::from_secs(3), cleanup.notified())
            .await
            .unwrap();
        let old_cleanup = cleanup_ids.lock().unwrap().remove(0);
        assert_eq!(old_cleanup, old_owner);
        if *owner.lock().unwrap() == Some(old_cleanup) {
            *owner.lock().unwrap() = None;
        }
        assert_eq!(*owner.lock().unwrap(), Some(new_owner));
        client.retire(&b.id().to_string());
        tokio::time::timeout(Duration::from_secs(3), cleanup.notified())
            .await
            .unwrap();
        assert_eq!(cleanup_ids.lock().unwrap().remove(0), new_owner);
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn a_retired_slot_cannot_resume_after_waiting_for_a_dial_lock() {
        let a = endpoint().await;
        let b = endpoint().await;
        let client = test_service(
            Arc::new(Mutex::new(vec![b.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let slot = Arc::new(ClientConnection::new(None));
        let dial_lock = slot.connection.lock().await;
        client
            .clients
            .lock()
            .unwrap()
            .insert(b.id().to_string(), slot.clone());
        let requester = client.clone();
        let request_endpoint = a.clone();
        let receiver_id = b.id();
        let request = tokio::spawn(async move {
            requester
                .request(&request_endpoint, receiver_id, Request::Snapshot)
                .await
        });
        while Arc::strong_count(&slot) < 3 {
            tokio::task::yield_now().await;
        }
        client.retire(&b.id().to_string());
        assert!(slot.retired.load(Ordering::Acquire));
        drop(dial_lock);
        let result = tokio::time::timeout(Duration::from_secs(1), request)
            .await
            .unwrap()
            .unwrap();
        assert!(result.unwrap_err().to_string().contains("detached"));
        assert!(slot.connection.lock().await.is_none());
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn reconnect_deduplicates_side_effects_and_controllers_have_separate_ids() {
        use nova_playback_api::{PlaybackAction, SessionId};
        let a = endpoint().await;
        let b = endpoint().await;
        let c = endpoint().await;
        let outsider = endpoint().await;
        let service = test_service(
            Arc::new(Mutex::new(vec![a.id().to_string(), c.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let seeks = Arc::new(AtomicUsize::new(0));
        let screenshots = Arc::new(AtomicUsize::new(0));
        let seek_count = seeks.clone();
        let screenshot_count = screenshots.clone();
        service.set_handler(Some(Arc::new(move |context, request, done| {
            if let Request::Command { action, .. } = request {
                assert!(context.is_authorized());
                match action {
                    PlaybackAction::SeekRelative(_) => {
                        seek_count.fetch_add(1, Ordering::Relaxed);
                    }
                    PlaybackAction::Screenshot => {
                        screenshot_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
            done(Response::Accepted);
        })));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, service)
            .spawn();
        let seek = wire(
            1,
            Request::Command {
                session: SessionId {
                    epoch: 99,
                    sequence: 1,
                },
                action: PlaybackAction::SeekRelative(10.0),
            },
        );
        let screenshot = wire(
            2,
            Request::Command {
                session: SessionId {
                    epoch: 99,
                    sequence: 1,
                },
                action: PlaybackAction::Screenshot,
            },
        );
        for controller in [&a, &c] {
            let conn = controller.connect(b.addr(), ALPN).await.unwrap();
            assert_eq!(rpc(&conn, &seek).await.unwrap(), Response::Accepted);
            assert_eq!(rpc(&conn, &screenshot).await.unwrap(), Response::Accepted);
            assert_eq!(rpc(&conn, &screenshot).await.unwrap(), Response::Accepted);
            conn.close(0u32.into(), b"reconnect");
            let conn = controller.connect(b.addr(), ALPN).await.unwrap();
            assert_eq!(rpc(&conn, &seek).await.unwrap(), Response::Accepted);
            assert_eq!(rpc(&conn, &screenshot).await.unwrap(), Response::Accepted);
            conn.close(0u32.into(), b"done");
        }
        assert_eq!(seeks.load(Ordering::Relaxed), 2);
        assert_eq!(screenshots.load(Ordering::Relaxed), 2);
        if let Ok(conn) = outsider.connect(b.addr(), ALPN).await {
            assert!(rpc(&conn, &seek).await.is_err());
        }
        assert_eq!(seeks.load(Ordering::Relaxed), 2);
        router.shutdown().await.unwrap();
        a.close().await;
        b.close().await;
        c.close().await;
        outsider.close().await;
    }

    #[tokio::test]
    async fn durable_boot_namespace_reconnects_and_rejects_prior_process_work() {
        let controller = endpoint().await;
        let receiver = endpoint().await;
        let service = test_service(
            Arc::new(Mutex::new(vec![controller.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let queued = Arc::new(Mutex::new(None));
        let pending = queued.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let notify = started.clone();
        let executions = Arc::new(AtomicUsize::new(0));
        let count = executions.clone();
        service.set_handler(Some(Arc::new(move |context, request, done| {
            if matches!(request, Request::Snapshot) {
                *pending.lock().unwrap() = Some((context, done));
                notify.notify_one();
            } else {
                count.fetch_add(1, Ordering::Relaxed);
                done(Response::Accepted);
            }
        })));
        let router = iroh::protocol::Router::builder(receiver.clone())
            .accept(ALPN, service)
            .spawn();
        let connection = controller.connect(receiver.addr(), ALPN).await.unwrap();
        // A pre-upgrade process used a wall clock which was later corrected
        // backwards. The first durable epoch supersedes it without consulting
        // that corrected clock; later boots advance from the persisted value.
        let legacy_epoch = 2_000_000_000_000_000_000;
        let old_connection = connection.clone();
        let old_query = tokio::spawn(async move {
            rpc(
                &old_connection,
                &WireRequest {
                    epoch: legacy_epoch,
                    id: 1,
                    request: Request::Snapshot,
                },
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        assert!(queued.lock().unwrap().as_ref().unwrap().0.is_authorized());

        let restarted = controller.connect(receiver.addr(), ALPN).await.unwrap();
        for epoch in [1 << 63, (1 << 63) + 1] {
            let request = WireRequest {
                epoch,
                id: 1,
                request: Request::ReleasePreview,
            };
            assert_eq!(rpc(&restarted, &request).await.unwrap(), Response::Accepted);
            assert_eq!(rpc(&restarted, &request).await.unwrap(), Response::Accepted);
        }
        assert_eq!(
            executions.load(Ordering::Relaxed),
            2,
            "dedup remains per boot"
        );
        let (context, done) = queued.lock().unwrap().take().unwrap();
        assert!(
            !context.is_authorized(),
            "old queued UI work loses authority"
        );
        done(Response::Accepted);
        let _ = old_query.await.unwrap();
        for epoch in [legacy_epoch, 1 << 63] {
            assert_eq!(
                rpc(
                    &connection,
                    &WireRequest {
                        epoch,
                        id: 99,
                        request: Request::ReleasePreview,
                    }
                )
                .await
                .unwrap(),
                Response::Error(PlaybackError::InvalidRequest)
            );
        }
        assert_eq!(
            executions.load(Ordering::Relaxed),
            2,
            "stale processes never execute"
        );
        router.shutdown().await.unwrap();
        controller.close().await;
        receiver.close().await;
    }

    #[tokio::test]
    async fn boot_epochs_and_adapter_replacement_invalidate_old_contexts() {
        let service = service();
        service.history.lock().unwrap().insert(
            "paired".into(),
            History {
                epoch: 42,
                ..History::default()
            },
        );
        let context = service.context("paired".into(), 1, 42, Arc::new(AtomicBool::new(true)));
        assert!(context.is_authorized());
        service.set_handler(None);
        assert!(!context.is_authorized());
        let context = service.context("paired".into(), 2, 42, Arc::new(AtomicBool::new(true)));
        assert!(context.is_authorized());
        assert_eq!(
            service
                .dispatch(
                    "paired",
                    WireRequest {
                        epoch: 43,
                        id: 1,
                        request: Request::Snapshot
                    }
                )
                .await,
            Response::Snapshot(PlaybackSnapshot::default())
        );
        assert!(!context.is_authorized());
        assert_eq!(
            service.dispatch("paired", wire(3, Request::Snapshot)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
    }

    #[tokio::test]
    async fn adapter_replacement_cannot_authorize_an_unstarted_dispatch() {
        let service = service();
        let executions = Arc::new(AtomicUsize::new(0));
        let count = executions.clone();
        let adapter: PlaybackHandler = Arc::new(move |_, _, done| {
            count.fetch_add(1, Ordering::Relaxed);
            done(Response::Accepted);
        });
        service.set_handler(Some(adapter.clone()));
        let pending = service.dispatch("paired", wire(1, Request::ReleasePreview));
        tokio::pin!(pending);
        // Poll acceptance without yielding to its spawned owner task. The same
        // race occurs for a connection admitted during the disabled interval.
        std::future::poll_fn(|cx| {
            assert!(std::future::Future::poll(pending.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        service.set_handler(None);
        service.set_handler(Some(adapter));
        assert_eq!(pending.await, Response::Error(PlaybackError::Unavailable));
        tokio::task::yield_now().await;
        assert_eq!(executions.load(Ordering::Relaxed), 0);
        assert_eq!(
            service
                .dispatch("paired", wire(1, Request::ReleasePreview))
                .await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(
            service
                .dispatch("paired", wire(2, Request::ReleasePreview))
                .await,
            Response::Accepted
        );
        assert_eq!(executions.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn disabling_closes_live_incoming_connections_and_revokes_queued_work() {
        let controller = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let receiver = Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let service = test_service(
            Arc::new(Mutex::new(vec![controller.id().to_string()])),
            Arc::new(AtomicBool::new(false)),
        );
        let queued = Arc::new(Mutex::new(None));
        let captured = queued.clone();
        let started = Arc::new(tokio::sync::Notify::new());
        let notified = started.clone();
        service.set_handler(Some(Arc::new(move |context, _, done| {
            *captured.lock().unwrap() = Some((context, done));
            notified.notify_one();
        })));
        let router = iroh::protocol::Router::builder(receiver.clone())
            .accept(ALPN, service.clone())
            .spawn();
        let connection = controller.connect(receiver.addr(), ALPN).await.unwrap();
        let (mut send, _recv) = connection.open_bi().await.unwrap();
        write_frame(&mut send, &wire(1, Request::Snapshot))
            .await
            .unwrap();
        send.finish().unwrap();
        tokio::time::timeout(Duration::from_secs(3), started.notified())
            .await
            .unwrap();
        service.set_handler(None);
        tokio::time::timeout(Duration::from_secs(3), connection.closed())
            .await
            .unwrap();
        let (context, done) = queued.lock().unwrap().take().unwrap();
        assert!(!context.is_authorized());
        assert!(service.inbound.lock().unwrap().is_empty());
        assert!(
            service
                .peers
                .lock()
                .unwrap()
                .contains(&controller.id().to_string()),
            "playback opt-out does not unpair sync"
        );
        done(Response::Accepted);
        router.shutdown().await.unwrap();
        controller.close().await;
        receiver.close().await;
    }

    #[tokio::test]
    async fn disabling_retires_cached_private_replies_without_allowing_replay() {
        let service = service();
        service.set_handler(Some(Arc::new(|_, _, done| {
            done(Response::Snapshot(PlaybackSnapshot {
                ready: true,
                title: "Private title".into(),
                ..Default::default()
            }))
        })));
        assert!(matches!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Snapshot(PlaybackSnapshot { ready: true, .. })
        ));
        service.set_handler(None);
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(
            service.dispatch("paired", wire(2, Request::Snapshot)).await,
            Response::Snapshot(PlaybackSnapshot::default())
        );
        let executions = Arc::new(AtomicUsize::new(0));
        let count = executions.clone();
        service.set_handler(Some(Arc::new(move |_, _, done| {
            count.fetch_add(1, Ordering::Relaxed);
            done(Response::Accepted);
        })));
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Error(PlaybackError::InvalidRequest)
        );
        assert_eq!(
            service.dispatch("paired", wire(3, Request::Snapshot)).await,
            Response::Accepted
        );
        assert_eq!(executions.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn revocation_invalidates_queued_ui_work() {
        let service = service();
        service.history.lock().unwrap().insert(
            "paired".into(),
            History {
                epoch: 42,
                ..History::default()
            },
        );
        let context = service.context("paired".into(), 1, 42, Arc::new(AtomicBool::new(true)));
        assert!(context.is_authorized());
        service.peers.lock().unwrap().clear();
        assert!(!context.is_authorized());
        assert_eq!(
            service.dispatch("paired", wire(1, Request::Snapshot)).await,
            Response::Error(PlaybackError::Unauthorized)
        );
    }
}
