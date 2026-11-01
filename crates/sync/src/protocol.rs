//! Wire protocol and the responder/initiator exchange.
//!
//! A sync is one bidirectional QUIC stream carrying length-prefixed postcard
//! frames (zstd-compressed, see [`crate::frame`]):
//!
//! 1. Each side sends [`Wire::Hello`] with a per-domain *hash* of its version
//!    map (no values). Domains whose hash the peer also reports are provably in
//!    sync, so an unchanged pass sends no version data at all.
//! 2. For the domains whose hashes differ (or that one side lacks), each side
//!    sends a [`Wire::Digest`] with its full version map. This exchange is
//!    sequential — one side writes before the other — so a large digest can't
//!    stall on QUIC flow control the way concurrent writes could.
//! 3. Each side sends the [`Wire::Records`] the peer lacks or has stale, then
//!    [`Wire::Done`], while reading the peer's records concurrently (so a large
//!    store can't stall on QUIC flow control).
//! 4. Each side applies what it received, then finishes its send stream. Each
//!    side waits for the peer's stream end, so no close can discard records the
//!    other side has not read. The connection itself is left open for reuse.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use iroh::endpoint::{Connection, RecvStream, SendStream, VarInt};
use iroh::protocol::ProtocolHandler;
use iroh::{Endpoint, EndpointAddr};
use serde::{Deserialize, Serialize};

use crate::frame;
use crate::hlc::Hlc;
use crate::merge::{Record, Version};
use crate::store::{Digest, Outbound, Store};
use crate::{RemoteCallback, SyncStatus};

/// Typed library identities and authorization-based peer membership. The
/// postcard layout is unchanged; older merge semantics are incompatible.
pub const ALPN: &[u8] = b"nova/sync/6";
/// ALPN for the one-shot "you have been removed" notice. Separate from the sync
/// ALPN so the sync wire schema (postcard, non-self-describing) is unchanged.
pub const REMOVE_ALPN: &[u8] = b"nova/remove/1";
const PROTO: u8 = 5;
/// Upper bound on a whole exchange (connect is timed out separately by the
/// caller). Generous: a first sync of a large library over a relay can take a
/// while, and aborting mid-flight drops the connection under the peer.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const REVOKED_CODE: u32 = 0x4e56;

#[derive(Serialize, Deserialize)]
enum Wire {
    Hello {
        proto: u8,
        device: u64,
        /// Friendly device name, so peers can label each other without
        /// re-pairing.
        name: String,
        /// Per-domain hash of this side's version map (no values). A domain
        /// whose hash matches the peer's is skipped, so an unchanged pass never
        /// sends its digest.
        digest_hashes: BTreeMap<String, [u8; 16]>,
    },
    /// Full version map for the domains whose hashes differed in `Hello`.
    Digest {
        digest: Digest,
    },
    Records {
        domain: String,
        entries: Vec<WireRecord>,
    },
    Done,
}

#[derive(Serialize, Deserialize)]
struct WireRecord {
    key: String,
    value: Option<String>,
    ts: u64,
    counter: u32,
    dev: u64,
    deleted: bool,
}

impl WireRecord {
    fn to_record(&self) -> Record {
        Record {
            value: self.value.clone(),
            version: Version::new(self.ts, self.counter, self.dev, self.deleted),
        }
    }

    /// The clock reading of this record's version.
    fn hlc(&self) -> Hlc {
        Hlc::new(self.ts, self.counter)
    }
}

impl From<&Outbound> for WireRecord {
    fn from(o: &Outbound) -> Self {
        Self {
            key: o.key.clone(),
            value: o.record.value.clone(),
            ts: o.record.version.ts,
            counter: o.record.version.counter,
            dev: o.record.version.dev,
            deleted: o.record.version.deleted,
        }
    }
}

/// Shared state for both the accept handler and the dialer.
///
/// `Clone` is shallow (every field is already a handle), so a spawned stream
/// task can own an independent handle to the same engine state.
#[derive(Clone)]
pub struct Handler {
    store: Arc<Mutex<Store>>,
    /// Stable id of this device (derived from the endpoint public key).
    pub device: u64,
    /// This device's endpoint id, so a peer's list can never add us as our
    /// own peer.
    pub self_id: String,
    peers: Arc<Mutex<Vec<String>>>,
    status: Arc<Mutex<SyncStatus>>,
    on_remote: Arc<Mutex<Option<RemoteCallback>>>,
    /// Last successful sync time per peer (unix secs); gates tombstone GC.
    acks: Arc<Mutex<crate::PeerAcks>>,
    /// Set when a peer's `peers` list tombstoned us: the device name of the
    /// peer that removed us, awaiting a UI notice.
    removed_notice: Arc<Mutex<Option<String>>>,
    pub(crate) joining: Arc<tokio::sync::Mutex<()>>,
    outbound: Arc<tokio::sync::Mutex<()>>,
}

/// Keep the outbound slot until every peer task has actually stopped. Dropping
/// a JoinSet requests cancellation, but those tasks may not drop immediately.
pub(crate) struct OutboundPass {
    handler: Handler,
    _slot: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for OutboundPass {
    fn drop(&mut self) {
        if let Ok(mut status) = self.handler.status.lock() {
            status.syncing = false;
            status.syncing_since = 0;
            for attempt in status.peer_attempts.values_mut() {
                attempt.in_flight = false;
            }
        }
    }
}

impl std::fmt::Debug for Handler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncHandler")
            .field("device", &self.device)
            .finish_non_exhaustive()
    }
}

impl Handler {
    pub(crate) async fn outbound_pass(&self) -> Arc<OutboundPass> {
        let slot = self.outbound.clone().lock_owned().await;
        Arc::new(OutboundPass {
            handler: self.clone(),
            _slot: slot,
        })
    }

    pub(crate) fn note_peer_attempt(&self, id: &str) {
        if let Ok(mut status) = self.status.lock() {
            let attempt = status.peer_attempts.entry(id.to_string()).or_default();
            attempt.last_attempt_secs = crate::now_secs();
            attempt.in_flight = true;
        }
    }
    pub(crate) fn note_peer_result(
        &self,
        id: &str,
        error: Option<String>,
        retry: u64,
        duration: Duration,
    ) {
        if let Ok(mut status) = self.status.lock() {
            let attempt = status.peer_attempts.entry(id.to_string()).or_default();
            attempt.in_flight = false;
            attempt.last_result_secs = crate::now_secs();
            if error.is_none() {
                attempt.last_success_secs = crate::now_secs();
            }
            attempt.last_error = error;
            attempt.retry_after_secs = retry;
            attempt.duration_ms = duration.as_millis().min(u64::MAX as u128) as u64;
        }
    }
    pub fn new(
        store: Arc<Mutex<Store>>,
        device: u64,
        self_id: String,
        peers: Arc<Mutex<Vec<String>>>,
        status: Arc<Mutex<SyncStatus>>,
        on_remote: Arc<Mutex<Option<RemoteCallback>>>,
        acks: Arc<Mutex<crate::PeerAcks>>,
    ) -> Self {
        Self {
            store,
            device,
            self_id,
            peers,
            status,
            on_remote,
            acks,
            removed_notice: Arc::new(Mutex::new(None)),
            joining: Arc::new(tokio::sync::Mutex::new(())),
            outbound: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Record that a full exchange with `id` completed: it has seen every
    /// record we held when our digest was built, whose clock reading was
    /// `at`. Using the digest snapshot (not completion time) means a tombstone
    /// created during the exchange is *not* considered acknowledged.
    /// Persisted so GC stays safe across restarts. Never records our own id.
    fn record_ack(&self, id: &str, at: crate::Hlc) -> Result<()> {
        if id == self.self_id {
            return Ok(());
        }
        if let Ok(mut acks) = self.acks.lock() {
            let mut next = acks.clone();
            let slot = next.entry(id.to_string()).or_insert(at);
            if at.newer_than(*slot) {
                *slot = at;
            }
            let result =
                nova_storage::try_set_str(crate::PEER_ACKS_KEY, &serde_json::to_string(&next)?);
            #[cfg(test)]
            let result = result.or_else(|e| {
                if e.kind == nova_storage::ErrorKind::Unavailable {
                    Ok(())
                } else {
                    Err(e)
                }
            });
            result?;
            *acks = next;
        }
        Ok(())
    }

    pub(crate) fn ack_clock(&self, id: &str) -> Option<crate::Hlc> {
        self.acks.lock().ok()?.get(id).copied()
    }

    /// This device's direct ack clock for `id` (unix secs), if we ever
    /// completed an exchange with it.
    pub(crate) fn ack_secs(&self, id: &str) -> Option<u64> {
        self.acks
            .lock()
            .ok()?
            .get(id)
            .map(|hlc| hlc.physical_ms / 1000)
    }

    /// Publish our sighting of `id` into the mesh `presence` domain
    /// (throttled inside). Called after a completed exchange.
    pub(crate) fn note_seen(&self, id: &str) {
        crate::presence_note(&self.store, &self.self_id, id, self.device);
    }

    /// Gated tombstone GC using the current peer set and ack clocks. Persists
    /// the store when anything was collected.
    pub(crate) fn gc_tombstones(&self, now_ms: u64) {
        let acks = self.acks.lock().map(|a| a.clone()).unwrap_or_default();
        let floor = {
            let peers = self.peers.lock().map(|p| p.clone()).unwrap_or_default();
            crate::ack_floor(&peers, &acks)
        };
        let mut store = self.store();
        if store.gc(now_ms, crate::store::TOMBSTONE_TTL_MS, floor) > 0
            && let Err(e) = store.save()
        {
            nova_storage::report(nova_storage::Error::new(
                nova_storage::ErrorKind::Transaction,
                e,
            ));
        }
    }

    fn is_peer(&self, id: &str) -> bool {
        self.peers
            .lock()
            .map(|p| p.iter().any(|x| x == id))
            .unwrap_or(false)
    }

    /// Refresh the name of an already-live peer (used while syncing). Never
    /// inserts a peer and never resurrects a tombstoned one: a removed device
    /// that reconnects must not be re-added without an explicit re-pair.
    pub(crate) fn add_peer_with_name(&self, id: &str, name: &str) {
        crate::peers_refresh(&self.store, &self.peers, id, name, self.device);
    }

    /// Add/refresh a peer from an explicit user action (pairing or manual add).
    /// This is the only path allowed to resurrect a tombstoned peer.
    pub(crate) fn add_peer_explicit(&self, id: &str, name: &str) -> Result<()> {
        crate::peers_add(&self.store, &self.peers, id, Some(name), self.device)
    }

    /// Drop a peer locally, tombstoning it so the removal replicates.
    pub(crate) fn drop_peer(&self, id: &str) {
        crate::peers_remove(&self.store, &self.peers, id, self.device);
    }

    /// The remote refused us as a peer ("not a peer"): it removed us (or we
    /// removed it earlier). Drop it and leave a UI notice so the removed side
    /// does not keep retrying a peer that will never accept it.
    pub(crate) fn note_rejected(&self, id: &str) {
        let name = crate::peer_names_from_store(&self.store)
            .remove(id)
            .unwrap_or_default();
        tracing::info!(
            peer = crate::short_id(id),
            "peer revoked our authorization; dropping it"
        );
        self.drop_peer(id);
        self.set_removed_notice(name);
        self.emit_remote(vec![crate::DOMAIN_PEERS.to_string()]);
    }

    /// Take (and clear) a pending "we were removed by X" notice.
    pub(crate) fn take_removed_notice(&self) -> Option<String> {
        self.removed_notice.lock().ok().and_then(|mut n| n.take())
    }

    fn set_removed_notice(&self, name: String) {
        if let Ok(mut n) = self.removed_notice.lock() {
            *n = Some(name);
        }
    }

    /// Rebuild the allowlist from the merged `peers` domain. Returns true when
    /// it changed (i.e. the remote introduced or removed peers).
    pub(crate) fn reconcile_peers(&self) -> bool {
        crate::reconcile_peers(&self.store, &self.peers, &self.self_id)
    }

    pub fn set_syncing(&self, syncing: bool) {
        if let Ok(mut s) = self.status.lock() {
            s.syncing = syncing;
            s.syncing_since = if syncing { crate::now_secs() } else { 0 };
        }
    }

    pub fn finish_sync(&self, error: Option<String>) {
        if let Ok(mut s) = self.status.lock() {
            s.syncing = false;
            s.syncing_since = 0;
            s.last_error = error;
        }
    }

    /// Record that the worker finished a wake-up. Bumped even for a pass with
    /// no eligible peer, so a caller can wait for one attempt to complete
    /// without racing the `syncing` flag.
    pub fn note_pass_done(&self) {
        if let Ok(mut s) = self.status.lock() {
            s.pass_count = s.pass_count.wrapping_add(1);
        }
    }

    fn emit_remote(&self, domains: Vec<String>) {
        let cb = self.on_remote.lock().ok().and_then(|c| c.clone());
        if let Some(cb) = cb {
            cb(domains);
        }
    }

    /// Replace the remote-change callback.
    pub fn set_on_remote(&self, cb: RemoteCallback) {
        if let Ok(mut slot) = self.on_remote.lock() {
            *slot = Some(cb);
        }
    }
}

impl ProtocolHandler for Handler {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let remote = conn.remote_id().to_string();
        if !self.is_peer(&remote) {
            let removed = self
                .store()
                .record(crate::DOMAIN_PEERS, &remote)
                .is_some_and(|r| r.is_deleted());
            conn.close(
                VarInt::from_u32(if removed { REVOKED_CODE } else { 0 }),
                if removed {
                    b"removed peer"
                } else {
                    b"not authorized"
                },
            );
            return Ok(());
        }
        // Serve streams for the life of the connection: the dialer reuses one
        // connection across passes, so returning after the first exchange would
        // drop the connection and strand the peer's next stream. Each bi stream
        // is one exchange, handled on its own task so streams can pipeline.
        loop {
            let (send, recv) = match conn.accept_bi().await {
                Ok(stream) => stream,
                // The peer (or the connection) went away: normal end of a
                // reused connection, not an error.
                Err(_) => return Ok(()),
            };
            if !self.is_peer(&remote) {
                conn.close(VarInt::from_u32(REVOKED_CODE), b"removed peer");
                return Ok(());
            }
            let handler = self.clone();
            let remote = remote.clone();
            tokio::spawn(async move {
                match tokio::time::timeout(
                    EXCHANGE_TIMEOUT,
                    exchange(send, recv, &remote, false, &handler),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) if is_disconnect(&e) => {
                        // Peer went away mid-exchange (restart, network change,
                        // or a timeout on its side). Transient; the next pass
                        // retries on a fresh connection.
                        tracing::debug!(error = %format_args!("{e:#}"), "incoming exchange dropped");
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(error = %format_args!("{e:#}"), "incoming sync failed");
                        handler.finish_sync(Some(e.to_string()));
                    }
                    Err(_) => {
                        tracing::warn!("incoming sync timed out");
                        handler.finish_sync(Some("incoming sync timed out".to_string()));
                    }
                }
            });
        }
    }
}

fn check_proto(proto: u8) -> Result<()> {
    if proto != PROTO {
        bail!("unsupported sync protocol version {proto}");
    }
    Ok(())
}

/// True for transient transport failures (peer went away, connection reset,
/// our own timeout aborting the exchange) as opposed to a protocol or data
/// error. These are retried by the next pass and should not be surfaced as
/// hard failures.
fn is_disconnect(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("connection lost")
        || text.contains("closed by peer")
        || text.contains("timed out")
        || text.contains("not connected")
        || text.contains("stopped")
}

fn outbound_frames(store: &Store, peer: &Digest) -> Vec<Wire> {
    let mut by_domain: BTreeMap<String, Vec<WireRecord>> = BTreeMap::new();
    for out in store.outbound(peer) {
        by_domain
            .entry(out.domain.clone())
            .or_default()
            .push(WireRecord::from(&out));
    }
    by_domain
        .into_iter()
        .map(|(domain, entries)| Wire::Records { domain, entries })
        .collect()
}

/// Result of applying a peer's frames.
struct Applied {
    /// Domains with at least one record that superseded the local version.
    changed: Vec<String>,
    /// A peer's `peers` list tombstoned us (that peer no longer trusts us).
    removed_us: bool,
}

fn apply_frames(
    store: &mut Store,
    frames: &[Wire],
    self_id: &str,
    _acks: &crate::PeerAcks,
) -> Applied {
    let mut changed: Vec<String> = Vec::new();
    let mut removed_us = false;
    for frame in frames {
        if let Wire::Records { domain, entries } = frame {
            let mut any = false;
            for entry in entries {
                if domain == crate::DOMAIN_PEERS {
                    // A peer's list includes us; never add ourselves as a peer.
                    // A tombstone here means the sender removed us.
                    if entry.key == self_id {
                        if entry.deleted
                            && crate::self_removal_is_current(store, entry.to_record().version)
                        {
                            removed_us = true;
                        }
                        continue;
                    }
                    // Store resolves membership by explicit authorization,
                    // never by later name updates or successful sync acks.
                }
                if store.apply(domain, &entry.key, entry.to_record()) {
                    any = true;
                    if domain == crate::DOMAIN_PEERS && entry.deleted {
                        tracing::debug!(
                            peer = crate::short_id(&entry.key),
                            "applied peer tombstone"
                        );
                    }
                }
            }
            if any && !changed.iter().any(|d| d == domain) {
                changed.push(domain.clone());
            }
        }
    }
    Applied {
        changed,
        removed_us,
    }
}

/// Hash each domain's version map. BTreeMap order plus postcard encoding make
/// this deterministic across devices, so equal hashes mean the maps are equal
/// for practical purposes. A 128-bit blake3 truncation makes the "equal hash,
/// different map" case (which would silently skip a needed sync) irrelevant.
fn hash_digest(digest: &Digest) -> BTreeMap<String, [u8; 16]> {
    digest
        .iter()
        .map(|(domain, keys)| {
            let bytes = postcard::to_allocvec(keys).unwrap_or_default();
            let hash = blake3::hash(&bytes);
            let short = <[u8; 16]>::try_from(&hash.as_bytes()[..16]).expect("16 bytes");
            (domain.clone(), short)
        })
        .collect()
}

/// True when any domain's version map could differ, i.e. the peer reported a
/// different hash for it or one side lacks the domain entirely.
fn needs_digest_exchange(
    local: &BTreeMap<String, [u8; 16]>,
    peer: &BTreeMap<String, [u8; 16]>,
) -> bool {
    local.len() != peer.len()
        || local
            .iter()
            .any(|(domain, hash)| peer.get(domain) != Some(hash))
}

/// Reconstruct the peer's full digest from the domains that agree (identical to
/// ours) plus the changed domains the peer sent, so `Store::outbound` can
/// compute what the peer lacks.
fn merge_peer_digest(
    local: &Digest,
    local_hashes: &BTreeMap<String, [u8; 16]>,
    peer_hashes: &BTreeMap<String, [u8; 16]>,
    peer_changed: Digest,
) -> Digest {
    let mut out = Digest::new();
    for (domain, keys) in local {
        if peer_hashes.get(domain) == local_hashes.get(domain) {
            out.insert(domain.clone(), keys.clone());
        }
    }
    for (domain, keys) in peer_changed {
        out.insert(domain, keys);
    }
    out
}

/// Run one sync exchange over a fresh stream on `conn`. `initiator` is true on
/// the dialing side. The connection is left open for reuse.
pub async fn run(conn: Connection, initiator: bool, handler: &Handler) -> Result<()> {
    let (send, recv) = if initiator {
        conn.open_bi().await.context("open bi stream")?
    } else {
        conn.accept_bi().await.context("accept bi stream")?
    };
    exchange(
        send,
        recv,
        &conn.remote_id().to_string(),
        initiator,
        handler,
    )
    .await
}

/// One exchange over an already-open bidirectional stream.
#[tracing::instrument(level = "debug", skip_all, fields(peer = crate::short_id(remote_id), initiator))]
async fn exchange(
    mut send: SendStream,
    mut recv: RecvStream,
    remote_id: &str,
    initiator: bool,
    handler: &Handler,
) -> Result<()> {
    anyhow::ensure!(handler.is_peer(remote_id), "peer authorization revoked");
    tokio::task::spawn_blocking(crate::flush_persistence).await??;
    // Digest and the clock reading it was taken at, captured together: every
    // record in the digest has a version at or below `snapshot`, so recording
    // `snapshot` as the peer's ack later exactly means "it saw all of these".
    let (local_digest, local_hashes, snapshot) = {
        let mut store = handler.store();
        store.save()?;
        let (digest, snapshot) = store.snapshot();
        let hashes = hash_digest(&digest);
        (digest, hashes, snapshot)
    };
    let local_hello = Wire::Hello {
        proto: PROTO,
        device: handler.device,
        name: crate::effective_device_name(&crate::read_settings()),
        digest_hashes: local_hashes.clone(),
    };

    // Hello exchange is small and sequential: each side reads before writing
    // its own, so a large frame cannot stall here.
    let (peer_hashes, peer_name) = if initiator {
        frame::write_frame_c(&mut send, &local_hello).await?;
        expect_hello(frame::read_frame_c(&mut recv).await?)?
    } else {
        let (hashes, name) = expect_hello(frame::read_frame_c(&mut recv).await?)?;
        frame::write_frame_c(&mut send, &local_hello).await?;
        (hashes, name)
    };
    // Learn the peer's name for the Settings list.
    handler.add_peer_with_name(remote_id, &peer_name);

    let exchange_digest = needs_digest_exchange(&local_hashes, &peer_hashes);
    let peer_digest = if exchange_digest {
        // Only the domains whose hash differed need their full version map.
        let local_changed: Digest = local_digest
            .iter()
            .filter(|(domain, _)| local_hashes.get(*domain) != peer_hashes.get(*domain))
            .map(|(domain, keys)| (domain.clone(), keys.clone()))
            .collect();
        // Sequential, like Hello: one side writes its (possibly large) digest
        // before the other, so the two writes cannot deadlock on flow control.
        let peer_changed = if initiator {
            frame::write_frame_c(
                &mut send,
                &Wire::Digest {
                    digest: local_changed,
                },
            )
            .await?;
            read_digest(&mut recv).await?
        } else {
            let peer_changed = read_digest(&mut recv).await?;
            frame::write_frame_c(
                &mut send,
                &Wire::Digest {
                    digest: local_changed,
                },
            )
            .await?;
            peer_changed
        };
        merge_peer_digest(&local_digest, &local_hashes, &peer_hashes, peer_changed)
    } else {
        // Every domain agrees, so nothing can be outbound; skip the work.
        local_digest.clone()
    };

    let frames = if exchange_digest {
        let store = handler.store();
        outbound_frames(&store, &peer_digest)
    } else {
        Vec::new()
    };
    let frame_count = frames.len();

    // Write and read concurrently. If both sides buffer their whole record set
    // before reading, a large library can exhaust the QUIC flow-control window
    // and stall until the exchange times out; pipelining removes that.
    //
    // `written` fires once our frames and `Done` are on the wire; `applied`
    // gates the stream end until we have applied; `finish` lets the responder
    // delay its stream end until after it records the ack (see below).
    let (written_tx, written_rx) = tokio::sync::oneshot::channel::<()>();
    let (applied_tx, applied_rx) = tokio::sync::oneshot::channel::<()>();
    let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
    let mut writer = WriterTask::spawn(write_side(send, frames, written_tx, applied_rx, finish_rx));

    let mut incoming = Vec::new();
    loop {
        let frame = frame::read_frame_c(&mut recv)
            .await?
            .context("peer closed before Done")?;
        match frame {
            Wire::Done => break,
            Wire::Records { .. } => incoming.push(frame),
            Wire::Hello { .. } | Wire::Digest { .. } => bail!("unexpected frame in records phase"),
        }
    }

    let (mut changed, removed_us) = if incoming.is_empty() {
        (Vec::new(), false)
    } else {
        tokio::task::spawn_blocking(crate::flush_persistence).await??;
        let acks = handler.acks.lock().map(|a| a.clone()).unwrap_or_default();
        let mut store = handler.store();
        // Removal takes this same store lock before changing the allowlist:
        // authorization and durable apply are one revocation boundary.
        anyhow::ensure!(
            handler.is_peer(remote_id),
            "peer authorization revoked before apply"
        );
        for frame in &incoming {
            if let Wire::Records { entries, .. } = frame {
                anyhow::ensure!(
                    entries.iter().all(|e| crate::store::valid_clock(e.hlc())),
                    "peer clock exceeds permitted drift; correct device clock"
                );
                if let Wire::Records { domain, .. } = frame {
                    anyhow::ensure!(
                        entries.iter().all(|e| crate::policy::valid(
                            domain,
                            &e.key,
                            &e.to_record()
                        )),
                        "invalid application record"
                    );
                }
            }
        }
        let applied = apply_frames(&mut store, &incoming, &handler.self_id, &acks);
        store.save()?;
        (applied.changed, applied.removed_us)
    };

    // A peer's list tombstoned us: we are no longer trusted, so drop that peer
    // too (mutual removal) and leave a notice for the UI. Without this the
    // removed side would keep the remover forever (the remover rejects our
    // syncs, so we could never learn otherwise).
    if removed_us {
        tracing::info!(
            peer = crate::short_id(remote_id),
            "peer removed us; dropping it"
        );
        handler.drop_peer(remote_id);
        handler.set_removed_notice(peer_name.clone());
        if !changed.iter().any(|d| d == crate::DOMAIN_PEERS) {
            changed.push(crate::DOMAIN_PEERS.to_string());
        }
    }

    // The remote may have introduced or removed peers; rebuild the allowlist
    // and report it so the app refreshes the list.
    if handler.reconcile_peers() && !changed.iter().any(|d| d == crate::DOMAIN_PEERS) {
        changed.push(crate::DOMAIN_PEERS.to_string());
    }
    // Projection is independent of transport completion. The durable pending
    // domain marker is replayed on attachment, poll, and restart if scheduling
    // this callback fails or the handshake is cancelled next.
    if !changed.is_empty() {
        handler.emit_remote(changed.clone());
    }

    tracing::debug!(
        digest_exchange = exchange_digest,
        sent_frames = frame_count,
        incoming_frames = incoming.len(),
        changed_domains = changed.len(),
        "durable exchange apply finished"
    );

    // Close handshake using stream end instead of a wire frame. Each side
    // finishes its send stream only *after* applying, so the peer's stream end
    // is a reliable "I have applied" signal; the connection is left open for
    // reuse, so the connection itself cannot carry that signal.
    //
    // The responder additionally delays finishing its stream until after it has
    // recorded the ack, so when the initiator observes the stream end it knows
    // the responder applied *and* acked — otherwise the initiator could return
    // before the responder's ack lands. (The initiator must finish eagerly, or
    // the responder could never observe its end.) The waits are bounded and the
    // writer is aborted if `exchange` returns early, so a peer that goes away
    // can never wedge the pass.
    let _ = applied_tx.send(());
    // Frames + `Done` written without error. Fires before we block on the
    // peer's end, so the responder can ack without waiting for its own finish.
    let written = written_rx.await.is_ok();
    let acked = if initiator {
        let _ = finish_tx.send(());
        let ended = matches!(
            tokio::time::timeout(EXCHANGE_TIMEOUT, wait_for_end(&mut recv)).await,
            Ok(true)
        );
        let finished = matches!(
            tokio::time::timeout(EXCHANGE_TIMEOUT, writer.join()).await,
            Ok(Ok(()))
        );
        written && ended && finished
    } else {
        let ended = matches!(
            tokio::time::timeout(EXCHANGE_TIMEOUT, wait_for_end(&mut recv)).await,
            Ok(true)
        );
        let ok = written && ended;
        if ok {
            handler.record_ack(remote_id, snapshot)?;
        }
        // Release our stream end now that the ack (if earned) is recorded.
        let _ = finish_tx.send(());
        let finished = matches!(
            tokio::time::timeout(EXCHANGE_TIMEOUT, writer.join()).await,
            Ok(Ok(()))
        );
        ok && finished
    };
    if acked {
        if initiator {
            handler.record_ack(remote_id, snapshot)?;
        }
        handler.note_seen(remote_id);
        if let Ok(mut status) = handler.status.lock() {
            status.success_count = status.success_count.saturating_add(1);
            status.last_sync_secs = crate::now_secs();
        }
    }

    anyhow::ensure!(acked, "sync completion handshake failed");
    Ok(())
}

/// Read a `Wire::Digest` frame (the peer's changed-domain version maps).
async fn read_digest(recv: &mut RecvStream) -> Result<Digest> {
    match frame::read_frame_c(recv).await? {
        Some(Wire::Digest { digest }) => Ok(digest),
        Some(_) => bail!("expected digest, got another frame"),
        None => bail!("peer closed before digest"),
    }
}

/// Write our side of the exchange: all outbound record frames, `Done`, signal
/// `written`, then finish the stream once the caller signals it has applied
/// (`applied`) and, for the responder, has recorded the ack (`finish`). The
/// peer's stream end therefore means "applied" (and, for the responder side,
/// "acked").
async fn write_side(
    mut send: SendStream,
    frames: Vec<Wire>,
    written: tokio::sync::oneshot::Sender<()>,
    applied: tokio::sync::oneshot::Receiver<()>,
    finish: tokio::sync::oneshot::Receiver<()>,
) -> Result<()> {
    for frame in &frames {
        frame::write_frame_c(&mut send, frame).await?;
    }
    frame::write_frame_c(&mut send, &Wire::Done).await?;
    let _ = written.send(());
    let _ = applied.await;
    let _ = finish.await;
    send.finish().context("finish send stream")?;
    Ok(())
}

/// Read and discard frames until the peer's stream ends. Returns false on a
/// transport error. Both sides use this as the "the peer applied" signal.
async fn wait_for_end(recv: &mut RecvStream) -> bool {
    match frame::read_frame_c::<Wire>(recv).await {
        Ok(Some(_)) => false,
        Ok(None) => true,
        Err(_) => false,
    }
}

/// A spawned writer task that is aborted if it is dropped before being joined,
/// so an early return can never leave a task blocked on the connection.
struct WriterTask(Option<tokio::task::JoinHandle<Result<()>>>);

impl WriterTask {
    fn spawn(fut: impl std::future::Future<Output = Result<()>> + Send + 'static) -> Self {
        Self(Some(tokio::spawn(fut)))
    }

    async fn join(&mut self) -> Result<()> {
        let result = match self.0.as_mut() {
            Some(handle) => match handle.await {
                Ok(result) => result,
                Err(e) => Err(anyhow::anyhow!("writer task: {e}")),
            },
            None => Ok(()),
        };
        self.0.take();
        result
    }
}

impl Drop for WriterTask {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}

fn expect_hello(frame: Option<Wire>) -> Result<(BTreeMap<String, [u8; 16]>, String)> {
    match frame {
        Some(Wire::Hello {
            proto,
            digest_hashes,
            name,
            ..
        }) => {
            check_proto(proto)?;
            Ok((digest_hashes, name))
        }
        Some(_) => bail!("expected hello, got another frame"),
        None => bail!("peer closed before hello"),
    }
}

// ---------------------------------------------------------------------------
// Removal notice
// ---------------------------------------------------------------------------

/// Body of the one-shot removal notice (its own ALPN, so the sync wire schema
/// is untouched).
#[derive(Serialize, Deserialize)]
struct RemoveNotice {
    proto: u8,
    /// The remover's friendly name, for the UI notice.
    name: String,
}

const REMOVE_PROTO: u8 = 1;

/// Best-effort: tell `peer` directly that we removed it, so it drops us instead
/// of keeping a dead peer and retrying forever. A failure is harmless (the peer
/// will simply keep failing to sync until the user re-pairs or removes us).
pub(crate) async fn notify_removed(endpoint: &Endpoint, addr: EndpointAddr, name: &str) {
    let connect = tokio::time::timeout(
        Duration::from_secs(10),
        endpoint.connect(addr, crate::configuration::protocol().remove),
    )
    .await;
    let Ok(Ok(conn)) = connect else {
        return;
    };
    let Ok((mut send, _recv)) = conn.open_bi().await else {
        return;
    };
    let notice = RemoveNotice {
        proto: REMOVE_PROTO,
        name: name.to_string(),
    };
    if frame::write_frame(&mut send, &notice).await.is_ok() {
        let _ = send.finish();
        let _ = tokio::time::timeout(Duration::from_secs(5), conn.closed()).await;
    }
}

/// Accepts a removal notice: drop the sender and surface a UI notice. The
/// connection is authenticated by endpoint id, so a peer can only remove
/// itself from our list, never another device.
#[derive(Debug)]
pub struct RemoveHandler {
    handler: Arc<Handler>,
}

impl RemoveHandler {
    pub fn new(handler: Arc<Handler>) -> Self {
        Self { handler }
    }
}

impl ProtocolHandler for RemoveHandler {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let remote = conn.remote_id().to_string();
        let result = async {
            let (mut send, mut recv) = conn.accept_bi().await.context("accept remove stream")?;
            let notice: RemoveNotice = frame::read_frame(&mut recv)
                .await?
                .context("peer closed before removal notice")?;
            if notice.proto != REMOVE_PROTO {
                bail!("unsupported removal protocol {}", notice.proto);
            }
            tracing::info!(peer = crate::short_id(&remote), "peer removed us");
            self.handler.drop_peer(&remote);
            self.handler.set_removed_notice(notice.name);
            self.handler.reconcile_peers();
            self.handler
                .emit_remote(vec![crate::DOMAIN_PEERS.to_string()]);
            let _ = frame::write_frame(&mut send, &0u8).await;
            let _ = send.finish();
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(e) = result {
            tracing::debug!(error = %format_args!("{e:#}"), "removal notice failed");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seeded(domain: &str, key: &str, value: &str, dev: u64) -> Arc<Mutex<Store>> {
        let store = Arc::new(Mutex::new(Store::default()));
        store
            .lock()
            .unwrap()
            .set(domain, key, Some(value.into()), 0, dev);
        store
    }

    fn handler(
        store: Arc<Mutex<Store>>,
        device: u64,
        self_id: &str,
        peers: Arc<Mutex<Vec<String>>>,
    ) -> Arc<Handler> {
        // Production allowlists are derived from durable membership, never
        // arbitrary vectors. Keep the transport fixtures faithful to that.
        for peer in peers.lock().unwrap().iter() {
            let mut records = store.lock().unwrap();
            if records.record(crate::DOMAIN_PEERS, peer).is_none() {
                records.set(crate::DOMAIN_PEERS, peer, Some("{}".into()), 1, device);
            }
        }
        Arc::new(Handler::new(
            store,
            device,
            self_id.to_string(),
            peers,
            Arc::new(Mutex::new(SyncStatus::default())),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(crate::PeerAcks::new())),
        ))
    }

    fn acks_of(handler: &Arc<Handler>) -> crate::PeerAcks {
        handler.acks.lock().map(|a| a.clone()).unwrap_or_default()
    }

    #[tokio::test]
    async fn cancelled_job_waiting_for_outbound_slot_preserves_live_pass() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let handler = handler(
            Arc::new(Mutex::new(Store::default())),
            1,
            "self",
            Arc::new(Mutex::new(Vec::new())),
        );
        let foreground_pass = handler.outbound_pass().await;
        handler.set_syncing(true);
        handler.note_peer_attempt("peer");
        let cancel = AtomicBool::new(false);
        let stop = AtomicBool::new(false);
        let job = crate::bounded_one_shot(&cancel, &stop, Duration::from_secs(5), async {
            let _pass = handler.outbound_pass().await;
            panic!("job overlapped the foreground pass");
        });
        tokio::pin!(job);
        tokio::select! {
            biased;
            _ = &mut job => panic!("job did not wait for the occupied slot"),
            _ = tokio::task::yield_now() => {}
        }
        cancel.store(true, Ordering::Release);
        assert_eq!(job.await, crate::OneShotOutcome::Cancelled);
        {
            let status = handler.status.lock().unwrap();
            assert!(status.syncing);
            assert!(status.peer_attempts["peer"].in_flight);
        }
        assert!(!stop.load(Ordering::Relaxed));
        drop(foreground_pass);
        let _next = handler.outbound_pass().await;
        let status = handler.status.lock().unwrap();
        assert!(!status.syncing);
        assert!(!status.peer_attempts["peer"].in_flight);
    }

    #[tokio::test]
    async fn outbound_slot_waits_for_cancelled_peer_tasks_before_clearing_status() {
        let handler = handler(
            Arc::new(Mutex::new(Store::default())),
            1,
            "self",
            Arc::new(Mutex::new(Vec::new())),
        );
        let pass = handler.outbound_pass().await;
        let mut peers = tokio::task::JoinSet::new();
        let peer_pass = pass.clone();
        let peer_handler = handler.clone();
        let (started, ready) = tokio::sync::oneshot::channel();
        peers.spawn(async move {
            let _pass = peer_pass;
            peer_handler.note_peer_attempt("peer");
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        ready.await.unwrap();
        handler.set_syncing(true);
        drop(pass);
        // The peer task still owns the slot and its published in-flight state.
        assert!(handler.outbound.try_lock().is_err());
        assert!(handler.status.lock().unwrap().peer_attempts["peer"].in_flight);
        drop(peers);
        let _next = tokio::time::timeout(Duration::from_secs(1), handler.outbound_pass())
            .await
            .unwrap();
        let status = handler.status.lock().unwrap();
        assert!(!status.syncing);
        assert_eq!(status.syncing_since, 0);
        assert!(!status.peer_attempts["peer"].in_flight);
    }

    #[tokio::test]
    async fn one_shot_budget_covers_slot_wait_and_network_refresh() {
        use std::sync::atomic::AtomicBool;

        let handler = handler(
            Arc::new(Mutex::new(Store::default())),
            1,
            "self",
            Arc::new(Mutex::new(Vec::new())),
        );
        let cancel = AtomicBool::new(false);
        let stop = AtomicBool::new(false);
        let owner = handler.outbound_pass().await;
        assert_eq!(
            crate::bounded_one_shot(&cancel, &stop, Duration::from_millis(10), async {
                let _pass = handler.outbound_pass().await;
                crate::OneShotOutcome::Completed
            })
            .await,
            crate::OneShotOutcome::TimedOut
        );
        drop(owner);
        // A stalled network refresh holds the same slot inside the deadline.
        assert_eq!(
            crate::bounded_one_shot(&cancel, &stop, Duration::from_millis(10), async {
                let _pass = handler.outbound_pass().await;
                std::future::pending::<crate::OneShotOutcome>().await
            })
            .await,
            crate::OneShotOutcome::TimedOut
        );
        assert!(handler.outbound.try_lock().is_ok());
    }

    async fn endpoint() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    }

    /// Endpoint with a shared in-memory address lookup, so dialing by id
    /// (as `run_pass` does) resolves to a direct address.
    async fn endpoint_with_lookup(
        lookup: &iroh::address_lookup::memory::MemoryLookup,
    ) -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .address_lookup(lookup.clone())
            .bind()
            .await
            .unwrap()
    }

    /// A store seeded with `peers` records (id, name).
    fn store_with_peers(peers: &[(&str, &str)]) -> Arc<Mutex<Store>> {
        let store = Arc::new(Mutex::new(Store::default()));
        {
            let mut store = store.lock().unwrap();
            for (id, name) in peers {
                let value = if name.is_empty() {
                    "{}".to_string()
                } else {
                    format!("{{\"name\":\"{name}\"}}")
                };
                store.set(crate::DOMAIN_PEERS, id, Some(value), 1, 1);
            }
        }
        store
    }

    /// A store seeded with a `peers` record for `peer` plus one library
    /// record. A real device always has the peer in the `peers` domain (the
    /// live allowlist is derived from it), so tests must too.
    fn store_with_peer(peer: &str, key: &str, value: &str, dev: u64) -> Arc<Mutex<Store>> {
        let store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, peer, Some("{}".into()), 1, dev);
            s.set("library", key, Some(value.into()), 0, dev);
        }
        store
    }

    fn allowlist(store: &Arc<Mutex<Store>>) -> Arc<Mutex<Vec<String>>> {
        let live = {
            let store = store.lock().unwrap();
            store
                .records(crate::DOMAIN_PEERS)
                .into_iter()
                .map(|(id, _)| id)
                .collect::<Vec<_>>()
        };
        Arc::new(Mutex::new(live))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_endpoints_converge() {
        let a_store = seeded("library", "a", "1", 1);
        let b_store = seeded("library", "b", "2", 2);
        let a = endpoint().await;
        let b = endpoint().await;
        let a_handler = handler(
            a_store.clone(),
            1,
            &a.id().to_string(),
            Arc::new(Mutex::new(vec![b.id().to_string()])),
        );
        // B trusts A (the allowlist boundary).
        let b_peers = Arc::new(Mutex::new(vec![a.id().to_string()]));
        let b_handler = handler(b_store.clone(), 2, &b.id().to_string(), b_peers);

        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        run(conn, true, &a_handler).await.unwrap();

        // Each side now holds both records.
        assert_eq!(a_store.lock().unwrap().records("library").len(), 2);
        assert_eq!(b_store.lock().unwrap().records("library").len(), 2);
    }

    #[derive(Debug)]
    struct IncompletePeer {
        after_done: bool,
        truncated_header: bool,
        revoke: Option<(Arc<Handler>, String)>,
    }

    impl ProtocolHandler for IncompletePeer {
        async fn accept(
            &self,
            conn: Connection,
        ) -> std::result::Result<(), iroh::protocol::AcceptError> {
            let (mut send, mut recv) = conn.accept_bi().await.unwrap();
            expect_hello(frame::read_frame_c(&mut recv).await.unwrap()).unwrap();
            frame::write_frame_c(
                &mut send,
                &Wire::Hello {
                    proto: PROTO,
                    device: 42,
                    name: "test".into(),
                    digest_hashes: BTreeMap::new(),
                },
            )
            .await
            .unwrap();
            read_digest(&mut recv).await.unwrap();
            frame::write_frame_c(
                &mut send,
                &Wire::Digest {
                    digest: Digest::new(),
                },
            )
            .await
            .unwrap();
            while !matches!(
                frame::read_frame_c::<Wire>(&mut recv).await.unwrap(),
                Some(Wire::Done)
            ) {}
            let record = Record::present(
                "durable".into(),
                Version::new(crate::now_ms(), 0, 42, false),
            );
            frame::write_frame_c(
                &mut send,
                &Wire::Records {
                    domain: "library".into(),
                    entries: vec![WireRecord::from(&Outbound {
                        domain: "library".into(),
                        key: "remote".into(),
                        record,
                    })],
                },
            )
            .await
            .unwrap();
            if let Some((handler, id)) = &self.revoke {
                handler.drop_peer(id);
            }
            if self.after_done {
                frame::write_frame_c(&mut send, &Wire::Done).await.unwrap();
                // A trailing frame violates the required Done + stream-end
                // sequence after apply has already happened.
                frame::write_frame_c(&mut send, &Wire::Done).await.unwrap();
            }
            if self.truncated_header {
                send.write_all(&[0, 0]).await.unwrap();
            }
            send.finish().unwrap();
            let _ = recv.read_to_end(1024).await;
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn incomplete_exchange_never_acks_and_replays_already_applied_records() {
        for (after_done, truncated_header) in [(false, false), (false, true), (true, false)] {
            let a = endpoint().await;
            let b = endpoint().await;
            let store = store_with_peer(&b.id().to_string(), "local", "1", 1);
            let handler = handler(store.clone(), 1, &a.id().to_string(), allowlist(&store));
            let _router = iroh::protocol::Router::builder(b.clone())
                .accept(
                    ALPN,
                    IncompletePeer {
                        after_done,
                        truncated_header,
                        revoke: None,
                    },
                )
                .spawn();
            assert!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    run(a.connect(b.addr(), ALPN).await.unwrap(), true, &handler)
                )
                .await
                .unwrap()
                .is_err()
            );
            assert!(handler.acks.lock().unwrap().is_empty());
            assert_eq!(handler.status.lock().unwrap().success_count, 0);
            let store = store.lock().unwrap();
            assert_eq!(store.record("library", "remote").is_some(), after_done);
            assert_eq!(
                store.pending_domains().contains(&"library".into()),
                after_done
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn revocation_before_done_blocks_in_flight_apply() {
        let a = endpoint().await;
        let b = endpoint().await;
        let bid = b.id().to_string();
        let store = store_with_peer(&bid, "local", "1", 1);
        let handler = handler(store.clone(), 1, &a.id().to_string(), allowlist(&store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(
                ALPN,
                IncompletePeer {
                    after_done: true,
                    truncated_header: false,
                    revoke: Some((handler.clone(), bid.clone())),
                },
            )
            .spawn();
        let error = tokio::time::timeout(
            Duration::from_secs(5),
            run(a.connect(b.addr(), ALPN).await.unwrap(), true, &handler),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(format!("{error:#}").contains("authorization revoked before apply"));
        assert!(store.lock().unwrap().record("library", "remote").is_none());
        assert!(
            store
                .lock()
                .unwrap()
                .record(crate::DOMAIN_PEERS, &bid)
                .unwrap()
                .is_deleted()
        );
        assert!(handler.acks.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn completed_exchanges_publish_and_replicate_presence() {
        let a_store = seeded("library", "a", "1", 1);
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        // B trusts A (the allowlist boundary, in the store so the
        // end-of-pass reconcile keeps it, not just in memory).
        let b_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = b_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &aid, Some("{}".into()), 1, 2);
            s.set("library", "b", Some("2".into()), 0, 2);
        }
        let a_handler = handler(
            a_store.clone(),
            1,
            &aid,
            Arc::new(Mutex::new(vec![bid.clone()])),
        );
        // B trusts A (the allowlist boundary).
        let b_peers = Arc::new(Mutex::new(vec![aid.clone()]));
        let b_handler = handler(b_store.clone(), 2, &bid, b_peers);

        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let a_key = format!("{aid}\x01{bid}");
        let b_key = format!("{bid}\x01{aid}");
        // Two exchanges: the first publishes each side's own sighting, the
        // second replicates it (a sighting written after the outbound frames
        // can't ride the same exchange).
        for _ in 0..2 {
            let conn = a.connect(b.addr(), ALPN).await.unwrap();
            run(conn, true, &a_handler).await.unwrap();
        }

        for store in [&a_store, &b_store] {
            let keys: Vec<String> = store
                .lock()
                .unwrap()
                .records(crate::DOMAIN_PRESENCE)
                .into_iter()
                .map(|(k, _)| k)
                .collect();
            assert!(keys.contains(&a_key), "missing {a_key} in {keys:?}");
            assert!(keys.contains(&b_key), "missing {b_key} in {keys:?}");
        }
        // And the max-reader sees both peers as recently alive.
        assert!(crate::presence_newest(&a_store, &aid).is_some());
        assert!(crate::presence_newest(&a_store, &bid).is_some());
    }

    #[test]
    fn digest_hashes_skip_equal_domains() {
        let mut store = Store::default();
        store.set("d", "k", Some("v".into()), 0, 1);
        let digest = store.digest();
        let hashes = hash_digest(&digest);
        // Identical maps never trigger a digest exchange.
        assert!(!needs_digest_exchange(&hashes, &hashes));
        // A domain missing on one side is a difference.
        assert!(needs_digest_exchange(&hashes, &BTreeMap::new()));
        // A changed version (or an added key) flips the hash.
        let mut other = store.digest();
        other
            .get_mut("d")
            .unwrap()
            .insert("k2".into(), Version::new(1, 0, 1, false));
        assert!(needs_digest_exchange(&hashes, &hash_digest(&other)));
    }

    #[test]
    fn merge_peer_digest_fills_unchanged_and_received_domains() {
        let mut local = Digest::new();
        local.insert(
            "same".into(),
            BTreeMap::from([("k".into(), Version::new(5, 0, 1, false))]),
        );
        local.insert(
            "mine".into(),
            BTreeMap::from([("m".into(), Version::new(7, 0, 1, false))]),
        );
        let local_hashes = hash_digest(&local);
        // The peer agrees on "same", lacks "mine", and has its own "theirs".
        let mut peer_hashes = BTreeMap::new();
        peer_hashes.insert("same".into(), local_hashes["same"]);
        peer_hashes.insert("theirs".into(), [9u8; 16]);
        let peer_changed = Digest::from([(
            "theirs".into(),
            BTreeMap::from([("t".into(), Version::new(3, 0, 2, false))]),
        )]);
        let merged = merge_peer_digest(&local, &local_hashes, &peer_hashes, peer_changed);
        // Unchanged domains come from our own map; received ones from the peer.
        assert_eq!(merged["same"]["k"].ts, 5);
        assert_eq!(merged["theirs"]["t"].ts, 3);
        // "mine" is absent from the peer, so `outbound` will send it.
        assert!(!merged.contains_key("mine"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn converged_exchange_acks_without_announcing_changes() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();

        // Both sides already know each other and hold one library record each.
        let a_store = store_with_peer(&bid, "a", "1", 1);
        let b_store = store_with_peer(&aid, "b", "2", 2);
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        // Two passes: the first converges the libraries, the second replicates
        // the presence sightings written after the first. (A sighting written
        // after a pass's outbound frames cannot ride the same exchange.)
        for _ in 0..2 {
            let conn = a.connect(b.addr(), ALPN).await.unwrap();
            run(conn, true, &a_handler).await.unwrap();
        }
        assert_eq!(a_store.lock().unwrap().records("library").len(), 2);

        // A third pass has no data to move (only the inherently asymmetric
        // peer lists differ): it must not announce a change, but must still
        // record the ack that gates tombstone GC.
        let announced = Arc::new(Mutex::new(Vec::<Vec<String>>::new()));
        let sink = announced.clone();
        a_handler.set_on_remote(Arc::new(move |domains| {
            sink.lock().unwrap().push(domains);
        }));
        a_handler.acks.lock().unwrap().clear();
        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        run(conn, true, &a_handler).await.unwrap();

        assert!(
            announced.lock().unwrap().is_empty(),
            "converged pass announced changes"
        );
        assert!(
            acks_of(&a_handler).contains_key(&bid),
            "converged pass did not record an ack"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mesh_introduces_peers_and_syncs_directly() {
        let a = endpoint().await;
        let b = endpoint().await;
        let c = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let cid = c.id().to_string();

        // A knows B and C; B and C know only A.
        let a_store = store_with_peers(&[(&bid, "Bob"), (&cid, "Charlie")]);
        let b_store = store_with_peers(&[(&aid, "Alice")]);
        let c_store = store_with_peers(&[(&aid, "Alice")]);
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let c_handler = handler(c_store.clone(), 3, &cid, allowlist(&c_store));

        let _rb = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();
        let _rc = iroh::protocol::Router::builder(c.clone())
            .accept(ALPN, c_handler.clone())
            .spawn();

        // A syncs with B and C, introducing each to the other.
        run(a.connect(b.addr(), ALPN).await.unwrap(), true, &a_handler)
            .await
            .unwrap();
        run(a.connect(c.addr(), ALPN).await.unwrap(), true, &a_handler)
            .await
            .unwrap();

        assert!(b_handler.peers.lock().unwrap().contains(&cid));
        assert!(c_handler.peers.lock().unwrap().contains(&bid));
        // The name traveled with the peer record.
        assert!(
            b_store
                .lock()
                .unwrap()
                .records(crate::DOMAIN_PEERS)
                .iter()
                .any(|(id, value)| id == &cid && value.contains("Charlie"))
        );
        // A never added itself.
        assert!(!a_handler.peers.lock().unwrap().contains(&aid));

        // B and C can now sync directly: seed a record in C and pull it into B.
        c_store
            .lock()
            .unwrap()
            .set("library", "shared", Some("v".into()), 0, 3);
        run(b.connect(c.addr(), ALPN).await.unwrap(), true, &b_handler)
            .await
            .unwrap();
        assert!(
            b_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(key, _)| key == "shared")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_pass_tolerates_bad_peer() {
        use std::collections::HashMap;

        // `run_pass` dials peers by id, so the test needs a lookup that
        // resolves B's id to a direct address.
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let a = endpoint_with_lookup(&lookup).await;
        let b = endpoint_with_lookup(&lookup).await;
        lookup.add_endpoint_info(b.addr());
        let aid = a.id().to_string();
        let bid = b.id().to_string();

        let a_store = seeded("library", "a", "1", 1);
        let b_store = seeded("library", "b", "2", 2);
        let a_handler = handler(
            a_store.clone(),
            1,
            &aid,
            Arc::new(Mutex::new(vec![bid.clone()])),
        );
        let b_handler = handler(b_store.clone(), 2, &bid, Arc::new(Mutex::new(vec![aid])));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        // One bogus peer plus the real one; the pass must still sync with B.
        let peers = Arc::new(Mutex::new(vec!["not-a-valid-id".to_string(), bid]));
        let health = Arc::new(Mutex::new(HashMap::new()));
        let conns = Arc::new(Mutex::new(std::collections::HashMap::new()));
        crate::run_pass(a.clone(), a_handler.clone(), peers, conns, health).await;

        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(key, _)| key == "b")
        );
        assert!(
            b_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(key, _)| key == "a")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn run_pass_reaches_peers_learned_mid_pass() {
        use std::collections::HashMap;

        // A knows only B; B knows A and C. A learns about C while syncing with
        // B, so `run_pass` must dial C in a follow-up round of the same wake-up
        // rather than waiting for the next interval.
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let a = endpoint_with_lookup(&lookup).await;
        let b = endpoint_with_lookup(&lookup).await;
        let c = endpoint_with_lookup(&lookup).await;
        lookup.add_endpoint_info(b.addr());
        lookup.add_endpoint_info(c.addr());
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let cid = c.id().to_string();

        let a_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = a_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &bid, Some("{}".into()), 1, 1);
            s.set("library", "a", Some("va".into()), 0, 1);
        }
        let b_store = store_with_peers(&[(&aid, "Alice"), (&cid, "Charlie")]);
        let c_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = c_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &aid, Some("{}".into()), 1, 1);
            s.set("library", "c", Some("vc".into()), 0, 3);
        }

        // Same Arc for the handler and the pass, since reconciliation updates
        // it in place when a peer is discovered.
        let a_peers = allowlist(&a_store);
        let a_handler = handler(a_store.clone(), 1, &aid, a_peers.clone());
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let c_handler = handler(c_store.clone(), 3, &cid, allowlist(&c_store));

        let _rb = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler)
            .spawn();
        let _rc = iroh::protocol::Router::builder(c.clone())
            .accept(ALPN, c_handler)
            .spawn();

        let health = Arc::new(Mutex::new(HashMap::new()));
        let conns = Arc::new(Mutex::new(std::collections::HashMap::new()));
        crate::run_pass(a.clone(), a_handler.clone(), a_peers.clone(), conns, health).await;

        // A discovered C from B and dialed it within the same pass.
        assert!(a_peers.lock().unwrap().contains(&cid), "A did not learn C");
        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(key, _)| key == "c"),
            "A never synced directly with C"
        );
        assert!(
            c_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(key, _)| key == "a"),
            "C never synced directly with A"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn reused_connection_carries_successive_passes() {
        use std::collections::HashMap;

        // `run_pass` dials peers by id, so the test needs a lookup that
        // resolves B's id to a direct address.
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let a = endpoint_with_lookup(&lookup).await;
        let b = endpoint_with_lookup(&lookup).await;
        lookup.add_endpoint_info(b.addr());
        let aid = a.id().to_string();
        let bid = b.id().to_string();

        let a_store = seeded("library", "a", "1", 1);
        let b_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = b_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &aid, Some("{}".into()), 1, 2);
            s.set("library", "b", Some("2".into()), 0, 2);
        }
        let a_handler = handler(
            a_store.clone(),
            1,
            &aid,
            Arc::new(Mutex::new(vec![bid.clone()])),
        );
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let peers = Arc::new(Mutex::new(vec![bid.clone()]));
        let health = Arc::new(Mutex::new(HashMap::new()));
        let conns = Arc::new(Mutex::new(std::collections::HashMap::new()));

        crate::run_pass(
            a.clone(),
            a_handler.clone(),
            peers.clone(),
            conns.clone(),
            health.clone(),
        )
        .await;
        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(k, _)| k == "b")
        );
        // The connection to B is kept for reuse instead of being redialed.
        assert!(
            conns.lock().unwrap().contains_key(&bid),
            "connection was not cached"
        );

        // A record added on B between passes is picked up over the same
        // connection: the responder must serve a second stream on it.
        b_store
            .lock()
            .unwrap()
            .set("library", "b2", Some("2".into()), 0, 2);
        crate::run_pass(
            a.clone(),
            a_handler.clone(),
            peers.clone(),
            conns.clone(),
            health.clone(),
        )
        .await;
        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(k, _)| k == "b2"),
            "reused connection did not carry the second pass"
        );

        // Closing a retained connection must not deadlock cache eviction.
        let closed = conns.lock().unwrap().get(&bid).unwrap().clone();
        closed.close(0u32.into(), b"test redial");
        closed.closed().await;
        assert!(conns.try_lock().is_ok());
        b_store
            .lock()
            .unwrap()
            .set("library", "b3", Some("3".into()), 0, 2);
        crate::run_pass(a.clone(), a_handler.clone(), peers, conns.clone(), health).await;
        assert!(conns.try_lock().is_ok(), "redial left the cache locked");
        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(k, _)| k == "b3")
        );
        a.close().await;
        b.close().await;
    }

    #[tokio::test]
    async fn explicit_worker_wake_retries_backed_off_peer() {
        let lookup = iroh::address_lookup::memory::MemoryLookup::new();
        let a = endpoint_with_lookup(&lookup).await;
        let b = endpoint_with_lookup(&lookup).await;
        lookup.add_endpoint_info(b.addr());
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let a_store = store_with_peer(&bid, "a", "1", 1);
        let b_store = store_with_peer(&aid, "b", "2", 2);
        let peers = allowlist(&a_store);
        let a_handler = handler(a_store.clone(), 1, &aid, peers.clone());
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler)
            .spawn();
        let health = Arc::new(Mutex::new(std::collections::HashMap::new()));
        crate::record_failure(&health, &bid);
        let notify = Arc::new(tokio::sync::Notify::new());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let force = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let worker = tokio::spawn(crate::sync_worker(crate::SyncWorker {
            endpoint: a.clone(),
            handler: a_handler.clone(),
            peers,
            conns: Arc::new(Mutex::new(std::collections::HashMap::new())),
            health: health.clone(),
            sync_notify: notify.clone(),
            stop: stop.clone(),
            force_attempt: force.clone(),
        }));
        // A periodic wake must respect backoff.
        notify.notify_one();
        tokio::time::timeout(Duration::from_secs(10), async {
            while a_handler.status.lock().unwrap().pass_count < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(k, _)| k == "b")
        );
        // An explicit request bypasses backoff once.
        force.store(true, std::sync::atomic::Ordering::Release);
        notify.notify_one();
        tokio::time::timeout(Duration::from_secs(20), async {
            while a_handler.status.lock().unwrap().pass_count < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            a_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .any(|(k, _)| k == "b")
        );
        assert!(!crate::should_skip(
            &health,
            &bid,
            std::time::Instant::now()
        ));
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        notify.notify_one();
        worker.await.unwrap();
        assert_eq!(a_handler.status.lock().unwrap().pass_count, 2);
        router.shutdown().await.unwrap();
        a.close().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn successful_sync_records_bidirectional_acks() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let a_handler = handler(
            seeded("library", "a", "1", 1),
            1,
            &aid,
            Arc::new(Mutex::new(vec![bid.clone()])),
        );
        let b_handler = handler(
            seeded("library", "b", "2", 2),
            2,
            &bid,
            Arc::new(Mutex::new(vec![aid.clone()])),
        );
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        run(conn, true, &a_handler).await.unwrap();

        // A completed exchange means each side saw the other's records,
        // tombstones included, so both record an ack for the other.
        assert!(
            acks_of(&a_handler).contains_key(&bid),
            "initiator did not ack the responder"
        );
        assert!(
            acks_of(&b_handler).contains_key(&aid),
            "responder did not ack the initiator"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn offline_peer_learns_deletion_instead_of_resurrecting_it() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();

        // B's stale live copy is written first; A's later deletion therefore
        // has a strictly newer HLC and wins the merge.
        let b_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = b_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &aid, Some("{}".into()), 0, 2);
            s.set("library", "gone", Some("v".into()), 0, 2);
        }
        let a_store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = a_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &bid, Some("{}".into()), 0, 1);
            s.set("library", "gone", Some("v".into()), 0, 1);
            s.set("library", "gone", None, 0, 1);
        }

        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        // B has never synced, so A must keep the tombstone (not GC it).
        let ts = a_store.lock().unwrap().digest()["library"]["gone"].ts;
        a_handler.gc_tombstones(ts + crate::store::TOMBSTONE_TTL_MS + 1);
        assert!(
            a_store
                .lock()
                .unwrap()
                .domains()
                .contains(&"library".to_string()),
            "A GC'd the tombstone before B acknowledged it"
        );

        // B comes back: it must learn the deletion, not resurrect its stale
        // live copy. The tombstone wins because it is newer.
        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        run(conn, true, &a_handler).await.unwrap();

        assert!(
            b_store
                .lock()
                .unwrap()
                .records("library")
                .iter()
                .all(|(key, _)| key != "gone"),
            "B resurrected the deleted record"
        );
        assert!(
            acks_of(&a_handler).contains_key(&bid),
            "A did not ack B after the exchange"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn large_store_syncs_without_stalling() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let a_store = Arc::new(Mutex::new(Store::default()));
        let b_store = Arc::new(Mutex::new(Store::default()));
        let blob = "x".repeat(1024);
        {
            let mut s = a_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &bid, Some("{}".into()), 0, 1);
            for i in 0..3000 {
                s.set("library", &format!("a{i}"), Some(blob.clone()), 0, 1);
            }
        }
        {
            let mut s = b_store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &aid, Some("{}".into()), 0, 2);
            for i in 0..3000 {
                s.set("library", &format!("b{i}"), Some(blob.clone()), 0, 2);
            }
        }
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let conn = a.connect(b.addr(), ALPN).await.unwrap();
        tokio::time::timeout(
            std::time::Duration::from_secs(60),
            run(conn, true, &a_handler),
        )
        .await
        .expect("large sync stalled (flow control?)")
        .unwrap();
        // Both sides converge to the union.
        assert_eq!(a_store.lock().unwrap().records("library").len(), 6000);
        assert_eq!(b_store.lock().unwrap().records("library").len(), 6000);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_bidirectional_syncs_have_no_errors() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let a_store = store_with_peer(&bid, "a", "1", 1);
        let b_store = store_with_peer(&aid, "b", "2", 2);
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _ra = iroh::protocol::Router::builder(a.clone())
            .accept(ALPN, a_handler.clone())
            .spawn();
        let _rb = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..30 {
            let (ae, be, ah) = (a.clone(), b.clone(), a_handler.clone());
            tasks.spawn(async move {
                let conn = ae.connect(be.addr(), ALPN).await.unwrap();
                run(conn, true, &ah).await
            });
            let (ae, be, bh) = (a.clone(), b.clone(), b_handler.clone());
            tasks.spawn(async move {
                let conn = be.connect(ae.addr(), ALPN).await.unwrap();
                run(conn, true, &bh).await
            });
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            a_handler.status.lock().unwrap().last_error,
            None,
            "A reported a sync error"
        );
        assert_eq!(
            b_handler.status.lock().unwrap().last_error,
            None,
            "B reported a sync error"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn repeated_syncs_leave_no_responder_error() {
        let a = endpoint().await;
        let b = endpoint().await;
        let aid = a.id().to_string();
        let bid = b.id().to_string();
        let a_store = store_with_peer(&bid, "a", "1", 1);
        let b_store = store_with_peer(&aid, "b", "2", 2);
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _router = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .spawn();

        for _ in 0..50 {
            let conn = a.connect(b.addr(), ALPN).await.unwrap();
            run(conn, true, &a_handler).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert_eq!(
            b_handler.status.lock().unwrap().last_error,
            None,
            "responder reported a sync error"
        );
    }

    #[test]
    fn tombstone_survives_gc_until_the_offline_peer_syncs() {
        let peer_id = "peer-r".to_string();
        let store = Arc::new(Mutex::new(Store::default()));
        {
            let mut s = store.lock().unwrap();
            s.set(crate::DOMAIN_PEERS, &peer_id, Some("{}".into()), 0, 1);
            s.set("library", "gone", Some("v".into()), 0, 1);
            s.set("library", "gone", None, 0, 1);
        }
        let ts = store.lock().unwrap().digest()["library"]["gone"].ts;
        let peers = Arc::new(Mutex::new(vec![peer_id.clone()]));
        // Peer R has never synced (`ack = 0`).
        let acks = Arc::new(Mutex::new(crate::PeerAcks::new()));
        let handler = Arc::new(Handler::new(
            store.clone(),
            1,
            "self".into(),
            peers,
            Arc::new(Mutex::new(SyncStatus::default())),
            Arc::new(Mutex::new(None)),
            acks.clone(),
        ));

        let now = ts + crate::store::TOMBSTONE_TTL_MS + 1;
        handler.gc_tombstones(now);
        assert!(
            store
                .lock()
                .unwrap()
                .domains()
                .contains(&"library".to_string()),
            "tombstone must survive while a peer has not synced past it"
        );

        // R finally syncs: its ack clock is now past the deletion.
        acks.lock()
            .unwrap()
            .insert(peer_id.clone(), crate::Hlc::new(now, 0));
        handler.gc_tombstones(now);
        assert!(
            !store
                .lock()
                .unwrap()
                .domains()
                .contains(&"library".to_string()),
            "tombstone should be reclaimed once every peer has acked"
        );
    }

    fn peer_tombstone(key: &str, ts: u64) -> Wire {
        Wire::Records {
            domain: crate::DOMAIN_PEERS.to_string(),
            entries: vec![WireRecord {
                key: key.to_string(),
                value: None,
                ts,
                counter: 0,
                dev: 2,
                deleted: true,
            }],
        }
    }

    #[test]
    fn peer_removal_is_authoritative_even_after_a_recent_sync() {
        let base = crate::now_ms().saturating_sub(100_000);
        let mut store = Store::default();
        store.apply(
            crate::DOMAIN_PEERS,
            "peer-x",
            Record {
                value: Some("{}".into()),
                version: Version::new(base, 0, 1, false),
            },
        );
        // We last synced directly with X after the removal was created.
        let mut acks = crate::PeerAcks::new();
        acks.insert("peer-x".to_string(), Hlc::new(base + 2_000, 0));

        let applied = apply_frames(
            &mut store,
            &[peer_tombstone("peer-x", base + 1_000)],
            "self",
            &acks,
        );
        assert!(!applied.removed_us);
        assert!(
            store
                .records(crate::DOMAIN_PEERS)
                .iter()
                .all(|(k, _)| k != "peer-x"),
            "a successful sync is not permission to undo a removal"
        );
    }

    #[test]
    fn peer_tombstone_is_applied_when_we_have_not_synced_since() {
        let base = crate::now_ms().saturating_sub(100_000);
        let mut store = Store::default();
        store.apply(
            crate::DOMAIN_PEERS,
            "peer-x",
            Record {
                value: Some("{}".into()),
                version: Version::new(base, 0, 1, false),
            },
        );
        // Last direct sync predates the removal: honour it.
        let mut acks = crate::PeerAcks::new();
        acks.insert("peer-x".to_string(), Hlc::new(base + 500, 0));

        apply_frames(
            &mut store,
            &[peer_tombstone("peer-x", base + 1_000)],
            "self",
            &acks,
        );
        assert!(
            store
                .records(crate::DOMAIN_PEERS)
                .iter()
                .all(|(k, _)| k != "peer-x"),
            "a removal we have not since contradicted must be applied"
        );
    }

    #[test]
    fn self_tombstone_is_reported_as_removed() {
        let mut store = Store::default();
        let applied = apply_frames(
            &mut store,
            &[peer_tombstone("self", 5)],
            "self",
            &crate::PeerAcks::new(),
        );
        assert!(applied.removed_us);
        // We never add ourselves.
        assert!(store.records(crate::DOMAIN_PEERS).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn removal_notice_drops_the_remover() {
        let a = endpoint().await;
        let b = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec(), REMOVE_ALPN.to_vec()])
            .bind()
            .await
            .unwrap();
        let aid = a.id().to_string();
        let bid = b.id().to_string();

        let a_store = store_with_peers(&[(&bid, "Bob")]);
        let b_store = store_with_peers(&[(&aid, "Alice")]);
        let a_handler = handler(a_store.clone(), 1, &aid, allowlist(&a_store));
        let b_handler = handler(b_store.clone(), 2, &bid, allowlist(&b_store));
        let _rb = iroh::protocol::Router::builder(b.clone())
            .accept(ALPN, b_handler.clone())
            .accept(REMOVE_ALPN, Arc::new(RemoveHandler::new(b_handler.clone())))
            .spawn();

        // A removes B and tells it directly.
        crate::peers_remove(&a_store, &a_handler.peers, &bid, 1);
        notify_removed(&a, b.addr(), "Alice").await;

        assert!(
            !b_handler.peers.lock().unwrap().contains(&aid),
            "B should have dropped A after the removal notice"
        );
        assert_eq!(
            b_handler.take_removed_notice().as_deref(),
            Some("Alice"),
            "B should surface a removal notice"
        );
    }

    #[test]
    fn note_rejected_drops_the_peer_with_a_notice() {
        let store = store_with_peers(&[("peer-x", "X")]);
        let handler = handler(store.clone(), 1, "self", allowlist(&store));
        assert!(
            handler
                .peers
                .lock()
                .unwrap()
                .contains(&"peer-x".to_string())
        );
        handler.note_rejected("peer-x");
        assert!(
            !handler
                .peers
                .lock()
                .unwrap()
                .contains(&"peer-x".to_string())
        );
        assert_eq!(handler.take_removed_notice().as_deref(), Some("X"));
    }
}
