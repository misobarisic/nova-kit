//! Invite-ticket pairing protocol.
//!
//! The target device (`host`) creates an invite and shares its ticket; the
//! other device (`joiner`) pastes it, dials the host on this ALPN, and proves
//! knowledge of the single-use secret. On success both sides add each other as
//! peers. If the host has `require_confirmation` set, it asks the UI before
//! trusting the joiner (see [`PairHandler::ask_user`] / `respond_pair`).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use iroh::EndpointAddr;
use iroh::endpoint::{Connection, VarInt};
use iroh::protocol::ProtocolHandler;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::frame;
use crate::store::Store;
use crate::ticket::match_code;

/// Pair only devices supporting typed library keys and membership grants.
pub const ALPN: &[u8] = b"nova/pair/5";
const PROTO: u8 = 4;
/// How long an invite stays valid.
pub const INVITE_TTL_SECS: u64 = 15 * 60;
/// Keep at most this many outstanding invites.
pub const MAX_INVITES: usize = 5;
/// How long the host waits for the user to accept/reject a confirmation.
const CONFIRM_TIMEOUT: Duration = Duration::from_secs(60);

const INVITES_KEY: &str = "sync:invites";
const PENDING_JOIN_KEY: &str = "sync:pending_join";

/// One outstanding invite (ticket halves).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Invite {
    /// Short id used by the UI to cancel a specific invite.
    pub id: String,
    /// Hex-encoded 16-byte secret.
    pub secret: String,
    pub created_at: u64,
    pub expires_at: u64,
}

/// A pairing request waiting for the user's decision.
#[derive(Clone, Debug)]
pub struct IncomingPair {
    /// The joiner's endpoint id.
    pub id: String,
    pub name: String,
    /// Short code the user can compare with the other device.
    pub code: String,
}

/// Pairing lifecycle events surfaced to the app's UI.
#[derive(Clone, Debug)]
pub enum PairEvent {
    /// A device is asking to pair; `require_confirmation` is on and the app
    /// must call `respond_pair` to accept or reject.
    Incoming(IncomingPair),
    /// Pairing completed (this device was added on both sides).
    Paired { name: String },
}

pub type PairCallback = Arc<dyn Fn(PairEvent) + Send + Sync>;

#[derive(Serialize, Deserialize)]
enum Wire {
    Request {
        proto: u8,
        secret: String,
        name: String,
    },
    Ok {
        name: String,
        code: String,
    },
    Rejected {
        reason: String,
    },
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

pub fn load_invites() -> Vec<Invite> {
    nova_storage::get_str(INVITES_KEY)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

pub fn save_invites(invites: &[Invite]) {
    match serde_json::to_string(invites) {
        Ok(s) => nova_storage::set_str(INVITES_KEY, &s),
        Err(e) => tracing::error!(error = %e, "serialize invites failed"),
    }
}

/// A pending outbound join (ticket + expiry), retried until it succeeds.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PendingJoin {
    pub ticket: String,
    pub expires_at: u64,
}

pub fn load_pending_join() -> Option<PendingJoin> {
    nova_storage::get_str(PENDING_JOIN_KEY).and_then(|s| serde_json::from_str(&s).ok())
}

pub fn save_pending_join(ticket: &str) {
    let entry = PendingJoin {
        ticket: ticket.to_string(),
        expires_at: crate::now_secs() + INVITE_TTL_SECS,
    };
    if let Ok(s) = serde_json::to_string(&entry) {
        nova_storage::set_str(PENDING_JOIN_KEY, &s);
    }
}

pub fn clear_pending_join() {
    nova_storage::remove(PENDING_JOIN_KEY);
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

/// Host-side pairing state, registered on the router alongside the sync
/// handler.
pub struct PairHandler {
    /// This device's endpoint id (for the match code).
    pub host_id: String,
    /// Stable numeric device id (for the peers-domain version).
    pub device: u64,
    /// Shared record store (the new peer is written to the `peers` domain).
    pub store: Arc<Mutex<Store>>,
    pub peers: Arc<Mutex<Vec<String>>>,
    pub invites: Arc<Mutex<Vec<Invite>>>,
    pub on_pair: Arc<Mutex<Option<PairCallback>>>,
    /// In-flight confirmation prompts, keyed by joiner endpoint id.
    pub pending: Arc<Mutex<HashMap<String, oneshot::Sender<bool>>>>,
    /// Wakes the sync worker so pairing immediately fans the new device out to
    /// the rest of the mesh. Set once during engine setup.
    sync_notify: Arc<Mutex<Option<Arc<tokio::sync::Notify>>>>,
    slots: tokio::sync::Semaphore,
}

impl std::fmt::Debug for PairHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PairHandler").finish_non_exhaustive()
    }
}

impl PairHandler {
    pub fn new(
        host_id: String,
        device: u64,
        store: Arc<Mutex<Store>>,
        peers: Arc<Mutex<Vec<String>>>,
        invites: Arc<Mutex<Vec<Invite>>>,
        on_pair: Arc<Mutex<Option<PairCallback>>>,
        pending: Arc<Mutex<HashMap<String, oneshot::Sender<bool>>>>,
    ) -> Self {
        Self {
            host_id,
            device,
            store,
            peers,
            invites,
            on_pair,
            pending,
            sync_notify: Arc::new(Mutex::new(None)),
            slots: tokio::sync::Semaphore::new(4),
        }
    }

    /// Wire the sync worker's wake-up handle (see [`Self::wake_sync`]).
    pub fn set_sync_notify(&self, notify: Arc<tokio::sync::Notify>) {
        if let Ok(mut slot) = self.sync_notify.lock() {
            *slot = Some(notify);
        }
    }

    /// Ask the sync worker to run a pass now, so a just-paired device is
    /// introduced to the whole mesh without waiting for the interval.
    fn wake_sync(&self) {
        tracing::debug!(trigger = "pairing", "sync requested");
        if let Some(notify) = self.sync_notify.lock().ok().and_then(|n| n.clone()) {
            notify.notify_one();
        }
    }

    pub fn set_on_pair(&self, cb: PairCallback) {
        if let Ok(mut slot) = self.on_pair.lock() {
            *slot = Some(cb);
        }
    }

    /// Resolve a pending confirmation prompt. No-op when the id is unknown.
    pub fn respond(&self, id: &str, accept: bool) {
        let sender = self.pending.lock().ok().and_then(|mut p| p.remove(id));
        if let Some(sender) = sender {
            let _ = sender.send(accept);
        }
    }

    async fn ask_user(&self, id: &str, name: &str, code: &str) -> bool {
        let (tx, rx) = oneshot::channel();
        {
            let Ok(mut pending) = self.pending.lock() else {
                return false;
            };
            if pending.contains_key(id) {
                return false; // one prompt per joiner at a time
            }
            pending.insert(id.to_string(), tx);
        }
        let cb = self.on_pair.lock().ok().and_then(|c| c.clone());
        match cb {
            Some(cb) => cb(PairEvent::Incoming(IncomingPair {
                id: id.to_string(),
                name: name.to_string(),
                code: code.to_string(),
            })),
            None => {
                // No UI to ask; refuse rather than trust blindly.
                self.pending.lock().ok().and_then(|mut p| p.remove(id));
                return false;
            }
        }
        match tokio::time::timeout(CONFIRM_TIMEOUT, rx).await {
            Ok(Ok(accept)) => accept,
            _ => {
                self.pending.lock().ok().and_then(|mut p| p.remove(id));
                false
            }
        }
    }
}

impl ProtocolHandler for PairHandler {
    async fn accept(&self, conn: Connection) -> Result<(), iroh::protocol::AcceptError> {
        let Ok(_slot) = self.slots.try_acquire() else {
            conn.close(0u32.into(), b"pairing busy");
            return Ok(());
        };
        if let Err(e) = tokio::time::timeout(Duration::from_secs(90), handle_incoming(conn, self))
            .await
            .unwrap_or_else(|_| Err(anyhow::anyhow!("pairing timed out")))
        {
            tracing::warn!(error = %format_args!("{e:#}"), "pairing failed");
        }
        Ok(())
    }
}

async fn handle_incoming(conn: Connection, handler: &PairHandler) -> Result<()> {
    let (mut send, mut recv) = conn.accept_bi().await.context("accept pairing stream")?;
    let frame: Wire = frame::read_frame(&mut recv)
        .await?
        .context("peer closed before pairing request")?;
    let Wire::Request {
        proto,
        secret,
        name,
    } = frame
    else {
        bail!("unexpected pairing frame");
    };
    if proto != PROTO {
        bail!("unsupported pairing protocol version {proto}");
    }

    let joiner_id = conn.remote_id().to_string();
    let secret_bytes = crate::hex_decode(&secret).context("bad secret encoding")?;
    let secret: [u8; 16] = secret_bytes
        .as_slice()
        .try_into()
        .map_err(|_| anyhow::anyhow!("secret must be 16 bytes"))?;

    let invite = find_invite(&handler.invites, &secret, crate::now_secs());
    let Some(invite_id) = invite else {
        frame::write_frame(
            &mut send,
            &Wire::Rejected {
                reason: "invite is invalid or expired".to_string(),
            },
        )
        .await?;
        send.finish().context("finish pairing reply")?;
        let _ = tokio::time::timeout(CONFIRM_TIMEOUT, conn.closed()).await;
        return Ok(());
    };

    let settings = crate::read_settings();
    let code = match_code(&secret, &handler.host_id, &joiner_id);

    let accepted = if settings.require_confirmation {
        handler.ask_user(&joiner_id, &name, &code).await
    } else {
        true
    };
    if !accepted {
        frame::write_frame(
            &mut send,
            &Wire::Rejected {
                reason: "declined".to_string(),
            },
        )
        .await?;
        send.finish().context("finish pairing reply")?;
        let _ = tokio::time::timeout(CONFIRM_TIMEOUT, conn.closed()).await;
        return Ok(());
    }

    // Invite consumption and trust commit atomically. A second simultaneous
    // request cannot spend the same bearer invitation after confirmation.
    {
        let mut invites = handler.invites.lock().unwrap_or_else(|e| e.into_inner());
        anyhow::ensure!(
            invites
                .iter()
                .any(|i| i.id == invite_id && i.expires_at > crate::now_secs()),
            "invite already consumed"
        );
        let next: Vec<_> = invites
            .iter()
            .filter(|i| i.id != invite_id)
            .cloned()
            .collect();
        let mut store = handler.store.lock().unwrap_or_else(|e| e.into_inner());
        crate::peer_upsert(
            &mut store,
            &joiner_id,
            Some(&name),
            crate::now_secs(),
            handler.device,
            true,
            true,
        );
        store.queue_extra(INVITES_KEY, Some(serde_json::to_string(&next)?));
        store.save()?;
        *invites = next;
        let mut peers = handler.peers.lock().unwrap_or_else(|e| e.into_inner());
        if !peers.contains(&joiner_id) {
            peers.push(joiner_id.clone());
        }
    }
    frame::write_frame(
        &mut send,
        &Wire::Ok {
            name: crate::effective_device_name(&settings),
            code,
        },
    )
    .await?;
    send.finish().context("finish pairing reply")?;
    // Wait for the joiner to read the reply and close, so a hard close here
    // cannot discard it (the same ordering used by the sync exchange).
    let closed = tokio::time::timeout(CONFIRM_TIMEOUT, conn.closed())
        .await
        .context("joiner did not confirm installed trust")?;
    anyhow::ensure!(
        matches!(closed, iroh::endpoint::ConnectionError::ApplicationClosed(ref c) if c.error_code == 0u32.into()),
        "pairing completion was interrupted"
    );
    handler.wake_sync();

    if let Some(cb) = handler.on_pair.lock().ok().and_then(|c| c.clone()) {
        cb(PairEvent::Paired { name: name.clone() });
    }
    Ok(())
}

fn find_invite(invites: &Arc<Mutex<Vec<Invite>>>, secret: &[u8; 16], now: u64) -> Option<String> {
    let invites = invites.lock().ok()?;
    let mut found = None;
    for invite in invites.iter() {
        if invite.expires_at <= now {
            continue;
        }
        let Some(bytes) = crate::hex_decode(&invite.secret) else {
            continue;
        };
        if bytes.len() != 16 {
            continue;
        }
        // Constant-ish time compare over the 16 secret bytes.
        let mut diff = 0u8;
        for (a, b) in bytes.iter().zip(secret.iter()) {
            diff |= a ^ b;
        }
        if diff == 0 {
            found = Some(invite.id.clone());
        }
    }
    found
}

// ---------------------------------------------------------------------------
// Joiner side
// ---------------------------------------------------------------------------

/// Dial `addr`, present the secret, and return the host's name on success.
#[cfg(test)]
pub(crate) async fn initiate(
    endpoint: &iroh::Endpoint,
    addr: EndpointAddr,
    secret: &[u8; 16],
    name: &str,
    host_id: &str,
) -> Result<String> {
    initiate_with_trust(endpoint, addr, secret, name, host_id, |_| Ok(())).await
}

pub(crate) async fn initiate_with_trust(
    endpoint: &iroh::Endpoint,
    addr: EndpointAddr,
    secret: &[u8; 16],
    name: &str,
    host_id: &str,
    trust: impl FnOnce(&str) -> Result<()>,
) -> Result<String> {
    let conn = endpoint
        .connect(addr, crate::configuration::protocol().pair)
        .await
        .context("could not reach the other device")?;
    let (mut send, mut recv) = conn.open_bi().await.context("open pairing stream")?;
    frame::write_frame(
        &mut send,
        &Wire::Request {
            proto: PROTO,
            secret: crate::hex_encode(secret),
            name: name.to_string(),
        },
    )
    .await?;
    send.finish().context("finish pairing request")?;

    let reply: Wire = frame::read_frame(&mut recv)
        .await?
        .context("the other device closed the pairing")?;
    let result = match reply {
        Wire::Ok { name, code } => {
            // Sanity: the host must show the same code we compute.
            let expected = match_code(secret, host_id, &endpoint.id().to_string());
            if code != expected {
                bail!("pairing code mismatch (expected {expected}, got {code})");
            }
            trust(&name)?;
            Ok(name)
        }
        Wire::Rejected { reason } => Err(anyhow::anyhow!("pairing rejected: {reason}")),
        Wire::Request { .. } => Err(anyhow::anyhow!("unexpected pairing reply")),
    };
    conn.close(VarInt::from_u32(0), b"done");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[test]
    fn find_invite_matches_only_live_secret() {
        let now = 1000;
        let secret = crate::ticket::generate_secret();
        let invites = Arc::new(Mutex::new(vec![Invite {
            id: "abc".into(),
            secret: crate::hex_encode(&secret),
            created_at: now,
            expires_at: now + 10,
        }]));
        assert_eq!(find_invite(&invites, &secret, now).as_deref(), Some("abc"));
        assert!(find_invite(&invites, &[0u8; 16], now).is_none());
        // Expired.
        assert!(find_invite(&invites, &secret, now + 11).is_none());
    }

    async fn endpoint() -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    }

    fn invite_for(secret: &[u8; 16]) -> Invite {
        Invite {
            id: "inv".into(),
            secret: crate::hex_encode(secret),
            created_at: 0,
            expires_at: u64::MAX,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn invite_pairs_both_sides_auto_accept() {
        let host = endpoint().await;
        let joiner = endpoint().await;

        let peers = Arc::new(Mutex::new(Vec::<String>::new()));
        let secret = crate::ticket::generate_secret();
        let handler = Arc::new(PairHandler::new(
            host.id().to_string(),
            1,
            Arc::new(Mutex::new(Store::default())),
            peers.clone(),
            Arc::new(Mutex::new(vec![invite_for(&secret)])),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let _router = iroh::protocol::Router::builder(host.clone())
            .accept(ALPN, handler.clone())
            .spawn();

        let host_id = host.id().to_string();
        let result = initiate(&joiner, host.addr(), &secret, "Joiner", &host_id).await;
        assert!(result.is_ok(), "{result:?}");
        // The host added the joiner; its invite was consumed.
        assert_eq!(peers.lock().unwrap().as_slice(), &[joiner.id().to_string()]);
        assert!(handler.invites.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn wrong_secret_is_rejected() {
        let host = endpoint().await;
        let joiner = endpoint().await;

        let handler = Arc::new(PairHandler::new(
            host.id().to_string(),
            1,
            Arc::new(Mutex::new(Store::default())),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(vec![invite_for(&[1u8; 16])])),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let _router = iroh::protocol::Router::builder(host.clone())
            .accept(ALPN, handler.clone())
            .spawn();

        let host_id = host.id().to_string();
        let result = initiate(&joiner, host.addr(), &[2u8; 16], "Joiner", &host_id).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn confirmation_can_be_accepted() {
        let handler = Arc::new(PairHandler::new(
            "host".into(),
            1,
            Arc::new(Mutex::new(Store::default())),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(HashMap::new())),
        ));
        let responder = handler.clone();
        handler.set_on_pair(Arc::new(move |event| {
            if let PairEvent::Incoming(pair) = event {
                responder.respond(&pair.id, true);
            }
        }));
        assert!(handler.ask_user("joiner", "Joiner", "000000").await);
    }
}
