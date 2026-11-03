//! Durable record store: a two-level `domain -> key -> Record` map persisted
//! to the app's redb KV store (via `nova-storage`) as a single JSON blob, plus
//! the hybrid logical clock that stamps local writes.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;
use crate::merge::{Record, Version, resolve};

/// Version digest exchanged at the start of a sync: every `(domain, key)`
/// with its version, no values. Both sides use it to compute what to send.
pub type Digest = BTreeMap<String, BTreeMap<String, Version>>;

/// A record this side wants to push to a peer.
pub struct Outbound {
    pub domain: String,
    pub key: String,
    pub record: Record,
}

/// Legacy whole-store blob key. Read once to migrate an old install, then
/// removed; new builds persist one row per record instead (below).
const LEGACY_RECORDS_KEY: &str = "sync:records";
/// Per-record row key prefix. The domain is length-prefixed so keys (which may
/// contain `:` and any separator we might pick) can never be misparsed:
/// `srec:{domain_byte_len}:{domain}{key}`.
const RECORD_PREFIX: &str = "srec:";
/// Key holding the persisted hybrid logical clock.
const CLOCK_KEY: &str = "sync:records:hlc";
const PENDING_KEY: &str = "sync:projection:pending";
/// How long a tombstone is retained before GC (covers offline peers).
pub const TOMBSTONE_TTL_MS: u64 = 30 * 24 * 60 * 60 * 1000;

/// Encode the persistence row key for `(domain, key)`.
fn record_row_key(domain: &str, key: &str) -> String {
    format!("{RECORD_PREFIX}{}:{domain}{key}", domain.len())
}

/// Decode a row key back into `(domain, key)`; `None` if malformed.
fn parse_record_row_key(row: &str) -> Option<(String, String)> {
    let rest = row.strip_prefix(RECORD_PREFIX)?;
    let (len, after) = rest.split_once(':')?;
    let dlen: usize = len.parse().ok()?;
    if after.len() < dlen || !after.is_char_boundary(dlen) {
        return None;
    }
    let (domain, key) = after.split_at(dlen);
    Some((domain.to_string(), key.to_string()))
}

/// How a tombstone may be collected. Computed from the per-peer ack map.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckFloor {
    /// No peers at all: plain TTL expiry applies.
    NoPeers,
    /// Some peer has never acknowledged: retain every tombstone.
    Blocked,
    /// Every peer has acknowledged up to this HLC; tombstones at or below it
    /// are safe to collect once past the TTL.
    At(Hlc),
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Store {
    #[serde(default)]
    domains: BTreeMap<String, BTreeMap<String, Record>>,
    /// Hybrid logical clock; persisted so monotonicity survives restarts.
    #[serde(default)]
    hlc: Hlc,
    /// Rows changed since the last `save`, so persistence writes only what
    /// moved instead of the whole store. Runtime-only (not serialized).
    #[serde(skip)]
    dirty: BTreeSet<(String, String)>,
    #[serde(default)]
    pending_domains: BTreeSet<String>,
    #[serde(skip)]
    extra: BTreeMap<String, Option<String>>,
    #[serde(skip)]
    blocked: bool,
    #[serde(skip)]
    clock_dirty: bool,
    #[serde(skip)]
    device: Option<u64>,
}

impl Store {
    /// Load from the KV store (empty when absent).
    ///
    /// A legacy whole-store `sync:records` blob is migrated to per-record rows
    /// once, then removed. A pre-HLC blob (second-resolution timestamps, no
    /// counter) is upgraded to the HLC layout first.
    ///
    /// Unreadable data is *not* silently discarded: the legacy blob (or an
    /// individual unreadable record row) is copied to a timestamped backup key
    /// and logged, so a parse failure can be recovered and never loses the
    /// peer list/history without a trace.
    pub fn load() -> Self {
        match Self::try_load() {
            Ok(store) => store,
            Err(e) => {
                nova_storage::report(nova_storage::Error::new(
                    nova_storage::ErrorKind::Schema,
                    format!("sync store unavailable: {e:#}"),
                ));
                Self {
                    blocked: true,
                    ..Self::default()
                }
            }
        }
    }

    pub fn try_load() -> Result<Self> {
        let _ = crate::configuration::protocol();
        let _ = crate::policy::policy();
        if let Some(raw) = nova_storage::try_get_str(LEGACY_RECORDS_KEY)? {
            return match legacy_to_store(&raw) {
                Ok(mut store) => {
                    // Recover poisoned legacy clocks/rows without losing the
                    // unaffected records or the original evidence.
                    let invalid: Vec<_> = store
                        .domains
                        .iter()
                        .flat_map(|(d, keys)| {
                            keys.iter()
                                .filter(move |(k, r)| {
                                    !valid_clock(r.version.hlc()) || !crate::policy::valid(d, k, r)
                                })
                                .map(move |(k, r)| {
                                    (
                                        d.clone(),
                                        k.clone(),
                                        serde_json::to_string(r).expect("record JSON"),
                                    )
                                })
                        })
                        .collect();
                    for (domain, key, raw) in invalid {
                        let row = record_row_key(&domain, &key);
                        let backup = format!(
                            "sync:quarantine:{}",
                            blake3::hash(format!("{row}\0{raw}").as_bytes()).to_hex()
                        );
                        store.queue_extra(
                            &backup,
                            Some(serde_json::json!({"key":row,"raw":raw}).to_string()),
                        );
                        store.domains.get_mut(&domain).unwrap().remove(&key);
                    }
                    store.domains.retain(|_, keys| !keys.is_empty());
                    if !valid_clock(store.hlc) {
                        let backup = format!(
                            "sync:quarantine:{}",
                            blake3::hash(format!("{LEGACY_RECORDS_KEY}\0{raw}").as_bytes())
                                .to_hex()
                        );
                        store.queue_extra(
                            &backup,
                            Some(
                                serde_json::json!({"key":LEGACY_RECORDS_KEY,"raw":raw}).to_string(),
                            ),
                        );
                        store.hlc = Hlc::default();
                    }
                    store.reconcile_clock();
                    // Write every record as a row, then drop the blob so the
                    // migration runs once.
                    store.mark_all_dirty();
                    store.queue_extra(LEGACY_RECORDS_KEY, None);
                    store.save()?;
                    tracing::info!("migrated sync records to per-record rows");
                    Ok(store)
                }
                Err(reason) => {
                    let backup = quarantine(LEGACY_RECORDS_KEY, &raw)?;
                    tracing::warn!(reason, backup, "quarantined unreadable sync records");
                    Self::load_rows()
                }
            };
        }
        Self::load_rows()
    }

    /// Rebuild the store from the per-record rows.
    fn load_rows() -> Result<Self> {
        let mut store = Self::default();
        if let Some(raw) = nova_storage::try_get_str(CLOCK_KEY)? {
            match serde_json::from_str::<Hlc>(&raw) {
                Ok(hlc) if valid_clock(hlc) => store.hlc = hlc,
                _ => {
                    quarantine(CLOCK_KEY, &raw)?;
                }
            }
        }
        let projection_metadata = nova_storage::try_get_str(PENDING_KEY)?;
        if let Some(raw) = &projection_metadata {
            store.pending_domains = serde_json::from_str(raw).context("projection metadata")?;
        }
        for (row, value) in nova_storage::try_scan_prefix(RECORD_PREFIX)? {
            let Some((domain, key)) = parse_record_row_key(&row) else {
                quarantine(&row, &value)?;
                continue;
            };
            match serde_json::from_str::<Record>(&value) {
                Ok(record)
                    if valid_clock(record.version.hlc())
                        && crate::policy::valid(&domain, &key, &record) =>
                {
                    store.domains.entry(domain).or_default().insert(key, record);
                }
                _ => {
                    quarantine(&row, &value)?;
                    tracing::warn!("quarantined invalid or future record; check sync:quarantine:");
                }
            }
        }
        store.reconcile_clock();
        store.ensure_projection_metadata(projection_metadata.is_some());
        Ok(store)
    }

    /// Older stores have no receipts proving their snapshots were applied.
    /// Replay once during migration rather than skipping potentially newer records.
    fn ensure_projection_metadata(&mut self, present: bool) {
        if !present && !self.domains.is_empty() {
            self.pending_domains.extend(self.domains.keys().cloned());
            self.clock_dirty = true;
        }
    }

    /// Mark every live record for rewrite (used by the legacy migration).
    fn mark_all_dirty(&mut self) {
        for (domain, keys) in &self.domains {
            for key in keys.keys() {
                self.dirty.insert((domain.clone(), key.clone()));
            }
        }
    }

    /// The raw record for `(domain, key)`, tombstone included. Used by the
    /// membership code to tell "absent" from "deleted".
    pub fn record(&self, domain: &str, key: &str) -> Option<&Record> {
        self.domains.get(domain).and_then(|m| m.get(key))
    }

    /// Materialize history for proven aliases of one episode. This is a view:
    /// candidate action clocks survive the merge, with no new record or HLC.
    /// Tombstoned aliases do not contribute history.
    #[cfg(feature = "nova-progress")]
    pub fn resolved_progress<'a>(&self, keys: impl IntoIterator<Item = &'a str>) -> Option<String> {
        self.resolved_progress_in("progress", keys)
    }

    /// Resolve aliases inside one profile; equal episode IDs in other profiles
    /// never participate in this view.
    #[cfg(feature = "nova-progress")]
    pub fn resolved_progress_in<'a>(
        &self,
        domain: &str,
        keys: impl IntoIterator<Item = &'a str>,
    ) -> Option<String> {
        let mut records = keys
            .into_iter()
            .filter_map(|key| self.record(domain, key))
            .filter(|record| !record.is_deleted());
        let mut merged = records.next()?.clone();
        for record in records {
            merged.value = Some(crate::progress::merge(&merged, record)?);
            if record.version.newer_than(&merged.version) {
                merged.version = record.version;
            }
        }
        merged.value
    }
    /// Move legacy records and their materialized baseline without creating new
    /// user actions. The caller queues snapshot migration before the same save.
    pub fn migrate_domain(&mut self, source: &str, target: &str) -> Result<bool> {
        if source == target {
            return Ok(false);
        }
        let baseline_source = format!("sync:baseline:{source}");
        let baseline_target = format!("sync:baseline:{target}");
        let baseline = self.extra_value(&baseline_source)?;
        let mut changed = false;
        if let Some(records) = self.domains.remove(source) {
            for (key, record) in records {
                self.dirty.insert((source.to_string(), key.clone()));
                let target_records = self.domains.entry(target.to_string()).or_default();
                if let Some(winner) = resolve(target_records.get(&key), &record) {
                    target_records.insert(key.clone(), winner);
                    self.dirty.insert((target.to_string(), key));
                }
            }
            changed = true;
        }
        if let Some(baseline) = baseline {
            if self.extra_value(&baseline_target)?.is_none() {
                self.queue_extra(&baseline_target, Some(baseline));
            }
            self.queue_extra(&baseline_source, None);
            changed = true;
        }
        if self.pending_domains.remove(source) {
            self.pending_domains.insert(target.to_string());
            self.clock_dirty = true;
            changed = true;
        }
        Ok(changed)
    }

    pub fn set_device(&mut self, device: u64) {
        self.device = Some(device);
    }

    /// Persist every record changed since the last save (plus the clock) as
    /// one batched transaction, instead of rewriting the whole store.
    pub fn save(&mut self) -> Result<()> {
        if !self.needs_save() {
            return Ok(());
        }
        #[cfg(test)]
        if nova_storage::try_get_str(CLOCK_KEY).is_err() {
            return self.save_with(|_| Ok(()));
        }
        self.save_with(nova_storage::try_write_batch)
    }

    fn save_with(
        &mut self,
        commit: impl FnOnce(&[(String, Option<String>)]) -> std::result::Result<(), nova_storage::Error>,
    ) -> Result<()> {
        anyhow::ensure!(
            !self.blocked,
            "sync store is unreadable; writes blocked for recovery"
        );
        let dirty = &self.dirty;
        let mut batch: Vec<(String, Option<String>)> = Vec::with_capacity(dirty.len() + 1);
        for (domain, key) in dirty {
            let row = record_row_key(domain, key);
            match self.domains.get(domain).and_then(|m| m.get(key)) {
                Some(record) => batch.push((row, Some(serde_json::to_string(record)?))),
                // Removed (e.g. an acked tombstone collected by GC).
                None => batch.push((row, None)),
            }
        }
        // The clock moves on every set/apply, including an apply that wins no
        // record; persist it so monotonicity survives restarts.
        batch.push((
            CLOCK_KEY.to_string(),
            Some(serde_json::to_string(&self.hlc)?),
        ));
        batch.push((
            PENDING_KEY.to_string(),
            Some(serde_json::to_string(&self.pending_domains)?),
        ));
        batch.extend(self.extra.iter().map(|(k, v)| (k.clone(), v.clone())));
        commit(&batch)?;
        self.dirty.clear();
        self.extra.clear();
        self.clock_dirty = false;
        Ok(())
    }
    pub fn needs_save(&self) -> bool {
        !self.dirty.is_empty() || !self.extra.is_empty() || self.clock_dirty
    }
    /// Retried app transactions must still publish their earlier mutations,
    /// even when preparing the same snapshot no longer changes memory.
    pub fn has_dirty_records(&self) -> bool {
        !self.dirty.is_empty()
    }

    pub fn queue_extra(&mut self, key: &str, value: Option<String>) {
        self.extra.insert(key.to_string(), value);
    }

    /// Pending extra keys in this prefix, including queued removals. This is
    /// read-only and does not scan durable storage. Transactions that delete a
    /// namespace must combine these with durable keys so queued-only writes
    /// cannot survive cleanup and publish secrets in the same save.
    pub fn queued_extra_keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.extra
            .range(prefix.to_owned()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect()
    }

    pub fn extra_value(&self, key: &str) -> Result<Option<String>> {
        match self.extra.get(key) {
            Some(v) => Ok(v.clone()),
            None => Ok(nova_storage::try_get_str(key)?),
        }
    }

    pub fn pending_domains(&self) -> Vec<String> {
        self.pending_domains.iter().cloned().collect()
    }

    /// Queue idempotent runtime publication in the same transaction as a local
    /// import. The caller must save before waking materialization workers.
    pub fn mark_pending(&mut self, domain: &str) {
        if self.pending_domains.insert(domain.to_owned()) {
            self.clock_dirty = true;
        }
    }

    pub fn mark_projected(
        &mut self,
        domain: &str,
        basis: &BTreeMap<String, Version>,
    ) -> Result<()> {
        if self.domain_matches_digest(domain, basis) {
            if self.pending_domains.remove(domain) {
                self.clock_dirty = true;
            }
            self.save()?;
        }
        Ok(())
    }

    /// Ensure the clock is at least as new as every stored record, so a local
    /// write always supersedes what we already hold (and the digest snapshot
    /// covers every record).
    fn reconcile_clock(&mut self) {
        let mut max = self.hlc;
        for keys in self.domains.values() {
            for record in keys.values() {
                let hlc = record.version.hlc();
                if hlc.newer_than(max) {
                    max = hlc;
                }
            }
        }
        self.hlc = max;
    }

    /// Record a local write. `observed_secs` is the caller's observation time
    /// (0 means "stamp now"); it is used only as a lower bound on the HLC, so
    /// a local edit always supersedes the last known version. Returns true when
    /// the value actually changed (a no-op re-notify doesn't tick the clock, so
    /// restarts don't re-push unchanged data).
    pub fn set(
        &mut self,
        domain: &str,
        key: &str,
        value: Option<String>,
        observed_secs: u64,
        dev: u64,
    ) -> bool {
        self.device = Some(dev);
        // A deleted profile ID is never reused. Delayed local actors must not
        // publish new child actions after the account-level retirement marker.
        if value.is_some()
            && let Some((profile, _)) = crate::parse_profile_domain(domain)
            && self
                .record("profile_deletions", profile)
                .is_some_and(|r| !r.is_deleted())
        {
            return false;
        }
        if let Some(cur) = self.domains.get(domain).and_then(|m| m.get(key))
            && cur.value == value
        {
            return false;
        }
        let hlc = self.hlc.tick(
            crate::now_ms(),
            observed_secs
                .saturating_mul(1000)
                .min(crate::now_ms().saturating_add(crate::hlc::MAX_DRIFT_MS)),
        );
        self.clock_dirty = true;
        let deleted = value.is_none();
        let version = Version::from_hlc(hlc, dev, deleted);
        let value = value.map(|value| {
            crate::policy::local(domain, key, self.record(domain, key), value, version)
        });
        let record = Record { value, version };
        if !crate::policy::valid(domain, key, &record) {
            return false;
        }
        self.domains
            .entry(domain.to_string())
            .or_default()
            .insert(key.to_string(), record);
        self.dirty.insert((domain.to_string(), key.to_string()));
        true
    }

    /// Apply a record received from a peer. Returns true when it superseded
    /// the local version. The remote clock is folded into ours first so a
    /// later local write always sorts after it (causality).
    pub fn apply(&mut self, domain: &str, key: &str, mut remote: Record) -> bool {
        if !valid_clock(remote.version.hlc()) || !crate::policy::valid(domain, key, &remote) {
            nova_storage::report(nova_storage::Error::new(
                nova_storage::ErrorKind::Schema,
                "peer clock exceeds permitted drift; correct device clock",
            ));
            return false;
        }
        self.hlc.observe(remote.version.hlc(), crate::now_ms());
        self.clock_dirty = true;
        if let Some(local) = self.record(domain, key)
            && !local.is_deleted()
            && !remote.is_deleted()
            && let Some(value) = crate::policy::merge(domain, key, local, &remote)
        {
            if local.value.as_ref() == Some(&value) && !remote.version.newer_than(&local.version) {
                return false;
            }
            let winner = if remote.version.newer_than(&local.version) {
                &remote
            } else {
                local
            };
            if winner.value.as_ref() != Some(&value) {
                let dev = self.device.unwrap_or(winner.version.dev);
                remote.version = Version::from_hlc(self.hlc.tick(crate::now_ms(), 0), dev, false);
            } else {
                remote.version = winner.version;
            }
            remote.value = Some(value);
        }
        if !crate::policy::valid(domain, key, &remote) {
            return false;
        }
        let slot = self.domains.entry(domain.to_string()).or_default();
        let resolved = if domain == crate::DOMAIN_PEERS {
            crate::resolve_peer(slot.get(key), &remote)
        } else {
            resolve(slot.get(key), &remote)
        };
        match resolved {
            Some(record) => {
                slot.insert(key.to_string(), record);
                self.dirty.insert((domain.to_string(), key.to_string()));
                self.pending_domains.insert(domain.to_string());
                true
            }
            None => false,
        }
    }

    /// Remove a legacy physical record whose authority has been retained by
    /// the caller in the same transaction (for example a migration barrier).
    pub fn remove_migrated_record(&mut self, domain: &str, key: &str) {
        if self
            .domains
            .get_mut(domain)
            .is_some_and(|rows| rows.remove(key).is_some())
        {
            self.dirty.insert((domain.to_owned(), key.to_owned()));
            self.pending_domains.insert(domain.to_owned());
        }
    }

    /// Atomically rekey legacy records without inventing user edits. Capture
    /// every source before removing any: an opaque old ID may equal another
    /// row's new key. The next save commits removals and merged targets together.
    pub fn migrate_record_keys(&mut self, domain: &str, keys: &[(String, String)]) {
        let records: Vec<_> = keys
            .iter()
            .filter(|(from, to)| from != to)
            .filter_map(|(from, to)| {
                self.record(domain, from)
                    .cloned()
                    .map(|record| (from.clone(), to.clone(), record))
            })
            .collect();
        for (from, _, _) in &records {
            self.remove_migrated_record(domain, from);
        }
        for (_, to, record) in records {
            self.apply(domain, &to, record);
        }
    }

    /// Digest plus the clock reading it was taken at. The ack for an exchange
    /// is this HLC: every record in the digest has a version at or below it.
    pub fn snapshot(&self) -> (Digest, Hlc) {
        (self.digest(), self.hlc)
    }

    /// Cheap optimistic-read token. Record writes and remote observations
    /// advance this clock, without requiring a copy of the complete digest.
    pub fn clock(&self) -> Hlc {
        self.hlc
    }

    /// Snapshot only one domain, including tombstones. An absent domain is
    /// distinct from a present domain with no records.
    pub fn domain_digest(&self, domain: &str) -> Option<BTreeMap<String, Version>> {
        self.domains.get(domain).map(|keys| {
            keys.iter()
                .map(|(key, record)| (key.clone(), record.version))
                .collect()
        })
    }

    fn domain_matches_digest(&self, domain: &str, basis: &BTreeMap<String, Version>) -> bool {
        self.domains.get(domain).is_some_and(|keys| {
            keys.len() == basis.len()
                && keys
                    .iter()
                    .zip(basis.iter())
                    .all(|((key, record), (other, version))| {
                        key == other && record.version == *version
                    })
        })
    }

    pub fn digest(&self) -> Digest {
        self.domains
            .iter()
            .map(|(domain, keys)| {
                (
                    domain.clone(),
                    keys.iter().map(|(k, r)| (k.clone(), r.version)).collect(),
                )
            })
            .collect()
    }

    /// Records this side should send to a peer: every record the peer lacks
    /// or has an older version of.
    pub fn outbound(&self, peer: &Digest) -> Vec<Outbound> {
        let mut out = Vec::new();
        for (domain, keys) in &self.domains {
            let peer_keys = peer.get(domain);
            for (key, record) in keys {
                let send = match peer_keys.and_then(|m| m.get(key)) {
                    // A membership removal can outrank a later name update.
                    // Exchange unequal versions so that authorization, rather
                    // than transport recency, decides the winner on both ends.
                    Some(version) if domain == crate::DOMAIN_PEERS => record.version != *version,
                    Some(version) => record.version.newer_than(version),
                    None => true,
                };
                if send {
                    out.push(Outbound {
                        domain: domain.clone(),
                        key: key.clone(),
                        record: record.clone(),
                    });
                }
            }
        }
        out
    }

    /// Live values for a domain (tombstones omitted), for the app to
    /// materialize its own types from.
    pub fn records(&self, domain: &str) -> Vec<(String, String)> {
        self.domains
            .get(domain)
            .map(|m| {
                m.iter()
                    .filter_map(|(k, r)| r.value.clone().map(|v| (k.clone(), v)))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Read one materialized record family without cloning the whole domain.
    /// Large integrations keep independent episode fields in the same domain.
    pub fn records_with_prefix(&self, domain: &str, prefix: &str) -> Vec<(String, String)> {
        self.domains
            .get(domain)
            .map(|records| {
                records
                    .range(prefix.to_owned()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .filter_map(|(key, record)| {
                        record
                            .value
                            .as_ref()
                            .map(|value| (key.clone(), value.clone()))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Versions for one record family, including tombstones. Durable
    /// capabilities use clocks so removing and restoring identical values
    /// cannot revive previously revoked authority.
    pub fn versions_with_prefix(&self, domain: &str, prefix: &str) -> Vec<(String, Version)> {
        self.domains
            .get(domain)
            .map(|records| {
                records
                    .range(prefix.to_owned()..)
                    .take_while(|(key, _)| key.starts_with(prefix))
                    .map(|(key, record)| (key.clone(), record.version))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Domains that currently hold at least one record (live or tombstone).
    pub fn domains(&self) -> Vec<String> {
        self.domains.keys().cloned().collect()
    }

    /// Drop tombstones older than `max_age_ms`, but never one that a peer has
    /// not acknowledged. `floor` comes from the per-peer ack map: `At(h)` means
    /// every current peer has seen records up to `h`, so a tombstone with
    /// `version <= h` is safe; `Blocked` retains everything; `NoPeers` applies
    /// plain TTL expiry. Live records are always kept.
    pub fn gc(&mut self, now_ms: u64, max_age_ms: u64, floor: AckFloor) -> usize {
        if floor == AckFloor::Blocked {
            return 0;
        }
        let mut removed: Vec<(String, String)> = Vec::new();
        for (domain, keys) in self.domains.iter_mut() {
            // A removed device may return after every current peer has acked.
            // Only explicit pairing may supersede this authorization barrier.
            if domain == crate::DOMAIN_PEERS {
                continue;
            }
            keys.retain(|key, r| {
                if !r.is_deleted() || now_ms.saturating_sub(r.version.ts) <= max_age_ms {
                    return true;
                }
                let keep = match floor {
                    AckFloor::NoPeers => false,
                    // Keep when the tombstone is newer than the ack floor.
                    AckFloor::At(h) => r.version.hlc().newer_than(h),
                    AckFloor::Blocked => true,
                };
                if !keep {
                    // Persist the removal (drops the row) on the next save.
                    removed.push((domain.clone(), key.clone()));
                }
                keep
            });
        }
        self.domains.retain(|_, keys| !keys.is_empty());
        let count = removed.len();
        self.dirty.extend(removed);
        count
    }
}

pub(crate) fn valid_clock(clock: Hlc) -> bool {
    clock.physical_ms <= crate::now_ms().saturating_add(crate::hlc::MAX_DRIFT_MS)
}

fn quarantine(key: &str, raw: &str) -> Result<String> {
    // Content addressing makes recovery idempotent; never use the active row
    // prefix for evidence. Delete the bad row only in the same durable batch.
    let hash = blake3::hash(format!("{key}\0{raw}").as_bytes());
    let backup = format!("sync:quarantine:{}", hash.to_hex());
    let evidence = serde_json::json!({"key": key, "raw": raw}).to_string();
    nova_storage::try_write_batch(&[(backup.clone(), Some(evidence)), (key.to_string(), None)])?;
    Ok(backup)
}

/// Parse a legacy whole-store blob into a `Store`, applying the pre-HLC
/// migration. Errors are returned as a short reason for the quarantine log.
fn legacy_to_store(raw: &str) -> Result<Store, &'static str> {
    let mut value = serde_json::from_str::<serde_json::Value>(raw).map_err(|_| "invalid JSON")?;
    migrate_legacy(&mut value);
    let mut store = serde_json::from_value::<Store>(value).map_err(|_| "unexpected schema")?;
    store.reconcile_clock();
    store.ensure_projection_metadata(false);
    Ok(store)
}

/// One-time migration of a pre-HLC `sync:records` blob: records have
/// second-resolution `ts` and no `counter`. Scale timestamps to milliseconds
/// and add a zero counter. Returns true when anything changed.
fn migrate_legacy(value: &mut serde_json::Value) -> bool {
    let Some(domains) = value.get_mut("domains").and_then(|d| d.as_object_mut()) else {
        return false;
    };
    let mut migrated = false;
    for keys in domains.values_mut() {
        let Some(keys) = keys.as_object_mut() else {
            continue;
        };
        for record in keys.values_mut() {
            let Some(version) = record.get_mut("version").and_then(|v| v.as_object_mut()) else {
                continue;
            };
            if !version.contains_key("counter") {
                version.insert("counter".to_string(), serde_json::json!(0));
                let ts = version.get("ts").and_then(|t| t.as_u64()).unwrap_or(0);
                version.insert("ts".to_string(), serde_json::json!(ts.saturating_mul(1000)));
                migrated = true;
            }
            version.entry("deleted").or_insert(serde_json::json!(false));
        }
    }
    migrated
}

#[cfg(test)]
mod tests {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    struct AllocationCounts {
        calls: usize,
        bytes: usize,
    }
    thread_local! {
        static ALLOCATION_COUNTS: std::cell::Cell<Option<AllocationCounts>> = const { std::cell::Cell::new(None) };
    }
    struct CountingAllocator;
    #[global_allocator]
    static TEST_ALLOCATOR: CountingAllocator = CountingAllocator;
    fn record_allocation(bytes: usize) {
        let _ = ALLOCATION_COUNTS.try_with(|slot| {
            if let Some(mut counts) = slot.get() {
                counts.calls += 1;
                counts.bytes += bytes;
                slot.set(Some(counts));
            }
        });
    }
    unsafe impl std::alloc::GlobalAlloc for CountingAllocator {
        unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
            let result = unsafe { std::alloc::System.alloc(layout) };
            if !result.is_null() {
                record_allocation(layout.size());
            }
            result
        }
        unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
            let result = unsafe { std::alloc::System.alloc_zeroed(layout) };
            if !result.is_null() {
                record_allocation(layout.size());
            }
            result
        }
        unsafe fn dealloc(&self, pointer: *mut u8, layout: std::alloc::Layout) {
            unsafe {
                std::alloc::System.dealloc(pointer, layout);
            }
        }
        unsafe fn realloc(
            &self,
            pointer: *mut u8,
            layout: std::alloc::Layout,
            size: usize,
        ) -> *mut u8 {
            let result = unsafe { std::alloc::System.realloc(pointer, layout, size) };
            if !result.is_null() {
                record_allocation(size);
            }
            result
        }
    }
    fn count_allocations<T>(f: impl FnOnce() -> T) -> (T, AllocationCounts) {
        struct Restore(Option<AllocationCounts>);
        impl Drop for Restore {
            fn drop(&mut self) {
                ALLOCATION_COUNTS.with(|slot| slot.set(self.0));
            }
        }
        let previous =
            ALLOCATION_COUNTS.with(|slot| slot.replace(Some(AllocationCounts::default())));
        let _restore = Restore(previous);
        let result = f();
        let counts = ALLOCATION_COUNTS.with(|slot| slot.get().unwrap());
        (result, counts)
    }

    #[test]
    fn domain_digest_preserves_full_digest_semantics_tombstones_and_empty_domains() {
        let mut store = super::Store::default();
        store.set("profile:alice:progress", "live", Some("value".into()), 0, 1);
        store.set("profile:alice:progress", "deleted", None, 0, 1);
        store.set("profile:bob:library", "live", Some("other".into()), 0, 2);
        store
            .domains
            .insert("empty".into(), std::collections::BTreeMap::new());
        for domain in [
            "profile:alice:progress",
            "profile:bob:library",
            "empty",
            "absent",
        ] {
            assert_eq!(
                store.domain_digest(domain),
                store.digest().get(domain).cloned()
            );
        }
        let digest = store.domain_digest("profile:alice:progress").unwrap();
        assert_eq!(digest.len(), 2);
        assert!(digest["deleted"].deleted);
        assert_eq!(
            store.domain_digest("empty"),
            Some(std::collections::BTreeMap::new())
        );
        assert_eq!(store.domain_digest("absent"), None);
    }

    #[test]
    fn domain_comparison_rejects_every_stale_key_or_version_without_global_digest() {
        let mut store = super::Store::default();
        store.set("selected", "a", Some("one".into()), 0, 1);
        let basis = store.domain_digest("selected").unwrap();
        assert!(store.domain_matches_digest("selected", &basis));
        assert!(!store.domain_matches_digest("absent", &std::collections::BTreeMap::new()));
        store
            .domains
            .insert("empty".into(), std::collections::BTreeMap::new());
        assert!(store.domain_matches_digest("empty", &std::collections::BTreeMap::new()));
        store.set("unrelated", "a", Some("changed".into()), 0, 2);
        assert!(store.domain_matches_digest("selected", &basis));
        let mut changed = store.clone();
        changed.set("selected", "a", Some("two".into()), 0, 1);
        assert!(!changed.domain_matches_digest("selected", &basis));
        let mut added = store.clone();
        added.set("selected", "b", Some("two".into()), 0, 1);
        assert!(!added.domain_matches_digest("selected", &basis));
        let mut removed = store.clone();
        removed.remove_migrated_record("selected", "a");
        assert!(!removed.domain_matches_digest("selected", &basis));
        let mut replaced = removed.clone();
        replaced.set("selected", "other", Some("one".into()), 0, 1);
        assert!(!replaced.domain_matches_digest("selected", &basis));
        let mut tombstoned = store.clone();
        tombstoned.set("selected", "a", None, 0, 1);
        assert!(!tombstoned.domain_matches_digest("selected", &basis));
        let tombstone_basis = tombstoned.domain_digest("selected").unwrap();
        assert!(tombstoned.domain_matches_digest("selected", &tombstone_basis));
        tombstoned.set("selected", "a", Some("resurrected".into()), 0, 1);
        assert!(!tombstoned.domain_matches_digest("selected", &tombstone_basis));
    }

    #[test]
    fn domain_equality_check_allocates_nothing_for_matches_and_mismatches() {
        let mut store = super::Store::default();
        for i in 0..1000 {
            store.set("unrelated", &format!("key-{i}"), Some("value".into()), 0, 1);
        }
        store.set("selected", "live", Some("value".into()), 0, 1);
        store.set("selected", "deleted", None, 0, 1);
        let basis = store.domain_digest("selected").unwrap();
        let empty = std::collections::BTreeMap::new();
        let (matches, allocations) = count_allocations(|| {
            (
                store.domain_matches_digest("selected", &basis),
                store.domain_matches_digest("selected", &empty),
                store.domain_matches_digest("absent", &empty),
            )
        });
        assert_eq!(matches, (true, false, false));
        assert_eq!(allocations, AllocationCounts::default());
    }

    #[test]
    fn projection_acknowledgement_retains_original_basis_and_save_behavior() {
        let mut store = super::Store::default();
        store.set("selected", "a", Some("one".into()), 0, 1);
        let old = store.domain_digest("selected").unwrap();
        store.set("selected", "a", Some("two".into()), 0, 1);
        store.mark_pending("selected");
        store.queue_extra("unrelated-extra", Some("keep".into()));
        // A blocked Store makes an attempted save observable without I/O.
        store.blocked = true;
        let before = format!("{store:?}");
        store.mark_projected("selected", &old).unwrap();
        assert_eq!(
            format!("{store:?}"),
            before,
            "stale basis must not attempt save or mutate pending/extras"
        );
        let current = store.domain_digest("selected").unwrap();
        assert!(
            store.mark_projected("selected", &current).is_err(),
            "matching basis must retain the existing save call"
        );
        assert!(!store.pending_domains().contains(&"selected".to_string()));
        assert!(
            store.extra.contains_key("unrelated-extra"),
            "failed save retains dirty extras under existing Store contract"
        );
        store.mark_pending("absent");
        let before = format!("{store:?}");
        store
            .mark_projected("absent", &std::collections::BTreeMap::new())
            .unwrap();
        assert_eq!(format!("{store:?}"), before, "absent is not present-empty");
    }

    #[test]
    fn queued_extra_prefix_keys_include_pending_removals_without_mutating_store() {
        let mut store = super::Store::default();
        store.queue_extra("profile:alice:tracking:credentials", Some("private".into()));
        store.queue_extra("profile:alice:removed", None);
        store.queue_extra("profile:alice2:tracking:credentials", Some("other".into()));
        store.queue_extra(
            "sync:baseline:profile:alice:tracking",
            Some("baseline".into()),
        );
        let before = format!("{store:?}");
        assert_eq!(
            store.queued_extra_keys_with_prefix("profile:alice:"),
            vec![
                "profile:alice:removed",
                "profile:alice:tracking:credentials"
            ]
        );
        assert_eq!(
            store.queued_extra_keys_with_prefix("sync:baseline:profile:alice:"),
            vec!["sync:baseline:profile:alice:tracking"]
        );
        assert!(store.queued_extra_keys_with_prefix("absent:").is_empty());
        assert_eq!(format!("{store:?}"), before);
    }

    #[test]
    fn staged_prefix_removal_preserves_failed_live_extras_and_unrelated_work() {
        let mut live = super::Store::default();
        live.queue_extra("profile:alice:credentials", Some("private".into()));
        live.queue_extra(
            "sync:baseline:profile:alice:tracking",
            Some("baseline".into()),
        );
        live.queue_extra("profile:bob:credentials", Some("keep".into()));
        let before = format!("{live:?}");
        let mut staged = live.clone();
        for prefix in ["profile:alice:", "sync:baseline:profile:alice:"] {
            for key in staged.queued_extra_keys_with_prefix(prefix) {
                staged.queue_extra(&key, None);
            }
        }
        assert!(
            staged
                .save_with(|_| Err(nova_storage::Error::new(
                    nova_storage::ErrorKind::Transaction,
                    "injected failure"
                )))
                .is_err()
        );
        assert_eq!(format!("{live:?}"), before);
        // An unrelated successful retry of the original live store still owns
        // the original writes, never the failed candidate's removals.
        let mut batch = Vec::new();
        live.save_with(|rows| {
            batch = rows.to_vec();
            Ok(())
        })
        .unwrap();
        assert!(batch.contains(&("profile:alice:credentials".into(), Some("private".into()))));
        assert!(batch.contains(&(
            "sync:baseline:profile:alice:tracking".into(),
            Some("baseline".into())
        )));
        staged
            .save_with(|rows| {
                batch = rows.to_vec();
                Ok(())
            })
            .unwrap();
        assert!(batch.contains(&("profile:alice:credentials".into(), None)));
        assert!(batch.contains(&("sync:baseline:profile:alice:tracking".into(), None)));
        assert!(batch.contains(&("profile:bob:credentials".into(), Some("keep".into()))));
    }

    #[test]
    #[cfg(feature = "nova-progress")]
    fn profile_progress_keeps_actions_and_aliases_separate() {
        let mut store = Store::default();
        let alice = crate::profile_domain("alice", "progress");
        let bob = crate::profile_domain("bob", "progress");
        let progress = |watched| {
            serde_json::json!({"watched": watched, "position_secs": 25,
            "duration_secs": 100, "updated_at_secs": 1, "play_count": 1})
            .to_string()
        };
        store.set(&alice, "episode", Some(progress(true)), 0, DEV_A);
        store.set(&bob, "episode", Some(progress(false)), 0, DEV_A);
        let watched: serde_json::Value =
            serde_json::from_str(&store.resolved_progress_in(&alice, ["episode"]).unwrap())
                .unwrap();
        let unwatched: serde_json::Value =
            serde_json::from_str(&store.resolved_progress_in(&bob, ["episode"]).unwrap()).unwrap();
        assert_eq!(watched["watched"], true);
        assert_eq!(unwatched["watched"], false);
        assert!(store.resolved_progress(["episode"]).is_none());
        assert_eq!(
            store.outbound(&Digest::new()).len(),
            2,
            "every profile is exchanged"
        );
        let mut peer = Store::default();
        for outbound in store.outbound(&Digest::new()) {
            peer.apply(&outbound.domain, &outbound.key, outbound.record);
        }
        assert_eq!(peer.records(&alice), store.records(&alice));
        assert_eq!(peer.records(&bob), store.records(&bob));
        let invalid = Record {
            value: Some(r#"{"__nova_progress_v1": false}"#.into()),
            version: Version::new(crate::now_ms(), 1, DEV_B, false),
        };
        assert!(!peer.apply(&alice, "invalid", invalid));
    }

    #[test]
    fn permanent_profile_deletion_blocks_delayed_local_actions_and_survives_gc() {
        let mut store = Store::default();
        let domain = crate::profile_domain("alice", "library");
        store.set(&domain, "show", Some("old".into()), 0, DEV_A);
        store.set(
            "profiles",
            "alice",
            Some(r#"{"name":"Alice"}"#.into()),
            0,
            DEV_A,
        );
        store.set("profile_deletions", "alice", Some("1".into()), 0, DEV_A);
        store.set("profiles", "alice", None, 0, DEV_A);
        assert!(!store.set(&domain, "show", Some("late actor".into()), 0, DEV_A));
        assert_eq!(
            store.record(&domain, "show").unwrap().value.as_deref(),
            Some("old")
        );
        store.gc(u64::MAX, 0, AckFloor::NoPeers);
        assert!(store.record("profiles", "alice").is_none());
        assert_eq!(
            store
                .record("profile_deletions", "alice")
                .unwrap()
                .value
                .as_deref(),
            Some("1")
        );
        assert!(!store.set(&domain, "other", Some("late actor".into()), 0, DEV_A));
    }

    #[test]
    fn legacy_domain_migration_retains_versions_baseline_and_pending_receipt() {
        let mut store = Store::default();
        let target = crate::profile_domain("default", "progress");
        let value =
            serde_json::json!({"watched":true,"position_secs":0,"play_count":1}).to_string();
        store.set("progress", "episode", Some(value.clone()), 0, DEV_A);
        let original = store.record("progress", "episode").unwrap().clone();
        let original_clock = store.clock();
        store.pending_domains.insert("progress".into());
        store.queue_extra(
            "sync:baseline:progress",
            Some("{\"episode\":\"snapshot\"}".into()),
        );
        store.queue_extra(&format!("sync:baseline:{target}"), None);
        assert!(store.migrate_domain("progress", &target).unwrap());
        assert_eq!(store.record(&target, "episode"), Some(&original));
        assert!(store.record("progress", "episode").is_none());
        assert_eq!(store.clock(), original_clock);
        assert!(store.pending_domains().contains(&target));
        assert!(!store.pending_domains().contains(&"progress".to_string()));
        assert!(
            store
                .extra_value("sync:baseline:progress")
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store
                .extra_value(&format!("sync:baseline:{target}"))
                .unwrap()
                .as_deref(),
            Some("{\"episode\":\"snapshot\"}")
        );
        store
            .save_with(|batch| {
                assert!(batch.contains(&(record_row_key("progress", "episode"), None)));
                assert!(
                    batch
                        .iter()
                        .any(|(key, value)| key == &record_row_key(&target, "episode")
                            && value.is_some())
                );
                Ok(())
            })
            .unwrap();
        assert!(!store.needs_save());
    }

    #[test]
    fn prefix_records_are_live_and_confined_to_the_requested_family() {
        let mut store = super::Store::default();
        for key in [
            "title:a:saved",
            "title:a:watched:1",
            "title:ab:saved",
            "other",
        ] {
            store.set("integration", key, Some(key.into()), 0, 1);
        }
        store.set("integration", "title:a:watched:1", None, 0, 1);
        assert_eq!(
            store.records_with_prefix("integration", "title:a:"),
            vec![("title:a:saved".into(), "title:a:saved".into())]
        );
        assert!(store.records_with_prefix("missing", "title:a:").is_empty());
    }
    use super::*;

    const DEV_A: u64 = 10;
    const DEV_B: u64 = 20;

    #[test]
    fn record_row_key_round_trips_awkward_keys() {
        for (domain, key) in [
            ("library", "tt123"),
            ("presence", "abc\x01def"),
            ("weird:domain", "key:with:colons"),
            ("d", ""),
            ("", "just-key"),
        ] {
            let row = record_row_key(domain, key);
            assert_eq!(
                parse_record_row_key(&row),
                Some((domain.to_string(), key.to_string())),
                "round trip failed for {domain:?}/{key:?} via {row:?}"
            );
        }
        assert_eq!(parse_record_row_key("nope"), None);
        assert_eq!(parse_record_row_key("srec:zz:x"), None);
        assert_eq!(parse_record_row_key("srec:9:short"), None);
        assert_eq!(parse_record_row_key("srec:1:é:key"), None);
    }

    #[test]
    fn failed_commit_retains_the_complete_batch_for_retry() {
        let mut store = Store::default();
        store.set("library", "a", Some("1".into()), 0, DEV_A);
        store.queue_extra("library", Some("snapshot".into()));
        store.queue_extra(LEGACY_RECORDS_KEY, None);
        let mut failed = Vec::new();
        assert!(
            store
                .save_with(|batch| {
                    failed = batch.to_vec();
                    Err(nova_storage::Error::new(
                        nova_storage::ErrorKind::Transaction,
                        "injected commit failure",
                    ))
                })
                .is_err()
        );
        assert!(store.needs_save());
        store
            .save_with(|batch| {
                assert_eq!(batch, failed);
                Ok(())
            })
            .unwrap();
        assert!(!store.needs_save());
        assert!(
            failed
                .iter()
                .any(|(k, v)| k == LEGACY_RECORDS_KEY && v.is_none())
        );
        assert!(
            failed
                .iter()
                .any(|(k, v)| k == "library" && v.as_deref() == Some("snapshot"))
        );
    }

    #[test]
    fn future_record_is_rejected_without_poisoning_local_order() {
        let mut store = Store::default();
        let future = Record::present("future".into(), Version::new(u64::MAX, 0, DEV_B, false));
        assert!(!store.apply("library", "a", future));
        store.set("library", "a", Some("local".into()), 0, DEV_A);
        assert!(valid_clock(
            store.record("library", "a").unwrap().version.hlc()
        ));
    }

    #[test]
    #[cfg(feature = "nova-progress")]
    fn three_peer_progress_gossip_converges_without_reinventing_actions() {
        let mut peers = [Store::default(), Store::default(), Store::default()];
        for (i, store) in peers.iter_mut().enumerate() {
            let dev = (i + 1) as u64;
            store.set_device(dev);
            let version = Version::new(crate::now_ms(), 0, dev, false);
            let value = serde_json::json!({"watched":i==0,"unwatched_at_secs":if i==1 {1} else {0},
                "position_secs":if i==0 {100} else if i==1 {0} else {12},
                "duration_secs":100,"updated_at_secs":1,"play_count":dev})
            .to_string();
            let value = crate::progress::local(None, value, version);
            assert!(store.apply(
                "progress",
                "episode",
                Record {
                    value: Some(value),
                    version
                }
            ));
        }
        for _ in 0..6 {
            let records: Vec<_> = peers
                .iter()
                .map(|p| p.record("progress", "episode").unwrap().clone())
                .collect();
            for store in &mut peers {
                for record in &records {
                    store.apply("progress", "episode", record.clone());
                }
            }
        }
        let record = peers[0].record("progress", "episode").unwrap().clone();
        for store in &mut peers {
            assert_eq!(store.record("progress", "episode").unwrap(), &record);
            assert!(!store.apply("progress", "episode", record.clone()));
        }
        let value: serde_json::Value =
            serde_json::from_str(record.value.as_ref().unwrap()).unwrap();
        assert_eq!(value["watched"], false);
        assert_eq!(value["position_secs"], 12);
        assert_eq!(value["play_count"], 3);
    }

    #[test]
    #[cfg(feature = "nova-progress")]
    fn alias_progress_views_keep_action_order_without_mutating_records() {
        let mut store = Store::default();
        let value = |watched, unwatch, position| {
            serde_json::json!({
                "watched":watched, "unwatched_at_secs":unwatch, "position_secs":position,
                "duration_secs":100, "updated_at_secs":1, "play_count":1
            })
            .to_string()
        };
        store.set("progress", "native", Some(value(false, 0, 80)), 0, DEV_A);
        store.set("progress", "canonical", Some(value(true, 0, 80)), 0, DEV_B);
        let decode =
            |raw: Option<String>| serde_json::from_str::<serde_json::Value>(&raw.unwrap()).unwrap();
        let before = store.snapshot();
        let forward = store.resolved_progress(["native", "canonical"]);
        assert_eq!(forward, store.resolved_progress(["canonical", "native"]));
        assert_eq!(decode(forward)["watched"], true);
        assert_eq!(store.snapshot(), before);
        store.set("progress", "native", Some(value(false, 1, 0)), 0, DEV_A);
        assert_eq!(
            decode(store.resolved_progress(["native", "canonical"]))["watched"],
            false
        );
        store.set("progress", "canonical", Some(value(true, 0, 80)), 0, DEV_B);
        assert_eq!(
            decode(store.resolved_progress(["native", "canonical"]))["watched"],
            true
        );
        store.set("progress", "canonical", None, 0, DEV_B);
        assert_eq!(
            decode(store.resolved_progress(["native", "canonical"]))["watched"],
            false
        );
        assert!(store.resolved_progress(["missing", "canonical"]).is_none());
    }

    #[test]
    fn missing_projection_receipts_require_one_replay() {
        let mut store = Store::default();
        store.set("library", "a", Some("local".into()), 0, 7);
        assert!(store.pending_domains().is_empty());
        store.ensure_projection_metadata(true);
        assert!(store.pending_domains().is_empty());
        store.ensure_projection_metadata(false);
        assert_eq!(store.pending_domains(), vec!["library"]);
        let basis = store.digest()["library"].clone();
        store.mark_projected("library", &basis).unwrap();
        assert!(store.pending_domains().is_empty());
        store.ensure_projection_metadata(true);
        assert!(store.pending_domains().is_empty());
    }

    #[test]
    fn legacy_blob_parses_and_migrates_pre_hlc() {
        // Seconds-resolution `ts`, no `counter`: migrated to ms + counter.
        let raw = r#"{"domains":{"library":{"a":{"value":"1","version":{"ts":1700000000,"dev":7,"deleted":false}}}},"hlc":{"physical_ms":1700000000000,"counter":0}}"#;
        let store = legacy_to_store(raw).unwrap();
        assert_eq!(store.pending_domains(), vec!["library"]);
        let v = store.digest()["library"]["a"];
        assert_eq!(v.ts, 1_700_000_000_000);
        assert_eq!(v.counter, 0);
        assert!(!v.deleted);
        // Unreadable input is reported, not panicked on.
        assert!(legacy_to_store("{not json").is_err());
        assert!(legacy_to_store("\"a string\"").is_err());
    }

    #[test]
    fn set_skips_unchanged_values() {
        let mut s = Store::default();
        assert!(s.set("library", "a", Some("1".into()), 100, DEV_A));
        assert!(!s.set("library", "a", Some("1".into()), 200, DEV_A));
        assert!(s.set("library", "a", Some("2".into()), 200, DEV_A));
    }

    #[test]
    fn local_set_is_monotonic_even_with_stale_clock() {
        let mut s = Store::default();
        s.set("d", "k", Some("a".into()), 0, DEV_A);
        let first = s.digest()["d"]["k"];
        // A later local write with an older caller timestamp must still win.
        s.set("d", "k", Some("b".into()), 1, DEV_A);
        assert!(s.digest()["d"]["k"].newer_than(&first));
    }

    #[test]
    fn outbound_only_includes_missing_or_newer() {
        let mut a = Store::default();
        a.set("d", "same", Some("x".into()), 0, DEV_A);
        a.set("d", "newer", Some("y".into()), 0, DEV_A);
        let a_digest = a.digest();
        // Peer version map: "same" matches exactly, "newer" is stale.
        let mut keys = BTreeMap::new();
        keys.insert("same".to_string(), a_digest["d"]["same"]);
        keys.insert("newer".to_string(), Version::new(1, 0, DEV_B, false));
        let mut peer = Digest::new();
        peer.insert("d".to_string(), keys);

        let out = a.outbound(&peer);
        let sent: Vec<&str> = out.iter().map(|o| o.key.as_str()).collect();
        assert!(sent.contains(&"newer"));
        assert!(!sent.contains(&"same"));
    }

    #[test]
    fn apply_merges_and_hides_tombstones() {
        let mut a = Store::default();
        a.set("d", "k", Some("v".into()), 0, DEV_A);
        let future = crate::now_ms() + crate::hlc::MAX_DRIFT_MS - 1000;
        let rec = Record::present("remote".into(), Version::new(future, 0, DEV_B, false));
        assert!(a.apply("d", "k", rec));
        assert_eq!(
            a.records("d"),
            vec![("k".to_string(), "remote".to_string())]
        );
        let del = Record::tombstone(Version::new(future, 0, DEV_B, true));
        // Same (ts, counter, dev, deleted=false vs true) tie: tombstone ranks
        // above, so a tombstone with an equal version still wins.
        assert!(a.apply("d", "k", del));
        assert!(a.records("d").is_empty());
    }

    #[test]
    fn gc_drops_old_acked_tombstones() {
        let mut s = Store::default();
        s.set("d", "k", Some("v".into()), 0, DEV_A);
        s.set("d", "k", None, 0, DEV_A);
        let version = s.digest()["d"]["k"];
        let now = version.ts + TOMBSTONE_TTL_MS + 1;
        // Older than the TTL and every peer acked at/after it, so it can go.
        s.gc(now, TOMBSTONE_TTL_MS, AckFloor::At(version.hlc()));
        assert!(s.domains().is_empty());
    }

    #[test]
    fn gc_retains_tombstone_not_yet_acked_by_all_peers() {
        let mut s = Store::default();
        s.set("d", "k", Some("v".into()), 0, DEV_A);
        s.set("d", "k", None, 0, DEV_A);
        let version = s.digest()["d"]["k"];
        let now = version.ts + TOMBSTONE_TTL_MS + 1;
        // A peer last synced just before the deletion: dropping it could let
        // that device resurrect the record.
        let before = Hlc::new(version.ts.saturating_sub(1), version.counter);
        s.gc(now, TOMBSTONE_TTL_MS, AckFloor::At(before));
        assert!(s.domains().contains(&"d".to_string()));
        assert!(s.records("d").is_empty());
        // A peer that never synced likewise blocks collection.
        s.gc(now, TOMBSTONE_TTL_MS, AckFloor::Blocked);
        assert!(s.domains().contains(&"d".to_string()));
    }

    #[test]
    fn gc_keeps_fresh_tombstones_and_live_records() {
        let mut s = Store::default();
        s.set("d", "live", Some("v".into()), 0, DEV_A);
        s.set("d", "gone", None, 0, DEV_A);
        let version = s.digest()["d"]["gone"];
        // Within the TTL window: retained even though fully acked.
        s.gc(version.ts + 1, TOMBSTONE_TTL_MS, AckFloor::NoPeers);
        assert_eq!(s.records("d"), vec![("live".to_string(), "v".to_string())]);
        assert!(s.domains().contains(&"d".to_string()));
        // Past the TTL with no peers: the tombstone goes, the live record stays.
        s.gc(
            version.ts + TOMBSTONE_TTL_MS + 1,
            TOMBSTONE_TTL_MS,
            AckFloor::NoPeers,
        );
        assert_eq!(s.records("d"), vec![("live".to_string(), "v".to_string())]);
    }

    #[test]
    fn migrate_legacy_scales_seconds_and_adds_counter() {
        // A pre-HLC blob: second timestamps, no counter, no clock.
        let mut value = serde_json::json!({
            "domains": {
                "library": {
                    "a": { "value": "x", "version": { "ts": 1_700_000_000u64, "dev": 7, "deleted": false } }
                }
            }
        });
        assert!(migrate_legacy(&mut value));
        let store: Store = serde_json::from_value(value).unwrap();
        let v = store.digest()["library"]["a"];
        assert_eq!(v.ts, 1_700_000_000_000);
        assert_eq!(v.counter, 0);
        // Idempotent: a second pass changes nothing.
        let mut value2 = serde_json::to_value(&store).unwrap();
        assert!(!migrate_legacy(&mut value2));
    }
}
