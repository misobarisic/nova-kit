//! Device-local smart-download admission and physical ownership. The caller
//! serializes this decision with manifest mutation; no separate spend counter
//! can drift from the durable manifest after a crash.
use crate::{DownloadJob, DownloadPhase};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const GIB: u64 = 1024 * 1024 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct DevicePolicy {
    pub enabled_profiles: BTreeSet<String>,
    pub allowance_bytes: u64,
    pub charging_only: bool,
    pub suppressed: BTreeMap<String, String>,
    pub receipts: BTreeMap<String, RetryReceipt>,
    pub metadata_refreshes: BTreeMap<String, u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
impl Default for DevicePolicy {
    fn default() -> Self {
        Self {
            enabled_profiles: BTreeSet::new(),
            allowance_bytes: 5 * GIB,
            charging_only: false,
            suppressed: BTreeMap::new(),
            receipts: BTreeMap::new(),
            metadata_refreshes: BTreeMap::new(),
            extra: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryReceipt {
    pub fingerprint: String,
    pub attempt: u8,
    pub next: Option<u64>,
    pub message: String,
}
impl RetryReceipt {
    pub fn failure(&mut self, now: u64, transient: bool) {
        self.next = transient
            .then(|| {
                [300, 1800, 7200]
                    .get(self.attempt as usize)
                    .map(|delay| now.saturating_add(*delay))
            })
            .flatten();
        self.attempt = self.attempt.saturating_add(1);
    }
    pub fn blocked(&self, fingerprint: &str, now: u64) -> bool {
        self.fingerprint == fingerprint
            && (self.next.is_none() || self.next.is_some_and(|at| at > now))
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Environment {
    pub connected: bool,
    pub wifi_or_ethernet: bool,
    pub unmetered: bool,
    pub charging: bool,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Network,
    Charging,
    Budget,
    FreeSpace,
    Disabled,
    NeedsSource,
    User,
    Background,
}
impl DevicePolicy {
    pub fn environmental_pause(&self, env: Environment) -> Option<PauseReason> {
        if !env.connected || !env.wifi_or_ethernet || !env.unmetered {
            Some(PauseReason::Network)
        } else if self.charging_only && !env.charging {
            Some(PauseReason::Charging)
        } else {
            None
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Owner {
    pub profile: String,
    pub policy: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct JobOwnership {
    pub manual_keep: bool,
    pub manual_origin: bool,
    pub pinned: bool,
    pub owners: BTreeSet<Owner>,
    pub reserved_bytes: u64,
    pub source_owner: String,
    pub source_revision: String,
    pub pause_reason: Option<PauseReason>,
    pub retry_attempt: u8,
    pub retry_at: Option<u64>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, serde_json::Value>,
}
impl Default for JobOwnership {
    fn default() -> Self {
        Self {
            manual_keep: true,
            manual_origin: true,
            pinned: false,
            owners: BTreeSet::new(),
            reserved_bytes: 0,
            source_owner: String::new(),
            source_revision: String::new(),
            pause_reason: None,
            retry_attempt: 0,
            retry_at: None,
            extra: BTreeMap::new(),
        }
    }
}
impl JobOwnership {
    pub fn automatic(&self) -> bool {
        !self.manual_keep && !self.owners.is_empty()
    }
    pub fn may_evict(&self, playing: bool) -> bool {
        !self.manual_keep && !self.pinned && self.owners.is_empty() && !playing
    }
    pub fn retry(&mut self, now: u64) {
        let delay = [300, 1800, 7200].get(self.retry_attempt as usize).copied();
        self.retry_at = delay.map(|delay| now.saturating_add(delay));
        self.retry_attempt = self.retry_attempt.saturating_add(1);
    }
}
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Ledger {
    pub automatic_bytes: u64,
    pub manual_bytes: u64,
    pub outstanding: u64,
}
impl Ledger {
    pub fn from_jobs(jobs: &[DownloadJob]) -> Self {
        // A shared torrent bundle/path is charged once. Reservations are per
        // selected physical job, never per personal policy reference.
        let mut physical = BTreeMap::<String, (u64, bool, u64)>::new();
        let mut ledger = Self::default();
        for job in jobs {
            let key = job
                .torrent_directory
                .as_ref()
                .or(job.artifact_path.as_ref())
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| job.id.clone());
            let entry = physical.entry(key).or_default();
            entry.0 = entry.0.max(job.bytes_downloaded);
            entry.1 |= job.offline.manual_keep;
            if !job.offline.manual_keep && job.phase != DownloadPhase::Completed {
                entry.2 = entry.2.max(
                    job.offline
                        .reserved_bytes
                        .saturating_sub(job.bytes_downloaded),
                );
            }
        }
        for (bytes, manual, outstanding) in physical.values() {
            if *manual {
                ledger.manual_bytes = ledger.manual_bytes.saturating_add(*bytes);
            } else {
                ledger.automatic_bytes = ledger.automatic_bytes.saturating_add(*bytes);
                ledger.outstanding = ledger.outstanding.saturating_add(*outstanding);
            }
        }
        ledger
    }
    pub fn admit(
        &self,
        estimate: Option<u64>,
        policy: &DevicePolicy,
        free: u64,
        capacity: u64,
    ) -> Result<u64, PauseReason> {
        let estimate = estimate
            .filter(|n| *n > 0)
            .ok_or(PauseReason::NeedsSource)?;
        // Five percent margin is an estimate, never described as exact.
        let reservation = estimate
            .checked_add(estimate / 20)
            .ok_or(PauseReason::Budget)?;
        if self
            .automatic_bytes
            .saturating_add(self.outstanding)
            .saturating_add(reservation)
            > policy.allowance_bytes
        {
            return Err(PauseReason::Budget);
        }
        if free
            .saturating_sub(self.outstanding)
            .saturating_sub(reservation)
            < GIB.max(capacity / 20)
        {
            return Err(PauseReason::FreeSpace);
        }
        Ok(reservation)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn no_double_spend_after_restart_or_shared_policy() {
        let mut a = DownloadJob::default();
        a.id = "a".into();
        a.bytes_downloaded = 100;
        a.offline.manual_keep = false;
        a.offline.reserved_bytes = 500;
        a.offline.owners.insert(Owner {
            profile: "a".into(),
            policy: "show".into(),
        });
        a.offline.owners.insert(Owner {
            profile: "b".into(),
            policy: "show".into(),
        });
        let raw = serde_json::to_string(&vec![a]).unwrap();
        let jobs: Vec<DownloadJob> = serde_json::from_str(&raw).unwrap();
        let ledger = Ledger::from_jobs(&jobs);
        assert_eq!(ledger.automatic_bytes, 100);
        assert_eq!(ledger.outstanding, 400);
        let policy = DevicePolicy {
            allowance_bytes: 1000,
            ..Default::default()
        };
        assert_eq!(
            ledger.admit(Some(600), &policy, 100 * GIB, 100 * GIB),
            Err(PauseReason::Budget)
        );
        assert!(
            ledger
                .admit(Some(400), &policy, 100 * GIB, 100 * GIB)
                .is_ok()
        );
    }
    #[test]
    fn shared_bundle_reservation_is_charged_once_and_manual_pin_prevents_eviction() {
        let mut first = DownloadJob {
            id: "first".into(),
            torrent_directory: Some("bundle".into()),
            bytes_downloaded: 100,
            ..Default::default()
        };
        first.offline.manual_keep = false;
        first.offline.manual_origin = false;
        first.offline.reserved_bytes = 500;
        let mut sibling = first.clone();
        sibling.id = "sibling".into();
        let ledger = Ledger::from_jobs(&[first.clone(), sibling.clone()]);
        assert_eq!((ledger.automatic_bytes, ledger.outstanding), (100, 400));
        sibling.offline.manual_keep = true;
        sibling.offline.pinned = true;
        let ledger = Ledger::from_jobs(&[first, sibling.clone()]);
        assert_eq!(
            (
                ledger.automatic_bytes,
                ledger.outstanding,
                ledger.manual_bytes
            ),
            (0, 0, 100)
        );
        sibling.offline.manual_keep = false;
        assert!(!sibling.offline.may_evict(false));
        let policy = DevicePolicy::default();
        assert_eq!(
            Ledger::default().admit(Some(GIB), &policy, 5 * GIB, 100 * GIB),
            Err(PauseReason::FreeSpace)
        );
    }

    #[test]
    fn metered_wifi_unknown_connectivity_and_power_fail_closed() {
        let mut env = Environment::default();
        let mut p = DevicePolicy::default();
        assert_eq!(p.environmental_pause(env), Some(PauseReason::Network));
        env.connected = true;
        env.wifi_or_ethernet = true;
        assert_eq!(p.environmental_pause(env), Some(PauseReason::Network));
        env.unmetered = true;
        assert_eq!(p.environmental_pause(env), None);
        p.charging_only = true;
        assert_eq!(p.environmental_pause(env), Some(PauseReason::Charging));
        env.charging = true;
        assert_eq!(p.environmental_pause(env), None);
    }
    #[test]
    fn partials_unknown_sizes_manual_keep_and_bounded_retry() {
        let p = DevicePolicy::default();
        assert_eq!(
            Ledger::default().admit(None, &p, 100 * GIB, 100 * GIB),
            Err(PauseReason::NeedsSource)
        );
        let mut ownership = JobOwnership::default();
        assert!(!ownership.may_evict(false));
        ownership.manual_keep = false;
        assert!(ownership.may_evict(false));
        assert!(!ownership.may_evict(true));
        ownership.retry(0);
        assert_eq!(ownership.retry_at, Some(300));
        ownership.retry(300);
        assert_eq!(ownership.retry_at, Some(2100));
        ownership.retry(2100);
        assert_eq!(ownership.retry_at, Some(9300));
        ownership.retry(9300);
        assert_eq!(ownership.retry_at, None);
    }
}
