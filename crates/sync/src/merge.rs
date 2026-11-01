//! Pure data model for the sync store: per-record versions and the
//! last-write-wins (LWW) merge rule, with tombstones for deletions.
//!
//! Nothing here touches the network or storage, so the merge semantics are
//! unit-testable in isolation. The app owns domain-specific meaning; this
//! module only decides which of two versions of the same record wins.

use serde::{Deserialize, Serialize};

use crate::hlc::Hlc;

/// Version of a single record: a hybrid logical clock (`ts` = physical ms,
/// `counter` = logical) plus a stable per-device id that breaks the rare exact
/// tie (and makes LWW deterministic when two devices write at the same HLC).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub ts: u64,
    pub counter: u32,
    pub dev: u64,
    pub deleted: bool,
}

impl Version {
    pub fn new(ts: u64, counter: u32, dev: u64, deleted: bool) -> Self {
        Self {
            ts,
            counter,
            dev,
            deleted,
        }
    }

    /// Build a version from a clock reading.
    pub fn from_hlc(hlc: Hlc, dev: u64, deleted: bool) -> Self {
        Self {
            ts: hlc.physical_ms,
            counter: hlc.counter,
            dev,
            deleted,
        }
    }

    /// The clock component (for observing received versions).
    pub fn hlc(&self) -> Hlc {
        Hlc::new(self.ts, self.counter)
    }

    /// Total ordering key. Tombstones sort above present values on an exact
    /// `(ts, counter, dev)` tie, so a delete that races an identical write
    /// still wins.
    fn rank(&self) -> (u64, u32, u64, u8) {
        (self.ts, self.counter, self.dev, self.deleted as u8)
    }

    /// True when `self` should replace `other`.
    pub fn newer_than(&self, other: &Version) -> bool {
        self.rank() > other.rank()
    }
}

/// One stored record: its serialized value (`None` = tombstone) and version.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    #[serde(default)]
    pub value: Option<String>,
    pub version: Version,
}

impl Record {
    #[cfg(test)]
    pub fn present(value: String, version: Version) -> Self {
        Self {
            value: Some(value),
            version,
        }
    }

    #[cfg(test)]
    pub fn tombstone(version: Version) -> Self {
        Self {
            value: None,
            version,
        }
    }

    pub fn is_deleted(&self) -> bool {
        self.value.is_none()
    }
}

/// Decide whether `remote` should replace the current local record.
/// Returns the winning record, or `None` when the local side stays.
pub fn resolve(local: Option<&Record>, remote: &Record) -> Option<Record> {
    match local {
        Some(local) if !remote.version.newer_than(&local.version) => None,
        Some(_) | None => Some(remote.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(ts: u64, dev: u64, deleted: bool) -> Version {
        Version::new(ts, 0, dev, deleted)
    }

    #[test]
    fn newer_timestamp_wins() {
        let local = Record::present("old".into(), v(100, 1, false));
        let remote = Record::present("new".into(), v(200, 1, false));
        let won = resolve(Some(&local), &remote).unwrap();
        assert_eq!(won.value.as_deref(), Some("new"));
    }

    #[test]
    fn older_timestamp_loses() {
        let local = Record::present("new".into(), v(200, 1, false));
        let remote = Record::present("old".into(), v(100, 1, false));
        assert!(resolve(Some(&local), &remote).is_none());
    }

    #[test]
    fn device_id_breaks_ties() {
        let local = Record::present("a".into(), v(100, 7, false));
        let remote = Record::present("b".into(), v(100, 9, false));
        let won = resolve(Some(&local), &remote).unwrap();
        assert_eq!(won.value.as_deref(), Some("b"));
    }

    #[test]
    fn tombstone_wins_on_exact_tie() {
        let local = Record::present("a".into(), v(100, 5, false));
        let remote = Record::tombstone(v(100, 5, true));
        let won = resolve(Some(&local), &remote).unwrap();
        assert!(won.is_deleted());
    }

    #[test]
    fn newer_tombstone_beats_value() {
        let local = Record::present("a".into(), v(100, 5, false));
        let remote = Record::tombstone(v(200, 5, true));
        let won = resolve(Some(&local), &remote).unwrap();
        assert!(won.is_deleted());
    }

    #[test]
    fn older_tombstone_loses_to_newer_value() {
        let local = Record::present("a".into(), v(200, 5, false));
        let remote = Record::tombstone(v(100, 5, true));
        assert!(resolve(Some(&local), &remote).is_none());
    }

    #[test]
    fn first_record_is_accepted() {
        let remote = Record::present("a".into(), v(1, 1, false));
        assert!(resolve(None, &remote).is_some());
    }
}
