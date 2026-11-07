//! The account's profile directory: identity, creation, atomic retirement and
//! local cleanup of retired profiles.
//!
//! The directory is synced in two account domains: `profiles` (id →
//! `{"name":…}`) and `profile_deletions` (id → `"1"`, a permanent retirement
//! that [`Store::set`] enforces by refusing every later live write to
//! `profile:{id}:*`). The device's selection is local and never synced, so it
//! stays with the application, as do the preconditions of a deletion (which
//! profile may be removed and what to tell the user) and every per-profile
//! store the application keeps beside the sync records.
//!
//! [`Directory`] owns the retirement bookkeeping and the sequences whose order
//! matters: retirement is staged on a clone of the record owner and published
//! only after one durable commit; cleanup re-tombstones children delivered by
//! stale peers and removes this device's copy. Applications describe what else
//! belongs to a profile through [`Host`].
use crate::{Record, Store, Version};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};

pub const DOMAIN: &str = "profiles";
pub const DELETIONS_DOMAIN: &str = "profile_deletions";
/// Device-local selection record; never synced.
pub const DEVICE_KEY: &str = "profiles:device:v1";
/// Shown for the untouched default profile; applications translate it.
pub const DEFAULT_NAME: &str = "Default";
pub const MAX_NAME_CHARS: usize = 64;
const MAX_RECORD_BYTES: usize = 4096;
const NAME_ERROR: &str = "Enter a profile name of up to 64 characters.";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Profile {
    pub id: String,
    pub name: String,
}

/// IDs appear in sync domain names and paths, so they are `[A-Za-z0-9_-]{1,64}`;
/// `:` would make `profile:{id}:{domain}` ambiguous.
pub fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}

pub fn validated_name(name: &str) -> Result<String, String> {
    let name = name.trim();
    if name.is_empty()
        || name.chars().count() > MAX_NAME_CHARS
        || name.chars().any(char::is_control)
    {
        return Err(NAME_ERROR.into());
    }
    Ok(name.into())
}

/// Record policy for the two directory domains. Other domains are not judged
/// here. Unknown fields from newer peers (for example a future policy) are
/// kept, so only the fields this version reads are checked.
pub fn validate_record(domain: &str, key: &str, record: &Record) -> bool {
    if domain != DOMAIN && domain != DELETIONS_DOMAIN {
        return true;
    }
    if !valid_id(key) {
        return false;
    }
    let Some(raw) = record.value.as_deref() else {
        // Profile removal tombstones its directory row. A retirement marker is
        // permanent: deleting it would let a deleted ID come back.
        return domain == DOMAIN && record.is_deleted();
    };
    if raw.len() > MAX_RECORD_BYTES {
        return false;
    }
    if domain == DELETIONS_DOMAIN {
        return raw == "1";
    }
    #[derive(Deserialize)]
    struct Row {
        name: String,
    }
    serde_json::from_str::<Row>(raw).is_ok_and(|row| validated_name(&row.name).is_ok())
}

/// Points of [`Directory::retire`] where a test may inject a failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// After each child record has been tombstoned on the staged copy.
    AfterTombstone,
    /// Before the durable key scans and the publish.
    BeforeScan,
}

/// What the application contributes to the directory's sequences.
pub trait Host: Send + Sync + 'static {
    /// Serialization held while a profile's records and data change. Created
    /// after the record owner is locked and dropped, with it, in the order
    /// fields are declared.
    type Guard;

    fn begin_write(&self, id: &str) -> Self::Guard;
    /// Before a profile's leftovers are tombstoned and removed.
    fn retiring(&self, _id: &str) {}
    /// Keep the data of a retired profile that is still on screen for now.
    fn keep_local_data(&self, _id: &str) -> bool {
        false
    }
    /// Remove this device's application-owned data of a retired profile after
    /// its durable keys are gone. Retried until it succeeds.
    fn remove_local_data(&self, _id: &str) -> Result<(), String> {
        Ok(())
    }
    /// After [`Directory::retire`] has published, with every lock released.
    fn deleted(&self, _id: &str) {}
    /// After [`Directory::list`] settled the live set.
    fn listed(&self, _profiles: &[Profile]) {}
    fn checkpoint(&self, _stage: Stage) -> Result<(), String> {
        Ok(())
    }
}

pub struct Directory<H: Host> {
    host: H,
    default_id: &'static str,
    /// Retired IDs known to this process. Lock-free with respect to the record
    /// owner because projection predicates run while that owner is locked.
    retired: RwLock<HashSet<String>>,
    /// Retired IDs whose local data has already been removed this run.
    cleaned: RwLock<HashSet<String>>,
}

fn store() -> Result<Arc<Mutex<Store>>, String> {
    crate::local_store().map_err(|e| e.to_string())
}

impl<H: Host> Directory<H> {
    pub fn new(host: H, default_id: &'static str) -> Self {
        Self {
            host,
            default_id,
            retired: RwLock::default(),
            cleaned: RwLock::default(),
        }
    }

    pub fn host(&self) -> &H {
        &self.host
    }

    pub fn is_retired(&self, id: &str) -> bool {
        self.retired
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(id)
    }

    pub fn retire_locally(&self, id: &str) {
        self.retired
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.to_owned());
    }

    /// Live profiles ordered by ID. Also refreshes the retirement set and removes
    /// local data of profiles that were deleted here or on a peer. A row this
    /// version cannot read is skipped, not fatal: the record policy keeps such
    /// rows out, and an older store may still hold one.
    pub fn list(&self) -> Result<Vec<Profile>, String> {
        let owner = store()?;
        let guard = owner.lock().unwrap_or_else(|e| e.into_inner());
        let deleted: HashSet<String> = guard
            .records(DELETIONS_DOMAIN)
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| valid_id(id))
            .collect();
        self.retired
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .extend(deleted.iter().cloned());
        #[derive(Deserialize)]
        struct Row {
            name: String,
        }
        let mut profiles = Vec::new();
        for (id, raw) in guard.records(DOMAIN) {
            if !valid_id(&id) || deleted.contains(&id) {
                continue;
            }
            if let Ok(row) = serde_json::from_str::<Row>(&raw) {
                profiles.push(Profile { id, name: row.name });
            }
        }
        drop(guard);
        profiles.sort_by(|a, b| a.id.cmp(&b.id));
        self.cleanup_retired(deleted)?;
        self.host.listed(&profiles);
        Ok(profiles)
    }

    /// Tombstone any live child record a stale peer delivered after retirement
    /// and remove this device's copy of the profile's data. Idempotent.
    fn cleanup_retired(&self, ids: HashSet<String>) -> Result<(), String> {
        for id in ids {
            self.host.retiring(&id);
            let owner = store()?;
            let device = crate::local_device().map_err(|e| e.to_string())?;
            let mut guard = owner.lock().unwrap_or_else(|e| e.into_inner());
            let _write = self.host.begin_write(&id);
            let prefix = format!("profile:{id}:");
            let mut changed = false;
            for domain in guard.domains() {
                if domain.starts_with(&prefix) {
                    for (key, _) in guard.records(&domain) {
                        changed |= guard.set(&domain, &key, None, 0, device);
                    }
                }
            }
            if changed {
                guard.save().map_err(|e| e.to_string())?;
            }
            drop(guard);
            if changed {
                crate::request_local_sync();
            }
            if self
                .cleaned
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&id)
            {
                continue;
            }
            // Never remove the data of the profile on screen; switching comes first.
            if self.host.keep_local_data(&id) {
                continue;
            }
            let mut edits = Vec::new();
            for prefix in [
                format!("profile:{id}:"),
                format!("sync:baseline:profile:{id}:"),
            ] {
                for (key, _) in nova_storage::try_scan_prefix(&prefix).map_err(|e| e.to_string())? {
                    edits.push((key, None));
                }
            }
            nova_storage::try_write_batch(&edits).map_err(|e| e.to_string())?;
            self.host.remove_local_data(&id)?;
            self.cleaned
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .insert(id);
        }
        Ok(())
    }

    /// Write a directory row. Refuses an ID that was retired.
    pub fn write(&self, id: &str, name: &str) -> Result<(), String> {
        let owner = store()?;
        let device = crate::local_device().map_err(|e| e.to_string())?;
        let mut guard = owner.lock().unwrap_or_else(|e| e.into_inner());
        if guard
            .record(DELETIONS_DOMAIN, id)
            .is_some_and(|record| !record.is_deleted())
        {
            return Err("This profile was deleted.".into());
        }
        guard.set(
            DOMAIN,
            id,
            Some(serde_json::json!({"name": name}).to_string()),
            0,
            device,
        );
        guard.save().map_err(|e| e.to_string())?;
        drop(guard);
        crate::request_local_sync();
        Ok(())
    }

    /// `p{device}_{sequence}`: the device ID is stable per account member and
    /// the durable local sequence never repeats, so concurrent devices never
    /// collide and a retired ID is never handed out again.
    pub fn create(&self, name: &str) -> Result<Profile, String> {
        let name = validated_name(name)?;
        let device = crate::local_device().map_err(|e| e.to_string())?;
        let sequence =
            nova_storage::try_allocate_local_sequence("profiles", 1).map_err(|e| e.to_string())?;
        let id = format!("p{device:x}_{sequence:x}");
        debug_assert!(valid_id(&id));
        if self.is_retired(&id) {
            return Err("profile id was retired".into());
        }
        self.write(&id, &name)?;
        Ok(Profile { id, name })
    }

    /// Seed the default profile into `store` unless it exists or was retired.
    /// The default is scaffolding, never an explicit rename: its neutral
    /// version loses to any real edit, so a device joining later cannot
    /// overwrite a name another device chose.
    pub fn seed_default(&self, store: &mut Store, device: u64) -> bool {
        if store.record(DOMAIN, self.default_id).is_some()
            || store
                .record(DELETIONS_DOMAIN, self.default_id)
                .is_some_and(|record| !record.is_deleted())
        {
            return false;
        }
        store.apply(
            DOMAIN,
            self.default_id,
            Record {
                value: Some(serde_json::json!({"name": DEFAULT_NAME}).to_string()),
                version: Version::new(0, 0, device, false),
            },
        )
    }

    /// Retire `id` permanently: tombstone every record of the profile, write
    /// the retirement marker, drop the directory row and queue removal of its
    /// durable keys and sync baselines, all on a clone of the record owner that
    /// replaces the live store only after one durable save. A failure leaves the
    /// live store, the durable state and the retirement set untouched.
    ///
    /// The caller has checked that the profile exists, is not on screen and is
    /// not the last one.
    pub fn retire(&self, id: &str) -> Result<(), String> {
        let owner = store()?;
        let device = crate::local_device().map_err(|e| e.to_string())?;
        let mut live = owner.lock().unwrap_or_else(|e| e.into_inner());
        let write = self.host.begin_write(id);
        let mut staged = live.clone();
        let prefix = format!("profile:{id}:");
        for domain in staged.domains() {
            if domain.starts_with(&prefix) {
                for (key, _) in staged.records(&domain) {
                    staged.set(&domain, &key, None, 0, device);
                    self.host.checkpoint(Stage::AfterTombstone)?;
                }
            }
        }
        staged.set(DELETIONS_DOMAIN, id, Some("1".into()), 0, device);
        staged.set(DOMAIN, id, None, 0, device);
        self.host.checkpoint(Stage::BeforeScan)?;
        for prefix in [prefix, format!("sync:baseline:profile:{id}:")] {
            // Durable scans cannot see newly queued credentials or baselines.
            // Clear those on the proposal too, without disturbing other owners'
            // extras.
            for key in staged.queued_extra_keys_with_prefix(&prefix) {
                staged.queue_extra(&key, None);
            }
            for (key, _) in nova_storage::try_scan_prefix(&prefix).map_err(|e| e.to_string())? {
                staged.queue_extra(&key, None);
            }
        }
        // Keep the owner and the write serializer locked until durable
        // publication. A failed proposal must not leak through a later
        // unrelated save.
        crate::publish(&mut live, staged, |staged| {
            staged.save().map_err(|e| e.to_string())
        })?;
        self.retire_locally(id);
        drop(live);
        drop(write);
        self.host.deleted(id);
        crate::request_local_sync();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(value: &str) -> Record {
        Record {
            value: Some(value.into()),
            version: Version::new(1, 0, 1, false),
        }
    }
    fn tombstone() -> Record {
        Record {
            value: None,
            version: Version::new(1, 0, 1, true),
        }
    }

    #[test]
    fn names_are_trimmed_bounded_and_printable() {
        assert_eq!(validated_name("  Mina  ").unwrap(), "Mina");
        assert_eq!(validated_name(&"é".repeat(64)).unwrap().chars().count(), 64);
        for bad in ["", "   ", "tab\there", "new\nline", &"x".repeat(65)] {
            assert!(validated_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn ids_are_path_and_domain_safe() {
        for good in ["default", "p1_2", "a-b", &"x".repeat(64)] {
            assert!(valid_id(good), "{good}");
        }
        for bad in ["", "a:b", "a/b", "a b", "é", &"x".repeat(65)] {
            assert!(!valid_id(bad), "{bad}");
        }
    }

    #[test]
    fn directory_records_are_validated() {
        assert!(validate_record(DOMAIN, "p1_2", &live(r#"{"name":"Mina"}"#)));
        assert!(validate_record(
            DOMAIN,
            "p1_2",
            &live(r#"{"name":"Mina","future":{"a":1}}"#)
        ));
        assert!(validate_record(DOMAIN, "p1_2", &tombstone()));
        assert!(!validate_record(DOMAIN, "a:b", &live(r#"{"name":"Mina"}"#)));
        assert!(!validate_record(DOMAIN, "p1_2", &live(r#"{"name":""}"#)));
        assert!(!validate_record(DOMAIN, "p1_2", &live("not json")));
        assert!(!validate_record(
            DOMAIN,
            "p1_2",
            &live(&format!(r#"{{"name":"{}"}}"#, "x".repeat(MAX_RECORD_BYTES)))
        ));
        assert!(validate_record(DELETIONS_DOMAIN, "p1_2", &live("1")));
        assert!(!validate_record(DELETIONS_DOMAIN, "p1_2", &live("0")));
        assert!(!validate_record(DELETIONS_DOMAIN, "p1_2", &tombstone()));
        // Other domains are not this policy's business.
        assert!(validate_record("library", "any key", &live("anything")));
    }
}
