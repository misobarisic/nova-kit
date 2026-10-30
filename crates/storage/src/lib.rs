//! Persistent key-value storage backed by a single redb database at
//! `<data_dir>/nova.redb`.
//!
//! Durable reads/writes work from any thread (redb serializes writers
//! internally). Startup opens the database explicitly. Fallible APIs preserve
//! missing-vs-failed outcomes; convenience wrappers record diagnostic failures.

mod codec;
mod collections;
pub use collections::{CollectionKind, register_collection};

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

const KV: TableDefinition<&str, &str> = TableDefinition::new("kv");
const CACHE: TableDefinition<&str, &[u8]> = TableDefinition::new("metadata_cache_v1");
// Device-local counters are separate from exportable KV/collection records.
const LOCAL_SEQUENCES: TableDefinition<&str, u64> = TableDefinition::new("local_sequences_v1");

static DB_PATH: OnceLock<std::path::PathBuf> = OnceLock::new();
static DB: OnceLock<Result<Database, Error>> = OnceLock::new();
static LAST_ERROR: Mutex<Option<Error>> = Mutex::new(None);
// Collection diffs are prepared outside redb's writer transaction. Serialize
// participating writers so a prepared deletion set cannot become stale.
static WRITE_SERIAL: Mutex<()> = Mutex::new(());

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Unavailable,
    Transaction,
    Schema,
    Serialization,
}

#[derive(Clone, Debug)]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for Error {}
impl Error {
    pub fn new(kind: ErrorKind, message: impl ToString) -> Self {
        Self {
            kind,
            message: message.to_string(),
        }
    }
}
pub fn report(error: Error) {
    eprintln!("nova storage: {error}");
    *LAST_ERROR.lock().unwrap_or_else(|e| e.into_inner()) = Some(error);
}
pub fn last_error() -> Option<Error> {
    LAST_ERROR.lock().unwrap_or_else(|e| e.into_inner()).clone()
}
fn database() -> Result<&'static Database, Error> {
    DB.get()
        .ok_or_else(|| Error::new(ErrorKind::Unavailable, "storage not initialized"))?
        .as_ref()
        .map_err(Clone::clone)
}
fn transaction(e: impl ToString) -> Error {
    Error::new(ErrorKind::Transaction, e)
}

/// Open (or create) the database inside `data_dir`. Call once at startup
/// before any `get_str`/`set_str`.
pub fn init_at(data_dir: &Path) {
    let _ = DB_PATH.set(data_dir.join("nova.redb"));
    let result = DB.get_or_init(|| {
        let path = DB_PATH
            .get()
            .ok_or_else(|| Error::new(ErrorKind::Unavailable, "missing database path"))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| Error::new(ErrorKind::Unavailable, e))?;
        }
        Database::create(path).map_err(|e| Error::new(ErrorKind::Unavailable, e))
    });
    if let Err(e) = result {
        report(e.clone());
    }
}

pub fn get_str(key: &str) -> Option<String> {
    match try_get_str(key) {
        Ok(v) => v,
        Err(e) => {
            report(e);
            None
        }
    }
}

pub fn try_get_str(key: &str) -> Result<Option<String>, Error> {
    read_from(database()?, key).map(|v| v.map(|(value, _)| value))
}

fn read_from(db: &Database, key: &str) -> Result<Option<(String, bool)>, Error> {
    let txn = db.begin_read().map_err(transaction)?;
    read_in(&txn, key)
}

fn read_in(txn: &redb::ReadTransaction, key: &str) -> Result<Option<(String, bool)>, Error> {
    match txn.open_table(CACHE) {
        Ok(table) => {
            if let Some(value) = table.get(key).map_err(transaction)? {
                // A corrupt new row must not silently fall back to stale data.
                return codec::decode(value.value()).map(|v| Some((v, true)));
            }
        }
        Err(redb::TableError::TableDoesNotExist(_)) => {}
        Err(e) => return Err(transaction(e)),
    }
    let table = match txn.open_table(KV) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
        Err(e) => return Err(transaction(e)),
    };
    table
        .get(key)
        .map_err(transaction)?
        .map(|g| {
            collections::read(txn, key, g.value())
                .map(|value| (value.unwrap_or_else(|| g.value().to_owned()), false))
        })
        .transpose()
}

pub fn set_str(key: &str, value: &str) {
    if let Err(e) = try_set_str(key, value) {
        report(e);
    }
}
pub fn try_set_str(key: &str, value: &str) -> Result<(), Error> {
    try_write_batch(&[(key.to_string(), Some(value.to_string()))])
}

/// Store local metadata as versioned binary values, compressing worthwhile
/// records with zstd level 3. Reads retain the string/JSON API. A legacy row is
/// moved atomically on its next cache write, even if its content is unchanged.
/// Oversized values keep the existing plain format rather than becoming unreadable.
pub fn try_set_cached_str(key: &str, value: &str) -> Result<(), Error> {
    write_to(database()?, &[(key.into(), Some(value.into()))], true)?;
    clear_transaction_error();
    Ok(())
}

pub fn remove(key: &str) {
    if let Err(e) = try_write_batch(&[(key.to_string(), None)]) {
        report(e);
    }
}

/// Insert/remove many keys in a single transaction (`None` removes the key).
/// Batch writes let a subsystem with per-record rows persist a whole dirty set
/// without paying one transaction per record.
pub fn write_batch(entries: &[(String, Option<String>)]) {
    if let Err(e) = try_write_batch(entries) {
        report(e);
    }
}
pub fn try_write_batch(entries: &[(String, Option<String>)]) -> Result<(), Error> {
    write_to(database()?, entries, false)?;
    clear_transaction_error();
    Ok(())
}

/// Durably reserve a device-local monotonic ID. `initial` applies only to a
/// missing counter; subsequent allocations do not depend on clocks or callers'
/// seeds. Failure never returns an ID, so callers must not fall back to one.
pub fn try_allocate_local_sequence(name: &str, initial: u64) -> Result<u64, Error> {
    let id = allocate_local_sequence(database()?, name, initial)?;
    clear_transaction_error();
    Ok(id)
}

fn allocate_local_sequence(db: &Database, name: &str, initial: u64) -> Result<u64, Error> {
    // Read and increment under redb's exclusive writer, not a separate read
    // snapshot: concurrent engine startups must never share a boot epoch.
    let txn = db.begin_write().map_err(transaction)?;
    let next = {
        let mut table = txn.open_table(LOCAL_SEQUENCES).map_err(transaction)?;
        let previous = table.get(name).map_err(transaction)?.map(|v| v.value());
        let next = match previous {
            Some(0) => return Err(Error::new(ErrorKind::Schema, "invalid zero local sequence")),
            Some(previous) => previous
                .checked_add(1)
                .ok_or_else(|| Error::new(ErrorKind::Schema, "local sequence exhausted"))?,
            None if initial > 0 => initial,
            None => return Err(Error::new(ErrorKind::Schema, "invalid zero sequence seed")),
        };
        table.insert(name, next).map_err(transaction)?;
        next
    };
    // A failed commit may have consumed the number, but must never publish it.
    txn.commit().map_err(transaction)?;
    Ok(next)
}

fn clear_transaction_error() {
    let mut error = LAST_ERROR.lock().unwrap_or_else(|e| e.into_inner());
    if error
        .as_ref()
        .is_some_and(|e| e.kind == ErrorKind::Transaction)
    {
        *error = None;
    }
}

fn unchanged(old: Option<&(String, bool)>, value: Option<&str>, cached: bool) -> bool {
    match (old, value) {
        (None, None) => true,
        (Some((old, location)), Some(value)) => {
            *location == cached && codec::equivalent(old, value)
        }
        _ => false,
    }
}

fn write_to(
    db: &Database,
    entries: &[(String, Option<String>)],
    cached: bool,
) -> Result<bool, Error> {
    let _serial = WRITE_SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    // Resolve repeated keys to their final atomic value before comparison.
    let entries = entries
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_deref()))
        .collect::<BTreeMap<_, _>>();
    let mut changes = Vec::new();
    let mut collections = Vec::new();
    let mut needs_write = false;
    let read = db.begin_read().map_err(transaction)?;
    for (key, value) in entries {
        if let Some(kind) = collections::write_policy(&read, key)? {
            let patch = collections::prepare(&read, key, value, kind)?;
            needs_write |= patch.changed();
            collections.push(patch);
            continue;
        }
        let location = cached && value.is_some_and(|v| v.len() <= codec::MAX_BYTES);
        // Explicit removal remains possible even for a corrupt compressed row.
        let same = if value.is_none() {
            let plain = match read.open_table(KV) {
                Ok(t) => t.get(key).map_err(transaction)?.is_some(),
                Err(redb::TableError::TableDoesNotExist(_)) => false,
                Err(e) => return Err(transaction(e)),
            };
            let encoded = match read.open_table(CACHE) {
                Ok(t) => t.get(key).map_err(transaction)?.is_some(),
                Err(redb::TableError::TableDoesNotExist(_)) => false,
                Err(e) => return Err(transaction(e)),
            };
            !plain && !encoded
        } else {
            unchanged(read_in(&read, key)?.as_ref(), value, location)
        };
        needs_write |= !same;
        changes.push((key, value, location));
    }
    drop(read);
    if !needs_write {
        return Ok(false); // No write transaction, dirty pages or fsync.
    }
    // Recheck the entire final batch under the writer, including rows which
    // matched the read snapshot: concurrent workers can change those too.
    // Prepare compression before holding the writer lock.
    let changes = changes
        .into_iter()
        .map(|(key, value, location)| {
            let encoded = if location {
                Some(codec::encode(value.unwrap())?)
            } else {
                None
            };
            Ok((key, value, encoded))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let txn = db.begin_write().map_err(transaction)?;
    let mut changed = false;
    {
        let mut table = txn.open_table(KV).map_err(transaction)?;
        let mut cache = txn.open_table(CACHE).map_err(transaction)?;
        for (key, value, encoded) in changes {
            // Recheck while owning the writer: another worker may have
            // persisted an equivalent response during compression.
            let old = if value.is_some() {
                if let Some(v) = cache.get(key).map_err(transaction)? {
                    Some((codec::decode(v.value())?, true))
                } else {
                    table
                        .get(key)
                        .map_err(transaction)?
                        .map(|v| (v.value().to_string(), false))
                }
            } else {
                None
            };
            if value.is_some() && unchanged(old.as_ref(), value, encoded.is_some()) {
                continue;
            }
            match value {
                Some(value) => {
                    if let Some(encoded) = encoded {
                        cache.insert(key, encoded.as_slice()).map_err(transaction)?;
                        table.remove(key).map_err(transaction)?;
                    } else {
                        table.insert(key, value).map_err(transaction)?;
                        cache.remove(key).map_err(transaction)?;
                    }
                    changed = true;
                }
                None => {
                    let a = table.remove(key).map_err(transaction)?.is_some();
                    let b = cache.remove(key).map_err(transaction)?.is_some();
                    changed |= a || b;
                }
            }
        }
    }
    for patch in collections {
        changed |= collections::apply(&txn, patch)?;
    }
    if changed {
        txn.commit().map_err(transaction)?;
    } else {
        txn.abort().map_err(transaction)?;
    }
    Ok(changed)
}

/// All `(key, value)` pairs whose key starts with `prefix`, in key order.
pub fn scan_prefix(prefix: &str) -> Vec<(String, String)> {
    match try_scan_prefix(prefix) {
        Ok(v) => v,
        Err(e) => {
            report(e);
            Vec::new()
        }
    }
}
pub fn try_scan_prefix(prefix: &str) -> Result<Vec<(String, String)>, Error> {
    scan_from(database()?, prefix)
}

/// Read several namespaces from one redb read transaction. This avoids both
/// cross-profile export skew and decoding unrelated metadata-cache values.
pub fn try_scan_prefixes(prefixes: &[String]) -> Result<Vec<(String, String)>, Error> {
    let prefixes = prefixes.iter().map(String::as_str).collect::<Vec<_>>();
    scan_prefixes_from(database()?, &prefixes)
}

/// Read exact keys and namespaces in one snapshot without decoding other rows.
pub fn try_read_keys_and_prefixes(
    keys: &[&str],
    prefixes: &[&str],
) -> Result<Vec<(String, String)>, Error> {
    read_keys_and_prefixes_from(database()?, keys, prefixes)
}

fn scan_from(db: &Database, prefix: &str) -> Result<Vec<(String, String)>, Error> {
    scan_prefixes_from(db, &[prefix])
}

fn scan_prefixes_from(db: &Database, prefixes: &[&str]) -> Result<Vec<(String, String)>, Error> {
    read_keys_and_prefixes_from(db, &[], prefixes)
}

fn read_keys_and_prefixes_from(
    db: &Database,
    keys: &[&str],
    prefixes: &[&str],
) -> Result<Vec<(String, String)>, Error> {
    let txn = db.begin_read().map_err(transaction)?;
    let mut out = BTreeMap::new();
    for &key in keys {
        if let Some((value, _)) = read_in(&txn, key)? {
            out.insert(key.to_owned(), value);
        }
    }
    for &prefix in prefixes {
        match txn.open_table(KV) {
            Ok(table) => {
                for entry in table.range(prefix..).map_err(transaction)? {
                    let (key, value) = entry.map_err(transaction)?;
                    if !key.value().starts_with(prefix) {
                        break;
                    }
                    let value = collections::read(&txn, key.value(), value.value())?
                        .unwrap_or_else(|| value.value().to_owned());
                    out.insert(key.value().to_string(), value);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(transaction(e)),
        }
        match txn.open_table(CACHE) {
            Ok(table) => {
                for entry in table.range(prefix..).map_err(transaction)? {
                    let (key, value) = entry.map_err(transaction)?;
                    if !key.value().starts_with(prefix) {
                        break;
                    }
                    out.insert(key.value().to_string(), codec::decode(value.value())?);
                }
            }
            Err(redb::TableError::TableDoesNotExist(_)) => {}
            Err(e) => return Err(transaction(e)),
        }
    }
    Ok(out.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_sequences_survive_restart_and_ignore_lower_clock_seeds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequences.redb");
        let initial = 1 << 63;
        let db = Database::create(&path).unwrap();
        assert_eq!(
            allocate_local_sequence(&db, "playback", initial).unwrap(),
            initial
        );
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(
            allocate_local_sequence(&db, "playback", 1).unwrap(),
            initial + 1
        );
        assert_eq!(
            allocate_local_sequence(&db, "playback", u64::MAX).unwrap(),
            initial + 2
        );
        assert!(
            scan_from(&db, "").unwrap().is_empty(),
            "local counters never enter KV exports"
        );
    }

    #[test]
    fn concurrent_local_sequence_allocations_are_unique_and_durable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sequences.redb");
        let db = std::sync::Arc::new(Database::create(&path).unwrap());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers = (0..8)
            .map(|_| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    allocate_local_sequence(&db, "playback", 100).unwrap()
                })
            })
            .collect::<Vec<_>>();
        let mut ids = workers
            .into_iter()
            .map(|w| w.join().unwrap())
            .collect::<Vec<_>>();
        ids.sort_unstable();
        assert_eq!(ids, (100..108).collect::<Vec<_>>());
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(allocate_local_sequence(&db, "playback", 1).unwrap(), 108);
    }

    #[test]
    fn invalid_or_exhausted_local_sequences_fail_without_replacing_data() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("sequences.redb")).unwrap();
        assert!(allocate_local_sequence(&db, "missing", 0).is_err());
        let txn = db.begin_write().unwrap();
        {
            let mut table = txn.open_table(LOCAL_SEQUENCES).unwrap();
            table.insert("invalid", 0).unwrap();
            table.insert("exhausted", u64::MAX).unwrap();
        }
        txn.commit().unwrap();
        for name in ["invalid", "exhausted"] {
            assert_eq!(
                allocate_local_sequence(&db, name, 100).unwrap_err().kind,
                ErrorKind::Schema
            );
        }
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(LOCAL_SEQUENCES).unwrap();
        assert!(table.get("missing").unwrap().is_none());
        assert_eq!(table.get("invalid").unwrap().unwrap().value(), 0);
        assert_eq!(table.get("exhausted").unwrap().unwrap().value(), u64::MAX);
    }

    #[test]
    fn incompatible_local_sequence_schema_is_preserved() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("sequences.redb")).unwrap();
        let wrong: TableDefinition<&str, &str> = TableDefinition::new("local_sequences_v1");
        let txn = db.begin_write().unwrap();
        txn.open_table(wrong)
            .unwrap()
            .insert("playback", "unreadable")
            .unwrap();
        txn.commit().unwrap();
        assert!(allocate_local_sequence(&db, "playback", 100).is_err());
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(wrong)
                .unwrap()
                .get("playback")
                .unwrap()
                .unwrap()
                .value(),
            "unreadable"
        );
    }

    fn write(db: &Database, key: &str, value: &str, cached: bool) -> bool {
        write_to(db, &[(key.into(), Some(value.into()))], cached).unwrap()
    }

    #[test]
    fn legacy_migration_is_atomic_and_equivalent_writes_leave_file_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.redb");
        let db = Database::create(&path).unwrap();
        let value =
            serde_json::json!({"episodes": vec!["artwork"; 1000], "version": 1}).to_string();
        assert!(write(&db, "episodes:old", &value, false));
        assert!(!read_from(&db, "episodes:old").unwrap().unwrap().1);
        assert!(write(&db, "episodes:old", &value, true));
        assert!(read_from(&db, "episodes:old").unwrap().unwrap().1);
        assert!(
            db.begin_read()
                .unwrap()
                .open_table(KV)
                .unwrap()
                .get("episodes:old")
                .unwrap()
                .is_none()
        );
        let before = std::fs::read(&path).unwrap();
        let reordered = serde_json::to_string_pretty(
            &serde_json::from_str::<serde_json::Value>(&value).unwrap(),
        )
        .unwrap();
        assert!(!write(&db, "episodes:old", &reordered, true));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(db);
        let db = Database::open(&path).unwrap();
        assert_eq!(read_from(&db, "episodes:old").unwrap().unwrap().0, value);
        assert!(write_to(&db, &[("episodes:old".into(), None)], false).unwrap());
        assert!(read_from(&db, "episodes:old").unwrap().is_none());
    }

    #[test]
    fn state_stays_plain_and_batch_compares_final_values() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        assert!(write(&db, "settings", r#"{"a":1,"b":2}"#, false));
        assert!(!read_from(&db, "settings").unwrap().unwrap().1);
        assert!(
            !write_to(
                &db,
                &[
                    ("settings".into(), Some("temporary".into())),
                    ("settings".into(), Some(r#"{"b":2,"a":1}"#.into())),
                    ("missing".into(), None),
                ],
                false
            )
            .unwrap()
        );
        assert!(write(&db, "settings", r#"{"a":2,"b":2}"#, false));
        assert_eq!(
            read_from(&db, "settings").unwrap().unwrap().0,
            r#"{"a":2,"b":2}"#
        );
    }

    #[test]
    fn prefix_scan_merges_tables_in_key_order() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        write(&db, "cache:b", "new", true);
        write(&db, "cache:a", "legacy", false);
        write(&db, "other", "excluded", true);
        assert_eq!(
            scan_from(&db, "cache:").unwrap(),
            vec![
                ("cache:a".into(), "legacy".into()),
                ("cache:b".into(), "new".into())
            ]
        );
    }

    #[test]
    fn consistent_multi_prefix_snapshot_ignores_unselected_corrupt_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("snapshot.redb")).unwrap();
        write(&db, "profile:a:settings", "{}", false);
        write(&db, "profile:b:library", "[]", false);
        write(&db, "profile:c:settings", "unselected", false);
        let txn = db.begin_write().unwrap();
        txn.open_table(CACHE)
            .unwrap()
            .insert("episodes:corrupt", b"broken".as_slice())
            .unwrap();
        txn.commit().unwrap();
        assert_eq!(
            scan_prefixes_from(&db, &["profile:a:", "profile:b:"]).unwrap(),
            vec![
                ("profile:a:settings".into(), "{}".into()),
                ("profile:b:library".into(), "[]".into())
            ]
        );
        assert!(scan_from(&db, "").is_err());
    }

    #[test]
    fn exact_keys_and_prefixes_ignore_unselected_corrupt_metadata() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("snapshot.redb")).unwrap();
        write(&db, "library", "legacy", false);
        write(&db, "library:unrelated", "ignored", false);
        write(&db, "tracking:one", "cached", true);
        let txn = db.begin_write().unwrap();
        txn.open_table(CACHE)
            .unwrap()
            .insert("episodes:corrupt", b"broken".as_slice())
            .unwrap();
        txn.commit().unwrap();
        assert_eq!(
            read_keys_and_prefixes_from(&db, &["library"], &["tracking:"]).unwrap(),
            vec![
                ("library".into(), "legacy".into()),
                ("tracking:one".into(), "cached".into())
            ]
        );
        assert!(read_keys_and_prefixes_from(&db, &["episodes:corrupt"], &[]).is_err());
    }

    #[test]
    fn corrupt_cache_is_not_overwritten_or_hidden_by_legacy_row() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        let txn = db.begin_write().unwrap();
        {
            txn.open_table(KV)
                .unwrap()
                .insert("cache:bad", "stale")
                .unwrap();
            txn.open_table(CACHE)
                .unwrap()
                .insert("cache:bad", b"broken".as_slice())
                .unwrap();
        }
        txn.commit().unwrap();
        assert_eq!(
            read_from(&db, "cache:bad").unwrap_err().kind,
            ErrorKind::Schema
        );
        assert!(scan_from(&db, "cache:").is_err());
        assert!(
            write_to(
                &db,
                &[("cache:bad".into(), Some("replacement".into()))],
                true
            )
            .is_err()
        );
        assert!(write_to(&db, &[("cache:bad".into(), None)], false).unwrap());
        assert!(read_from(&db, "cache:bad").unwrap().is_none());
    }

    #[test]
    fn concurrent_equivalent_cache_writes_commit_once() {
        let dir = tempfile::tempdir().unwrap();
        let db = std::sync::Arc::new(Database::create(dir.path().join("test.redb")).unwrap());
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers = (0..8)
            .map(|_| {
                let db = db.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    write(
                        &db,
                        "episodes:concurrent",
                        &"episode metadata".repeat(1000),
                        true,
                    )
                })
            })
            .collect::<Vec<_>>();
        assert_eq!(
            workers
                .into_iter()
                .map(|w| w.join().unwrap())
                .filter(|changed| *changed)
                .count(),
            1
        );
    }
}
