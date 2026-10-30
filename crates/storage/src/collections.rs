//! Independently encoded authoritative JSON rows. Collection descriptors stay
//! in kv so older readers encounter an unsupported shape rather than absence.
use super::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::LazyLock;

const ROWS: TableDefinition<&str, &[u8]> = TableDefinition::new("state_rows_v1");
const MAGIC: &str = "__nova_rows_v1";
static POLICIES: LazyLock<Mutex<BTreeMap<String, CollectionKind>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CollectionKind {
    Object,
    Array { id_field: String },
    CompositeArray { id_fields: Vec<String> },
}

/// App-owned names remain outside the storage leaf. Registration affects
/// future writes; reading an existing descriptor needs no registration.
pub fn register_collection(key: &str, kind: CollectionKind) {
    POLICIES.lock().unwrap().insert(key.to_owned(), kind);
}
pub(super) fn write_policy(
    txn: &redb::ReadTransaction,
    key: &str,
) -> Result<Option<CollectionKind>, Error> {
    if let Some(kind) = POLICIES.lock().unwrap().get(key).cloned() {
        return Ok(Some(kind));
    }
    // Removal and updates of an existing collection also work before app
    // registration (for example, a maintenance tool using the storage API).
    match txn.open_table(KV) {
        Ok(table) => table
            .get(key)
            .map_err(transaction)?
            .map(|raw| descriptor(raw.value()).map(|desc| desc.map(|d| d.kind)))
            .transpose()
            .map(Option::flatten),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(None),
        Err(error) => Err(transaction(error)),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Descriptor {
    kind: CollectionKind,
    count: usize,
    revision: u64,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
struct Row {
    order: usize,
    value: Value,
}
fn schema(message: impl ToString) -> Error {
    Error::new(ErrorKind::Schema, message)
}
fn prefix(key: &str) -> String {
    format!("r:{}:{key}", key.len())
}
fn descriptor(raw: &str) -> Result<Option<Descriptor>, Error> {
    // Ordinary scalar reads do not parse large JSON values a second time.
    if !raw.starts_with("{\"__nova_rows_v1\":") {
        return Ok(None);
    }
    let value: Value = serde_json::from_str(raw).map_err(schema)?;
    if value.as_object().is_none_or(|object| object.len() != 1) {
        return Err(schema("invalid collection descriptor"));
    }
    serde_json::from_value(value[MAGIC].clone())
        .map(Some)
        .map_err(schema)
}
fn marker(value: &Descriptor) -> Result<String, Error> {
    serde_json::to_string(&serde_json::json!({(MAGIC):value})).map_err(schema)
}
fn encode_row(row: &Row) -> Result<Vec<u8>, Error> {
    let raw = serde_json::to_string(row).map_err(schema)?;
    if raw.len() <= codec::MAX_BYTES {
        codec::encode(&raw)
    } else {
        // Retain the old unbounded plain JSON contract for unusually large
        // records; only compressed frames need a decompression bound.
        let mut bytes = vec![0];
        bytes.extend_from_slice(raw.as_bytes());
        Ok(bytes)
    }
}
fn decode_row(bytes: &[u8]) -> Result<Row, Error> {
    let raw = if let Some(raw) = bytes.strip_prefix(&[0]) {
        std::str::from_utf8(raw).map_err(schema)?.to_owned()
    } else {
        codec::decode(bytes)?
    };
    serde_json::from_str(&raw).map_err(schema)
}
fn row_identity(value: &Value, kind: &CollectionKind) -> Result<String, Error> {
    let field = |name: &str| {
        value
            .get(name)
            .and_then(Value::as_str)
            .ok_or_else(|| schema("array row requires a string identity"))
    };
    match kind {
        CollectionKind::Array { id_field } => Ok(field(id_field)?.to_owned()),
        CollectionKind::CompositeArray { id_fields } if !id_fields.is_empty() => {
            let parts = id_fields
                .iter()
                .map(|name| field(name))
                .collect::<Result<Vec<_>, _>>()?;
            serde_json::to_string(&parts).map_err(schema)
        }
        _ => Err(schema("invalid array identity policy")),
    }
}

fn parse_rows(raw: &str, kind: &CollectionKind) -> Result<BTreeMap<String, Row>, Error> {
    let value: Value = serde_json::from_str(raw).map_err(schema)?;
    match (kind, value) {
        (CollectionKind::Object, Value::Object(values)) => Ok(values
            .into_iter()
            .map(|(key, value)| (key, Row { order: 0, value }))
            .collect()),
        (
            CollectionKind::Array { .. } | CollectionKind::CompositeArray { .. },
            Value::Array(values),
        ) => {
            let mut rows = BTreeMap::new();
            for (order, value) in values.into_iter().enumerate() {
                let id = row_identity(&value, kind)?;
                if rows.insert(id, Row { order, value }).is_some() {
                    return Err(schema("duplicate collection row identity"));
                }
            }
            Ok(rows)
        }
        _ => Err(schema("unsupported collection JSON shape")),
    }
}
fn stored_rows(
    txn: &redb::ReadTransaction,
    key: &str,
    desc: &Descriptor,
) -> Result<BTreeMap<String, Row>, Error> {
    let table = match txn.open_table(ROWS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) if desc.count == 0 => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(transaction(error)),
    };
    let prefix = prefix(key);
    let mut rows = BTreeMap::new();
    for entry in table.range(prefix.as_str()..).map_err(transaction)? {
        let (id, bytes) = entry.map_err(transaction)?;
        let Some(id) = id.value().strip_prefix(&prefix) else {
            break;
        };
        let row = decode_row(bytes.value())?;
        rows.insert(id.to_owned(), row);
    }
    if rows.len() != desc.count {
        return Err(schema(
            "collection row count mismatch; original data retained",
        ));
    }
    match &desc.kind {
        CollectionKind::Array { .. } | CollectionKind::CompositeArray { .. } => {
            let mut orders = Vec::with_capacity(rows.len());
            for (id, row) in &rows {
                if row_identity(&row.value, &desc.kind)? != *id {
                    return Err(schema(
                        "collection identity mismatch; original data retained",
                    ));
                }
                orders.push(row.order);
            }
            orders.sort_unstable();
            if orders.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(schema("invalid collection order; original data retained"));
            }
        }
        CollectionKind::Object if rows.values().any(|row| row.order != 0) => {
            return Err(schema("invalid object row order; original data retained"));
        }
        CollectionKind::Object => {}
    }
    Ok(rows)
}
pub(super) fn read(
    txn: &redb::ReadTransaction,
    key: &str,
    raw: &str,
) -> Result<Option<String>, Error> {
    let Some(desc) = descriptor(raw)? else {
        return Ok(None);
    };
    let rows = stored_rows(txn, key, &desc)?;
    let value = match desc.kind {
        CollectionKind::Object => {
            Value::Object(rows.into_iter().map(|(id, row)| (id, row.value)).collect())
        }
        CollectionKind::Array { .. } | CollectionKind::CompositeArray { .. } => {
            let mut rows = rows.into_values().collect::<Vec<_>>();
            rows.sort_by_key(|row| row.order);
            Value::Array(rows.into_iter().map(|row| row.value).collect())
        }
    };
    serde_json::to_string(&value).map(Some).map_err(schema)
}

pub(super) struct Prepared {
    pub key: String,
    pub previous: Option<String>,
    marker: Option<String>,
    rows: Vec<(String, Option<Vec<u8>>)>,
    cache_row: bool,
}
impl Prepared {
    pub fn changed(&self) -> bool {
        self.previous != self.marker || !self.rows.is_empty() || self.cache_row
    }
}
pub(super) fn prepare(
    txn: &redb::ReadTransaction,
    key: &str,
    next: Option<&str>,
    kind: CollectionKind,
) -> Result<Prepared, Error> {
    let previous = match txn.open_table(KV) {
        Ok(table) => table
            .get(key)
            .map_err(transaction)?
            .map(|value| value.value().to_owned()),
        Err(redb::TableError::TableDoesNotExist(_)) => None,
        Err(error) => return Err(transaction(error)),
    };
    let desc = if next.is_none() {
        None
    } else {
        previous.as_deref().map(descriptor).transpose()?.flatten()
    };
    let cache_row = match txn.open_table(CACHE) {
        Ok(table) => table.get(key).map_err(transaction)?.is_some(),
        Err(redb::TableError::TableDoesNotExist(_)) => false,
        Err(error) => return Err(transaction(error)),
    };
    if next.is_some() && cache_row {
        return Err(schema(
            "authoritative collection has a conflicting cache row; original data retained",
        ));
    }
    if desc.as_ref().is_some_and(|desc| desc.kind != kind) {
        // Refining a single-field identity is an atomic rekey, not a lossy
        // rewrite: validate all old rows under the old descriptor first and
        // remove their physical keys in the same transaction as new rows.
        let refinement = matches!((&desc.as_ref().unwrap().kind, &kind),
            (CollectionKind::Array { id_field }, CollectionKind::CompositeArray { id_fields })
                if id_fields.contains(id_field));
        if !refinement {
            return Err(schema("collection kind changed"));
        }
    }
    let old = if next.is_none() {
        // Explicit deletion remains possible even when a payload is corrupt.
        BTreeMap::new()
    } else if let Some(desc) = &desc {
        stored_rows(txn, key, desc)?
    } else {
        // Validate legacy data before migrating; never hide an unreadable row.
        if let Some(raw) = &previous {
            parse_rows(raw, &kind)?;
        }
        if !physical_ids(txn, key)?.is_empty() {
            return Err(schema("orphaned collection rows; original data retained"));
        }
        BTreeMap::new()
    };
    let mut current = next.map(|raw| parse_rows(raw, &kind)).transpose()?;
    if matches!(
        kind,
        CollectionKind::Array { .. } | CollectionKind::CompositeArray { .. }
    ) && let Some(current) = &mut current
    {
        retain_stable_order(&old, current);
    }
    let mut changes = Vec::new();
    let row_prefix = prefix(key);
    if let Some(current) = &current {
        for (id, row) in current {
            if old.get(id) != Some(row) {
                changes.push((format!("{row_prefix}{id}"), Some(encode_row(row)?)));
            }
        }
        for id in old.keys().filter(|id| !current.contains_key(*id)) {
            changes.push((format!("{row_prefix}{id}"), None));
        }
    } else {
        changes.extend(physical_ids(txn, key)?.into_iter().map(|id| (id, None)));
    }
    let next_marker = if let Some(current) = current {
        let revision = if changes.is_empty() {
            desc.as_ref().map_or(1, |desc| desc.revision)
        } else {
            desc.as_ref()
                .map_or(1, |desc| desc.revision.saturating_add(1))
        };
        Some(marker(&Descriptor {
            kind: kind.clone(),
            count: current.len(),
            revision,
        })?)
    } else {
        None
    };
    Ok(Prepared {
        key: key.to_owned(),
        previous,
        marker: next_marker,
        rows: changes,
        cache_row,
    })
}

fn retain_stable_order(old: &BTreeMap<String, Row>, current: &mut BTreeMap<String, Row>) {
    // A removal shifts array indices but does not change the surviving titles.
    // Retain their ranks (gaps are valid) and append new ranks, avoiding needless
    // recompression. A deliberate reorder/insertion renumbers to the new order.
    let mut previous: Vec<_> = old.iter().collect();
    previous.sort_by_key(|(_, row)| row.order);
    let previous: Vec<_> = previous
        .into_iter()
        .filter(|(id, _)| current.contains_key(*id))
        .map(|(id, _)| id)
        .collect();
    let mut desired: Vec<_> = current.iter().collect();
    desired.sort_by_key(|(_, row)| row.order);
    let desired: Vec<_> = desired.into_iter().map(|(id, _)| id.clone()).collect();
    let surviving: Vec<_> = desired.iter().filter(|id| old.contains_key(*id)).collect();
    if previous != surviving
        || desired
            .iter()
            .skip_while(|id| old.contains_key(*id))
            .any(|id| old.contains_key(id))
    {
        return;
    }
    let added = desired.len() - surviving.len();
    let start = if added == 0 {
        0
    } else if let Some(max) = old.values().map(|row| row.order).max() {
        let Some(start) = max.checked_add(1) else {
            return;
        };
        if start.checked_add(added - 1).is_none() {
            return;
        }
        start
    } else {
        0
    };
    let mut rank = start;
    for id in desired {
        let row = current.get_mut(&id).unwrap();
        if let Some(previous) = old.get(&id) {
            row.order = previous.order;
        } else {
            row.order = rank;
            rank = rank.saturating_add(1);
        }
    }
}

fn physical_ids(txn: &redb::ReadTransaction, key: &str) -> Result<Vec<String>, Error> {
    let table = match txn.open_table(ROWS) {
        Ok(table) => table,
        Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
        Err(error) => return Err(transaction(error)),
    };
    let prefix = prefix(key);
    let mut ids = Vec::new();
    for entry in table.range(prefix.as_str()..).map_err(transaction)? {
        let (id, _) = entry.map_err(transaction)?;
        if !id.value().starts_with(&prefix) {
            break;
        }
        ids.push(id.value().to_owned());
    }
    Ok(ids)
}

pub(super) fn apply(txn: &redb::WriteTransaction, patch: Prepared) -> Result<bool, Error> {
    let mut kv = txn.open_table(KV).map_err(transaction)?;
    let current = kv
        .get(patch.key.as_str())
        .map_err(transaction)?
        .map(|raw| raw.value().to_owned());
    if current != patch.previous {
        // All participating writers use WRITE_SERIAL, so this indicates an
        // out-of-band writer; abort the entire transaction rather than applying
        // a stale deletion set. The caller can retry from the new snapshot.
        return Err(Error::new(
            ErrorKind::Transaction,
            "collection changed during preparation",
        ));
    }
    if !patch.changed() {
        return Ok(false);
    }
    let mut rows = txn.open_table(ROWS).map_err(transaction)?;
    for (id, value) in patch.rows {
        if let Some(value) = value {
            rows.insert(id.as_str(), value.as_slice())
                .map_err(transaction)?;
        } else {
            rows.remove(id.as_str()).map_err(transaction)?;
        }
    }
    if let Some(marker) = patch.marker {
        kv.insert(patch.key.as_str(), marker.as_str())
            .map_err(transaction)?;
    } else {
        kv.remove(patch.key.as_str()).map_err(transaction)?;
    }
    txn.open_table(CACHE)
        .map_err(transaction)?
        .remove(patch.key.as_str())
        .map_err(transaction)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redb::ReadableTableMetadata;

    fn database() -> (tempfile::TempDir, Database) {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("test.redb")).unwrap();
        (dir, db)
    }
    fn bytes(db: &Database, key: &str, id: &str) -> Vec<u8> {
        db.begin_read()
            .unwrap()
            .open_table(ROWS)
            .unwrap()
            .get(format!("{}{id}", prefix(key)).as_str())
            .unwrap()
            .unwrap()
            .value()
            .to_vec()
    }
    fn value(db: &Database, key: &str) -> Value {
        serde_json::from_str(&read_from(db, key).unwrap().unwrap().0).unwrap()
    }
    #[test]
    fn composite_identity_migrates_existing_rows_and_keeps_same_id_types() {
        let (dir, db) = database();
        let key = "test:collection:typed-array";
        register_collection(
            key,
            CollectionKind::Array {
                id_field: "id".into(),
            },
        );
        let old = serde_json::json!([{"id":"123","type_":"movie","name":"Film"}]);
        write_to(&db, &[(key.into(), Some(old.to_string()))], false).unwrap();
        register_collection(
            key,
            CollectionKind::CompositeArray {
                id_fields: vec!["type_".into(), "id".into()],
            },
        );
        let next = serde_json::json!([
            {"id":"123","type_":"movie","name":"Film"},
            {"id":"123","type_":"series","name":"Show"}
        ]);
        write_to(&db, &[(key.into(), Some(next.to_string()))], false).unwrap();
        assert_eq!(value(&db, key), next);
        let duplicates = serde_json::json!([next[0].clone(), next[0].clone()]);
        assert!(write_to(&db, &[(key.into(), Some(duplicates.to_string()))], false).is_err());
        assert_eq!(
            value(&db, key),
            next,
            "invalid writes must retain previous rows"
        );
        drop(db);
        let db = Database::open(dir.path().join("test.redb")).unwrap();
        assert_eq!(value(&db, key), next);
        let txn = db.begin_read().unwrap();
        assert_eq!(
            physical_ids(&txn, key).unwrap().len(),
            2,
            "legacy physical row removed"
        );
    }

    #[test]
    fn migration_updates_only_changed_rows_and_preserves_order_after_reopen() {
        let (dir, db) = database();
        let key = "test:collection:array";
        let old = serde_json::json!([
            {"id":"b","extension":{"future":true},"description":"metadata ".repeat(2000)},
            {"id":"a","title":"original"}
        ]);
        write_to(&db, &[(key.into(), Some(old.to_string()))], false).unwrap();
        register_collection(
            key,
            CollectionKind::Array {
                id_field: "id".into(),
            },
        );
        assert!(write_to(&db, &[(key.into(), Some(old.to_string()))], false).unwrap());
        let unchanged = bytes(&db, key, "b");
        assert_eq!(unchanged[5], 1);
        let mut next = old.clone();
        next[1]["title"] = Value::String("updated".into());
        assert!(write_to(&db, &[(key.into(), Some(next.to_string()))], false).unwrap());
        assert_eq!(bytes(&db, key, "b"), unchanged);
        assert!(
            !write_to(
                &db,
                &[(
                    key.into(),
                    Some(serde_json::to_string_pretty(&next).unwrap())
                )],
                false
            )
            .unwrap()
        );
        assert_eq!(
            scan_from(&db, "test:collection:").unwrap(),
            vec![(key.into(), next.to_string())]
        );
        // Old typed readers cannot confuse the descriptor with an empty save.
        let txn = db.begin_read().unwrap();
        let table = txn.open_table(KV).unwrap();
        assert!(
            serde_json::from_str::<Vec<Value>>(table.get(key).unwrap().unwrap().value()).is_err()
        );
        drop(table);
        drop(txn);
        drop(db);
        let db = Database::open(dir.path().join("test.redb")).unwrap();
        assert_eq!(value(&db, key), next);
    }
    #[test]
    fn objects_and_scalar_bookkeeping_commit_or_abort_together() {
        let (_dir, db) = database();
        let key = "test:collection:object";
        register_collection(key, CollectionKind::Object);
        write_to(
            &db,
            &[
                (
                    key.into(),
                    Some(r#"{"episode:a":{"position":2},"episode:b":{"position":4}}"#.into()),
                ),
                ("clock".into(), Some("1".into())),
            ],
            false,
        )
        .unwrap();
        let before = bytes(&db, key, "episode:b");
        assert!(
            write_to(
                &db,
                &[
                    (key.into(), Some("[]".into())),
                    ("clock".into(), Some("2".into()))
                ],
                false
            )
            .is_err()
        );
        assert_eq!(read_from(&db, "clock").unwrap().unwrap().0, "1");
        write_to(
            &db,
            &[
                (key.into(), Some(r#"{"episode:b":{"position":4}}"#.into())),
                ("clock".into(), Some("2".into())),
            ],
            false,
        )
        .unwrap();
        assert_eq!(bytes(&db, key, "episode:b"), before);
        assert_eq!(
            value(&db, key),
            serde_json::json!({"episode:b":{"position":4}})
        );
        assert_eq!(read_from(&db, "clock").unwrap().unwrap().0, "2");
        write_to(&db, &[(key.into(), None)], false).unwrap();
        assert!(read_from(&db, key).unwrap().is_none());
        assert_eq!(
            db.begin_read()
                .unwrap()
                .open_table(ROWS)
                .unwrap()
                .len()
                .unwrap(),
            0
        );
    }
    #[test]
    fn corrupt_rows_block_updates_but_explicit_removal_is_possible() {
        let (_dir, db) = database();
        let key = "test:collection:corrupt";
        register_collection(key, CollectionKind::Object);
        write_to(&db, &[(key.into(), Some(r#"{"a":1}"#.into()))], false).unwrap();
        let txn = db.begin_write().unwrap();
        txn.open_table(ROWS)
            .unwrap()
            .insert(format!("{}a", prefix(key)).as_str(), b"corrupt".as_slice())
            .unwrap();
        txn.commit().unwrap();
        assert!(read_from(&db, key).is_err());
        assert!(write_to(&db, &[(key.into(), Some("{}".into()))], false).is_err());
        assert_eq!(bytes(&db, key, "a"), b"corrupt");
        assert!(write_to(&db, &[(key.into(), None)], false).unwrap());
    }
    #[test]
    fn length_prefixes_keep_arbitrary_collection_names_separate() {
        let (_dir, db) = database();
        for key in ["test:prefix", "test:prefix2"] {
            register_collection(key, CollectionKind::Object);
            write_to(
                &db,
                &[(key.into(), Some(r#"{"":1,"a:b":2}"#.into()))],
                false,
            )
            .unwrap();
        }
        write_to(&db, &[("test:prefix".into(), None)], false).unwrap();
        assert_eq!(
            value(&db, "test:prefix2"),
            serde_json::json!({"":1,"a:b":2})
        );
    }
    #[test]
    fn orphaned_rows_are_retained_instead_of_publishing_an_unreadable_migration() {
        let (_dir, db) = database();
        let key = "test:collection:orphan";
        register_collection(key, CollectionKind::Object);
        write_to(&db, &[(key.into(), Some(r#"{"a":1}"#.into()))], false).unwrap();
        // Simulate an incompatible out-of-band writer replacing the descriptor.
        let txn = db.begin_write().unwrap();
        txn.open_table(KV).unwrap().insert(key, "{}").unwrap();
        txn.commit().unwrap();
        let evidence = bytes(&db, key, "a");
        assert!(write_to(&db, &[(key.into(), Some(r#"{"b":2}"#.into()))], false).is_err());
        assert_eq!(bytes(&db, key, "a"), evidence);
        assert_eq!(read_from(&db, key).unwrap().unwrap().0, "{}");
        assert!(write_to(&db, &[(key.into(), None)], false).unwrap());
        assert!(
            physical_ids(&db.begin_read().unwrap(), key)
                .unwrap()
                .is_empty()
        );
    }
    #[test]
    fn library_removal_and_append_keep_surviving_bytes_but_reordering_is_preserved() {
        let (_dir, db) = database();
        let key = "test:collection:stable-order";
        register_collection(
            key,
            CollectionKind::Array {
                id_field: "id".into(),
            },
        );
        write_to(
            &db,
            &[(
                key.into(),
                Some(r#"[{"id":"a"},{"id":"b"},{"id":"c"}]"#.into()),
            )],
            false,
        )
        .unwrap();
        let b = bytes(&db, key, "b");
        let c = bytes(&db, key, "c");
        write_to(
            &db,
            &[(
                key.into(),
                Some(r#"[{"id":"b"},{"id":"c"},{"id":"d"}]"#.into()),
            )],
            false,
        )
        .unwrap();
        assert_eq!(bytes(&db, key, "b"), b);
        assert_eq!(bytes(&db, key, "c"), c);
        assert_eq!(
            value(&db, key),
            serde_json::json!([{"id":"b"},{"id":"c"},{"id":"d"}])
        );
        write_to(
            &db,
            &[(
                key.into(),
                Some(r#"[{"id":"d"},{"id":"c"},{"id":"b"}]"#.into()),
            )],
            false,
        )
        .unwrap();
        assert_eq!(
            value(&db, key),
            serde_json::json!([{"id":"d"},{"id":"c"},{"id":"b"}])
        );
    }
}
