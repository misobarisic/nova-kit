//! Store persistence across restarts: a legacy whole-store `sync:records` blob
//! is migrated once to per-record rows, and subsequent writes round-trip
//! without rewriting the whole store.
//!
//! Lives in `tests/` so it gets its own process: `nova-storage` opens a
//! process-wide database once, and the crate's unit tests share the same
//! process.

use nova_sync::Store;

#[test]
fn legacy_blob_migrates_and_rows_round_trip() {
    let dir = std::env::temp_dir().join(format!("nova-sync-store-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    nova_storage::init_at(&dir);

    // A pre-existing whole-store blob (HLC layout) with a live record and a
    // tombstone.
    let legacy = r#"{
        "domains": {
            "library": { "a": { "value": "1", "version": { "ts": 1700000000000, "counter": 0, "dev": 7, "deleted": false } } },
            "progress": { "gone": { "value": null, "version": { "ts": 1700000001000, "counter": 1, "dev": 8, "deleted": true } } }
        },
        "hlc": { "physical_ms": 1700000002000, "counter": 0 }
    }"#;
    nova_storage::set_str("sync:records", legacy);

    let store = Store::load();
    assert_eq!(
        store.records("library"),
        vec![("a".to_string(), "1".to_string())]
    );
    assert!(store.record("progress", "gone").unwrap().is_deleted());
    // The blob is consumed so the migration runs only once...
    assert!(nova_storage::get_str("sync:records").is_none());

    // ...and a reload rebuilds the same state from the per-record rows.
    let reloaded = Store::load();
    assert_eq!(reloaded.records("library"), store.records("library"));
    assert!(reloaded.record("progress", "gone").unwrap().is_deleted());
    assert_eq!(reloaded.snapshot().1, store.snapshot().1);

    // A later single-record write persists just its row and survives a reload.
    let mut s = Store::load();
    s.set("library", "b", Some("2".to_string()), 0, 9);
    s.save().unwrap();
    let after = Store::load();
    assert_eq!(
        after.record("library", "b").unwrap().value.as_deref(),
        Some("2")
    );
    assert_eq!(
        after.record("library", "a").unwrap().value.as_deref(),
        Some("1")
    );

    // GC must persist even if this domain still has a live record.
    let mut s = Store::try_load().unwrap();
    let gone = s.record("progress", "gone").unwrap().version;
    s.set("progress", "live", Some("1".into()), 0, 9);
    assert_eq!(
        s.gc(
            gone.ts + 31 * 24 * 60 * 60 * 1000,
            30 * 24 * 60 * 60 * 1000,
            nova_sync::AckFloor::NoPeers
        ),
        1
    );
    s.save().unwrap();
    let after = Store::try_load().unwrap();
    assert!(after.record("progress", "gone").is_none());
    assert!(after.record("progress", "live").is_some());

    // Bad keys, poisoned clocks, and old active-prefix backups recover once.
    nova_storage::try_write_batch(&[
        ("srec:1:é:key".into(), Some("bad".into())),
        ("srec:7:library:bad".into(), Some("{broken".into())),
        (
            "srec:7:library:bad.corrupt.1".into(),
            Some("{broken".into()),
        ),
        (
            "srec:7:library:future".into(),
            Some(
                serde_json::to_string(&nova_sync::Record {
                    value: Some("bad".into()),
                    version: nova_sync::Version::new(u64::MAX, 0, 1, false),
                })
                .unwrap(),
            ),
        ),
        (
            "sync:records:hlc".into(),
            Some(r#"{"physical_ms":18446744073709551615,"counter":0}"#.into()),
        ),
    ])
    .unwrap();
    let recovered = Store::try_load().unwrap();
    let evidence = nova_storage::try_scan_prefix("sync:quarantine:").unwrap();
    assert_eq!(evidence.len(), 5);
    assert!(recovered.record("library", "a").is_some());
    assert!(recovered.record("library", "future").is_none());
    Store::try_load().unwrap();
    assert_eq!(
        nova_storage::try_scan_prefix("sync:quarantine:").unwrap(),
        evidence
    );
}
