//! The socket-free owner persists offline intent and never diffs against an
//! unseen remote value. A separate process isolates the storage singleton.
use nova_sync::{Record, Store, Version, commit_snapshot, local_store};

#[test]
fn offline_intent_pending_projection_and_baseline_survive_reload() {
    let dir = std::env::temp_dir().join(format!("nova-local-mutations-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    nova_storage::init_at(&dir);
    assert!(nova_sync::engine().is_none());
    commit_snapshot(
        "library",
        &[("a".into(), r#"{"title":"a"}"#.into(), 0)],
        Some(("library", "snapshot-a".into())),
        false,
        false,
    )
    .unwrap();
    let owner = local_store().unwrap();
    let remote_version = Version::new(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 1000,
        0,
        123,
        false,
    );
    {
        let mut store = owner.lock().unwrap();
        assert!(store.apply(
            "library",
            "remote",
            Record {
                value: Some(r#"{"title":"remote","future_field":true}"#.into()),
                version: remote_version
            }
        ));
        assert!(store.apply(
            "library",
            "a",
            Record {
                value: None,
                version: Version {
                    deleted: true,
                    ..remote_version
                }
            }
        ));
        store.save().unwrap();
    }
    // Unrelated local edit neither resurrects the tombstone nor deletes the
    // remote addition whose callback has not yet run.
    let unchanged = ("a".into(), r#"{"title":"a"}"#.into(), 0);
    commit_snapshot(
        "library",
        &[unchanged, ("b".into(), r#"{"title":"b"}"#.into(), 0)],
        None,
        false,
        false,
    )
    .unwrap();
    let reloaded = Store::try_load().unwrap();
    assert!(reloaded.record("library", "a").unwrap().is_deleted());
    assert!(reloaded.record("library", "remote").is_some());
    assert!(reloaded.record("library", "b").is_some());
    assert_eq!(reloaded.pending_domains(), vec!["library"]);
    assert_eq!(
        nova_storage::try_get_str("library").unwrap().as_deref(),
        Some("snapshot-a")
    );

    // Projection advances only the baseline. A later offline delete is real.
    let records: Vec<_> = reloaded
        .records("library")
        .into_iter()
        .map(|(k, v)| (k, v, 0))
        .collect();
    commit_snapshot("library", &records, None, true, false).unwrap();
    let basis = owner.lock().unwrap().digest()["library"].clone();
    owner
        .lock()
        .unwrap()
        .mark_projected("library", &basis)
        .unwrap();
    commit_snapshot(
        "library",
        &[("remote".into(), r#"{"title":"edited"}"#.into(), 0)],
        None,
        false,
        false,
    )
    .unwrap();
    let reloaded = Store::try_load().unwrap();
    assert!(reloaded.record("library", "b").unwrap().is_deleted());
    let value: serde_json::Value = serde_json::from_str(
        reloaded
            .record("library", "remote")
            .unwrap()
            .value
            .as_ref()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(value["future_field"], true);
    assert_eq!(value["title"], "edited");
    assert!(reloaded.pending_domains().is_empty());
    assert!(nova_sync::engine().is_none());
}
