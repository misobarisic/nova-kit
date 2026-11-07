//! The profile directory against a real record owner. One test function runs
//! every scenario because the storage singleton belongs to the process.
use nova_sync::profiles::directory::{
    DELETIONS_DOMAIN, DOMAIN, Directory, Host, Stage, validate_record,
};
use nova_sync::{Record, RecordPolicy, Store, Version};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Events(Mutex<Vec<String>>);

struct TestHost {
    events: Arc<Events>,
    fail: Mutex<Option<Stage>>,
    keep: Mutex<Option<String>>,
}
impl Host for TestHost {
    type Guard = ();
    fn begin_write(&self, id: &str) {
        self.events.0.lock().unwrap().push(format!("write {id}"));
    }
    fn retiring(&self, id: &str) {
        self.events.0.lock().unwrap().push(format!("retiring {id}"));
    }
    fn keep_local_data(&self, id: &str) -> bool {
        self.keep.lock().unwrap().as_deref() == Some(id)
    }
    fn remove_local_data(&self, id: &str) -> Result<(), String> {
        self.events.0.lock().unwrap().push(format!("remove {id}"));
        Ok(())
    }
    fn deleted(&self, id: &str) {
        self.events.0.lock().unwrap().push(format!("deleted {id}"));
    }
    fn checkpoint(&self, stage: Stage) -> Result<(), String> {
        if *self.fail.lock().unwrap() == Some(stage) {
            Err("injected".into())
        } else {
            Ok(())
        }
    }
}

struct Policy;
impl RecordPolicy for Policy {
    fn validate(&self, domain: &str, key: &str, record: &Record) -> bool {
        validate_record(domain, key, record)
    }
}

fn live(value: &str) -> Record {
    Record {
        value: Some(value.into()),
        version: Version::new(1, 0, 1, false),
    }
}
fn snapshot(store: &Store) -> Vec<(String, Vec<(String, String)>)> {
    let mut all: Vec<_> = store
        .domains()
        .into_iter()
        .map(|domain| {
            let rows = store.records(&domain);
            (domain, rows)
        })
        .collect();
    all.sort();
    all
}

#[test]
fn directory_lifecycle_and_legacy_rows() {
    let dir = std::env::temp_dir().join(format!("nova-profile-directory-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    nova_storage::init_at(&dir);
    nova_sync::configure_record_policy(Arc::new(Policy)).unwrap();
    let events = Arc::new(Events::default());
    let directory = Directory::new(
        TestHost {
            events: events.clone(),
            fail: Mutex::new(None),
            keep: Mutex::new(None),
        },
        "default",
    );
    let owner = nova_sync::local_store().unwrap();

    // Rows exactly as the applications have always written them load unchanged
    // under the policy, together with a retirement marker.
    {
        let mut store = owner.lock().unwrap();
        assert!(store.apply(DOMAIN, "default", live(r#"{"name":"Default"}"#)));
        assert!(store.apply(DOMAIN, "p1_2", live(r#"{"name":"Mina"}"#)));
        assert!(store.apply(DOMAIN, "p9_9", live(r#"{"name":"Gone"}"#)));
        assert!(store.apply(DELETIONS_DOMAIN, "p9_9", live("1")));
        store.save().unwrap();
    }
    let reloaded = Store::try_load().unwrap();
    assert_eq!(reloaded.records(DOMAIN).len(), 3);
    assert_eq!(reloaded.records(DELETIONS_DOMAIN).len(), 1);
    let profiles = directory.list().unwrap();
    assert_eq!(
        profiles
            .iter()
            .map(|p| (p.id.as_str(), p.name.as_str()))
            .collect::<Vec<_>>(),
        vec![("default", "Default"), ("p1_2", "Mina")]
    );
    assert!(directory.is_retired("p9_9"));
    assert!(!directory.is_retired("p1_2"));
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .contains(&"remove p9_9".to_string())
    );

    // A record the policy refuses never reaches the owner.
    {
        let mut store = owner.lock().unwrap();
        assert!(!store.apply(DOMAIN, "bad:id", live(r#"{"name":"x"}"#)));
        assert!(!store.apply(DOMAIN, "p3_3", live(r#"{"name":""}"#)));
        assert!(!store.apply(DOMAIN, "p3_3", live("not json")));
        assert!(!store.apply(DELETIONS_DOMAIN, "p3_3", live("0")));
    }

    // Retirement tombstones children, writes the marker and clears durable keys.
    let created = directory.create("  Ana ").unwrap();
    assert_eq!(created.name, "Ana");
    let id = created.id;
    {
        let mut store = owner.lock().unwrap();
        let device = nova_sync::local_device().unwrap();
        store.set(
            &format!("profile:{id}:library"),
            "a",
            Some("1".into()),
            0,
            device,
        );
        store.queue_extra(
            &format!("sync:baseline:profile:{id}:library"),
            Some("{}".into()),
        );
        store.save().unwrap();
    }
    nova_storage::try_set_str(&format!("profile:{id}:settings"), "{}").unwrap();

    // A failure at either injection point changes nothing, in memory or on disk.
    let before_memory = snapshot(&owner.lock().unwrap());
    let before_disk = snapshot(&Store::try_load().unwrap());
    for stage in [Stage::AfterTombstone, Stage::BeforeScan] {
        *directory.host().fail.lock().unwrap() = Some(stage);
        assert_eq!(directory.retire(&id), Err("injected".into()));
        assert_eq!(snapshot(&owner.lock().unwrap()), before_memory, "{stage:?}");
        assert_eq!(
            snapshot(&Store::try_load().unwrap()),
            before_disk,
            "{stage:?}"
        );
        assert!(!directory.is_retired(&id));
        assert!(
            nova_storage::try_get_str(&format!("profile:{id}:settings"))
                .unwrap()
                .is_some()
        );
    }
    *directory.host().fail.lock().unwrap() = None;

    directory.retire(&id).unwrap();
    assert!(directory.is_retired(&id));
    assert!(events.0.lock().unwrap().contains(&format!("deleted {id}")));
    for store in [owner.lock().unwrap().clone(), Store::try_load().unwrap()] {
        assert!(store.record(DOMAIN, &id).unwrap().is_deleted());
        assert!(!store.record(DELETIONS_DOMAIN, &id).unwrap().is_deleted());
        assert!(
            store
                .record(&format!("profile:{id}:library"), "a")
                .unwrap()
                .is_deleted()
        );
    }
    assert!(
        nova_storage::try_get_str(&format!("profile:{id}:settings"))
            .unwrap()
            .is_none()
    );
    assert!(
        nova_storage::try_get_str(&format!("sync:baseline:profile:{id}:library"))
            .unwrap()
            .is_none()
    );
    assert!(directory.list().unwrap().iter().all(|p| p.id != id));
    // The retired ID cannot be written again.
    assert_eq!(
        directory.write(&id, "Back"),
        Err("This profile was deleted.".into())
    );

    // A stale peer delivering a live child after retirement is re-tombstoned,
    // and local data of the profile on screen survives until it is released.
    let keep_id = directory.create("Keep").unwrap().id;
    *directory.host().keep.lock().unwrap() = Some(keep_id.clone());
    nova_storage::try_set_str(&format!("profile:{keep_id}:settings"), "{}").unwrap();
    directory.retire(&keep_id).unwrap();
    {
        let mut store = owner.lock().unwrap();
        let domain = format!("profile:{keep_id}:library");
        store.apply(
            &domain,
            "late",
            Record {
                value: Some("1".into()),
                version: Version::new(u64::MAX / 2, 0, 5, false),
            },
        );
    }
    directory.list().unwrap();
    assert!(
        owner
            .lock()
            .unwrap()
            .record(&format!("profile:{keep_id}:library"), "late")
            .is_none_or(|r| r.is_deleted())
    );
    *directory.host().keep.lock().unwrap() = None;
    directory.list().unwrap();
    assert!(
        events
            .0
            .lock()
            .unwrap()
            .contains(&format!("remove {keep_id}"))
    );

    // The default is seeded neutrally and never over a retired identity.
    let mut fresh = Store::try_load().unwrap();
    assert!(!directory.seed_default(&mut fresh, 7), "already present");
}
