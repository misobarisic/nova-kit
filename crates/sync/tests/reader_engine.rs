#![cfg(not(feature = "nova-progress"))]
//! A separate process from the Nova unit tests so application configuration and
//! the storage owner exercise their real one-app-per-process lifetime.
use nova_sync::{ProtocolConfig, Record, RecordPolicy, Store, Version};
use std::sync::Arc;

struct ReaderPolicy;
impl RecordPolicy for ReaderPolicy {
    fn validate(&self, domain: &str, key: &str, record: &Record) -> bool {
        if domain != "progress" {
            return true;
        }
        key.starts_with("source:")
            && record.value.as_deref().is_none_or(|raw| {
                serde_json::from_str::<serde_json::Value>(raw)
                    .ok()
                    .is_some_and(|v| v["page"].as_u64().is_some())
            })
    }
    fn merge_value(
        &self,
        domain: &str,
        _: &str,
        local: &Record,
        remote: &Record,
    ) -> Option<String> {
        if domain != "reader-tags" {
            return None;
        }
        let mut tags = std::collections::BTreeSet::<String>::new();
        for record in [local, remote] {
            tags.extend(serde_json::from_str::<Vec<String>>(record.value.as_deref()?).ok()?);
        }
        serde_json::to_string(&tags).ok()
    }
}

#[test]
fn reader_namespace_generic_progress_policy_persistence_and_alpn_isolation() {
    nova_sync::configure_protocol(ProtocolConfig::reader()).unwrap();
    let policy: Arc<dyn RecordPolicy> = Arc::new(ReaderPolicy);
    nova_sync::configure_record_policy(policy.clone()).unwrap();
    nova_sync::configure_record_policy(policy).unwrap();
    assert!(nova_sync::configure_record_policy(Arc::new(ReaderPolicy)).is_err());
    assert!(nova_sync::configure_protocol(ProtocolConfig::nova()).is_err());
    let directory =
        std::env::temp_dir().join(format!("nova-sync-reader-test-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    nova_storage::init_at(&directory);
    let id = iroh::SecretKey::generate().public();
    let code = nova_sync::encode_ticket(&id, &[7; 16]);
    assert!(code.starts_with("NR1"));
    assert_eq!(nova_sync::decode_ticket(&code).unwrap(), (id, [7; 16]));
    assert!(nova_sync::decode_ticket(&code.replacen("NR1", "NV1", 1)).is_err());
    assert_eq!(
        nova_sync::decode_ticket(&code.to_ascii_lowercase())
            .unwrap()
            .0,
        id
    );

    let mut store = Store::try_load().unwrap();
    assert!(!store.set("progress", "invalid", Some("{}".into()), 0, 1));
    assert!(store.set(
        "progress",
        "source:series:chapter",
        Some(r#"{"page":9,"watched":true}"#.into()),
        0,
        1
    ));
    assert!(store.set(
        "progress",
        "source:series:chapter",
        Some(r#"{"page":2,"watched":false}"#.into()),
        0,
        1
    ));
    let row = store.record("progress", "source:series:chapter").unwrap();
    assert!(!row.value.as_ref().unwrap().contains("__nova_progress_v1"));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(row.value.as_ref().unwrap()).unwrap()["page"],
        2
    );
    let invalid = Record {
        value: Some("{}".into()),
        version: Version::new(row.version.ts, row.version.counter + 1, 2, false),
    };
    assert!(!store.apply("progress", "source:series:chapter", invalid.clone()));
    assert!(store.set("reader-tags", "item", Some(r#"["one"]"#.into()), 0, 1));
    let old = store.record("reader-tags", "item").unwrap().version;
    assert!(store.apply(
        "reader-tags",
        "item",
        Record {
            value: Some(r#"["two"]"#.into()),
            version: old
        }
    ));
    assert_eq!(
        store
            .record("reader-tags", "item")
            .unwrap()
            .value
            .as_deref(),
        Some(r#"["one","two"]"#)
    );
    store.queue_extra("reader:snapshot", Some("atomic snapshot".into()));
    store.save().unwrap();
    let loaded = Store::try_load().unwrap();
    assert_eq!(
        loaded.record("progress", "source:series:chapter"),
        store.record("progress", "source:series:chapter")
    );
    assert_eq!(
        loaded.extra_value("reader:snapshot").unwrap().as_deref(),
        Some("atomic snapshot")
    );
    // The engine uses its existing corruption quarantine for policy failures
    // encountered on disk as well as malformed JSON/clocks.
    let record_rows = nova_storage::try_scan_prefix("srec:").unwrap();
    assert!(!record_rows.is_empty());
    let progress_row = record_rows
        .iter()
        .find(|(_, raw)| raw.contains("watched"))
        .unwrap();
    nova_storage::try_set_str(&progress_row.0, &serde_json::to_string(&invalid).unwrap()).unwrap();
    let loaded = Store::try_load().unwrap();
    assert!(loaded.record("progress", "source:series:chapter").is_none());
    assert!(
        !nova_storage::try_scan_prefix("sync:quarantine:")
            .unwrap()
            .is_empty()
    );

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let reader = ProtocolConfig::reader();
        let nova = ProtocolConfig::nova();
        let server = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .alpns(vec![
                reader.sync.to_vec(),
                reader.pair.to_vec(),
                reader.package.to_vec(),
                reader.remove.to_vec(),
            ])
            .bind()
            .await
            .unwrap();
        let client = iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .bind()
            .await
            .unwrap();
        // Every public entry protocol rejects Nova negotiation, before frames
        // or application records can be exchanged.
        for alpn in [nova.sync, nova.pair, nova.package, nova.remove] {
            let connect = client.connect(server.addr(), alpn);
            let accept = async {
                if let Some(incoming) = server.accept().await {
                    let _ = incoming.await;
                }
            };
            let (result, _) = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                tokio::join!(connect, accept)
            })
            .await
            .unwrap();
            assert!(result.is_err());
        }
        client.close().await;
        server.close().await;
    });
    let _ = std::fs::remove_dir_all(directory);
}
