//! Exercise the public API across process restarts. Each app process gets one
//! storage singleton; a second process must reopen the existing database.
use nova_storage::{CollectionKind, register_collection};
use std::process::Command;

#[test]
fn public_api_reopens_existing_data() {
    const CHILD_DIR: &str = "NOVA_STORAGE_TEST_DIR";
    const CHILD_MODE: &str = "NOVA_STORAGE_TEST_MODE";
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        let dir = std::path::PathBuf::from(dir);
        nova_storage::init_at(&dir);
        assert!(dir.join("nova.redb").is_file());
        match std::env::var(CHILD_MODE).unwrap().as_str() {
            "write" => {
                register_collection(
                    "library",
                    CollectionKind::CompositeArray {
                        id_fields: vec!["media_type".into(), "id".into()],
                    },
                );
                nova_storage::try_write_batch(&[
                    ("settings".into(), Some(r#"{"enabled":true}"#.into())),
                    ("library".into(), Some(r#"[{"media_type":"series","id":"shared-id"},{"media_type":"movie","id":"shared-id"}]"#.into())),
                ]).unwrap();
                nova_storage::try_set_cached_str("meta:item", &"artwork".repeat(2048)).unwrap();
                assert_eq!(
                    nova_storage::try_allocate_local_sequence("boot", 42).unwrap(),
                    42
                );
            }
            "read" => {
                // Reads must work without re-registering collection policies.
                assert_eq!(
                    nova_storage::try_get_str("settings").unwrap().as_deref(),
                    Some(r#"{"enabled":true}"#)
                );
                let library: serde_json::Value =
                    serde_json::from_str(&nova_storage::try_get_str("library").unwrap().unwrap())
                        .unwrap();
                assert_eq!(library.as_array().unwrap().len(), 2);
                assert_eq!(library[0]["media_type"], "series");
                assert_eq!(library[1]["media_type"], "movie");
                assert_eq!(
                    nova_storage::try_get_str("meta:item").unwrap(),
                    Some("artwork".repeat(2048))
                );
                assert_eq!(
                    nova_storage::try_read_keys_and_prefixes(&["settings"], &["meta:"])
                        .unwrap()
                        .len(),
                    2
                );
                assert_eq!(
                    nova_storage::try_allocate_local_sequence("boot", 1).unwrap(),
                    43
                );
            }
            mode => panic!("unexpected child mode: {mode}"),
        }
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    for mode in ["write", "read"] {
        let output = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "public_api_reopens_existing_data", "--nocapture"])
            .env(CHILD_DIR, dir.path())
            .env(CHILD_MODE, mode)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
