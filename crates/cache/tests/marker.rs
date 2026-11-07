//! The cleanup marker round-trips through the real storage singleton. This
//! file owns its process, so initializing storage here affects no other test.
use nova_cache::gc::Marker;
use std::path::Path;

#[test]
fn marker_is_scoped_to_its_directory_and_tolerates_disposable_garbage() {
    let storage = tempfile::tempdir().unwrap();
    nova_storage::init_at(storage.path());
    let marker = Marker::new("cache_gc:test:v1");
    let cache = Path::new("/cache/chapters");

    assert_eq!(marker.load(cache).unwrap(), None);
    marker.save(cache, 1234).unwrap();
    assert_eq!(marker.load(cache).unwrap(), Some(1234));
    // A cache that moved must not inherit the old directory's schedule.
    assert_eq!(marker.load(Path::new("/cache/moved")).unwrap(), None);

    // Corrupt text and the bare-number form older readers wrote both mean
    // "never cleaned" rather than an error or a stale schedule.
    nova_storage::try_set_str("cache_gc:test:v1", "broken").unwrap();
    assert_eq!(marker.load(cache).unwrap(), None);
    nova_storage::try_set_str("cache_gc:test:v1", "1234").unwrap();
    assert_eq!(marker.load(cache).unwrap(), None);

    // The stored form is the one Nova has always written.
    marker.save(cache, 99).unwrap();
    assert_eq!(
        nova_storage::try_get_str("cache_gc:test:v1")
            .unwrap()
            .as_deref(),
        Some(r#"{"directory":"/cache/chapters","completed":99}"#)
    );
}
