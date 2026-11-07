//! Storage failures are reported, not swallowed. Storage is never initialized
//! in this process.
use nova_cache::gc::Marker;
use std::path::Path;

#[test]
fn unavailable_storage_is_an_error_for_both_directions() {
    let marker = Marker::new("cache_gc:unavailable:v1");
    let cache = Path::new("/cache");
    assert!(marker.load(cache).is_err());
    assert!(marker.save(cache, 1).is_err());
}
