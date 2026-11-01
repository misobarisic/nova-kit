//! Full-resolution encoded entries using Nova's actual variant, publication and
//! incremental-GC primitives. Applications own image admission and transforms.
use super::*;

type Flight = Arc<(Mutex<Option<Result<Vec<u8>, String>>>, Condvar)>;
static FLIGHTS: LazyLock<Mutex<HashMap<String, Flight>>> = LazyLock::new(Default::default);
static PINS: LazyLock<Mutex<HashMap<(PathBuf, String), usize>>> = LazyLock::new(Default::default);
static GENERATIONS: LazyLock<Mutex<HashMap<PathBuf, Weak<AtomicU64>>>> =
    LazyLock::new(Default::default);

fn recipe() -> ImageRecipe {
    ImageRecipe {
        max_side: None,
        compress: false,
        format: None,
        quality: 0,
        jxl_lossless_jpeg: false,
    }
}

#[derive(Clone)]
pub struct Cache {
    directory: PathBuf,
    max_entry_bytes: u64,
    generation: Arc<AtomicU64>,
}

impl Cache {
    pub fn new(directory: PathBuf, max_entry_bytes: u64) -> Self {
        let mut generations = GENERATIONS.lock().unwrap();
        generations.retain(|_, generation| generation.strong_count() > 0);
        let generation = generations
            .get(&directory)
            .and_then(Weak::upgrade)
            .unwrap_or_else(|| {
                let generation = Arc::new(AtomicU64::new(0));
                generations.insert(directory.clone(), Arc::downgrade(&generation));
                generation
            });
        Self {
            directory,
            max_entry_bytes,
            generation,
        }
    }
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    pub fn tracked_usage(&self) -> Option<(u64, usize)> {
        gc::tracked_usage(&self.directory)
    }
    /// Memory-only recency update; no index serialization or filesystem access.
    pub fn touch(&self, key: &str) {
        gc::touch_variant(&self.directory, &image_recipe_cache_key(key, recipe()));
    }

    /// Publish a background transform only if its namespace/source epoch is
    /// still current and no valid entry already exists. The final guard must be
    /// cheap and must not call back into this cache. Encoded pixels are unchanged.
    pub fn put_if_current(
        &self,
        key: &str,
        bytes: &[u8],
        expected_generation: u64,
        inspect: impl Fn(&[u8]) -> Result<(u32, u32), String>,
        still_current: impl FnOnce() -> bool,
    ) -> Result<bool, String> {
        if expected_generation != self.generation() {
            return Ok(false);
        }
        if bytes.is_empty() || bytes.len() as u64 > self.max_entry_bytes {
            return Err("encoded cache entry exceeds byte limit".into());
        }
        let (width, height) = inspect(bytes)?;
        if width == 0 || height == 0 {
            return Err("encoded cache entry has empty dimensions".into());
        }
        let _mutation = CACHE_MUTATION.read().unwrap();
        let lock = url_lock(&self.directory, key);
        let _guard = lock.lock().unwrap();
        if expected_generation != self.generation() {
            return Ok(false);
        }
        if self.get_locked(key, &inspect)?.is_some() {
            return Ok(false);
        }
        fs::create_dir_all(&self.directory).map_err(|e| e.to_string())?;
        let variant = DiskVariant {
            metadata: VariantMetadata {
                version: 2,
                url: key.to_owned(),
                uses: vec![ImageUse::Fixed(None)],
                width,
                height,
                recipe: recipe(),
                checksum: fnv1a(bytes),
            },
            encoded: bytes.to_vec(),
        };
        write_variant_if(
            &recipe_disk_path(&self.directory, key, recipe()),
            &variant,
            still_current,
        )
        .map_err(|e| e.to_string())
    }
    /// Metadata-only inventory; run off the UI thread for large namespaces.
    pub fn usage(&self) -> (u64, usize) {
        fs::read_dir(&self.directory)
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_str().is_some_and(gc::owned_file))
            .filter_map(|entry| fs::symlink_metadata(entry.path()).ok())
            .filter(|stat| stat.is_file())
            .fold((0u64, 0usize), |(bytes, count), stat| {
                (bytes.saturating_add(stat.len()), count + 1)
            })
    }
    /// Keep all known keys of an active chapter pinned until the lease drops.
    /// The GC also protects other entries in the same application-defined group.
    pub fn pin(&self, keys: impl IntoIterator<Item = String>) -> Lease {
        let keys = keys.into_iter().collect::<HashSet<_>>();
        let mut pins = PINS.lock().unwrap();
        for key in &keys {
            *pins
                .entry((self.directory.clone(), key.clone()))
                .or_default() += 1;
        }
        Lease {
            directory: self.directory.clone(),
            keys,
        }
    }
    pub fn get(
        &self,
        key: &str,
        inspect: impl Fn(&[u8]) -> Result<(u32, u32), String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let _mutation = CACHE_MUTATION.read().unwrap();
        let lock = url_lock(&self.directory, key);
        let _guard = lock.lock().unwrap();
        self.get_locked(key, &inspect)
    }
    fn get_locked(
        &self,
        key: &str,
        inspect: &impl Fn(&[u8]) -> Result<(u32, u32), String>,
    ) -> Result<Option<Vec<u8>>, String> {
        let path = recipe_disk_path(&self.directory, key, recipe());
        let stat = match fs::symlink_metadata(&path) {
            Ok(stat) => stat,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e.to_string()),
        };
        // Bound the container before read_variant allocates its payload.
        let valid_size =
            stat.is_file() && stat.len() <= self.max_entry_bytes.saturating_add(64 * 1024 + 12);
        #[cfg(unix)]
        if stat.is_file() {
            use std::os::unix::fs::PermissionsExt;
            if stat.permissions().mode() & 0o777 != 0o600 {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                    .map_err(|e| e.to_string())?;
            }
        }
        if valid_size
            && let Some(variant) = read_variant(&path)
            && variant.metadata.url == key
            && variant.metadata.recipe == recipe()
            && variant.encoded.len() as u64 <= self.max_entry_bytes
            && inspect(&variant.encoded)
                .is_ok_and(|size| size == (variant.metadata.width, variant.metadata.height))
        {
            gc::record_existing(&path, &variant.metadata);
            return Ok(Some(variant.encoded));
        }
        // Malformed owned entries are a miss; valid source can repair them.
        fs::remove_file(&path).map_err(|e| e.to_string())?;
        gc::removed(&path);
        Ok(None)
    }
    pub fn get_or_fetch(
        &self,
        key: &str,
        fetch: impl FnOnce() -> Result<Vec<u8>, String>,
        inspect: impl Fn(&[u8]) -> Result<(u32, u32), String>,
    ) -> Result<Vec<u8>, String> {
        let generation = self.generation();
        let flight_key = format!("{}:{generation}:{key}", self.directory.display());
        let (flight, leader) = {
            let mut flights = FLIGHTS.lock().unwrap();
            if let Some(flight) = flights.get(&flight_key) {
                (flight.clone(), false)
            } else {
                let flight = Arc::new((Mutex::new(None), Condvar::new()));
                flights.insert(flight_key.clone(), flight.clone());
                (flight, true)
            }
        };
        if !leader {
            let mut result = flight.0.lock().unwrap();
            while result.is_none() {
                result = flight.1.wait(result).unwrap();
            }
            return if generation == self.generation() {
                result.as_ref().unwrap().clone()
            } else {
                Err("cache cleared while fetching image".into())
            };
        }
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Disk cache failures do not make valid network images unavailable.
            if let Some(bytes) = self.get(key, &inspect).ok().flatten() {
                return Ok(bytes);
            }
            let bytes = fetch()?;
            if bytes.is_empty() || bytes.len() as u64 > self.max_entry_bytes {
                return Err("encoded cache entry exceeds byte limit".into());
            }
            let (width, height) = inspect(&bytes)?;
            if width == 0 || height == 0 {
                return Err("encoded cache entry has empty dimensions".into());
            }
            let _mutation = CACHE_MUTATION.read().unwrap();
            let lock = url_lock(&self.directory, key);
            let _guard = lock.lock().unwrap();
            if generation != self.generation() {
                return Err("cache cleared while fetching image".into());
            }
            let variant = DiskVariant {
                metadata: VariantMetadata {
                    version: 2,
                    url: key.to_owned(),
                    uses: vec![ImageUse::Fixed(None)],
                    width,
                    height,
                    recipe: recipe(),
                    checksum: fnv1a(&bytes),
                },
                encoded: bytes,
            };
            if fs::create_dir_all(&self.directory).is_ok() {
                let _ = write_variant(&recipe_disk_path(&self.directory, key, recipe()), &variant);
            }
            Ok(variant.encoded)
        }))
        .unwrap_or_else(|_| Err("cache image provider panicked".into()));
        let _mutation = CACHE_MUTATION.read().unwrap();
        let result = if generation == self.generation() {
            result
        } else {
            Err("cache cleared while fetching image".into())
        };
        *flight.0.lock().unwrap() = Some(result.clone());
        FLIGHTS.lock().unwrap().remove(&flight_key);
        flight.1.notify_all();
        result
    }
    /// Removes only Kit-owned variants/index/temporary files in this namespace.
    pub fn clear(&self) -> (u64, usize) {
        let _maintenance = MaintenanceGuard::acquire();
        let _mutation = CACHE_MUTATION.write().unwrap();
        self.generation.fetch_add(1, Ordering::AcqRel);
        CACHE_GENERATION.fetch_add(1, Ordering::AcqRel);
        gc::cleared(&self.directory);
        let mut removed = (0u64, 0usize);
        if let Ok(entries) = fs::read_dir(&self.directory) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                if !name.to_str().is_some_and(gc::owned_file) {
                    continue;
                }
                if let Ok(stat) = fs::symlink_metadata(entry.path())
                    && stat.is_file()
                    && fs::remove_file(entry.path()).is_ok()
                {
                    removed.0 = removed.0.saturating_add(stat.len());
                    removed.1 += 1;
                }
            }
        }
        removed
    }
}

pub struct Lease {
    directory: PathBuf,
    keys: HashSet<String>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut pins = PINS.lock().unwrap();
        for key in &self.keys {
            let entry = (self.directory.clone(), key.clone());
            if let Some(count) = pins.get_mut(&entry) {
                *count -= 1;
                if *count == 0 {
                    pins.remove(&entry);
                }
            }
        }
        drop(pins);
        gc::wake();
    }
}
pub(super) fn protected(directory: &Path) -> HashSet<String> {
    PINS.lock()
        .unwrap()
        .keys()
        .filter(|(dir, _)| dir == directory)
        .map(|(_, key)| key.clone())
        .collect()
}
pub(super) fn pin_guard() -> std::sync::MutexGuard<'static, HashMap<(PathBuf, String), usize>> {
    PINS.lock().unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{scratch, serial};
    use std::{
        sync::{atomic::AtomicUsize, mpsc},
        time::Duration,
    };

    fn inspect(bytes: &[u8]) -> Result<(u32, u32), String> {
        if bytes == b"native tall page" {
            Ok((720, 20000))
        } else {
            Err("bad source".into())
        }
    }

    #[test]
    fn encoded_pages_preserve_native_bytes_and_validate_hits_without_reprocessing() {
        let _serial = serial();
        let dir = scratch("blob-native");
        let cache = Cache::new(dir.clone(), 64);
        let bytes = cache
            .get_or_fetch("chapter/1", || Ok(b"native tall page".to_vec()), inspect)
            .unwrap();
        assert_eq!(bytes, b"native tall page");
        assert_eq!(
            cache
                .get_or_fetch(
                    "chapter/1",
                    || panic!("cache hit must not fetch or reencode"),
                    inspect
                )
                .unwrap(),
            bytes
        );
        let file = recipe_disk_path(&dir, "chapter/1", recipe());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
            assert!(cache.get("chapter/1", inspect).unwrap().is_some());
            assert_eq!(
                fs::metadata(&file).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
        let variant = read_variant(&file).unwrap();
        assert_eq!(
            (variant.metadata.width, variant.metadata.height),
            (720, 20000)
        );
        assert_eq!(variant.encoded, bytes);
        fs::write(&file, b"corrupt").unwrap();
        assert_eq!(
            cache
                .get_or_fetch("chapter/1", || Ok(bytes.clone()), inspect)
                .unwrap(),
            bytes
        );
        fs::write(dir.join("unowned.txt"), b"keep").unwrap();
        assert!(cache.usage().0 > 0);
        cache.clear();
        assert_eq!(fs::read(dir.join("unowned.txt")).unwrap(), b"keep");
        assert_eq!(cache.usage(), (0, 0));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn encoded_page_single_flight_and_clear_cannot_resurrect_old_source() {
        let _serial = serial();
        let dir = scratch("blob-flight");
        let cache = Cache::new(dir.clone(), 64);
        let calls = Arc::new(AtomicUsize::new(0));
        let (started, rx) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let worker_cache = cache.clone();
        let worker_calls = calls.clone();
        let first = std::thread::spawn(move || {
            worker_cache.get_or_fetch(
                "chapter/1",
                || {
                    worker_calls.fetch_add(1, Ordering::SeqCst);
                    started.send(()).unwrap();
                    wait.recv().unwrap();
                    Ok(b"native tall page".to_vec())
                },
                inspect,
            )
        });
        rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let follower_cache = cache.clone();
        let follower_calls = calls.clone();
        let second = std::thread::spawn(move || {
            follower_cache.get_or_fetch(
                "chapter/1",
                || {
                    follower_calls.fetch_add(1, Ordering::SeqCst);
                    Ok(b"native tall page".to_vec())
                },
                inspect,
            )
        });
        // Wait deterministically until follower has acquired its flight handle.
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while FLIGHTS
            .lock()
            .unwrap()
            .values()
            .next()
            .is_none_or(|flight| Arc::strong_count(flight) < 3)
        {
            assert!(std::time::Instant::now() < deadline);
            std::thread::yield_now();
        }
        cache.clear();
        release.send(()).unwrap();
        assert!(first.join().unwrap().unwrap_err().contains("cleared"));
        assert!(second.join().unwrap().unwrap_err().contains("cleared"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(cache.get("chapter/1", inspect).unwrap().is_none());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn encoded_cache_is_optional_but_admission_and_cancellation_are_not() {
        let _serial = serial();
        let dir = scratch("blob-errors");
        let blocked = dir.join("file-instead-of-directory");
        fs::write(&blocked, b"file").unwrap();
        let cache = Cache::new(blocked, 64);
        assert_eq!(
            cache
                .get_or_fetch("page", || Ok(b"native tall page".to_vec()), inspect)
                .unwrap(),
            b"native tall page"
        );
        assert!(
            cache
                .get_or_fetch("page", || Err("cancelled".into()), inspect)
                .unwrap_err()
                .contains("cancelled")
        );
        assert!(
            Cache::new(dir.join("small"), 2)
                .get_or_fetch("page", || Ok(b"native tall page".to_vec()), inspect)
                .is_err()
        );
        assert!(
            cache
                .get_or_fetch("page", || Ok(b"invalid".to_vec()), inspect)
                .is_err()
        );
        let first = cache.pin(["page".into()]);
        let second = cache.pin(["page".into()]);
        assert!(protected(cache.directory()).contains("page"));
        drop(first);
        assert!(protected(cache.directory()).contains("page"));
        drop(second);
        assert!(protected(cache.directory()).is_empty());
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn background_publication_is_namespace_scoped_conditional_and_never_rewrites_hits() {
        let _serial = serial();
        let dir = scratch("blob-background");
        let posters = Cache::new(dir.join("posters"), 64);
        let pages = Cache::new(dir.join("pages"), 64);
        let page_epoch = pages.generation();
        let poster_epoch = posters.generation();
        posters.clear();
        assert_eq!(pages.generation(), page_epoch);
        assert_ne!(posters.generation(), poster_epoch);
        assert!(
            !posters
                .put_if_current(
                    "old",
                    b"native tall page",
                    poster_epoch,
                    |_| panic!("stale work must stop before admission"),
                    || true
                )
                .unwrap()
        );
        assert!(
            !pages
                .put_if_current(
                    "cancelled",
                    b"native tall page",
                    page_epoch,
                    inspect,
                    || false
                )
                .unwrap()
        );
        assert!(
            pages
                .put_if_current("page", b"native tall page", page_epoch, inspect, || true)
                .unwrap()
        );
        let path = recipe_disk_path(pages.directory(), "page", recipe());
        let original = fs::read(&path).unwrap();
        assert!(
            !pages
                .put_if_current("page", b"native tall page", page_epoch, inspect, || true)
                .unwrap()
        );
        assert_eq!(fs::read(path).unwrap(), original);
        assert!(pages.get("cancelled", inspect).unwrap().is_none());
        assert_eq!(
            Cache::new(pages.directory().to_owned(), 64).generation(),
            page_epoch
        );
        fs::remove_dir_all(dir).unwrap();
    }
}
