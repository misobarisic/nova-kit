//! Batched disk-artwork usage and incremental eviction. Database policy stays
//! behind an app-supplied backend; cache hits never call that backend.
use super::*;
use std::{
    collections::BTreeMap,
    io::{self, Read},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::{Duration, Instant, UNIX_EPOCH},
};

pub const BATCH_INTERVAL: Duration = Duration::from_secs(120);
pub const ROUTINE_INTERVAL_SECS: u64 = 24 * 60 * 60;
const INDEX_NAME: &str = ".image-usage-v1.json";
const MAX_INDEX_BYTES: u64 = 32 * 1024 * 1024;

/// Marker and title references are local metadata supplied by the application.
pub trait Backend: Send + Sync + 'static {
    fn policy(&self) -> DiskPolicy {
        DiskPolicy::from_settings(&active_cache_settings())
    }
    /// URLs/opaque keys that must not be evicted, including their whole group.
    fn protected_urls(&self) -> io::Result<HashSet<String>> {
        Ok(HashSet::new())
    }
    /// Applications can make every page of a chapter one eviction unit.
    fn eviction_group(&self, url: &str) -> String {
        url.to_owned()
    }
    fn last_cleanup(&self, directory: &Path) -> io::Result<Option<u64>>;
    fn save_cleanup(&self, directory: &Path, completed: u64) -> io::Result<()>;
    fn preferred_urls(&self) -> io::Result<HashSet<String>>;
    fn completed(&self, report: Report);
    fn failed(&self, error: &io::Error);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DiskPolicy {
    pub enabled: bool,
    pub limit_bytes: Option<u64>,
}
impl DiskPolicy {
    pub fn from_settings(settings: &CacheSettings) -> Self {
        Self {
            enabled: settings.cache_images,
            limit_bytes: settings
                .image_disk_cache_limit_mb
                .map(|mb| u64::from(mb) * 1024 * 1024),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Report {
    pub removed_files: usize,
    pub removed_bytes: u64,
    pub remaining_bytes: u64,
    pub deferred: bool,
}

pub fn cleanup_due(previous: Option<u64>, now: u64) -> bool {
    previous.is_none_or(|previous| previous > now || now - previous >= ROUTINE_INTERVAL_SECS)
}

#[derive(Clone, Serialize, Deserialize)]
struct Record {
    url: String,
    key: String,
    bytes: u64,
    modified: u128,
    last_used: u64,
    invalid: bool,
    #[serde(skip)]
    revision: u64,
}

#[derive(Serialize, Deserialize)]
struct Index {
    version: u32,
    clean_shutdown: bool,
    records: BTreeMap<String, Record>,
}

#[derive(Default)]
struct State {
    initialized: bool,
    records: BTreeMap<String, Record>,
    by_key: HashMap<String, String>,
    bytes: u64,
    revision: u64,
    epoch: u64,
    dirty: bool,
    deleted: HashMap<String, u64>,
}

impl State {
    fn insert(&mut self, name: String, mut record: Record) {
        self.revision = self.revision.wrapping_add(1);
        record.revision = self.revision;
        if let Some(old) = self.records.remove(&name) {
            self.bytes = self.bytes.saturating_sub(old.bytes);
            self.by_key.remove(&old.key);
        }
        self.bytes = self.bytes.saturating_add(record.bytes);
        if !record.key.is_empty() {
            self.by_key.insert(record.key.clone(), name.clone());
        }
        self.deleted.remove(&name);
        self.records.insert(name, record);
        self.dirty = true;
    }
    fn remove(&mut self, name: &str) {
        if let Some(old) = self.records.remove(name) {
            self.bytes = self.bytes.saturating_sub(old.bytes);
            self.by_key.remove(&old.key);
            self.revision = self.revision.wrapping_add(1);
            self.deleted.insert(name.to_owned(), self.revision);
            self.dirty = true;
        }
    }
    fn touch(&mut self, key: &str, now: u64) {
        if let Some(name) = self.by_key.get(key)
            && let Some(record) = self.records.get_mut(name)
        {
            self.revision = self.revision.wrapping_add(1);
            record.revision = self.revision;
            record.last_used = now;
            self.dirty = true;
        }
    }
}

struct Tracker {
    directory: PathBuf,
    state: Mutex<State>,
    wake: SyncSender<()>,
    stop: AtomicBool,
}

static TRACKERS: LazyLock<Mutex<HashMap<PathBuf, Weak<Tracker>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn tracker(directory: &Path) -> Option<Arc<Tracker>> {
    TRACKERS.lock().unwrap().get(directory)?.upgrade()
}
/// Memory-only validated inventory snapshot. None until startup inventory is ready.
pub fn tracked_usage(directory: &Path) -> Option<(u64, usize)> {
    let tracker = tracker(directory)?;
    let state = tracker.state.lock().unwrap();
    state
        .initialized
        .then_some((state.bytes, state.records.len()))
}
pub(super) fn touch_variant(directory: &Path, key: &str) {
    if let Some(tracker) = tracker(directory) {
        tracker
            .state
            .lock()
            .unwrap()
            .touch(key, nova_config::now_secs());
    }
}

/// Coalesced notifications are also used for a changed or synced size limit.
pub fn wake() {
    let trackers = {
        let mut registered = TRACKERS.lock().unwrap();
        registered.retain(|_, tracker| tracker.strong_count() > 0);
        registered
            .values()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>()
    };
    for tracker in trackers {
        let _ = tracker.wake.try_send(());
    }
}

pub(super) fn touch_decoded(key: &str) {
    if !nova_config::disk_image_cache_enabled() {
        return;
    }
    let trackers = TRACKERS
        .lock()
        .unwrap()
        .values()
        .filter_map(Weak::upgrade)
        .collect::<Vec<_>>();
    for tracker in trackers {
        tracker
            .state
            .lock()
            .unwrap()
            .touch(key, nova_config::now_secs());
    }
}

pub(super) fn record_variant(path: &Path, metadata: &VariantMetadata, bytes: u64) {
    let Some(directory) = path.parent() else {
        return;
    };
    let Some(tracker) = tracker(directory) else {
        return;
    };
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return;
    };
    let Ok(file) = fs::symlink_metadata(path) else {
        return;
    };
    if !file.is_file() {
        return;
    }
    let mut state = tracker.state.lock().unwrap();
    let key = image_recipe_cache_key(&metadata.url, metadata.recipe);
    if state.records.get(name).is_some_and(|record| {
        record.bytes == bytes && record.modified == modified(&file) && !record.invalid
    }) {
        state.touch(&key, nova_config::now_secs());
        return;
    }
    state.insert(
        name.to_owned(),
        Record {
            url: metadata.url.clone(),
            key: image_recipe_cache_key(&metadata.url, metadata.recipe),
            bytes,
            modified: modified(&file),
            last_used: nova_config::now_secs(),
            invalid: false,
            revision: 0,
        },
    );
    drop(state);
    let _ = tracker.wake.try_send(());
}

pub(super) fn record_existing(path: &Path, metadata: &VariantMetadata) {
    // Loading already performs disk I/O. Direct decoded hits only touch memory.
    if let Ok(file) = fs::symlink_metadata(path) {
        record_variant(path, metadata, file.len());
    }
}

pub(super) fn removed(path: &Path) {
    if let Some(directory) = path.parent()
        && let Some(tracker) = tracker(directory)
        && let Some(name) = path.file_name().and_then(|name| name.to_str())
    {
        tracker.state.lock().unwrap().remove(name);
    }
}

pub(super) fn mark_invalid(path: &Path) {
    if let Some(directory) = path.parent()
        && let Some(tracker) = tracker(directory)
        && let Some(name) = path.file_name().and_then(|name| name.to_str())
    {
        let mut state = tracker.state.lock().unwrap();
        let revision = state.revision.wrapping_add(1);
        if let Some(record) = state.records.get_mut(name) {
            record.invalid = true;
            record.revision = revision;
            state.revision = revision;
            state.dirty = true;
            let _ = tracker.wake.try_send(());
        }
    }
}

pub(super) fn cleared(directory: &Path) {
    if let Some(tracker) = tracker(directory) {
        let mut state = tracker.state.lock().unwrap();
        let epoch = state.epoch.wrapping_add(1);
        *state = State {
            epoch,
            ..State::default()
        };
    }
}

fn modified(metadata: &fs::Metadata) -> u128 {
    metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |time| time.as_nanos())
}

fn owned_variant(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() == 41
        && bytes[16] == b'-'
        && &bytes[33..] == b".variant"
        && bytes[..16]
            .iter()
            .chain(&bytes[17..33])
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

pub(super) fn owned_file(name: &str) -> bool {
    owned_variant(name) || owned_temporary(name) || name == INDEX_NAME
}

fn owned_temporary(name: &str) -> bool {
    let Some((base, suffix)) = name.rsplit_once(".tmp-") else {
        return false;
    };
    let Some((pid, sequence)) = suffix.split_once('-') else {
        return false;
    };
    (owned_variant(base) || base == INDEX_NAME)
        && pid
            .parse::<u32>()
            .is_ok_and(|value| value.to_string() == pid)
        && sequence
            .parse::<u64>()
            .is_ok_and(|value| value.to_string() == sequence)
}

fn read_record(path: &Path, name: &str) -> io::Result<Option<Record>> {
    let stat = fs::symlink_metadata(path)?;
    if !stat.is_file() {
        return Ok(None);
    }
    let mut file = fs::File::open(path)?;
    let parsed = (|| -> Option<VariantMetadata> {
        let mut prefix = [0; 12];
        file.read_exact(&mut prefix).ok()?;
        if &prefix[..8] != MAGIC {
            return None;
        }
        let length = u32::from_le_bytes(prefix[8..].try_into().ok()?) as usize;
        if length > 64 * 1024 || stat.len() <= 12 + length as u64 {
            return None;
        }
        let mut header = vec![0; length];
        file.read_exact(&mut header).ok()?;
        let metadata: VariantMetadata = serde_json::from_slice(&header).ok()?;
        if metadata.version != 2
            || metadata.uses.is_empty()
            || metadata.width == 0
            || metadata.height == 0
            || metadata
                .recipe
                .max_side
                .is_some_and(|side| metadata.width.max(metadata.height) > side)
            || recipe_disk_path(path.parent()?, &metadata.url, metadata.recipe)
                .file_name()?
                .to_str()?
                != name
        {
            return None;
        }
        Some(metadata)
    })();
    Ok(Some(Record {
        key: parsed
            .as_ref()
            .map(|metadata| image_recipe_cache_key(&metadata.url, metadata.recipe))
            .unwrap_or_default(),
        url: parsed
            .as_ref()
            .map(|metadata| metadata.url.clone())
            .unwrap_or_default(),
        bytes: stat.len(),
        modified: modified(&stat),
        last_used: (modified(&stat) / 1_000_000_000).min(nova_config::now_secs() as u128) as u64,
        invalid: parsed.is_none(),
        revision: 0,
    }))
}

impl Tracker {
    fn reconcile(&self) -> io::Result<bool> {
        self.reconcile_before_commit(|| {})
    }

    fn reconcile_before_commit(&self, before_commit: impl FnOnce()) -> io::Result<bool> {
        let generation = CACHE_GENERATION.load(Ordering::Acquire);
        let (revision, epoch) = {
            let state = self.state.lock().unwrap();
            (state.revision, state.epoch)
        };
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        let mut found = BTreeMap::new();
        let mut chunk = Instant::now();
        for (i, entry) in entries.into_iter().flatten().enumerate() {
            if self.stop.load(Ordering::Acquire) {
                return Ok(false);
            }
            let entry = entry?;
            let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
                continue;
            };
            if owned_variant(&name) {
                let _mutation = CACHE_MUTATION.read().unwrap();
                let lock = entry_lock(&self.directory.join(format!("{}.lock", &name[..16])));
                let _guard = lock.lock().unwrap();
                match read_record(&entry.path(), &name) {
                    Ok(Some(record)) => {
                        found.insert(name, record);
                    }
                    Ok(None) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error),
                }
            }
            if i % 128 == 127 || chunk.elapsed() >= Duration::from_millis(20) {
                thread::yield_now();
                chunk = Instant::now();
            }
        }
        before_commit();
        let _mutation = CACHE_MUTATION.read().unwrap();
        let mut state = self.state.lock().unwrap();
        if state.epoch != epoch || CACHE_GENERATION.load(Ordering::Acquire) != generation {
            return Ok(false);
        }
        // Preserve accesses and publications newer than this scan. A scan
        // must never erase a file committed while directory enumeration ran.
        let previous = state.records.clone();
        for (name, old) in &previous {
            if old.revision > revision {
                found.insert(name.clone(), old.clone());
            } else if let Some(record) = found.get_mut(name)
                && record.modified == old.modified
                && record.bytes == old.bytes
            {
                record.last_used = old.last_used.min(nova_config::now_secs());
                record.invalid |= old.invalid;
            }
        }
        found.retain(|name, _| {
            state
                .deleted
                .get(name)
                .is_none_or(|removed| *removed <= revision)
        });
        state.deleted.retain(|_, removed| *removed > revision);
        state.records.clear();
        state.by_key.clear();
        state.bytes = 0;
        for (name, record) in found {
            state.insert(name, record);
        }
        state.initialized = true;
        state.dirty = true;
        Ok(true)
    }

    fn initialize(&self) -> io::Result<bool> {
        let _mutation = CACHE_MUTATION.read().unwrap();
        let path = self.directory.join(INDEX_NAME);
        let loaded = fs::File::open(path)
            .ok()
            .and_then(|file| {
                if file.metadata().ok()?.len() > MAX_INDEX_BYTES {
                    return None;
                }
                serde_json::from_reader::<_, Index>(file.take(MAX_INDEX_BYTES + 1)).ok()
            })
            .filter(|index| {
                index.version == 1 && index.records.keys().all(|name| owned_variant(name))
            });
        let trusted = loaded.as_ref().is_some_and(|index| index.clean_shutdown);
        if let Some(index) = loaded {
            let mut state = self.state.lock().unwrap();
            for (name, record) in index.records {
                if !state.records.contains_key(&name) {
                    state.insert(name, record);
                }
            }
        }
        drop(_mutation);
        if !trusted && !self.reconcile()? {
            return Ok(false);
        }
        // Persist the running marker once; subsequent index writes are batched.
        self.flush(false, true)?;
        self.state.lock().unwrap().initialized = true;
        Ok(true)
    }

    fn flush_batch(&self, elapsed: Duration, previous: &mut Duration) -> io::Result<()> {
        if elapsed.saturating_sub(*previous) >= BATCH_INTERVAL {
            self.flush(false, false)?;
            *previous = elapsed;
        }
        Ok(())
    }

    fn flush(&self, clean: bool, force: bool) -> io::Result<()> {
        let Some(_maintenance) = MaintenanceGuard::try_acquire() else {
            return if force {
                Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "image maintenance busy",
                ))
            } else {
                Ok(())
            };
        };
        let _mutation = CACHE_MUTATION.read().unwrap();
        let (index, revision, epoch) = {
            let state = self.state.lock().unwrap();
            if !force && !state.dirty {
                return Ok(());
            }
            (
                Index {
                    version: 1,
                    clean_shutdown: clean,
                    records: state.records.clone(),
                },
                state.revision,
                state.epoch,
            )
        };
        let path = self.directory.join(INDEX_NAME);
        if index.records.is_empty() {
            match fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        } else {
            fs::create_dir_all(&self.directory)?;
            let raw = serde_json::to_vec(&index)?;
            if raw.len() as u64 > MAX_INDEX_BYTES {
                return Err(io::Error::other("image usage index exceeds limit"));
            }
            atomic_write(&path, &raw)?;
        }
        let mut state = self.state.lock().unwrap();
        if state.revision == revision && state.epoch == epoch {
            state.dirty = false;
        }
        Ok(())
    }

    #[cfg(test)]
    fn evict(
        &self,
        limit: Option<u64>,
        preferred: &HashSet<String>,
        now: u64,
    ) -> io::Result<Report> {
        self.evict_before_delete(limit, preferred, now, || {})
    }

    #[cfg(test)]
    fn evict_before_delete(
        &self,
        limit: Option<u64>,
        preferred: &HashSet<String>,
        now: u64,
        before_delete: impl FnOnce(),
    ) -> io::Result<Report> {
        self.evict_grouped(
            limit,
            preferred,
            &HashSet::new(),
            &|url| url.to_owned(),
            now,
            before_delete,
        )
    }

    fn evict_grouped(
        &self,
        limit: Option<u64>,
        preferred: &HashSet<String>,
        protected: &HashSet<String>,
        group_for: &dyn Fn(&str) -> String,
        now: u64,
        before_delete: impl FnOnce(),
    ) -> io::Result<Report> {
        let (mut candidates, epoch, snapshot_revision) = {
            let state = self.state.lock().unwrap();
            (
                state
                    .records
                    .iter()
                    .map(|(name, record)| (name.clone(), record.clone()))
                    .collect::<Vec<_>>(),
                state.epoch,
                state.revision,
            )
        };
        let mut group_usage: HashMap<String, (u64, bool, bool)> = HashMap::new();
        let mut group_members: HashMap<String, Vec<String>> = HashMap::new();
        let mut checked_revision: HashMap<String, u64> = HashMap::new();
        let mut protected = protected.clone();
        protected.extend(blob::protected(&self.directory));
        for key in &protected {
            group_usage.entry(group_for(key)).or_default().2 = true;
        }
        for (name, record) in &candidates {
            let group = group_for(&record.url);
            group_members
                .entry(group.clone())
                .or_default()
                .push(name.clone());
            checked_revision.insert(group.clone(), snapshot_revision);
            let usage = group_usage.entry(group).or_default();
            usage.0 = usage.0.max(record.last_used);
            usage.1 |= preferred.contains(&record.url);
            usage.2 |= protected.contains(&record.url);
        }
        candidates.sort_by_key(|(name, record)| {
            let group = group_for(&record.url);
            let usage = group_usage.get(&group).copied().unwrap_or_default();
            (
                !record.invalid,
                usage.1,
                usage.0,
                group,
                record.last_used,
                name.clone(),
            )
        });
        let target = limit.map(|limit| limit.saturating_mul(9) / 10);
        let over_limit = limit.is_some_and(|limit| self.state.lock().unwrap().bytes > limit);
        let mut report = Report::default();
        let mut chunk = Instant::now();
        let mut deferred = false;
        before_delete();
        let mut selected_group: Option<String> = None;
        for (i, (name, candidate)) in candidates.into_iter().enumerate() {
            if i % 128 == 127 || chunk.elapsed() >= Duration::from_millis(20) {
                thread::yield_now();
                chunk = Instant::now();
            }
            if self.stop.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "image cleanup stopped",
                ));
            }
            let group = group_for(&candidate.url);
            let usage = group_usage.get(&group).copied().unwrap_or_default();
            // Pins are checked again immediately before deletion, not only in
            // the inventory snapshot. A newly opened chapter stays protected.
            let pins = blob::pin_guard();
            if usage.2
                || pins
                    .keys()
                    .any(|(directory, key)| directory == &self.directory && group_for(key) == group)
            {
                deferred = true;
                continue;
            }
            if !candidate.invalid
                && selected_group.as_ref() != Some(&group)
                && (!over_limit
                    || target.is_none_or(|target| self.state.lock().unwrap().bytes <= target))
            {
                continue;
            }
            // Recently displayed artwork and current source publications get
            // another turn, including pending accesses not yet flushed to disk.
            // Refresh just this group's member records after concurrent touches.
            // Cache deletions update the checked revision below, avoiding an
            // O(all records) scan for every file in a large namespace.
            let newest_use = {
                let state = self.state.lock().unwrap();
                if checked_revision.get(&group) != Some(&state.revision) {
                    let newest = group_members
                        .get(&group)
                        .into_iter()
                        .flatten()
                        .filter_map(|name| state.records.get(name))
                        .map(|record| record.last_used)
                        .max()
                        .unwrap_or(usage.0);
                    group_usage.entry(group.clone()).or_default().0 = newest;
                    checked_revision.insert(group.clone(), state.revision);
                    newest
                } else {
                    usage.0
                }
            };
            if !candidate.invalid && now.saturating_sub(newest_use) < BATCH_INTERVAL.as_secs() {
                deferred = true;
                continue;
            }
            if !candidate.invalid {
                selected_group = Some(group.clone());
            }
            let Some(_maintenance) = MaintenanceGuard::try_acquire() else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "image maintenance busy",
                ));
            };
            let _mutation = CACHE_MUTATION.read().unwrap();
            let lock = entry_lock(&self.directory.join(format!("{}.lock", &name[..16])));
            let _guard = lock.lock().unwrap();
            let active = format!(
                "{}:{}:{}",
                self.directory.display(),
                CACHE_GENERATION.load(Ordering::Acquire),
                candidate.key
            );
            if PUBLICATIONS
                .lock()
                .unwrap()
                .get(&active)
                .is_some_and(|state| state.active > 0)
            {
                deferred = true;
                continue;
            }
            let mut state = self.state.lock().unwrap();
            // Clear can recreate the same name, size, timestamp and local
            // revision. Its epoch must invalidate the entire old snapshot.
            if state.epoch != epoch {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "image cache cleared during eviction",
                ));
            }
            if state
                .records
                .get(&name)
                .is_none_or(|current| current.revision != candidate.revision)
            {
                deferred = true;
                continue;
            }
            let path = self.directory.join(&name);
            match fs::symlink_metadata(&path) {
                Ok(stat)
                    if stat.is_file()
                        && stat.len() == candidate.bytes
                        && modified(&stat) == candidate.modified =>
                {
                    fs::remove_file(path)?;
                    state.remove(&name);
                    report.removed_files += 1;
                    report.removed_bytes = report.removed_bytes.saturating_add(candidate.bytes);
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => state.remove(&name),
                Err(error) => return Err(error),
                _ => {
                    deferred = true;
                }
            }
            checked_revision.insert(group, state.revision);
            drop(state);
            drop(_guard);
            drop(_mutation);
            drop(_maintenance);
        }
        report.remaining_bytes = self.state.lock().unwrap().bytes;
        report.deferred = deferred
            && (self
                .state
                .lock()
                .unwrap()
                .records
                .values()
                .any(|record| record.invalid)
                || over_limit && limit.is_some_and(|limit| report.remaining_bytes > limit));
        Ok(report)
    }

    fn remove_temporary(&self, now: u64) -> io::Result<Report> {
        let mut report = Report::default();
        let entries = match fs::read_dir(&self.directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(report),
            Err(error) => return Err(error),
        };
        let mut chunk = Instant::now();
        for (i, entry) in entries.enumerate() {
            if i % 128 == 127 || chunk.elapsed() >= Duration::from_millis(20) {
                thread::yield_now();
                chunk = Instant::now();
            }
            if self.stop.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "image cleanup stopped",
                ));
            }
            let entry = entry?;
            if !entry.file_name().to_str().is_some_and(owned_temporary) {
                continue;
            }
            let Some(_maintenance) = MaintenanceGuard::try_acquire() else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "image maintenance busy",
                ));
            };
            let _mutation = CACHE_MUTATION.read().unwrap();
            let stat = fs::symlink_metadata(entry.path())?;
            if stat.is_file()
                && now.saturating_sub((modified(&stat) / 1_000_000_000) as u64)
                    >= ROUTINE_INTERVAL_SECS
            {
                fs::remove_file(entry.path())?;
                report.removed_files += 1;
                report.removed_bytes += stat.len();
            }
        }
        Ok(report)
    }
}

/// Retained by the app until its event loop exits. Orderly shutdown joins the
/// worker after it flushes; an abrupt process death leaves the running marker.
pub struct Worker {
    tracker: Arc<Tracker>,
    thread: Option<JoinHandle<()>>,
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.tracker.stop.store(true, Ordering::Release);
        let _ = self.tracker.wake.try_send(());
        if let Some(worker) = self.thread.take() {
            let _ = worker.join();
        }
    }
}

pub fn start(directory: PathBuf, backend: Arc<dyn Backend>) -> io::Result<Worker> {
    let (wake, receiver) = mpsc::sync_channel(1);
    let tracker = Arc::new(Tracker {
        directory: directory.clone(),
        state: Mutex::new(State::default()),
        wake,
        stop: AtomicBool::new(false),
    });
    TRACKERS
        .lock()
        .unwrap()
        .insert(directory, Arc::downgrade(&tracker));
    let work = tracker.clone();
    let thread = thread::Builder::new()
        .name("nova-image-cache-gc".into())
        .spawn(move || {
            let mut initialized = false;
            let mut last_cleanup = None;
            let started = Instant::now();
            let mut last_flush = Duration::ZERO;
            let mut failed_at: Option<Instant> = None;
            let mut retry_deferred_on_wake = false;
            while !work.stop.load(Ordering::Acquire) {
                let policy = backend.policy();
                if policy.enabled
                    && failed_at.is_none_or(|failed| failed.elapsed() >= BATCH_INTERVAL)
                {
                    let result = (|| -> io::Result<()> {
                        if !initialized {
                            if !work.initialize()? {
                                return Err(io::Error::new(
                                    io::ErrorKind::WouldBlock,
                                    "image inventory changed during initialization",
                                ));
                            }
                            last_cleanup = backend.last_cleanup(&work.directory)?;
                            initialized = true;
                        }
                        work.flush_batch(started.elapsed(), &mut last_flush)?;
                        let now = nova_config::now_secs();
                        let routine = cleanup_due(last_cleanup, now);
                        let limit = policy.limit_bytes;
                        let need = {
                            let state = work.state.lock().unwrap();
                            routine
                                || limit.is_some_and(|limit| state.bytes > limit)
                                || state.records.values().any(|record| record.invalid)
                        };
                        if need {
                            if routine && !work.reconcile()? {
                                return Err(io::Error::new(
                                    io::ErrorKind::WouldBlock,
                                    "image inventory changed during reconciliation",
                                ));
                            }
                            let preferred = backend.preferred_urls()?;
                            let protected = backend.protected_urls()?;
                            let report = work.evict_grouped(
                                limit,
                                &preferred,
                                &protected,
                                &|url| backend.eviction_group(url),
                                now,
                                || {},
                            )?;
                            if report.removed_files > 0 {
                                backend.completed(report);
                            }
                            if report.deferred {
                                return Err(io::Error::new(
                                    io::ErrorKind::WouldBlock,
                                    "image eviction deferred for active entries",
                                ));
                            }
                            if routine {
                                let temporary = work.remove_temporary(now)?;
                                if temporary.removed_files > 0 {
                                    backend.completed(Report {
                                        remaining_bytes: report.remaining_bytes,
                                        ..temporary
                                    });
                                }
                                backend.save_cleanup(&work.directory, now)?;
                                last_cleanup = Some(now);
                            }
                        }
                        Ok(())
                    })();
                    if let Err(error) = result {
                        if error.kind() != io::ErrorKind::WouldBlock
                            && error.kind() != io::ErrorKind::Interrupted
                        {
                            backend.failed(&error);
                        }
                        failed_at = Some(Instant::now());
                        retry_deferred_on_wake = error.kind() == io::ErrorKind::WouldBlock;
                    } else {
                        failed_at = None;
                        retry_deferred_on_wake = false;
                    }
                }
                let timeout = if !policy.enabled || failed_at.is_some() {
                    BATCH_INTERVAL
                } else {
                    BATCH_INTERVAL
                        .saturating_sub(started.elapsed().saturating_sub(last_flush))
                        .max(Duration::from_secs(1))
                };
                if receiver.recv_timeout(timeout).is_ok() && retry_deferred_on_wake {
                    // Dropping an active-chapter lease or changing a quota
                    // should retry immediately; real I/O errors retain backoff.
                    failed_at = None;
                }
            }
            if initialized
                && backend.policy().enabled
                && let Err(error) = work.flush(true, true)
            {
                backend.failed(&error);
            }
        })?;
    Ok(Worker {
        tracker,
        thread: Some(thread),
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{scratch, serial, source_image};
    use super::*;

    struct Fixture {
        tracker: Arc<Tracker>,
        settings: CacheSettings,
    }
    impl Fixture {
        fn new(label: &str) -> Self {
            let directory = scratch(label);
            let (wake, _) = mpsc::sync_channel(1);
            let tracker = Arc::new(Tracker {
                directory: directory.clone(),
                state: Mutex::new(State::default()),
                wake,
                stop: AtomicBool::new(false),
            });
            TRACKERS
                .lock()
                .unwrap()
                .insert(directory, Arc::downgrade(&tracker));
            let settings = active_cache_settings();
            nova_config::set_cache_settings(CacheSettings::default());
            Self { tracker, settings }
        }
        fn image(&self, title: &str, used: u64) -> (String, String) {
            let url = format!(
                "https://gc.test/{}/{title}",
                self.tracker
                    .directory
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
            );
            let settings = CacheSettings::default();
            cached_pixels_with(
                &self.tracker.directory,
                &url,
                ImageUse::Fixed(Some(32)),
                &settings,
                || Some(source_image(16, 12, false)),
            )
            .unwrap();
            let key = image_cache_key(&url, ImageUse::Fixed(Some(32)));
            let mut state = self.tracker.state.lock().unwrap();
            state.touch(&key, used);
            let name = state.by_key[&key].clone();
            (url, name)
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            TRACKERS.lock().unwrap().remove(&self.tracker.directory);
            nova_config::set_cache_settings(self.settings.clone());
            let _ = fs::remove_dir_all(&self.tracker.directory);
        }
    }

    #[test]
    fn dropping_active_chapter_lease_retries_deferred_worker_without_two_minute_delay() {
        struct TestBackend {
            entered: mpsc::Sender<()>,
            completed: mpsc::Sender<Report>,
        }
        impl Backend for TestBackend {
            fn policy(&self) -> DiskPolicy {
                DiskPolicy {
                    enabled: true,
                    limit_bytes: Some(1),
                }
            }
            fn last_cleanup(&self, _: &Path) -> io::Result<Option<u64>> {
                Ok(Some(nova_config::now_secs()))
            }
            fn save_cleanup(&self, _: &Path, _: u64) -> io::Result<()> {
                Ok(())
            }
            fn preferred_urls(&self) -> io::Result<HashSet<String>> {
                Ok(HashSet::new())
            }
            fn protected_urls(&self) -> io::Result<HashSet<String>> {
                let _ = self.entered.send(());
                Ok(HashSet::new())
            }
            fn completed(&self, report: Report) {
                let _ = self.completed.send(report);
            }
            fn failed(&self, error: &io::Error) {
                panic!("unexpected worker failure: {error}")
            }
        }
        let _serial = serial();
        let fixture = Fixture::new("gc-lease-wake");
        let (key, name) = fixture.image("active-chapter-page", 1);
        fixture.tracker.flush(true, true).unwrap();
        let cache = blob::Cache::new(fixture.tracker.directory.clone(), 1024);
        let lease = cache.pin([key]);
        let (entered, entry) = mpsc::channel();
        let (completed, reports) = mpsc::channel();
        let worker = start(
            fixture.tracker.directory.clone(),
            Arc::new(TestBackend { entered, completed }),
        )
        .unwrap();
        entry.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(fixture.tracker.directory.join(&name).exists());
        drop(lease);
        assert_eq!(
            reports
                .recv_timeout(Duration::from_secs(2))
                .expect("lease wake must bypass deferred-work backoff")
                .removed_files,
            1
        );
        assert!(!fixture.tracker.directory.join(name).exists());
        drop(worker);
    }

    #[test]
    fn encoded_memory_hits_touch_the_actual_usage_key_without_disk_access() {
        let _serial = serial();
        let fixture = Fixture::new("gc-blob-touch");
        let cache = blob::Cache::new(fixture.tracker.directory.clone(), 64);
        assert_eq!(cache.tracked_usage(), None);
        assert!(
            cache
                .put_if_current(
                    "opaque-page-key",
                    b"encoded page",
                    cache.generation(),
                    |_| Ok((720, 20000)),
                    || true
                )
                .unwrap()
        );
        let (name, key) = {
            let state = fixture.tracker.state.lock().unwrap();
            let (name, record) = state.records.iter().next().unwrap();
            (name.clone(), record.key.clone())
        };
        fixture.tracker.state.lock().unwrap().touch(&key, 1);
        fixture.tracker.initialize().unwrap();
        let usage = cache.tracked_usage().unwrap();
        assert_eq!(usage.1, 1);
        assert!(usage.0 > 0);
        fs::remove_file(fixture.tracker.directory.join(name)).unwrap();
        fs::remove_file(fixture.tracker.directory.join(INDEX_NAME)).unwrap();
        assert_eq!(
            cache.tracked_usage(),
            Some(usage),
            "tracked quota admission is a memory-only snapshot"
        );
        let before = nova_config::now_secs();
        cache.touch("opaque-page-key");
        let state = fixture.tracker.state.lock().unwrap();
        assert!(state.records.values().next().unwrap().last_used >= before);
        assert!(state.dirty);
        drop(state);
        assert_eq!(
            fs::read_dir(&fixture.tracker.directory).unwrap().count(),
            0,
            "touch only updates the in-memory usage index"
        );
    }

    #[test]
    fn chapter_groups_evict_completely_by_latest_use_and_leave_poster_namespace_alone() {
        let _serial = serial();
        let pages = Fixture::new("gc-chapters");
        let posters = Fixture::new("gc-independent-posters");
        let (_, poster) = posters.image("keep-many-small-posters", 1);
        let (a1, a1name) = pages.image("chapter-a-page1", 1);
        let (a2, a2name) = pages.image("chapter-a-page2", 50);
        let (b1, b1name) = pages.image("chapter-b-page1", 20);
        let (b2, b2name) = pages.image("chapter-b-page2", 30);
        let groups = HashMap::from([
            (a1, "a".into()),
            (a2, "a".into()),
            (b1, "b".into()),
            (b2, "b".into()),
        ]);
        let total = pages.tracker.state.lock().unwrap().bytes;
        let report = pages
            .tracker
            .evict_grouped(
                Some(total - 1),
                &HashSet::new(),
                &HashSet::new(),
                &|url| groups.get(url).cloned().unwrap_or_else(|| url.to_owned()),
                1000,
                || {},
            )
            .unwrap();
        assert_eq!(
            report.removed_files, 2,
            "budget crossing must finish the selected chapter"
        );
        assert!(pages.tracker.directory.join(a1name).exists());
        assert!(pages.tracker.directory.join(a2name).exists());
        assert!(!pages.tracker.directory.join(b1name).exists());
        assert!(!pages.tracker.directory.join(b2name).exists());
        assert!(posters.tracker.directory.join(poster).exists());
    }

    #[test]
    fn chapter_lease_acquired_after_inventory_protects_the_whole_group() {
        let _serial = serial();
        let fixture = Fixture::new("gc-late-chapter-lease");
        let (one, one_name) = fixture.image("one", 1);
        let (two, two_name) = fixture.image("two", 1);
        let groups = HashMap::from([(one, "chapter".into()), (two.clone(), "chapter".into())]);
        let cache = blob::Cache::new(fixture.tracker.directory.clone(), 1024);
        let mut lease = None;
        let report = fixture
            .tracker
            .evict_grouped(
                Some(1),
                &HashSet::new(),
                &HashSet::new(),
                &|url| groups.get(url).cloned().unwrap_or_else(|| url.to_owned()),
                1000,
                || {
                    lease = Some(cache.pin([two]));
                },
            )
            .unwrap();
        assert!(report.deferred);
        assert_eq!(report.removed_files, 0);
        assert!(fixture.tracker.directory.join(one_name).exists());
        assert!(fixture.tracker.directory.join(two_name).exists());
        drop(lease);
        assert_eq!(
            fixture
                .tracker
                .evict_grouped(
                    Some(1),
                    &HashSet::new(),
                    &HashSet::new(),
                    &|url| groups.get(url).cloned().unwrap_or_else(|| url.to_owned()),
                    1000,
                    || {}
                )
                .unwrap()
                .removed_files,
            2
        );
    }

    #[test]
    fn usage_is_batched_for_two_minutes_and_idle_batches_do_not_write() {
        let _serial = serial();
        let fixture = Fixture::new("gc-batch");
        let (_, name) = fixture.image("poster", 100);
        fixture.tracker.flush(false, true).unwrap();
        let path = fixture.tracker.directory.join(INDEX_NAME);
        let initial = fs::read(&path).unwrap();
        let image = fs::read(fixture.tracker.directory.join(&name)).unwrap();
        let key = fixture.tracker.state.lock().unwrap().records[&name]
            .key
            .clone();
        for time in 101..1101 {
            fixture.tracker.state.lock().unwrap().touch(&key, time);
        }
        let mut previous = Duration::ZERO;
        fixture
            .tracker
            .flush_batch(Duration::from_secs(119), &mut previous)
            .unwrap();
        assert_eq!(fs::read(&path).unwrap(), initial);
        fixture
            .tracker
            .flush_batch(Duration::from_secs(120), &mut previous)
            .unwrap();
        let saved = fs::read(&path).unwrap();
        let index: Index = serde_json::from_slice(&saved).unwrap();
        assert_eq!(index.records.len(), 1);
        assert_eq!(index.records[&name].last_used, 1100);
        assert_eq!(
            fs::read(fixture.tracker.directory.join(&name)).unwrap(),
            image,
            "recency must not rewrite image bytes"
        );
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        fixture
            .tracker
            .flush_batch(Duration::from_secs(240), &mut previous)
            .unwrap();
        assert_eq!(fs::metadata(&path).unwrap().modified().unwrap(), modified);
        assert_eq!(fs::read(&path).unwrap(), saved);
    }

    #[test]
    fn size_eviction_prefers_library_then_falls_back_and_unlimited_keeps_valid_images() {
        let _serial = serial();
        let fixture = Fixture::new("gc-library");
        let (library, library_name) = fixture.image("library", 1);
        let (_, old_name) = fixture.image("old-browse", 2);
        let (_, recent_name) = fixture.image("recent-browse", 100);
        let preferred = HashSet::from([library]);
        let total = fixture.tracker.state.lock().unwrap().bytes;
        assert_eq!(
            fixture
                .tracker
                .evict(None, &preferred, 1000)
                .unwrap()
                .removed_files,
            0
        );
        let report = fixture
            .tracker
            .evict(Some(total - 1), &preferred, 1000)
            .unwrap();
        assert_eq!(report.removed_files, 1);
        assert!(report.remaining_bytes <= (total - 1) * 9 / 10);
        assert!(!fixture.tracker.directory.join(old_name).exists());
        assert!(fixture.tracker.directory.join(&library_name).exists());
        assert!(fixture.tracker.directory.join(recent_name).exists());
        let report = fixture.tracker.evict(Some(1), &preferred, 1000).unwrap();
        assert_eq!(report.removed_files, 2);
        assert_eq!(report.remaining_bytes, 0);
        assert!(!fixture.tracker.directory.join(library_name).exists());
    }

    #[test]
    fn pending_memory_hits_and_in_flight_publications_are_not_evicted() {
        let _serial = serial();
        let fixture = Fixture::new("gc-active");
        let (url, name) = fixture.image("active", 1);
        let key = image_cache_key(&url, ImageUse::Fixed(Some(32)));
        let ticket = PublicationTicket::acquire(
            &fixture.tracker.directory,
            &url,
            ImageRecipe::resolve(ImageUse::Fixed(Some(32)), CacheSettings::default()),
            CACHE_GENERATION.load(Ordering::Acquire),
        );
        let report = fixture
            .tracker
            .evict(Some(1), &HashSet::new(), 1000)
            .unwrap();
        assert!(report.deferred);
        assert_eq!(report.removed_files, 0);
        drop(ticket);
        assert!(decoded_cache_get(&key).is_some());
        let report = fixture
            .tracker
            .evict(Some(1), &HashSet::new(), nova_config::now_secs())
            .unwrap();
        assert!(
            report.deferred,
            "a decoded hit must protect the same disk variant before the batch is flushed"
        );
        assert!(fixture.tracker.directory.join(name).exists());
    }

    #[test]
    fn reconciliation_and_pending_batches_cannot_resurrect_cleared_or_retired_entries() {
        let _serial = serial();
        let fixture = Fixture::new("gc-clear");
        let (_, name) = fixture.image("retired", 1);
        fixture.tracker.flush(false, true).unwrap();
        assert!(
            fixture
                .tracker
                .reconcile_before_commit(|| {
                    fs::remove_file(fixture.tracker.directory.join(&name)).unwrap();
                    fixture.tracker.state.lock().unwrap().remove(&name);
                })
                .unwrap()
        );
        assert!(fixture.tracker.state.lock().unwrap().records.is_empty());
        fixture.image("cleared", 1);
        assert!(
            !fixture
                .tracker
                .reconcile_before_commit(|| {
                    clear_poster_cache_dir(&fixture.tracker.directory);
                })
                .unwrap()
        );
        fixture.tracker.flush(false, false).unwrap();
        assert_eq!(fs::read_dir(&fixture.tracker.directory).unwrap().count(), 0);
        assert!(fixture.tracker.state.lock().unwrap().records.is_empty());
    }

    #[test]
    fn corrupt_index_rebuilds_owned_headers_and_failure_preserves_dirty_usage() {
        let _serial = serial();
        let fixture = Fixture::new("gc-rebuild");
        let (_, name) = fixture.image("valid", 1);
        fs::write(fixture.tracker.directory.join(INDEX_NAME), "broken").unwrap();
        assert!(fixture.tracker.initialize().unwrap());
        assert_eq!(fixture.tracker.state.lock().unwrap().records.len(), 1);
        fixture.tracker.flush(true, true).unwrap();
        let saved: Index =
            serde_json::from_slice(&fs::read(fixture.tracker.directory.join(INDEX_NAME)).unwrap())
                .unwrap();
        assert!(saved.clean_shutdown);
        let path = fixture.tracker.directory.join(INDEX_NAME);
        fs::remove_file(&path).unwrap();
        fs::create_dir(&path).unwrap();
        let key = fixture.tracker.state.lock().unwrap().records[&name]
            .key
            .clone();
        fixture.tracker.state.lock().unwrap().touch(&key, 1000);
        assert!(fixture.tracker.flush(false, false).is_err());
        assert!(fixture.tracker.state.lock().unwrap().dirty);
        assert!(fixture.tracker.directory.join(name).exists());
    }

    #[test]
    fn structural_corruption_is_removed_but_unrelated_files_and_symlinks_are_preserved() {
        let _serial = serial();
        let fixture = Fixture::new("gc-owned");
        let name = "0000000000000000-0000000000000000.variant";
        fs::write(fixture.tracker.directory.join(name), "broken").unwrap();
        fs::write(fixture.tracker.directory.join("notes.txt"), "unrelated").unwrap();
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(
                fixture.tracker.directory.join("notes.txt"),
                fixture
                    .tracker
                    .directory
                    .join("1111111111111111-1111111111111111.variant"),
            )
            .unwrap();
        }
        fixture.tracker.reconcile().unwrap();
        assert_eq!(
            fixture
                .tracker
                .evict(None, &HashSet::new(), 1000)
                .unwrap()
                .removed_files,
            1
        );
        assert!(!fixture.tracker.directory.join(name).exists());
        assert_eq!(
            fs::read(fixture.tracker.directory.join("notes.txt")).unwrap(),
            b"unrelated"
        );
        assert!(!owned_variant("éééééééé-0000000000000000.variant"));
    }

    #[test]
    fn routine_gate_handles_restart_future_clocks_and_exact_deadline() {
        assert!(cleanup_due(None, 100));
        assert!(!cleanup_due(Some(100), 100));
        assert!(!cleanup_due(Some(100), 100 + ROUTINE_INTERVAL_SECS - 1));
        assert!(cleanup_due(Some(100), 100 + ROUTINE_INTERVAL_SECS));
        assert!(cleanup_due(Some(101), 100));
    }

    #[test]
    fn restart_reconciles_unclean_index_and_temporary_cleanup_owns_only_old_files() {
        let _serial = serial();
        let fixture = Fixture::new("gc-restart");
        let (_, name) = fixture.image("removed-after-crash", 1);
        fixture.tracker.flush(false, true).unwrap();
        fs::remove_file(fixture.tracker.directory.join(&name)).unwrap();
        *fixture.tracker.state.lock().unwrap() = State::default();
        assert!(fixture.tracker.initialize().unwrap());
        assert!(fixture.tracker.state.lock().unwrap().records.is_empty());
        let now = nova_config::now_secs();
        for (suffix, age) in [
            (".tmp-12-0", ROUTINE_INTERVAL_SECS),
            (".tmp-12-1", 60),
            (".tmp-012-2", ROUTINE_INTERVAL_SECS),
        ] {
            let file = fs::File::create(fixture.tracker.directory.join(format!("{name}{suffix}")))
                .unwrap();
            file.set_times(
                fs::FileTimes::new().set_modified(UNIX_EPOCH + Duration::from_secs(now - age)),
            )
            .unwrap();
        }
        let report = fixture.tracker.remove_temporary(now).unwrap();
        assert_eq!(report.removed_files, 1);
        assert!(
            fixture
                .tracker
                .directory
                .join(format!("{name}.tmp-12-1"))
                .exists()
        );
        assert!(
            fixture
                .tracker
                .directory
                .join(format!("{name}.tmp-012-2"))
                .exists()
        );
        let (url, name) = fixture.image("disabled-hit", 1);
        nova_config::set_cache_settings(CacheSettings {
            cache_images: false,
            ..Default::default()
        });
        touch_decoded(&image_cache_key(&url, ImageUse::Fixed(Some(32))));
        assert_eq!(
            fixture.tracker.state.lock().unwrap().records[&name].last_used,
            1
        );
    }

    #[test]
    fn worker_pauses_when_disabled_wakes_for_settings_and_flushes_on_shutdown() {
        struct TestBackend(mpsc::Sender<u64>);
        impl Backend for TestBackend {
            fn last_cleanup(&self, _: &Path) -> io::Result<Option<u64>> {
                Ok(None)
            }
            fn save_cleanup(&self, _: &Path, now: u64) -> io::Result<()> {
                self.0.send(now).map_err(io::Error::other)
            }
            fn preferred_urls(&self) -> io::Result<HashSet<String>> {
                Ok(HashSet::new())
            }
            fn completed(&self, _: Report) {}
            fn failed(&self, error: &io::Error) {
                panic!("unexpected worker error: {error}");
            }
        }
        let _serial = serial();
        let fixture = Fixture::new("gc-worker");
        fixture.image("poster", 1);
        nova_config::set_cache_settings(CacheSettings {
            cache_images: false,
            ..Default::default()
        });
        let (sender, receiver) = mpsc::channel();
        let worker = start(
            fixture.tracker.directory.clone(),
            Arc::new(TestBackend(sender)),
        )
        .unwrap();
        assert!(receiver.recv_timeout(Duration::from_millis(50)).is_err());
        assert!(!fixture.tracker.directory.join(INDEX_NAME).exists());
        nova_config::set_cache_settings(CacheSettings::default());
        wake();
        receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("settings wake completes the due pass");
        drop(worker);
        let index: Index =
            serde_json::from_slice(&fs::read(fixture.tracker.directory.join(INDEX_NAME)).unwrap())
                .unwrap();
        assert!(index.clean_shutdown);
        assert_eq!(index.records.len(), 1);
    }

    #[test]
    fn eviction_cannot_delete_a_recreated_file_after_clear_even_with_identical_metadata() {
        let _serial = serial();
        let fixture = Fixture::new("gc-evict-clear");
        let (_, name) = fixture.image("recreated", 1);
        let path = fixture.tracker.directory.join(&name);
        let original_time = fs::metadata(&path).unwrap().modified().unwrap();
        let result = fixture
            .tracker
            .evict_before_delete(Some(1), &HashSet::new(), 1000, || {
                clear_poster_cache_dir(&fixture.tracker.directory);
                fixture.image("recreated", 1);
                fs::File::options()
                    .write(true)
                    .open(&path)
                    .unwrap()
                    .set_times(fs::FileTimes::new().set_modified(original_time))
                    .unwrap();
            });
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::WouldBlock);
        assert!(path.exists());
        assert_eq!(fixture.tracker.state.lock().unwrap().records.len(), 1);
    }
}
