//! Durable file helpers shared by the applications: atomic replacement, private
//! directories and filesystem capacity.
//!
//! Every application used to carry its own copy of these with different
//! durability. The strongest variant lives here; callers that deliberately want
//! less (disposable caches) pick it explicitly through [`Durability`].
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// How much of a replacement is forced to stable storage before it returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Durability {
    /// Flush the new file's contents before the rename. A crash can still
    /// forget the rename itself, which leaves the previous file in place.
    File,
    /// Also flush the parent directory so the rename survives a crash. This is
    /// what [`atomic_write`] uses.
    FileAndDirectory,
}

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

/// A sibling name no other writer, thread or process can be using: concurrent
/// replacements of one path never share a temporary file.
fn unique_temporary(path: &Path) -> io::Result<PathBuf> {
    let name = path
        .file_name()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no file name"))?
        .to_string_lossy();
    Ok(path.with_file_name(format!(
        "{name}.tmp-{}-{}",
        std::process::id(),
        NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed)
    )))
}

/// Replace `path` with `bytes` so readers see either the old or the new
/// contents, never a prefix, and a crash after return cannot lose the write.
/// The file is created owner-only (0600) on Unix.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    atomic_write_with(path, bytes, Durability::FileAndDirectory)
}

/// [`atomic_write`] with an explicit durability level.
pub fn atomic_write_with(path: &Path, bytes: &[u8], durability: Durability) -> io::Result<()> {
    atomic_write_if(path, bytes, durability, || true).map(|_| ())
}

/// Replace `path` only if `still_current` returns true once the new contents
/// are durable in a unique temporary file. Returns whether the file was
/// replaced; a declined write leaves no temporary file behind.
pub fn atomic_write_if(
    path: &Path,
    bytes: &[u8],
    durability: Durability,
    still_current: impl FnOnce() -> bool,
) -> io::Result<bool> {
    let temporary = unique_temporary(path)?;
    write_through(&temporary, path, bytes, durability, still_current)
}

/// Like [`atomic_write`], but stages the bytes at a caller-chosen `temporary`
/// path instead of a unique one. This exists for on-disk layouts whose
/// recovery code recognizes a fixed temporary name; any file already at that
/// name is removed first so a planted symlink is never followed.
pub fn atomic_write_via(path: &Path, temporary: &Path, bytes: &[u8]) -> io::Result<()> {
    match fs::remove_file(temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    write_through(temporary, path, bytes, Durability::FileAndDirectory, || {
        true
    })
    .map(|_| ())
}

fn write_through(
    temporary: &Path,
    path: &Path,
    bytes: &[u8],
    durability: Durability,
    still_current: impl FnOnce() -> bool,
) -> io::Result<bool> {
    // The guard also cleans up if a writer unwinds.
    struct Pending<'a>(&'a Path, bool);
    impl Drop for Pending<'_> {
        fn drop(&mut self) {
            if self.1 {
                let _ = fs::remove_file(self.0);
            }
        }
    }
    let mut pending = Pending(temporary, true);
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    if !still_current() {
        return Ok(false);
    }
    // std replaces an existing destination on Windows as well as Unix.
    fs::rename(temporary, path)?;
    pending.1 = false;
    if durability == Durability::FileAndDirectory {
        sync_parent(path)?;
    }
    Ok(true)
}

#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    };
    File::open(parent)?.sync_all()
}

// Windows cannot open a directory as a file; NTFS journals the rename itself.
#[cfg(not(unix))]
fn sync_parent(_: &Path) -> io::Result<()> {
    Ok(())
}

/// Create `path` (and missing parents) as an owner-only directory, refusing a
/// symlink in its place, and repair looser permissions on an existing one.
pub fn private_dir(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(path)?;
    if fs::symlink_metadata(path)?.file_type().is_symlink() {
        return Err(io::Error::other("private directory cannot be a symlink"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

/// Bytes available to this user on the filesystem holding `path`. The path
/// need not exist yet: the nearest existing ancestor is queried and nothing is
/// created. `None` when the platform cannot say.
pub fn available_bytes(path: &Path) -> Option<u64> {
    filesystem_capacity(path).map(|(available, _)| available)
}

/// Available bytes and total capacity of the filesystem holding `path`.
pub fn filesystem_capacity(path: &Path) -> Option<(u64, u64)> {
    let mut existing = path;
    while !existing.exists() {
        existing = existing.parent()?;
    }
    platform_capacity(existing)
}

#[cfg(unix)]
// libc's filesystem counter widths differ between Unix targets.
#[allow(clippy::unnecessary_cast)]
fn platform_capacity(path: &Path) -> Option<(u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    let mut info = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // statvfs initializes the result only on success; f_bavail accounts for
    // blocks reserved for other users, unlike the raw free-block count.
    if unsafe { libc::statvfs(path.as_ptr(), info.as_mut_ptr()) } != 0 {
        return None;
    }
    let info = unsafe { info.assume_init() };
    Some((
        (info.f_bavail as u64).checked_mul(info.f_frsize as u64)?,
        (info.f_blocks as u64).checked_mul(info.f_frsize as u64)?,
    ))
}

#[cfg(windows)]
fn platform_capacity(path: &Path) -> Option<(u64, u64)> {
    use std::os::windows::ffi::OsStrExt;
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetDiskFreeSpaceExW(
            path: *const u16,
            available: *mut u64,
            total: *mut u64,
            free: *mut u64,
        ) -> i32;
    }
    let path: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut available = 0;
    let mut total = 0;
    if unsafe {
        GetDiskFreeSpaceExW(
            path.as_ptr(),
            &mut available,
            &mut total,
            std::ptr::null_mut(),
        )
    } == 0
    {
        None
    } else {
        Some((available, total))
    }
}

#[cfg(not(any(unix, windows)))]
fn platform_capacity(_: &Path) -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leftovers(directory: &Path) -> Vec<String> {
        fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".tmp"))
            .collect()
    }

    #[test]
    fn replacement_is_complete_and_leaves_no_temporary() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.json");
        atomic_write(&path, b"old").unwrap();
        atomic_write(&path, b"new contents").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"new contents");
        assert!(leftovers(directory.path()).is_empty());
    }

    #[test]
    fn temporary_names_are_unique_per_call() {
        let path = Path::new("/x/state.json");
        let first = unique_temporary(path).unwrap();
        let second = unique_temporary(path).unwrap();
        assert_ne!(first, second);
        assert_eq!(first.parent(), path.parent());
        assert!(first.to_string_lossy().contains("state.json.tmp-"));
    }

    #[test]
    fn concurrent_writers_never_share_a_temporary_file() {
        let directory = tempfile::tempdir().unwrap();
        let path = std::sync::Arc::new(directory.path().join("shared.json"));
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
        let workers: Vec<_> = (0..8)
            .map(|writer| {
                let (path, barrier) = (path.clone(), barrier.clone());
                std::thread::spawn(move || {
                    barrier.wait();
                    for round in 0..25 {
                        let body = format!("{writer}:{round}:").repeat(512);
                        atomic_write(&path, body.as_bytes()).unwrap();
                    }
                })
            })
            .collect();
        for worker in workers {
            worker.join().unwrap();
        }
        // The survivor is exactly one writer's complete payload.
        let text = String::from_utf8(fs::read(&*path).unwrap()).unwrap();
        let unit = text.split(':').take(2).collect::<Vec<_>>().join(":") + ":";
        assert_eq!(text, unit.repeat(512));
        assert!(leftovers(directory.path()).is_empty());
    }

    #[test]
    fn a_declined_conditional_write_keeps_the_old_file_and_cleans_up() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("variant");
        atomic_write(&path, b"valid").unwrap();
        let written = atomic_write_if(&path, b"stale", Durability::File, || false).unwrap();
        assert!(!written);
        assert_eq!(fs::read(&path).unwrap(), b"valid");
        assert!(leftovers(directory.path()).is_empty());
    }

    #[test]
    fn a_failed_write_cleans_up_its_temporary() {
        let directory = tempfile::tempdir().unwrap();
        // Renaming a file over a non-empty directory fails after the data was
        // staged, which is the late failure that used to strand temporaries.
        let path = directory.path().join("target");
        fs::create_dir(&path).unwrap();
        fs::write(path.join("child"), b"x").unwrap();
        assert!(atomic_write(&path, b"data").is_err());
        assert!(leftovers(directory.path()).is_empty());
    }

    #[test]
    fn a_fixed_temporary_replaces_a_planted_leftover() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("manifest.json");
        let temporary = directory.path().join("manifest.pending");
        fs::write(&temporary, b"killed writer").unwrap();
        atomic_write_via(&path, &temporary, b"{}").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"{}");
        assert!(!temporary.exists());
    }

    #[cfg(unix)]
    #[test]
    fn written_files_and_private_directories_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("secret");
        atomic_write(&path, b"token").unwrap();
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let private = directory.path().join("a").join("b");
        fs::create_dir_all(&private).unwrap();
        fs::set_permissions(&private, fs::Permissions::from_mode(0o755)).unwrap();
        private_dir(&private).unwrap();
        assert_eq!(
            fs::metadata(&private).unwrap().permissions().mode() & 0o777,
            0o700
        );
        let link = directory.path().join("link");
        std::os::unix::fs::symlink(&private, &link).unwrap();
        assert!(private_dir(&link).is_err());
    }

    #[test]
    fn missing_root_uses_existing_filesystem_without_creating_it() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("not").join("created");
        assert!(available_bytes(&root).is_some());
        assert!(!root.exists());
    }
}
