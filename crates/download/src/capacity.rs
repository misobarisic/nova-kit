//! Query the filesystem holding the actual download directory. This creates
//! neither directories nor files; a not-yet-created root uses its ancestor.
use std::path::Path;

pub fn available_bytes(path: &Path) -> Option<u64> {
    filesystem_capacity(path).map(|(available, _)| available)
}

/// Available bytes and total filesystem capacity, using the same filesystem.
pub fn filesystem_capacity(path: &Path) -> Option<(u64, u64)> {
    let mut existing = path;
    while !existing.exists() {
        existing = existing.parent()?;
    }
    platform_available(existing)
}

#[cfg(unix)]
// libc's filesystem counter widths differ between Unix targets.
#[allow(clippy::unnecessary_cast)]
fn platform_available(path: &Path) -> Option<(u64, u64)> {
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
fn platform_available(path: &Path) -> Option<(u64, u64)> {
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
fn platform_available(_: &Path) -> Option<(u64, u64)> {
    None
}

#[cfg(test)]
mod tests {
    #[test]
    fn missing_download_root_uses_existing_filesystem_without_creating_it() {
        let root = std::env::temp_dir()
            .join(format!("nova-capacity-{}", std::process::id()))
            .join("not-created");
        assert!(!root.exists());
        assert!(super::available_bytes(&root).is_some());
        assert!(!root.exists());
    }
}
