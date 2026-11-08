//! Bounded, validating ZIP reading for untrusted archives (addon packages,
//! backups, comic imports).
//!
//! The `zip` crate indexes entries by name and silently coalesces duplicate
//! central-directory records, so `len()` and the name index cannot be trusted
//! to enforce an entry limit or uniqueness. [`Archive::open`] therefore walks
//! the raw central directory itself, and refuses an archive whose records
//! disagree with the crate's view, before any entry is handed out.
//!
//! Everything is bounded by [`Limits`]: the number of records (checked from the
//! end-of-central-directory record before the crate parses the directory), the
//! declared and actual size of each entry, and the total expanded size.
use ::zip::ZipArchive;
use std::collections::HashSet;
use std::io::{self, Read, Seek, SeekFrom, Write};

const CENTRAL_SIGNATURE: &[u8; 4] = b"PK\x01\x02";
const CENTRAL_HEADER: u64 = 46;
const END_SIGNATURE: &[u8; 4] = b"PK\x05\x06";
const END_RECORD: usize = 22;
const ZIP64_LOCATOR_SIGNATURE: &[u8; 4] = b"PK\x06\x07";
const ZIP64_LOCATOR: usize = 20;
const ZIP64_END_SIGNATURE: &[u8; 4] = b"PK\x06\x06";
/// The longest comment an end record can carry is 65535 bytes.
const END_SEARCH: u64 = (END_RECORD + ZIP64_LOCATOR + 65_535) as u64;
/// Generous per-record allowance for names, extra fields and comments when
/// bounding the directory's byte size from its record limit.
const DIRECTORY_BYTES_PER_RECORD: u64 = 4096;

/// Size limits for one archive. All sizes are bytes of uncompressed content.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Most central-directory records accepted, counting directories.
    pub max_entries: usize,
    /// Largest single entry, checked against its declared size when the
    /// archive is opened and against the bytes actually read.
    pub max_entry_bytes: u64,
    /// Largest sum of declared sizes, and of bytes read over the archive's life.
    pub max_expanded_bytes: u64,
}

/// Entries under a name prefix whose content is streamed rather than buffered
/// (downloaded chapter pages in a backup, for example). They count toward the
/// record limit but not toward [`Limits::max_entry_bytes`] or
/// [`Limits::max_expanded_bytes`], so an archive of a few large files and one
/// of many small ones can share the same limits for everything else. Read them
/// with [`Archive::copy`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Streamed {
    /// Entry names that start with this prefix are streamed.
    pub prefix: &'static str,
    /// Most streamed records accepted, in addition to [`Limits::max_entries`].
    pub max_entries: usize,
}

/// How an entry's name is judged safe.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathRule {
    /// Relative, `/`-separated, no empty/`.`/`..` components and none of
    /// `\`, `:` or NUL.
    Strict,
    /// [`PathRule::Strict`] plus a 240 byte limit and no control characters.
    Printable,
    /// Whatever the `zip` crate's `enclosed_name` accepts (stays inside the
    /// extraction root). Looser about `\` and `:` for archives from foreign tools.
    Enclosed,
}

/// Which entries are acceptable at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub paths: PathRule,
    /// Accept directory entries (their trailing `/` is ignored for the path
    /// check). They are still listed so callers can skip them.
    pub directories: bool,
    /// Reject every Unix file type other than a regular file. When false only
    /// symbolic links are refused. Entries without Unix mode bits always pass.
    pub regular_files_only: bool,
}

impl Policy {
    /// Plain files only, [`PathRule::Strict`] names, symbolic links refused.
    pub const FILES: Policy = Policy {
        paths: PathRule::Strict,
        directories: false,
        regular_files_only: false,
    };
}

#[derive(Debug)]
pub enum Error {
    /// Not a readable ZIP archive, or an entry could not be opened (including
    /// encrypted entries).
    Archive(::zip::result::ZipError),
    /// The central directory is cut short or points outside the file.
    TruncatedDirectory,
    /// More records than [`Limits::max_entries`], or a directory too large for
    /// that many records.
    TooManyEntries,
    /// Two central-directory records share a name, or the directory holds
    /// records the `zip` crate coalesced or hid.
    DuplicateEntry,
    /// The name escapes the archive root or uses forbidden characters.
    UnsafePath,
    /// A symbolic link, device or other entry that is not a plain file.
    NotRegular,
    /// A directory entry where the policy admits only files.
    Directory,
    Encrypted,
    EntryTooLarge,
    ExpandedTooLarge,
    /// An entry's content length differs from its declared size.
    SizeMismatch,
    /// No entry has the requested name or index.
    Missing,
    Io(io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Archive(error) => write!(f, "invalid ZIP archive: {error}"),
            Error::TruncatedDirectory => f.write_str("truncated ZIP directory"),
            Error::TooManyEntries => f.write_str("too many ZIP entries"),
            Error::DuplicateEntry => f.write_str("duplicate ZIP entry"),
            Error::UnsafePath => f.write_str("unsafe ZIP entry path"),
            Error::NotRegular => f.write_str("ZIP entry is not a regular file"),
            Error::Directory => f.write_str("unexpected ZIP directory entry"),
            Error::Encrypted => f.write_str("encrypted ZIP entry"),
            Error::EntryTooLarge => f.write_str("ZIP entry exceeds the size limit"),
            Error::ExpandedTooLarge => f.write_str("expanded ZIP archive exceeds the size limit"),
            Error::SizeMismatch => f.write_str("ZIP entry size mismatch"),
            Error::Missing => f.write_str("ZIP entry not found"),
            Error::Io(error) => write!(f, "ZIP read failed: {error}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}
impl From<::zip::result::ZipError> for Error {
    fn from(error: ::zip::result::ZipError) -> Self {
        Error::Archive(error)
    }
}

/// One validated central-directory record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub index: usize,
    /// The name exactly as stored, including a directory's trailing `/`.
    pub name: String,
    pub size: u64,
    pub is_dir: bool,
}

/// A validated archive. Entries are known-safe; contents are read on demand
/// with [`Archive::read`].
pub struct Archive<R> {
    zip: ZipArchive<R>,
    entries: Vec<Entry>,
    limits: Limits,
    read: u64,
}

impl<R: Read + Seek> Archive<R> {
    pub fn open(reader: R, limits: Limits, policy: Policy) -> Result<Self, Error> {
        Self::open_inner(reader, limits, policy, None)
    }

    /// [`Archive::open`] for an archive that also carries [`Streamed`] entries.
    pub fn open_streaming(
        reader: R,
        limits: Limits,
        policy: Policy,
        streamed: Streamed,
    ) -> Result<Self, Error> {
        Self::open_inner(reader, limits, policy, Some(streamed))
    }

    fn open_inner(
        mut reader: R,
        limits: Limits,
        policy: Policy,
        streamed: Option<Streamed>,
    ) -> Result<Self, Error> {
        let capacity = limits
            .max_entries
            .saturating_add(streamed.map_or(0, |streamed| streamed.max_entries));
        let length = reader.seek(SeekFrom::End(0))?;
        // Bound the work the zip crate will do before it parses anything.
        if let Some(end) = read_end_record(&mut reader, length)?
            && (end.records > capacity as u64
                || end.size > (capacity as u64).saturating_mul(DIRECTORY_BYTES_PER_RECORD))
        {
            return Err(Error::TooManyEntries);
        }
        reader.rewind()?;
        let zip = ZipArchive::new(reader)?;
        let start = zip.central_directory_start();
        let listed = zip.len();
        let mut reader = zip.into_inner();
        let records = count_records(&mut reader, start, length, capacity)?;
        if records != listed {
            return Err(Error::DuplicateEntry);
        }
        reader.rewind()?;
        let mut zip = ZipArchive::new(reader)?;
        let mut entries = Vec::with_capacity(zip.len());
        let mut names = HashSet::with_capacity(zip.len());
        let mut declared = 0u64;
        let (mut ordinary, mut streamed_records) = (0usize, 0usize);
        for index in 0..zip.len() {
            let file = zip.by_index_raw(index)?;
            if file.encrypted() {
                return Err(Error::Encrypted);
            }
            let name = file.name().to_owned();
            let is_dir = file.is_dir();
            if is_dir && !policy.directories {
                return Err(Error::Directory);
            }
            let checked = if is_dir {
                name.strip_suffix('/').unwrap_or(&name)
            } else {
                &name
            };
            let safe = match policy.paths {
                PathRule::Strict => strict_path(checked),
                PathRule::Printable => printable_path(checked),
                PathRule::Enclosed => file.enclosed_name().is_some(),
            };
            if !safe {
                return Err(Error::UnsafePath);
            }
            let kind = file.unix_mode().map(|mode| mode & 0o170000);
            let linked = kind == Some(0o120000);
            let irregular =
                kind.is_some_and(|kind| kind != 0 && kind != 0o100000 && kind != 0o040000);
            if linked || (policy.regular_files_only && !is_dir && irregular) {
                return Err(Error::NotRegular);
            }
            if !names.insert(name.clone()) {
                return Err(Error::DuplicateEntry);
            }
            let size = file.size();
            if streamed.is_some_and(|streamed| name.starts_with(streamed.prefix)) {
                streamed_records += 1;
            } else {
                ordinary += 1;
                if size > limits.max_entry_bytes {
                    return Err(Error::EntryTooLarge);
                }
                declared = declared.checked_add(size).ok_or(Error::ExpandedTooLarge)?;
                if declared > limits.max_expanded_bytes {
                    return Err(Error::ExpandedTooLarge);
                }
            }
            entries.push(Entry {
                index,
                name,
                size,
                is_dir,
            });
        }
        if ordinary > limits.max_entries
            || streamed.is_some_and(|streamed| streamed_records > streamed.max_entries)
        {
            return Err(Error::TooManyEntries);
        }
        Ok(Self {
            zip,
            entries,
            limits,
            read: 0,
        })
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn find(&self, name: &str) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.name == name)
    }

    /// Read one entry fully. `max_bytes` is the caller's own cap for this entry
    /// (never above [`Limits::max_entry_bytes`]); the archive-wide
    /// [`Limits::max_expanded_bytes`] is enforced across all reads. A body
    /// that is longer or shorter than its declared size is refused.
    pub fn read(&mut self, index: usize, max_bytes: u64) -> Result<Vec<u8>, Error> {
        let entry = self.entries.get(index).ok_or(Error::Missing)?;
        let cap = max_bytes.min(self.limits.max_entry_bytes);
        let remaining = self.limits.max_expanded_bytes.saturating_sub(self.read);
        let declared = entry.size;
        if declared > cap {
            return Err(Error::EntryTooLarge);
        }
        let mut file = self.zip.by_index(index)?;
        let mut data = Vec::with_capacity(declared.min(1 << 20) as usize);
        file.by_ref()
            .take(cap.min(remaining).saturating_add(1))
            .read_to_end(&mut data)?;
        let length = data.len() as u64;
        if length > cap {
            return Err(Error::EntryTooLarge);
        }
        if length > remaining {
            return Err(Error::ExpandedTooLarge);
        }
        if length != declared {
            return Err(Error::SizeMismatch);
        }
        self.read += length;
        Ok(data)
    }

    pub fn read_by_name(&mut self, name: &str, max_bytes: u64) -> Result<Vec<u8>, Error> {
        let index = self.find(name).ok_or(Error::Missing)?.index;
        self.read(index, max_bytes)
    }

    /// Stream one entry into `out` without buffering it. `max_bytes` is the
    /// caller's cap for this entry. The content must match the declared size,
    /// and the `zip` crate verifies its CRC when the entry ends. Bytes copied
    /// here do not count toward [`Limits::max_expanded_bytes`]: that budget is
    /// for entries a caller holds in memory.
    pub fn copy(
        &mut self,
        index: usize,
        max_bytes: u64,
        out: &mut impl Write,
    ) -> Result<u64, Error> {
        let entry = self.entries.get(index).ok_or(Error::Missing)?;
        if entry.is_dir {
            return Err(Error::Directory);
        }
        let declared = entry.size;
        if declared > max_bytes {
            return Err(Error::EntryTooLarge);
        }
        let mut file = self.zip.by_index(index)?;
        let copied = io::copy(&mut file.by_ref().take(max_bytes.saturating_add(1)), out)?;
        if copied > max_bytes {
            return Err(Error::EntryTooLarge);
        }
        if copied != declared {
            return Err(Error::SizeMismatch);
        }
        Ok(copied)
    }
}

/// Relative `/`-separated path with no empty, `.` or `..` component.
pub fn strict_path(path: &str) -> bool {
    !path.is_empty()
        && !path.contains(['\\', '\0', ':'])
        && !path.starts_with('/')
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

/// [`strict_path`] limited to 240 bytes without control characters.
pub fn printable_path(path: &str) -> bool {
    path.len() <= 240 && strict_path(path) && !path.chars().any(char::is_control)
}

struct EndRecord {
    records: u64,
    size: u64,
}

/// The directory's declared record count and byte size, or `None` when the end
/// record cannot be located (the `zip` crate then produces the real error).
fn read_end_record<R: Read + Seek>(
    reader: &mut R,
    length: u64,
) -> Result<Option<EndRecord>, Error> {
    let tail_start = length.saturating_sub(END_SEARCH);
    reader.seek(SeekFrom::Start(tail_start))?;
    let mut tail = Vec::new();
    reader.by_ref().take(END_SEARCH).read_to_end(&mut tail)?;
    let le16 = |at: usize| u64::from(u16::from_le_bytes([tail[at], tail[at + 1]]));
    let le32 = |at: usize| u64::from(u32::from_le_bytes(tail[at..at + 4].try_into().unwrap()));
    let Some(position) = (0..=tail.len().saturating_sub(END_RECORD))
        .rev()
        .find(|&at| {
            tail.get(at..at + 4) == Some(END_SIGNATURE.as_slice())
                && tail.len() >= at + END_RECORD
                && at + END_RECORD + le16(at + 20) as usize == tail.len()
        })
    else {
        return Ok(None);
    };
    let (mut records, mut size) = (le16(position + 10), le32(position + 12));
    if (records == 0xFFFF || size == 0xFFFF_FFFF || le32(position + 16) == 0xFFFF_FFFF)
        && position >= ZIP64_LOCATOR
        && tail.get(position - ZIP64_LOCATOR..position - ZIP64_LOCATOR + 4)
            == Some(ZIP64_LOCATOR_SIGNATURE.as_slice())
    {
        let at = position - ZIP64_LOCATOR + 8;
        let offset = u64::from_le_bytes(tail[at..at + 8].try_into().unwrap());
        if offset.saturating_add(56) > length {
            return Ok(None);
        }
        reader.seek(SeekFrom::Start(offset))?;
        let mut zip64 = [0u8; 56];
        reader.read_exact(&mut zip64)?;
        if &zip64[..4] != ZIP64_END_SIGNATURE {
            return Ok(None);
        }
        records = u64::from_le_bytes(zip64[32..40].try_into().unwrap());
        size = u64::from_le_bytes(zip64[40..48].try_into().unwrap());
    }
    Ok(Some(EndRecord { records, size }))
}

/// Count consecutive central-directory records starting at `offset`, using the
/// raw bytes rather than the `zip` crate's name index.
fn count_records<R: Read + Seek>(
    reader: &mut R,
    mut offset: u64,
    length: u64,
    max_entries: usize,
) -> Result<usize, Error> {
    let mut records = 0usize;
    let mut header = [0u8; CENTRAL_HEADER as usize];
    loop {
        if offset.saturating_add(4) > length {
            break;
        }
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut header[..4])?;
        if &header[..4] != CENTRAL_SIGNATURE {
            break;
        }
        if offset.saturating_add(CENTRAL_HEADER) > length {
            return Err(Error::TruncatedDirectory);
        }
        reader.read_exact(&mut header[4..])?;
        let field = |at: usize| u64::from(u16::from_le_bytes([header[at], header[at + 1]]));
        offset = offset
            .checked_add(CENTRAL_HEADER + field(28) + field(30) + field(32))
            .ok_or(Error::TruncatedDirectory)?;
        if offset > length {
            return Err(Error::TruncatedDirectory);
        }
        records += 1;
        if records > max_entries {
            return Err(Error::TooManyEntries);
        }
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};

    const LIMITS: Limits = Limits {
        max_entries: 16,
        max_entry_bytes: 1024,
        max_expanded_bytes: 4096,
    };

    fn build(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut zip = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = ::zip::write::SimpleFileOptions::default();
        for (name, body) in files {
            zip.start_file(*name, options).unwrap();
            zip.write_all(body).unwrap();
        }
        zip.finish().unwrap().into_inner()
    }

    fn open(bytes: &[u8], policy: Policy) -> Result<Archive<Cursor<&[u8]>>, Error> {
        Archive::open(Cursor::new(bytes), LIMITS, policy)
    }

    /// Rename `from` to the equal-length `to` in the local and central headers.
    fn rename(bytes: &mut [u8], from: &str, to: &str) {
        assert_eq!(from.len(), to.len());
        for i in 0..bytes.len() - from.len() {
            if &bytes[i..i + from.len()] == from.as_bytes() {
                bytes[i..i + to.len()].copy_from_slice(to.as_bytes());
            }
        }
    }

    #[test]
    fn plain_archives_round_trip() {
        let bytes = build(&[("a.txt", b"alpha"), ("dir/b.txt", b"beta")]);
        let mut archive = open(&bytes, Policy::FILES).unwrap();
        assert_eq!(archive.entries().len(), 2);
        assert_eq!(archive.read_by_name("dir/b.txt", 100).unwrap(), b"beta");
        assert_eq!(archive.read(0, 100).unwrap(), b"alpha");
    }

    #[test]
    fn duplicate_central_records_are_refused() {
        let mut bytes = build(&[("a.txt", b"one"), ("b.txt", b"two")]);
        rename(&mut bytes, "b.txt", "a.txt");
        // The crate's own index folds the two records into one entry, which is
        // why its `len()` cannot be trusted to detect this.
        assert_eq!(ZipArchive::new(Cursor::new(&bytes[..])).unwrap().len(), 1);
        assert!(matches!(
            open(&bytes, Policy::FILES),
            Err(Error::DuplicateEntry)
        ));
    }

    #[test]
    fn record_floods_are_counted_before_the_name_index_collapses_them() {
        // Every record is renamed to one of two names, so the crate sees two.
        let names: Vec<String> = (0..40).map(|i| format!("{i:02}.png")).collect();
        let files: Vec<(&str, &[u8])> = names.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
        let mut bytes = build(&files);
        for name in &names[1..] {
            rename(&mut bytes, name, "00.png");
        }
        assert!(matches!(
            open(&bytes, Policy::FILES),
            Err(Error::TooManyEntries) | Err(Error::DuplicateEntry)
        ));
        assert!(matches!(
            open(&build(&files), Policy::FILES),
            Err(Error::TooManyEntries)
        ));
    }

    #[test]
    fn unsafe_names_links_and_directories_follow_the_policy() {
        for name in ["../x", "/abs", "a//b", "a\\b", "c:x", "./x"] {
            let bytes = build(&[(name, b"x")]);
            assert!(
                matches!(open(&bytes, Policy::FILES), Err(Error::UnsafePath)),
                "{name}"
            );
        }
        let bytes = build(&[("dir/", b""), ("dir/a", b"x")]);
        assert!(matches!(open(&bytes, Policy::FILES), Err(Error::Directory)));
        let allowed = Policy {
            directories: true,
            ..Policy::FILES
        };
        let archive = open(&bytes, allowed).unwrap();
        assert!(archive.entries()[0].is_dir);
        // Control characters and long names only fail the printable rule.
        let bytes = build(&[("a\u{7}b", b"x")]);
        assert!(open(&bytes, Policy::FILES).is_ok());
        let printable = Policy {
            paths: PathRule::Printable,
            ..Policy::FILES
        };
        assert!(matches!(open(&bytes, printable), Err(Error::UnsafePath)));
    }

    #[test]
    fn symlinks_are_refused() {
        let mut zip = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
        zip.add_symlink("link", "a.txt", ::zip::write::SimpleFileOptions::default())
            .unwrap();
        let bytes = zip.finish().unwrap().into_inner();
        for regular_files_only in [false, true] {
            let policy = Policy {
                regular_files_only,
                ..Policy::FILES
            };
            assert!(matches!(open(&bytes, policy), Err(Error::NotRegular)));
        }
    }

    #[test]
    fn size_limits_apply_to_declared_and_total_bytes() {
        let big = vec![7u8; 2000];
        let bytes = build(&[("big", &big)]);
        assert!(matches!(
            open(&bytes, Policy::FILES),
            Err(Error::EntryTooLarge)
        ));
        let medium = vec![7u8; 1000];
        let bytes = build(&[
            ("a", &medium),
            ("b", &medium),
            ("c", &medium),
            ("d", &medium),
            ("e", &medium),
        ]);
        assert!(matches!(
            open(&bytes, Policy::FILES),
            Err(Error::ExpandedTooLarge)
        ));
        let bytes = build(&[("a", &medium)]);
        let mut archive = open(&bytes, Policy::FILES).unwrap();
        assert!(matches!(archive.read(0, 10), Err(Error::EntryTooLarge)));
    }

    #[test]
    fn reads_are_capped_across_the_archive() {
        let limits = Limits {
            max_entries: 4,
            max_entry_bytes: 100,
            max_expanded_bytes: 150,
        };
        let bytes = build(&[("a", &[1u8; 100])]);
        let mut archive = Archive::open(Cursor::new(&bytes[..]), limits, Policy::FILES).unwrap();
        assert_eq!(archive.read(0, 100).unwrap().len(), 100);
        assert!(matches!(archive.read(0, 100), Err(Error::ExpandedTooLarge)));
        let bytes = build(&[("a", &[1u8; 100]), ("b", &[1u8; 100])]);
        assert!(matches!(
            Archive::open(Cursor::new(&bytes[..]), limits, Policy::FILES),
            Err(Error::ExpandedTooLarge)
        ));
    }

    #[test]
    fn archives_with_trailing_hidden_records_are_refused() {
        // Append a second central record after the declared ones by growing
        // the directory without updating the end record's count.
        let bytes = build(&[("a.txt", b"one"), ("b.txt", b"two")]);
        let mut reader = Cursor::new(bytes.clone());
        let start = ZipArchive::new(&mut reader)
            .unwrap()
            .central_directory_start() as usize;
        let end = bytes.len() - END_RECORD;
        let first_record = &bytes[start..bytes.len() - END_RECORD];
        let mut forged = bytes[..end].to_vec();
        forged.extend_from_slice(first_record);
        forged.extend_from_slice(&bytes[end..]);
        assert!(open(&forged, Policy::FILES).is_err());
    }

    #[test]
    fn streamed_entries_skip_the_byte_limits_but_not_the_record_or_path_rules() {
        let streamed = Streamed {
            prefix: "blobs/",
            max_entries: 3,
        };
        let open = |bytes: &[u8]| {
            Archive::open_streaming(Cursor::new(bytes.to_vec()), LIMITS, Policy::FILES, streamed)
        };
        let big = vec![9u8; 5000];
        // Over the 1024 byte entry limit and the 4096 total, yet streamed.
        let bytes = build(&[("a.json", b"{}"), ("blobs/one", &big), ("blobs/two", &big)]);
        assert!(matches!(
            open_plain(&bytes),
            Err(Error::EntryTooLarge | Error::ExpandedTooLarge)
        ));
        let mut archive = open(&bytes).unwrap();
        assert_eq!(archive.entries().len(), 3);
        let mut copied = Vec::new();
        let index = archive.find("blobs/two").unwrap().index;
        assert_eq!(archive.copy(index, 5000, &mut copied).unwrap(), 5000);
        assert_eq!(copied, big);
        // The caller's own cap still applies, and nothing is written past it.
        let mut capped = Vec::new();
        assert!(matches!(
            archive.copy(index, 4999, &mut capped),
            Err(Error::EntryTooLarge)
        ));
        assert!(capped.is_empty());
        // An ordinary entry beside the streamed ones is still bounded.
        let bytes = build(&[("a.json", &big), ("blobs/one", b"x")]);
        assert!(matches!(open(&bytes), Err(Error::EntryTooLarge)));
        // More streamed records than allowed, and more ordinary ones.
        let many: Vec<String> = (0..4).map(|i| format!("blobs/{i}")).collect();
        let files: Vec<(&str, &[u8])> = many.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
        assert!(matches!(open(&build(&files)), Err(Error::TooManyEntries)));
        let ordinary: Vec<String> = (0..17).map(|i| format!("{i:02}.json")).collect();
        let files: Vec<(&str, &[u8])> = ordinary.iter().map(|n| (n.as_str(), &b"x"[..])).collect();
        assert!(matches!(open(&build(&files)), Err(Error::TooManyEntries)));
        // Streaming does not relax the name rules.
        let bytes = build(&[("blobs/../escape", b"x")]);
        assert!(matches!(open(&bytes), Err(Error::UnsafePath)));
    }

    fn open_plain(bytes: &[u8]) -> Result<Archive<Cursor<Vec<u8>>>, Error> {
        Archive::open(Cursor::new(bytes.to_vec()), LIMITS, Policy::FILES)
    }

    #[test]
    fn copy_refuses_a_body_that_disagrees_with_its_directory_entry() {
        let mut zip = ::zip::ZipWriter::new(Cursor::new(Vec::new()));
        let stored = ::zip::write::SimpleFileOptions::default()
            .compression_method(::zip::CompressionMethod::Stored);
        zip.start_file("a", stored).unwrap();
        zip.write_all(b"abcdef").unwrap();
        let bytes = zip.finish().unwrap().into_inner();
        let mut archive = open(&bytes, Policy::FILES).unwrap();
        let mut sink = Vec::new();
        assert_eq!(archive.copy(0, 100, &mut sink).unwrap(), 6);
        assert_eq!(sink, b"abcdef");
        // Stored bytes edited in place fail the CRC check at the end of the entry.
        let mut tampered = bytes.clone();
        let at = tampered.windows(6).position(|w| w == b"abcdef").unwrap();
        tampered[at] ^= 0xFF;
        let mut archive = open(&tampered, Policy::FILES).unwrap();
        assert!(archive.copy(0, 100, &mut Vec::new()).is_err());
        assert!(matches!(
            archive.copy(5, 100, &mut Vec::new()),
            Err(Error::Missing)
        ));
    }

    #[test]
    fn garbage_is_an_archive_error() {
        assert!(matches!(
            open(b"not a zip at all", Policy::FILES),
            Err(Error::Archive(_))
        ));
    }
}
