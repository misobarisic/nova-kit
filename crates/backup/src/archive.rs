//! Portable, bounded ZIP/JSON format. No extraction or native storage pages.
//!
//! The container, its limits and its integrity checks live here; what a
//! component means is supplied by the application as a [`Schema`].
use nova_storage::zip::{self as bounded, Archive as Zip, Limits, Policy};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Cursor, Write};

/// Largest archive, and the largest total expanded content.
pub const MAX_BYTES: usize = 128 * 1024 * 1024;
/// Largest single file in an archive.
pub const MAX_FILE: u64 = 32 * 1024 * 1024;
pub const MAX_ENTRIES: usize = 4096;
pub const MAX_RECORDS: usize = 200_000;
pub type LocalValues = BTreeMap<String, Option<String>>;
pub type UndoLocal = (LocalValues, LocalValues);

pub fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format: String,
    pub version: u32,
    pub created_at: u64,
    pub app_version: String,
    pub archive_id: String,
    pub profiles: Vec<Profile>,
    #[serde(default)]
    pub packages: Vec<Package>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Package {
    pub id: String,
    pub version: String,
    pub path: String,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Profile {
    pub archive_profile_id: String,
    pub source_profile_id: String,
    pub name: String,
    pub components: Vec<Component>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Component {
    pub name: String,
    pub schema_version: u32,
    pub path: String,
    pub item_count: usize,
    pub sha256: String,
}
/// Logical records use adapter-defined semantic identities, never source HLCs.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Payload {
    pub version: u32,
    #[serde(deserialize_with = "unique_records")]
    pub records: BTreeMap<String, Value>,
}
fn unique_records<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<BTreeMap<String, Value>, D::Error> {
    struct Unique;
    impl<'de> serde::de::Visitor<'de> for Unique {
        type Value = BTreeMap<String, Value>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("unique backup record identities")
        }
        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> Result<Self::Value, M::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, Value>()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate backup record identity"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(Unique)
}
pub struct Archive {
    pub manifest: Manifest,
    pub payloads: BTreeMap<(String, String), Payload>,
    pub checksum: String,
    pub unsupported: Vec<String>,
    pub undo_after: Option<BTreeMap<(String, String), Payload>>,
    pub packages: BTreeMap<String, Vec<u8>>,
    pub undo_local: Option<UndoLocal>,
}

/// Identity of an addon package bundled in an archive, as the application's
/// package validator reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PackageIdentity {
    pub id: String,
    pub version: String,
    pub sha256: String,
}

/// What the application decides about an archive's contents.
pub trait Schema {
    /// The `format` string a manifest must carry.
    fn format(&self) -> &str;
    /// Whether this build understands the component. Unknown components are
    /// integrity-checked and reported in [`Archive::unsupported`], not decoded.
    fn supported(&self, component: &str) -> bool;
    /// Semantic validation of a decoded, supported payload.
    fn validate(&self, component: &str, payload: &Payload) -> Result<(), String>;
    /// Largest bundled package accepted.
    fn max_package_bytes(&self) -> u64;
    /// Fully validate a bundled package and report its identity.
    fn package(&self, bytes: &[u8]) -> Result<PackageIdentity, String>;
    /// SHA-256 values of the packages a payload's records declare, one entry
    /// per record (`None` for a record without one). Components that declare
    /// no packages return nothing, which is the default.
    fn declared_packages(&self, _component: &str, _payload: &Payload) -> Vec<Option<String>> {
        Vec::new()
    }
}

pub fn encode(
    manifest: Manifest,
    payloads: &BTreeMap<(String, String), Payload>,
) -> Result<Vec<u8>, String> {
    encode_with_packages(manifest, payloads, &BTreeMap::new())
}
pub fn encode_with_packages(
    mut manifest: Manifest,
    payloads: &BTreeMap<(String, String), Payload>,
    packages: &BTreeMap<String, Vec<u8>>,
) -> Result<Vec<u8>, String> {
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let mut total = 0u64;
    if manifest
        .profiles
        .iter()
        .map(|profile| profile.components.len())
        .sum::<usize>()
        + manifest.packages.len()
        + 1
        > MAX_ENTRIES
    {
        return Err("Too many backup files".into());
    }
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);
    for profile in &mut manifest.profiles {
        for component in &mut profile.components {
            let payload = payloads
                .get(&(profile.archive_profile_id.clone(), component.name.clone()))
                .ok_or("Missing export component")?;
            let bytes = serde_json::to_vec(payload).map_err(|e| e.to_string())?;
            total = total
                .checked_add(bytes.len() as u64)
                .ok_or("Backup size overflow")?;
            if total > MAX_BYTES as u64 {
                return Err("Expanded backup exceeds the size limit".into());
            }
            if bytes.len() as u64 > MAX_FILE || payload.records.len() > MAX_RECORDS {
                return Err("Backup component exceeds the size limit".into());
            }
            component.sha256 = digest(&bytes);
            component.item_count = payload.records.len();
            zip.start_file(&component.path, options)
                .map_err(|e| e.to_string())?;
            zip.write_all(&bytes).map_err(|e| e.to_string())?;
        }
    }
    for package in &manifest.packages {
        let bytes = packages
            .get(&package.sha256)
            .ok_or("Missing addon package file")?;
        total = total
            .checked_add(bytes.len() as u64)
            .ok_or("Backup size overflow")?;
        if total > MAX_BYTES as u64 {
            return Err("Expanded backup exceeds the size limit".into());
        }
        zip.start_file(&package.path, options)
            .map_err(|e| e.to_string())?;
        zip.write_all(bytes).map_err(|e| e.to_string())?;
    }
    zip.start_file("manifest.json", options)
        .map_err(|e| e.to_string())?;
    zip.write_all(&serde_json::to_vec_pretty(&manifest).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let bytes = zip.finish().map_err(|e| e.to_string())?.into_inner();
    if bytes.len() > MAX_BYTES {
        return Err("Backup exceeds the size limit".into());
    }
    Ok(bytes)
}

/// Decode and fully validate an archive. Duplicate, unsafe or oversized
/// entries, bad checksums and any [`Schema`] rejection fail the whole archive.
pub fn decode(bytes: &[u8], schema: &impl Schema) -> Result<Archive, String> {
    if bytes.len() > MAX_BYTES {
        return Err("Backup exceeds the size limit".into());
    }
    let mut zip = Zip::open(
        Cursor::new(bytes),
        Limits {
            max_entries: MAX_ENTRIES,
            max_entry_bytes: MAX_FILE,
            max_expanded_bytes: MAX_BYTES as u64,
        },
        Policy::FILES,
    )
    .map_err(open_error)?;
    let manifest_bytes =
        zip.read_by_name("manifest.json", MAX_FILE)
            .map_err(|error| match error {
                bounded::Error::Missing => "Backup manifest is missing".to_owned(),
                other => other.to_string(),
            })?;
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|e| format!("Invalid backup manifest: {e}"))?;
    if manifest.format != schema.format() || manifest.version != 1 {
        return Err("Unsupported backup format version".into());
    }
    let mut profiles = BTreeSet::new();
    let mut listed = BTreeSet::new();
    let mut payloads = BTreeMap::new();
    let mut unsupported = Vec::new();
    let mut packages = BTreeMap::new();
    for profile in &manifest.profiles {
        if !profiles.insert(profile.archive_profile_id.clone())
            || profile.name.trim().is_empty()
            || profile.name.chars().count() > 64
        {
            return Err("Invalid or duplicate backup profile".into());
        }
        let mut components = BTreeSet::new();
        for component in &profile.components {
            if !components.insert(&component.name)
                || !bounded::strict_path(&component.path)
                || !listed.insert(&component.path)
            {
                return Err("Duplicate or invalid backup component".into());
            }
            let data = zip
                .read_by_name(&component.path, MAX_FILE)
                .map_err(|_| format!("Missing backup component: {}", component.name))?;
            if digest(&data) != component.sha256 {
                return Err(format!("Checksum failed for {}", component.name));
            }
            if component.schema_version != 1 || !schema.supported(&component.name) {
                unsupported.push(component.name.clone());
                continue;
            }
            let payload: Payload = serde_json::from_slice(&data)
                .map_err(|e| format!("Invalid {}: {e}", component.name))?;
            if payload.version != 1
                || payload.records.len() != component.item_count
                || payload.records.len() > MAX_RECORDS
            {
                return Err(format!(
                    "Invalid record count/version for {}",
                    component.name
                ));
            }
            schema.validate(&component.name, &payload)?;
            payloads.insert(
                (profile.archive_profile_id.clone(), component.name.clone()),
                payload,
            );
        }
    }
    for package in &manifest.packages {
        if package.path != format!("packages/{}.zip", package.sha256)
            || !bounded::strict_path(&package.path)
            || !listed.insert(&package.path)
        {
            return Err("Duplicate or invalid addon package path".into());
        }
        let data = zip
            .read_by_name(&package.path, schema.max_package_bytes())
            .map_err(|error| match error {
                bounded::Error::Missing => "Missing addon package file".to_owned(),
                // An oversized body used to be truncated and then fail its digest.
                bounded::Error::EntryTooLarge => "Addon package checksum failed".to_owned(),
                other => other.to_string(),
            })?;
        if digest(&data) != package.sha256 {
            return Err("Addon package checksum failed".into());
        }
        let parsed = schema.package(&data)?;
        if parsed.id != package.id
            || parsed.version != package.version
            || parsed.sha256 != package.sha256
        {
            return Err("Addon package identity mismatch".into());
        }
        packages.insert(package.sha256.clone(), data);
    }
    for ((_, name), payload) in &payloads {
        if schema.declared_packages(name, payload).iter().any(|hash| {
            hash.as_ref()
                .is_none_or(|hash| !packages.contains_key(hash))
        }) {
            return Err("Missing declared addon package file".into());
        }
    }
    Ok(Archive {
        manifest,
        payloads,
        checksum: digest(bytes),
        unsupported,
        undo_after: None,
        packages,
        undo_local: None,
    })
}

fn open_error(error: bounded::Error) -> String {
    use bounded::Error::*;
    match error {
        Archive(error) => format!("Invalid backup ZIP: {error}"),
        TooManyEntries => "Too many backup files".into(),
        UnsafePath | DuplicateEntry | Directory => "Duplicate or invalid backup path".into(),
        NotRegular => "Invalid backup symbolic link".into(),
        EntryTooLarge | ExpandedTooLarge => "Expanded backup exceeds the size limit".into(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct Fixture;
    impl Schema for Fixture {
        fn format(&self) -> &str {
            "fixture-backup"
        }
        fn supported(&self, component: &str) -> bool {
            component != "future"
        }
        fn validate(&self, component: &str, payload: &Payload) -> Result<(), String> {
            if component == "strict" && payload.records.contains_key("forbidden") {
                return Err("forbidden record".into());
            }
            Ok(())
        }
        fn max_package_bytes(&self) -> u64 {
            1024
        }
        fn package(&self, bytes: &[u8]) -> Result<PackageIdentity, String> {
            Ok(PackageIdentity {
                id: "pkg".into(),
                version: "1.0.0".into(),
                sha256: digest(bytes),
            })
        }
        fn declared_packages(&self, component: &str, payload: &Payload) -> Vec<Option<String>> {
            if component != "packages" {
                return Vec::new();
            }
            payload
                .records
                .values()
                .map(|value| value["sha256"].as_str().map(str::to_owned))
                .collect()
        }
    }

    fn component(name: &str) -> Component {
        Component {
            name: name.into(),
            schema_version: 1,
            path: format!("profiles/p1/{name}.json"),
            item_count: 0,
            sha256: String::new(),
        }
    }

    fn manifest(components: &[&str]) -> Manifest {
        Manifest {
            format: "fixture-backup".into(),
            version: 1,
            created_at: 1,
            app_version: "test".into(),
            archive_id: "archive".into(),
            profiles: vec![Profile {
                archive_profile_id: "p1".into(),
                source_profile_id: "default".into(),
                name: "Default".into(),
                components: components.iter().map(|name| component(name)).collect(),
            }],
            packages: Vec::new(),
        }
    }

    fn payload(rows: &[(&str, Value)]) -> Payload {
        Payload {
            version: 1,
            records: rows
                .iter()
                .map(|(key, value)| ((*key).into(), value.clone()))
                .collect(),
        }
    }

    fn payloads(names: &[&str]) -> BTreeMap<(String, String), Payload> {
        names
            .iter()
            .map(|name| {
                (
                    ("p1".into(), (*name).into()),
                    payload(&[("k", json!({"name": name}))]),
                )
            })
            .collect()
    }

    /// Rename `from` to the equal-length `to` in local and central headers.
    fn rename(bytes: &mut [u8], from: &str, to: &str) {
        assert_eq!(from.len(), to.len());
        for i in 0..bytes.len() - from.len() {
            if &bytes[i..i + from.len()] == from.as_bytes() {
                bytes[i..i + to.len()].copy_from_slice(to.as_bytes());
            }
        }
    }

    #[test]
    fn components_round_trip_with_their_checksums() {
        let bytes = encode(manifest(&["alpha", "beta"]), &payloads(&["alpha", "beta"])).unwrap();
        let decoded = decode(&bytes, &Fixture).unwrap();
        assert_eq!(decoded.payloads.len(), 2);
        assert_eq!(decoded.checksum, digest(&bytes));
        assert!(decoded.unsupported.is_empty());
        assert_eq!(decoded.manifest.profiles[0].components[0].item_count, 1);
    }

    #[test]
    fn duplicate_archive_entries_are_refused() {
        // Two same-length entries renamed to one name: the zip crate's index
        // would show a single entry and a checksum for whichever survived.
        let bytes = encode(
            manifest(&["alpha", "omega"]),
            &payloads(&["alpha", "omega"]),
        )
        .unwrap();
        let mut forged = bytes.clone();
        rename(&mut forged, "omega.json", "alpha.json");
        assert!(decode(&bytes, &Fixture).is_ok());
        let error = decode(&forged, &Fixture).err().unwrap();
        assert!(
            error.contains("Duplicate or invalid backup path"),
            "{error}"
        );
    }

    #[test]
    fn unsupported_components_are_checked_but_not_decoded() {
        let mut payloads = payloads(&["alpha", "future"]);
        payloads.insert(("p1".into(), "future".into()), payload(&[("x", json!(1))]));
        let bytes = encode(manifest(&["alpha", "future"]), &payloads).unwrap();
        let decoded = decode(&bytes, &Fixture).unwrap();
        assert_eq!(decoded.unsupported, vec!["future".to_owned()]);
        assert_eq!(decoded.payloads.len(), 1);
    }

    #[test]
    fn wrong_format_checksum_and_schema_rejections_fail_the_archive() {
        let bytes = encode(manifest(&["alpha"]), &payloads(&["alpha"])).unwrap();
        struct Other;
        impl Schema for Other {
            fn format(&self) -> &str {
                "other"
            }
            fn supported(&self, _: &str) -> bool {
                true
            }
            fn validate(&self, _: &str, _: &Payload) -> Result<(), String> {
                Ok(())
            }
            fn max_package_bytes(&self) -> u64 {
                0
            }
            fn package(&self, _: &[u8]) -> Result<PackageIdentity, String> {
                Err("no packages".into())
            }
        }
        assert!(
            decode(&bytes, &Other)
                .err()
                .unwrap()
                .contains("Unsupported backup")
        );

        let mut strict = payloads(&["strict"]);
        strict.insert(
            ("p1".into(), "strict".into()),
            payload(&[("forbidden", json!(1))]),
        );
        let bytes = encode(manifest(&["strict"]), &strict).unwrap();
        assert_eq!(decode(&bytes, &Fixture).err().unwrap(), "forbidden record");

        let mut tampered = encode(manifest(&["alpha"]), &payloads(&["alpha"])).unwrap();
        // Stored (not deflated) bytes would be edited in place; deflated data
        // is corrupted instead, which must surface as a failed read or digest.
        let at = tampered.len() / 3;
        tampered[at] ^= 0xFF;
        assert!(decode(&tampered, &Fixture).is_err());
    }

    #[test]
    fn bundled_packages_must_match_their_manifest_and_declarations() {
        let package = b"package bytes".to_vec();
        let hash = digest(&package);
        let mut manifest = manifest(&["packages"]);
        manifest.packages.push(Package {
            id: "pkg".into(),
            version: "1.0.0".into(),
            path: format!("packages/{hash}.zip"),
            sha256: hash.clone(),
        });
        let mut payloads = payloads(&["packages"]);
        payloads.insert(
            ("p1".into(), "packages".into()),
            payload(&[("pkg", json!({"sha256": hash}))]),
        );
        let files = BTreeMap::from([(hash.clone(), package)]);
        let bytes = encode_with_packages(manifest.clone(), &payloads, &files).unwrap();
        let decoded = decode(&bytes, &Fixture).unwrap();
        assert_eq!(decoded.packages.len(), 1);

        // A record that points at a package the archive does not carry.
        payloads.insert(
            ("p1".into(), "packages".into()),
            payload(&[("pkg", json!({"sha256": "0".repeat(64)}))]),
        );
        let bytes = encode_with_packages(manifest.clone(), &payloads, &files).unwrap();
        assert_eq!(
            decode(&bytes, &Fixture).err().unwrap(),
            "Missing declared addon package file"
        );

        let mut mismatched = manifest;
        mismatched.packages[0].id = "other".into();
        let bytes = encode_with_packages(mismatched, &payloads, &files).unwrap();
        assert_eq!(
            decode(&bytes, &Fixture).err().unwrap(),
            "Addon package identity mismatch"
        );
    }

    #[test]
    fn oversized_input_and_missing_manifest_are_reported() {
        assert_eq!(
            decode(&vec![0u8; MAX_BYTES + 1], &Fixture).err().unwrap(),
            "Backup exceeds the size limit"
        );
        let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
        zip.start_file("other.json", zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(b"{}").unwrap();
        let bytes = zip.finish().unwrap().into_inner();
        assert_eq!(
            decode(&bytes, &Fixture).err().unwrap(),
            "Backup manifest is missing"
        );
    }
}
