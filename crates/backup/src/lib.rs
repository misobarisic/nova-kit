//! Backup archive format: a bounded ZIP of JSON component payloads described by
//! a manifest, with SHA-256 integrity for every file.
//!
//! This crate owns the container only: encoding, strict decoding and the
//! limits. What a component *means* (which components exist, how a payload is
//! validated, how an addon package is checked) is the application's
//! [`archive::Schema`], and applying or undoing a restore stays with each app.
pub mod archive;
