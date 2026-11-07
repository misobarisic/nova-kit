//! Filesystem capacity queries now live in `nova_storage::fs`, which the
//! applications share; these re-exports keep the download API stable.
pub use nova_storage::fs::{available_bytes, filesystem_capacity};
