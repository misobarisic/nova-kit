//! Shared Slint source components. The host owns the compiled global state;
//! see `slint/host-globals.slint` for the minimal NovaUiShared contract.
//! Cargo metadata locates these sources for both Git and local dependencies.
pub const SLINT_SOURCE_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/slint");
