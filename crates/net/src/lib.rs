//! Network policy shared by Nova and Nova Reader.
//!
//! The pieces are small on purpose: each used to exist as a copy in every
//! transport that needed it, and the copies drifted.
//!
//! * [`is_public_ip`] decides whether a resolved address may be contacted on
//!   behalf of an addon.
//! * `tls::client_config` (feature `rustls`) is the one Android client
//!   configuration: bundled Mozilla roots and an explicit aws-lc-rs provider,
//!   so no transport depends on the platform verifier or on whichever provider
//!   rustls would otherwise pick.
//! * `redirect::https_only` (feature `reqwest`) is the bounded, HTTPS-only,
//!   credential-free redirect policy.
//! * [`read_capped`] reads a response or file body with a hard byte cap.
mod body;
mod ip;
#[cfg(feature = "reqwest")]
pub mod redirect;
#[cfg(feature = "rustls")]
pub mod tls;

pub use body::{ReadError, read_capped};
pub use ip::is_public_ip;
