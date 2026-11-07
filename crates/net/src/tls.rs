//! The Android TLS client configuration.
//!
//! Android's platform verifier needs a JavaVM and a Kotlin runtime that
//! cargo-apk2 does not package, so every Android transport uses bundled Mozilla
//! roots with an explicitly chosen crypto provider instead. Desktop transports
//! use reqwest's defaults and do not call this.
use std::sync::Arc;

/// The configuration type, re-exported so callers that only hand it to
/// `reqwest::ClientBuilder::tls_backend_preconfigured` need no `rustls` dependency.
pub use rustls::ClientConfig;

/// A client configuration verifying servers against the bundled Mozilla roots,
/// using the aws-lc-rs provider and rustls' safe default protocol versions.
pub fn client_config() -> Result<ClientConfig, rustls::Error> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()?
    .with_root_certificates(roots)
    .with_no_client_auth())
}

#[cfg(test)]
mod tests {
    #[test]
    fn the_bundled_roots_build_a_usable_configuration() {
        let config = super::client_config().unwrap();
        assert!(config.alpn_protocols.is_empty());
        // Independent calls are interchangeable: nothing is process-global.
        assert!(super::client_config().is_ok());
    }

    #[test]
    fn reqwest_accepts_the_configuration_for_blocking_and_async_clients() {
        reqwest::blocking::Client::builder()
            .tls_backend_preconfigured(super::client_config().unwrap())
            .build()
            .unwrap();
        reqwest::Client::builder()
            .tls_backend_preconfigured(super::client_config().unwrap())
            .build()
            .unwrap();
    }
}
