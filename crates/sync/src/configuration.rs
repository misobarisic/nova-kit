//! Immutable per-process application wire identity. Configure before loading any
//! identity or store. Defaults are byte-for-byte compatible with Nova.
use std::sync::OnceLock;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolConfig {
    pub sync: &'static [u8],
    pub pair: &'static [u8],
    pub package: &'static [u8],
    pub remove: &'static [u8],
    pub playback: &'static [u8],
    pub ticket_prefix: &'static str,
}
impl Default for ProtocolConfig {
    fn default() -> Self {
        Self::nova()
    }
}
impl ProtocolConfig {
    pub const fn nova() -> Self {
        Self {
            sync: b"nova/sync/6",
            pair: b"nova/pair/5",
            package: b"nova/addon-package/1",
            remove: crate::protocol::REMOVE_ALPN,
            playback: b"nova/playback/5",
            ticket_prefix: "NV1",
        }
    }
    pub const fn reader() -> Self {
        Self {
            sync: b"nova-reader/sync/6",
            pair: b"nova-reader/pair/5",
            package: b"nova-reader/addon-package/1",
            remove: b"nova-reader/remove/1",
            playback: b"nova-reader/playback/5",
            ticket_prefix: "NR1",
        }
    }
}
static PROTOCOL: OnceLock<ProtocolConfig> = OnceLock::new();
/// Select the application's namespace before any sync access. Repeating the
/// same selection is harmless; changing it after first use is rejected.
pub fn configure_protocol(config: ProtocolConfig) -> anyhow::Result<()> {
    anyhow::ensure!(
        !config.ticket_prefix.is_empty() && config.ticket_prefix.is_ascii(),
        "invalid ticket prefix"
    );
    for alpn in [
        config.sync,
        config.pair,
        config.package,
        config.remove,
        config.playback,
    ] {
        anyhow::ensure!(!alpn.is_empty() && alpn.len() <= 255, "invalid ALPN");
    }
    let selected = PROTOCOL.get_or_init(|| config);
    anyhow::ensure!(*selected == config, "sync protocol already initialized");
    Ok(())
}
pub(crate) fn protocol() -> &'static ProtocolConfig {
    PROTOCOL.get_or_init(ProtocolConfig::default)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn wire_namespaces_are_disjoint_and_nova_is_unchanged() {
        let nova = ProtocolConfig::nova();
        assert_eq!(nova.sync, crate::ALPN);
        assert_eq!(nova.pair, crate::PAIR_ALPN);
        assert_eq!(nova.package, crate::PACKAGE_ALPN);
        assert_eq!(nova.remove, crate::protocol::REMOVE_ALPN);
        assert_eq!(nova.ticket_prefix, "NV1");
        let reader = ProtocolConfig::reader();
        for a in [
            nova.sync,
            nova.pair,
            nova.package,
            nova.remove,
            nova.playback,
        ] {
            for b in [
                reader.sync,
                reader.pair,
                reader.package,
                reader.remove,
                reader.playback,
            ] {
                assert_ne!(a, b);
            }
        }
        assert_ne!(reader.ticket_prefix, nova.ticket_prefix);
    }
}
