//! Application validation applies equally to disk loads and local/remote writes.
//! Engine-owned membership/presence records also pass through the policy: custom
//! implementations should accept domains they do not own.
use crate::{Record, Version};
use std::sync::{Arc, OnceLock};

pub trait RecordPolicy: Send + Sync + 'static {
    fn validate(&self, _domain: &str, _key: &str, _record: &Record) -> bool {
        true
    }
    /// Optional convergent value merge for two live records. None selects the
    /// engine's ordinary last-write-wins rule. Return the same value regardless
    /// of argument order. Membership records retain their engine policy.
    fn merge_value(
        &self,
        _domain: &str,
        _key: &str,
        _local: &Record,
        _remote: &Record,
    ) -> Option<String> {
        None
    }
    /// Encode explicit local actions after the engine allocates their version.
    fn prepare_local(
        &self,
        _domain: &str,
        _key: &str,
        _old: Option<&Record>,
        value: String,
        _version: Version,
    ) -> String {
        value
    }
}
struct GenericPolicy;
impl RecordPolicy for GenericPolicy {}
static POLICY: OnceLock<Arc<dyn RecordPolicy>> = OnceLock::new();
/// Install before Store::load/set/apply or local_store. An initialized policy
/// cannot be replaced while records or a running engine depend on it. Repeated
/// foreground/headless initialization must clone the same shared Arc; a distinct
/// instance is rejected even if it has the same Rust type.
pub fn configure_record_policy(policy: Arc<dyn RecordPolicy>) -> anyhow::Result<()> {
    let selected = POLICY.get_or_init(|| policy.clone());
    anyhow::ensure!(
        Arc::ptr_eq(selected, &policy),
        "sync record policy already initialized with another instance"
    );
    Ok(())
}
pub(crate) fn policy() -> &'static dyn RecordPolicy {
    POLICY.get_or_init(|| Arc::new(GenericPolicy)).as_ref()
}
pub(crate) fn valid(domain: &str, key: &str, record: &Record) -> bool {
    policy().validate(domain, key, record)
        && (!crate::is_progress_domain(domain) || crate::progress::valid(record))
}
pub(crate) fn local(
    domain: &str,
    key: &str,
    old: Option<&Record>,
    value: String,
    version: Version,
) -> String {
    let value = if crate::is_progress_domain(domain) {
        crate::progress::local(old, value, version)
    } else {
        value
    };
    policy().prepare_local(domain, key, old, value, version)
}
pub(crate) fn merge(domain: &str, key: &str, local: &Record, remote: &Record) -> Option<String> {
    if domain == crate::DOMAIN_PEERS {
        return None;
    }
    if crate::is_progress_domain(domain) {
        crate::progress::merge(local, remote)
    } else {
        policy().merge_value(domain, key, local, remote)
    }
}
