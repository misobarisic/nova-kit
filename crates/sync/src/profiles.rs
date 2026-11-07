//! Profile namespaces remain ordinary domain strings on the wire. The account's
//! membership and profile directory therefore travel alongside every profile.

pub mod directory;

/// Qualify one domain with an immutable profile ID. IDs cannot contain `:`.
pub fn profile_domain(profile: &str, domain: &str) -> String {
    assert!(!profile.is_empty() && !profile.contains(':'));
    format!("profile:{profile}:{domain}")
}

/// Decode a qualified profile domain, rejecting empty IDs and domain names.
pub fn parse_profile_domain(domain: &str) -> Option<(&str, &str)> {
    let (profile, domain) = domain.strip_prefix("profile:")?.split_once(':')?;
    (!profile.is_empty() && !domain.is_empty()).then_some((profile, domain))
}

/// Whether Nova watch-register semantics apply to this domain.
/// Legacy domains remain readable during the one-time local migration.
/// Always false without the `nova-progress` extension; generic applications
/// may use a domain named `progress` without adopting the Nova video schema.
pub fn is_progress_domain(domain: &str) -> bool {
    cfg!(feature = "nova-progress")
        && (domain == "progress"
            || parse_profile_domain(domain).is_some_and(|(_, domain)| domain == "progress"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_domains_are_unambiguous() {
        assert_eq!(
            parse_profile_domain(&profile_domain("alice", "progress")),
            Some(("alice", "progress"))
        );
        assert_eq!(
            is_progress_domain("profile:alice:progress"),
            cfg!(feature = "nova-progress")
        );
        assert!(!is_progress_domain("profile:alice:not_progress"));
        assert_eq!(parse_profile_domain("profile::progress"), None);
        assert_eq!(parse_profile_domain("profile:alice:"), None);
        assert_eq!(parse_profile_domain("peers"), None);
    }
}
