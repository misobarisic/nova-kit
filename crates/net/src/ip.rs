use std::net::IpAddr;

/// Whether `ip` is a globally routable unicast address. Private, loopback,
/// link-local, shared (CGNAT), documentation, benchmarking, multicast,
/// reserved and 6to4 space is refused for IPv4, and everything outside global
/// unicast `2000::/3` (minus documentation, 6to4 and reserved blocks) for IPv6.
/// IPv4-mapped IPv6 addresses are judged as the IPv4 address they carry.
pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let [a, b, c, _] = ip.octets();
            !ip.is_private()
                && !ip.is_loopback()
                && !ip.is_link_local()
                && !ip.is_broadcast()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && a != 0
                && !(a == 100 && (64..=127).contains(&b))
                && !(a == 192 && b == 0 && c == 0)
                && !(a == 192 && b == 0 && c == 2)
                // 6to4 relay anycast (RFC 3068, deprecated); the IPv6 rule
                // below already refuses the 2002::/16 6to4 prefix.
                && !(a == 192 && b == 88 && c == 99)
                && !(a == 198 && (b == 18 || b == 19))
                && !(a == 198 && b == 51 && c == 100)
                && !(a == 203 && b == 0 && c == 113)
                && a < 224
        }
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return is_public_ip(IpAddr::V4(mapped));
            }
            let segments = ip.segments();
            (0x2000..=0x3fff).contains(&segments[0])
                && !ip.is_loopback()
                && !ip.is_unspecified()
                && !ip.is_multicast()
                && !ip.is_unique_local()
                && !ip.is_unicast_link_local()
                && !(segments[0] == 0x2001 && segments[1] == 0x0db8)
                && !(segments[0] == 0x2001 && segments[1] <= 0x01ff)
                && segments[0] != 0x2002
                && !(segments[0] == 0x3fff && segments[1] <= 0x0fff)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn public(text: &str) -> bool {
        is_public_ip(text.parse().unwrap())
    }

    #[test]
    fn ordinary_global_addresses_are_public() {
        for text in [
            "8.8.8.8",
            "1.1.1.1",
            "93.184.216.34",
            "2606:4700:4700::1111",
        ] {
            assert!(public(text), "{text}");
        }
    }

    #[test]
    fn special_use_ipv4_ranges_are_refused() {
        for text in [
            "0.1.2.3",
            "10.0.0.1",
            "100.64.0.1",
            "100.127.255.255",
            "127.0.0.1",
            "169.254.1.1",
            "172.16.0.1",
            "192.0.0.8",
            "192.0.2.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.19.255.255",
            "198.51.100.1",
            "203.0.113.9",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
        ] {
            assert!(!public(text), "{text}");
        }
    }

    #[test]
    fn the_6to4_relay_anycast_range_is_refused_like_the_6to4_prefix() {
        for text in ["192.88.99.0", "192.88.99.1", "192.88.99.255", "2002::1"] {
            assert!(!public(text), "{text}");
        }
        // Neighbouring space is unaffected.
        for text in ["192.88.98.1", "192.88.100.1"] {
            assert!(public(text), "{text}");
        }
    }

    #[test]
    fn ipv6_non_global_space_is_refused() {
        for text in [
            "::1",
            "::",
            "fe80::1",
            "fc00::1",
            "ff02::1",
            "2001:db8::1",
            "2001:1::1",
            "3fff::1",
            "100::1",
        ] {
            assert!(!public(text), "{text}");
        }
    }

    #[test]
    fn mapped_addresses_follow_the_embedded_ipv4_address() {
        assert!(public("::ffff:8.8.8.8"));
        assert!(!public("::ffff:10.0.0.1"));
        assert!(!public("::ffff:192.88.99.1"));
    }
}
