//! Hybrid logical clock (HLC) for record versions.
//!
//! An HLC is a physical millisecond time paired with a logical counter. Local
//! writes `tick` it forward; applied remote versions are folded in with
//! `observe`, which preserves causality (a write that happened after seeing a
//! remote record always sorts after it) even when clocks are skewed or jump
//! backward. The `(physical_ms, counter)` pair is the total order key; the
//! device id breaks the rare exact tie.
//!
//! `observe` caps how far a peer's clock may drag ours forward
//! ([`MAX_DRIFT_MS`]): a device with a wildly wrong clock cannot poison the
//! timestamps we generate. It still wins its own records until real time
//! catches up, which is inherent to LWW.

use serde::{Deserialize, Serialize};

/// How far ahead of our own wall clock an observed remote time may push our
/// HLC. Bounds clock poisoning; devices are assumed roughly NTP-synced.
pub const MAX_DRIFT_MS: u64 = 60 * 60 * 1000;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hlc {
    /// Physical component: milliseconds since the Unix epoch.
    pub physical_ms: u64,
    /// Logical component: disambiguates events at the same physical time.
    pub counter: u32,
}

impl Hlc {
    pub fn new(physical_ms: u64, counter: u32) -> Self {
        Self {
            physical_ms,
            counter,
        }
    }

    /// Total order key.
    fn rank(self) -> (u64, u32) {
        (self.physical_ms, self.counter)
    }

    /// True when `self` should replace `other`.
    pub fn newer_than(self, other: Hlc) -> bool {
        self.rank() > other.rank()
    }

    /// Advance the counter at the current physical time, rolling into the next
    /// millisecond if the counter would overflow.
    fn bump(&mut self) {
        if self.counter == u32::MAX {
            self.physical_ms = self.physical_ms.saturating_add(1);
            self.counter = 0;
        } else {
            self.counter += 1;
        }
    }

    /// Local write: return a timestamp strictly greater than the clock and at
    /// least `now_ms`/`hint_ms`.
    pub fn tick(&mut self, now_ms: u64, hint_ms: u64) -> Hlc {
        let physical = self.physical_ms.max(now_ms).max(hint_ms);
        if physical > self.physical_ms {
            self.physical_ms = physical;
            self.counter = 0;
        } else {
            self.bump();
        }
        *self
    }

    /// Receive a remote timestamp, folding it into the clock (standard HLC
    /// receive step) with a forward-drift cap on the remote physical time.
    pub fn observe(&mut self, remote: Hlc, now_ms: u64) -> Hlc {
        let remote = Hlc {
            physical_ms: remote.physical_ms.min(now_ms.saturating_add(MAX_DRIFT_MS)),
            counter: remote.counter,
        };
        let physical = self.physical_ms.max(remote.physical_ms).max(now_ms);
        if physical == self.physical_ms && physical == remote.physical_ms {
            self.counter = self.counter.max(remote.counter);
            self.bump();
        } else if physical == self.physical_ms {
            self.bump();
        } else if physical == remote.physical_ms {
            self.physical_ms = physical;
            self.counter = remote.counter;
            self.bump();
        } else {
            self.physical_ms = physical;
            self.counter = 0;
        }
        *self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tick_is_strictly_monotonic() {
        let mut h = Hlc::default();
        let a = h.tick(1_000, 0);
        let b = h.tick(1_000, 0);
        let c = h.tick(1_000, 0);
        assert!(b.newer_than(a) && c.newer_than(b));
        assert_eq!(a.physical_ms, 1_000);
        assert_eq!(a.counter, 0);
        assert_eq!(c.counter, 2);
    }

    #[test]
    fn tick_follows_a_forward_wall_clock() {
        let mut h = Hlc::default();
        let a = h.tick(1_000, 0);
        let b = h.tick(5_000, 0);
        assert!(b.newer_than(a));
        assert_eq!(b.physical_ms, 5_000);
        assert_eq!(b.counter, 0);
    }

    #[test]
    fn tick_honours_a_hint_but_never_goes_backward() {
        let mut h = Hlc::default();
        h.tick(1_000, 0);
        // An older hint must not rewind the clock.
        let a = h.tick(500, 100);
        assert_eq!(a.physical_ms, 1_000);
        // A newer hint advances it.
        let b = h.tick(500, 9_000);
        assert_eq!(b.physical_ms, 9_000);
    }

    #[test]
    fn observe_preserves_causality_after_a_remote_write() {
        let mut h = Hlc::default();
        // A remote record dated slightly ahead of our clock.
        h.observe(Hlc::new(2_000, 3), 1_000);
        // Our next local write must sort after it.
        let local = h.tick(1_000, 0);
        assert!(local.newer_than(Hlc::new(2_000, 3)));
    }

    #[test]
    fn observe_handles_a_backward_clock() {
        let mut h = Hlc::default();
        h.tick(10_000, 0);
        // Wall clock jumps back; the HLC must not.
        let observed = h.observe(Hlc::new(5_000, 0), 4_000);
        assert!(observed.physical_ms >= 10_000);
        let next = h.tick(4_000, 0);
        assert!(next.newer_than(observed));
    }

    #[test]
    fn observe_caps_forward_drift() {
        let mut h = Hlc::default();
        let now = 1_000_000;
        // A peer a year ahead may only drag us to now + MAX_DRIFT_MS.
        h.observe(Hlc::new(now + 365 * 24 * 3600 * 1000, 0), now);
        assert_eq!(h.physical_ms, now + MAX_DRIFT_MS);
    }

    #[test]
    fn counter_overflow_rolls_into_the_next_millisecond() {
        let mut h = Hlc::new(1_000, u32::MAX);
        let next = h.tick(1_000, 0);
        assert_eq!(next.physical_ms, 1_001);
        assert_eq!(next.counter, 0);
    }
}
