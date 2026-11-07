//! One outstanding request whose answer arrives later on a foreign thread, such
//! as a document picker or a camera scan.
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Slot<T> {
    pending: Mutex<Option<(u64, T)>>,
    next: AtomicU64,
}

impl<T> Slot<T> {
    pub const fn new() -> Self {
        Self {
            pending: Mutex::new(None),
            next: AtomicU64::new(1),
        }
    }

    /// Claim the slot for `value` and return its request ID. A busy slot hands
    /// the value back untouched so the caller can answer it.
    pub fn begin(&self, value: T) -> Result<u64, T> {
        let request = self.next.fetch_add(1, Ordering::Relaxed);
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.is_some() {
            return Err(value);
        }
        *pending = Some((request, value));
        Ok(request)
    }

    pub fn is_pending(&self) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
    }

    /// Take the value if `request` is still the outstanding one. A late or
    /// repeated answer for another request leaves the slot alone.
    pub fn complete(&self, request: u64) -> Option<T> {
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        if pending.as_ref().is_none_or(|(id, _)| *id != request) {
            return None;
        }
        pending.take().map(|(_, value)| value)
    }

    /// Take whatever is outstanding, for sources that cannot name a request.
    pub fn take(&self) -> Option<T> {
        self.pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .map(|(_, value)| value)
    }
}

impl<T> Default for Slot<T> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_busy_slot_returns_the_value() {
        let slot = Slot::new();
        let first = slot.begin("first").unwrap();
        assert_eq!(slot.begin("second"), Err("second"));
        assert!(slot.is_pending());
        assert_eq!(slot.complete(first), Some("first"));
        assert!(!slot.is_pending());
        assert!(slot.begin("third").is_ok());
    }

    #[test]
    fn only_the_outstanding_request_completes() {
        let slot = Slot::new();
        let request = slot.begin(1).unwrap();
        assert_eq!(slot.complete(request + 1), None);
        assert_eq!(slot.complete(request), Some(1));
        assert_eq!(slot.complete(request), None);
    }

    #[test]
    fn take_clears_an_unnamed_request() {
        let slot = Slot::new();
        slot.begin("scan").unwrap();
        assert_eq!(slot.take(), Some("scan"));
        assert_eq!(slot.take(), None);
    }
}
