//! The download foreground service as seen from Rust: start it on the first
//! busy tick, refresh its notification at most once a second, stop it when the
//! queue empties, and try again on the next tick after the platform refused to
//! start it.
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// How often the notification text is refreshed while a download runs. The
/// caller ticks every 250 ms; re-issuing JNI twice a second is plenty.
pub const NOTIFICATION_REFRESH: Duration = Duration::from_secs(1);

/// The platform side of the service.
pub trait Driver {
    /// Stop the service. Safe to call when it is not running.
    fn stop(&self);
    /// Start the service first when `start` is set, then refresh its
    /// notification. Returns whether the service is running afterwards: false
    /// when the platform rejected the start or there was no context to ask.
    fn apply(&self, start: bool, title: &str, text: &str) -> bool;
}

pub struct DownloadService<D> {
    driver: D,
    active: AtomicBool,
    /// Last notification text and when it was pushed, for throttling.
    last: Mutex<Option<(String, Instant)>>,
}

impl<D: Driver> DownloadService<D> {
    pub const fn new(driver: D) -> Self {
        Self {
            driver,
            active: AtomicBool::new(false),
            last: Mutex::new(None),
        }
    }

    pub fn driver(&self) -> &D {
        &self.driver
    }

    /// Start, update, or stop the service. `busy` is whether any download is
    /// queued or transferring; `title`/`text` label the ongoing notification.
    /// Cheap to call every tick: platform work is throttled to state changes
    /// and one refresh per second.
    pub fn set(&self, busy: bool, title: &str, text: &str) {
        self.set_at(busy, title, text, Instant::now());
    }

    /// Whether the service is currently requested (a rejected start clears it).
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    /// [`set`](Self::set) with the clock supplied, for tests of the throttle.
    pub fn set_at(&self, busy: bool, title: &str, text: &str, now: Instant) {
        let was_active = self.active.swap(busy, Ordering::AcqRel);
        let changed = was_active != busy;
        if !busy {
            if changed {
                self.driver.stop();
            }
            return;
        }
        // Active: throttle text updates, but always act on the rising edge.
        let body = format!("{title}\n{text}");
        {
            let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
            if !changed
                && last.as_ref().is_some_and(|(previous, at)| {
                    previous == &body && now.saturating_duration_since(*at) < NOTIFICATION_REFRESH
                })
            {
                return;
            }
            *last = Some((body, now));
        }
        if !self.driver.apply(!was_active, title, text) {
            // The caller serializes start/update/stop calls. A platform
            // rejection must leave the next busy tick able to retry without an
            // idle cycle.
            self.active.store(false, Ordering::Release);
            *self.last.lock().unwrap_or_else(|e| e.into_inner()) = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    #[derive(Default)]
    struct Fake {
        calls: RefCell<Vec<String>>,
        /// Outcomes of upcoming `apply` calls; empty means success.
        outcomes: RefCell<Vec<bool>>,
    }
    impl Driver for Fake {
        fn stop(&self) {
            self.calls.borrow_mut().push("stop".into());
        }
        fn apply(&self, start: bool, title: &str, text: &str) -> bool {
            self.calls.borrow_mut().push(format!(
                "{}{title}/{text}",
                if start { "start+" } else { "" }
            ));
            let mut outcomes = self.outcomes.borrow_mut();
            if outcomes.is_empty() {
                true
            } else {
                outcomes.remove(0)
            }
        }
    }
    // The Fake is not Sync; the service only needs it on one thread here.
    fn service() -> DownloadService<Fake> {
        DownloadService::new(Fake::default())
    }
    fn calls(service: &DownloadService<Fake>) -> Vec<String> {
        service.driver().calls.borrow().clone()
    }

    #[test]
    fn starts_once_throttles_and_stops_on_the_falling_edge() {
        let service = service();
        let t0 = Instant::now();
        service.set_at(true, "Downloading", "1 of 2", t0);
        service.set_at(
            true,
            "Downloading",
            "1 of 2",
            t0 + Duration::from_millis(250),
        );
        service.set_at(
            true,
            "Downloading",
            "2 of 2",
            t0 + Duration::from_millis(500),
        );
        service.set_at(
            true,
            "Downloading",
            "2 of 2",
            t0 + Duration::from_millis(1600),
        );
        service.set_at(false, "", "", t0 + Duration::from_secs(5));
        service.set_at(false, "", "", t0 + Duration::from_secs(6));
        assert_eq!(
            calls(&service),
            [
                "start+Downloading/1 of 2",
                "Downloading/2 of 2",
                "Downloading/2 of 2",
                "stop"
            ]
        );
    }

    #[test]
    fn a_rejected_start_is_retried_on_the_next_busy_tick() {
        let service = service();
        service.driver().outcomes.borrow_mut().push(false);
        let t0 = Instant::now();
        service.set_at(true, "Downloading", "Preparing", t0);
        // Same text a moment later: without the reset this would be throttled.
        service.set_at(
            true,
            "Downloading",
            "Preparing",
            t0 + Duration::from_millis(250),
        );
        assert_eq!(
            calls(&service),
            ["start+Downloading/Preparing", "start+Downloading/Preparing"]
        );
        // Now running: further ticks only refresh.
        service.set_at(
            true,
            "Downloading",
            "Preparing",
            t0 + Duration::from_secs(2),
        );
        assert_eq!(calls(&service)[2], "Downloading/Preparing");
    }
}
