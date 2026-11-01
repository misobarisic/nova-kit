//! A separate process isolates the process-wide database and engine singleton.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

#[test]
fn background_jobs_borrow_activity_engine_and_preserve_foreground_handoff() {
    let dir = std::env::temp_dir().join(format!("nova-sync-job-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    nova_storage::init_at(&dir);
    nova_sync::write_settings(&nova_sync::SyncSettings {
        enabled: true,
        enable_local_discovery: false,
        ..Default::default()
    });

    let foreground = nova_sync::foreground_engine().unwrap();
    let borrowed = nova_sync::background_engine().unwrap().unwrap();
    assert!(Arc::ptr_eq(&borrowed.engine, &foreground));
    // A retained Activity engine must actually accept a job-owned attempt.
    assert_eq!(
        borrowed
            .engine
            .one_shot(&AtomicBool::new(false), Duration::from_secs(10)),
        nova_sync::OneShotOutcome::Skipped
    ); // no paired peers in this fixture
    assert_eq!(
        borrowed
            .engine
            .one_shot(&AtomicBool::new(true), Duration::from_secs(10)),
        nova_sync::OneShotOutcome::Cancelled
    );
    drop(borrowed);
    assert!(Arc::ptr_eq(&nova_sync::engine().unwrap(), &foreground));
    nova_sync::uninstall();
    drop(foreground);

    let headless = nova_sync::background_engine().unwrap().unwrap();
    assert!(nova_sync::background_engine().unwrap().is_none());
    let attached = nova_sync::foreground_engine_guarded(|| true, false)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&headless.engine, &attached));
    drop(headless);
    assert!(Arc::ptr_eq(&nova_sync::engine().unwrap(), &attached));
    nova_sync::uninstall_guarded(|| true);
    drop(attached);

    let headless = nova_sync::background_engine().unwrap().unwrap();
    drop(headless);
    assert!(!nova_sync::is_running());
    let _ = std::fs::remove_dir_all(dir);
}
