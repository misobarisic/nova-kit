//! Compile the source kit against its minimal host contract, outside Nova.
use std::{collections::HashMap, path::PathBuf, process::Command};

#[test]
fn reusable_source_components_compile() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if std::env::var_os("NOVA_UI_BASE_COMPILE_CHILD").is_some() {
        let config = slint_build::CompilerConfiguration::new().with_library_paths(HashMap::from([
            (
                "NovaUiBase".into(),
                PathBuf::from(nova_ui_base::SLINT_SOURCE_DIR),
            ),
            (
                "NovaUiShared".into(),
                manifest.join("slint/host-globals.slint"),
            ),
        ]));
        slint_build::compile_with_config("tests/components.slint", config).unwrap();
        return;
    }
    // Keep Cargo's build environment scoped to the child; tests do not mutate
    // process-global environment variables or require a display/backend.
    let output_dir = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "reusable_source_components_compile",
            "--nocapture",
        ])
        .env("NOVA_UI_BASE_COMPILE_CHILD", "1")
        .env("OUT_DIR", output_dir.path())
        .env("CARGO_MANIFEST_DIR", manifest)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_dir.path().join("components.rs").is_file());
}

#[test]
fn shared_sync_components_compile() {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if std::env::var_os("NOVA_UI_BASE_SYNC_COMPILE_CHILD").is_some() {
        let config = slint_build::CompilerConfiguration::new().with_library_paths(HashMap::from([
            ("NovaUiBase".into(), manifest.join("slint")),
            (
                "NovaUiShared".into(),
                manifest.join("tests/sync-ui-globals.slint"),
            ),
            (
                "NovaSyncHost".into(),
                manifest.join("tests/sync-ui-host.slint"),
            ),
        ]));
        slint_build::compile_with_config("tests/sync-components.slint", config).unwrap();
        return;
    }
    let output_dir = tempfile::tempdir().unwrap();
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "shared_sync_components_compile", "--nocapture"])
        .env("NOVA_UI_BASE_SYNC_COMPILE_CHILD", "1")
        .env("OUT_DIR", output_dir.path())
        .env("CARGO_MANIFEST_DIR", manifest)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output_dir.path().join("sync-components.rs").is_file());
}
