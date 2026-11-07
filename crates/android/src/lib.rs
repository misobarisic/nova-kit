//! Shared Android glue for Nova and Nova Reader.
//!
//! All Android API calls live in Java; Rust loads a class through the app's
//! class loader (reachable from the Application Context retained during
//! foreground or headless bootstrap) and calls a static method. The JVM-facing
//! parts are in [`context`], behind the `jni` feature that the applications
//! enable for Android. The decisions around
//! them ([`service`], [`slot`]) and the Java sources ([`java`]) build on every
//! host.

#[cfg(feature = "jni")]
pub mod context;
pub mod java;
pub mod service;
pub mod slot;

#[cfg(test)]
mod contract {
    /// Background work must not depend on an Activity or on `ndk-context`, whose
    /// accessor panics before an Activity has published its pointers.
    #[test]
    fn the_context_never_consults_ndk_context() {
        let source = include_str!("context.rs");
        assert!(!source.contains("ndk_context::android_context()"));
        assert!(source.contains("let (vm, context) = APP_CONTEXT.get()?;"));
        assert!(source.contains("\"getApplicationContext\""));
    }
}
