//! The process-wide Application context and the JNI conventions built on it.
//!
//! Only the Application is retained, so background work never depends on an
//! Activity surviving rotation or being initialized by `ndk-context`. Both the
//! foreground entry point and headless jobs (which have no Activity) feed the
//! same slot.
use crate::service::Driver;
use jni::JavaVM;
use jni::objects::{GlobalRef, JClass, JObject, JString, JValue};
use jni::sys::jobject;
use std::path::PathBuf;
use std::sync::OnceLock;

static APP_CONTEXT: OnceLock<(JavaVM, GlobalRef)> = OnceLock::new();

/// Retain the Application behind `context` unless one is already retained.
pub fn retain_app_context(env: &mut jni::JNIEnv, context: &JObject) -> jni::errors::Result<()> {
    if APP_CONTEXT.get().is_none() {
        let app = env
            .call_method(
                context,
                "getApplicationContext",
                "()Landroid/content/Context;",
                &[],
            )?
            .l()?;
        let global = env.new_global_ref(app)?;
        let vm = env.get_java_vm()?;
        let _ = APP_CONTEXT.set((vm, global));
    }
    Ok(())
}

/// Called before foreground app startup; jobs initialize the same slot from
/// their own Context. Neither path requires a previously launched Activity.
pub fn initialize_foreground_context(vm: *mut std::ffi::c_void, activity: *mut std::ffi::c_void) {
    // SAFETY: AndroidApp owns these JNI references throughout android_main.
    let result = (|| -> jni::errors::Result<()> {
        let vm = unsafe { JavaVM::from_raw(vm.cast()) }?;
        let mut env = vm.attach_current_thread()?;
        let activity = unsafe { JObject::from_raw(activity.cast()) };
        retain_app_context(&mut env, &activity)
    })();
    if let Err(error) = result {
        eprintln!("nova-android: unable to initialize Android context: {error}");
    }
}

/// Clear a pending Java exception so it cannot surface in an unrelated later
/// JNI call on this thread. UI callers may already be attached: dropping their
/// nested AttachGuard does not clear one as detaching a fresh worker would.
/// Returns whether an exception was pending.
pub fn clear_pending_exception(env: &mut jni::JNIEnv) -> bool {
    let pending = env.exception_check().unwrap_or(false);
    if pending {
        let _ = env.exception_clear();
    }
    pending
}

/// Run `f` with the retained Application Context. Returns `None` before either
/// foreground or headless bootstrap, without calling ndk-context's panicking
/// accessor, and when the call failed or left a Java exception (cleared).
/// Foreground UI actions resolve a live Activity separately in Java.
pub fn with_app_context<R>(
    f: impl FnOnce(&mut jni::JNIEnv, jobject) -> jni::errors::Result<R>,
) -> Option<R> {
    let (vm, context) = APP_CONTEXT.get()?;
    let mut guard = vm.attach_current_thread().ok()?;
    let result = f(&mut guard, context.as_obj().as_raw());
    let pending_exception = clear_pending_exception(&mut guard);
    match result {
        Ok(value) if !pending_exception => Some(value),
        Ok(_) => {
            eprintln!("nova-android: Android call left a Java exception");
            None
        }
        Err(error) => {
            eprintln!("nova-android: Android call failed: {error}");
            None
        }
    }
}

/// Like [`with_app_context`] for callers that report the failure: the closure
/// gets the context object itself.
pub fn call(
    f: impl FnOnce(&mut jni::JNIEnv, &JObject) -> jni::errors::Result<()>,
) -> Result<(), String> {
    let (vm, context) = APP_CONTEXT.get().ok_or("Android context unavailable")?;
    let mut env = vm.attach_current_thread().map_err(|e| e.to_string())?;
    let result = f(&mut env, context.as_obj()).map_err(|e| e.to_string());
    // Never leave a pending Java exception attached to an event-loop thread.
    clear_pending_exception(&mut env);
    result
}

/// Load an app class through the Context's class loader (the reliable way to
/// find app-dex classes from a native thread).
pub fn load_class<'local>(
    env: &mut jni::JNIEnv<'local>,
    context: &JObject,
    dotted: &str,
) -> jni::errors::Result<JClass<'local>> {
    let loader = env
        .call_method(context, "getClassLoader", "()Ljava/lang/ClassLoader;", &[])?
        .l()?;
    let name = env.new_string(dotted)?;
    Ok(env
        .call_method(
            loader,
            "loadClass",
            "(Ljava/lang/String;)Ljava/lang/Class;",
            &[JValue::Object(&name)],
        )?
        .l()?
        .into())
}

/// Resolve an app class through the Context's class loader, caching the global
/// ref.
pub fn app_class<'local>(
    env: &mut jni::JNIEnv<'local>,
    context: &JObject<'local>,
    dotted: &str,
    cache: &OnceLock<GlobalRef>,
) -> jni::errors::Result<JClass<'local>> {
    if let Some(global) = cache.get() {
        // Alias the cached global ref; `JClass`'s Drop is a no-op for local
        // views, so the cache keeps ownership.
        return Ok(unsafe { JClass::from_raw(global.as_obj().as_raw()) });
    }
    let class = load_class(env, context, dotted)?;
    let global = env.new_global_ref(&class)?;
    let _ = cache.set(global);
    Ok(class)
}

/// The app's private files directory, read from any Context. Headless jobs
/// call this before opening storage because no Activity has set the path.
pub fn files_dir(env: &mut jni::JNIEnv, context: &JObject) -> Result<PathBuf, String> {
    let files = env
        .call_method(context, "getFilesDir", "()Ljava/io/File;", &[])
        .map_err(|e| e.to_string())?
        .l()
        .map_err(|e| e.to_string())?;
    let path = env
        .call_method(&files, "getAbsolutePath", "()Ljava/lang/String;", &[])
        .map_err(|e| e.to_string())?
        .l()
        .map_err(|e| e.to_string())?;
    let path = unsafe { JString::from_raw(path.into_raw()) };
    let path: String = env.get_string(&path).map_err(|e| e.to_string())?.into();
    Ok(PathBuf::from(path))
}

/// [`Driver`] over a Java class with static `start(Context)`, `stop(Context)`
/// and `update(Context, String, String)` methods.
pub struct JniDriver {
    class: &'static str,
    cache: OnceLock<GlobalRef>,
}

impl JniDriver {
    pub const fn new(class: &'static str) -> Self {
        Self {
            class,
            cache: OnceLock::new(),
        }
    }
}

impl Driver for JniDriver {
    fn stop(&self) {
        with_app_context(|env, raw| {
            let context = unsafe { JObject::from_raw(raw) };
            let class = app_class(env, &context, self.class, &self.cache)?;
            env.call_static_method(
                &class,
                "stop",
                "(Landroid/content/Context;)V",
                &[JValue::Object(&context)],
            )?;
            Ok(())
        });
    }

    fn apply(&self, start: bool, title: &str, text: &str) -> bool {
        let mut started = !start;
        with_app_context(|env, raw| {
            let context = unsafe { JObject::from_raw(raw) };
            let class = app_class(env, &context, self.class, &self.cache)?;
            if start {
                env.call_static_method(
                    &class,
                    "start",
                    "(Landroid/content/Context;)V",
                    &[JValue::Object(&context)],
                )?;
                started = true;
            }
            let title = env.new_string(title)?;
            let text = env.new_string(text)?;
            env.call_static_method(
                &class,
                "update",
                "(Landroid/content/Context;Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(&context),
                    JValue::Object(&title),
                    JValue::Object(&text),
                ],
            )?;
            Ok(())
        });
        started
    }
}
