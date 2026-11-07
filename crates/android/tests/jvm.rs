//! The JNI glue against a real JVM on the host. Android's framework is replaced
//! by the few classes in `tests/java`. Needs a JDK and the `invocation` feature:
//! `cargo test -p nova-android --features invocation --test jvm`.
#![cfg(feature = "invocation")]
use jni::objects::{JObject, JValue};
use jni::{InitArgsBuilder, JNIVersion, JavaVM};
use nova_android::context::{
    JniDriver, app_class, clear_pending_exception, files_dir, initialize_foreground_context,
    load_class, with_app_context,
};
use nova_android::service::DownloadService;
use std::path::PathBuf;
use std::sync::OnceLock;

const SERVICE: &str = "dev.example.Fixture$Service";
const FIXTURE: &str = "dev/example/Fixture";

fn compile() -> PathBuf {
    let out = std::env::temp_dir().join(format!("nova-android-jvm-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let sources = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/java");
    let mut files = Vec::new();
    let mut stack = vec![sources];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    let status = std::process::Command::new("javac")
        .args(["-source", "8", "-target", "8", "-Xlint:-options", "-d"])
        .arg(&out)
        .args(&files)
        .status()
        .expect("a JDK (javac) is required for this test");
    assert!(status.success());
    out
}

fn int(env: &mut jni::JNIEnv, method: &str) -> i32 {
    env.call_static_method(FIXTURE, method, "()I", &[])
        .unwrap()
        .i()
        .unwrap()
}
fn tick(service: &DownloadService<JniDriver>, busy: bool, text: &str) {
    let text = text.to_owned();
    // Worker threads attach themselves, like the download monitor does.
    std::thread::scope(|scope| {
        scope.spawn(|| service.set(busy, "Downloading", &text));
    });
}

#[test]
fn context_class_lookup_exceptions_and_download_service_against_a_jvm() {
    let classes = compile();
    let args = InitArgsBuilder::new()
        .version(JNIVersion::V8)
        .option(format!("-Djava.class.path={}", classes.display()))
        .option(format!("-Dfixture.files={}", classes.display()))
        .option("-Xcheck:jni")
        .build()
        .unwrap();
    let vm = JavaVM::new(args).unwrap();
    let mut env = vm.attach_current_thread().unwrap();

    // Nothing is retained before bootstrap, and nothing panics.
    assert!(with_app_context(|_, _| Ok(())).is_none());

    let activity = env
        .call_static_method(
            FIXTURE,
            "bootstrapActivity",
            "()Ldev/example/Fixture$Activity;",
            &[],
        )
        .unwrap()
        .l()
        .unwrap();
    initialize_foreground_context(vm.get_java_vm_pointer().cast(), activity.as_raw().cast());
    env.delete_local_ref(activity).unwrap();

    // The Application is retained, not the Activity.
    let retained = with_app_context(|env, raw| {
        let context = unsafe { JObject::from_raw(raw) };
        env.call_static_method(
            FIXTURE,
            "isApplication",
            "(Landroid/content/Context;)Z",
            &[JValue::Object(&context)],
        )?
        .z()
    });
    assert_eq!(retained, Some(true));

    // The files directory is read from any Context.
    let dir = with_app_context(|env, raw| {
        let context = unsafe { JObject::from_raw(raw) };
        Ok(files_dir(env, &context))
    })
    .unwrap()
    .unwrap();
    assert_eq!(dir, classes);

    // Classes load through the Context's loader and are cached once found.
    static CACHE: OnceLock<jni::objects::GlobalRef> = OnceLock::new();
    for _ in 0..2 {
        let name = with_app_context(|env, raw| {
            let context = unsafe { JObject::from_raw(raw) };
            let class = app_class(env, &context, SERVICE, &CACHE)?;
            let name = env
                .call_method(&class, "getName", "()Ljava/lang/String;", &[])?
                .l()?;
            Ok(String::from(env.get_string(&name.into())?))
        });
        assert_eq!(name.as_deref(), Some(SERVICE));
    }
    assert!(CACHE.get().is_some());
    assert!(
        with_app_context(|env, raw| {
            let context = unsafe { JObject::from_raw(raw) };
            load_class(env, &context, "dev.example.Missing").map(|_| ())
        })
        .is_none(),
        "a missing class fails the call, not the thread"
    );

    // A Java exception fails the call and is cleared, so the next call works.
    let exploded = with_app_context(|env, raw| {
        let context = unsafe { JObject::from_raw(raw) };
        env.call_static_method(
            FIXTURE,
            "explode",
            "(Landroid/content/Context;)V",
            &[JValue::Object(&context)],
        )
        .map(|_| ())
    });
    assert!(exploded.is_none());
    assert!(!clear_pending_exception(&mut env), "already cleared");
    assert_eq!(with_app_context(|_, _| Ok(7)), Some(7));

    // The download service starts once, refreshes, stops, and a start the
    // platform rejects is retried on the next busy tick.
    static DOWNLOADS: DownloadService<JniDriver> = DownloadService::new(JniDriver::new(SERVICE));
    tick(&DOWNLOADS, true, "1 of 2");
    assert_eq!((int(&mut env, "starts"), int(&mut env, "updates")), (1, 1));
    tick(&DOWNLOADS, true, "1 of 2");
    assert_eq!(
        int(&mut env, "updates"),
        1,
        "same text within a second is throttled"
    );
    tick(&DOWNLOADS, true, "2 of 2");
    assert_eq!((int(&mut env, "starts"), int(&mut env, "updates")), (1, 2));
    tick(&DOWNLOADS, false, "");
    tick(&DOWNLOADS, false, "");
    assert_eq!(int(&mut env, "stops"), 1);

    env.call_static_method(FIXTURE, "rejectNextStarts", "(I)V", &[JValue::Int(1)])
        .unwrap();
    tick(&DOWNLOADS, true, "retry");
    assert_eq!(
        int(&mut env, "starts"),
        1,
        "the platform rejected the start"
    );
    tick(&DOWNLOADS, true, "retry");
    assert_eq!(
        (int(&mut env, "starts"), int(&mut env, "updates")),
        (2, 3),
        "the next tick starts it again instead of waiting for an idle cycle"
    );
}
