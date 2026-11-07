//! Java sources both applications compile, rendered per application.
//!
//! cargo-apk2 compiles `android/java` from a fixed directory, so each
//! application keeps the rendered file in its tree and checks in a test that it
//! still equals what this module renders: an edit made in one place cannot
//! drift from the other.

/// What differs between the applications' copies of the download service.
#[derive(Clone, Copy, Debug)]
pub struct Names<'a> {
    /// Java package, for example `dev.misob.nova`.
    pub package: &'a str,
    /// Class-name prefix; the service is `{prefix}BackgroundService` and the
    /// notification-permission helper lives in `{prefix}Activity`.
    pub prefix: &'a str,
    /// Notification channel ID.
    pub channel_id: &'a str,
    /// Tag of the partial wake lock held while the service runs.
    pub wake_lock_tag: &'a str,
}

const BACKGROUND_SERVICE: &str = include_str!("../templates/BackgroundService.java.in");

impl Names<'_> {
    /// File name of the rendered download service.
    pub fn background_service_file(&self) -> String {
        format!("{}BackgroundService.java", self.prefix)
    }

    /// Source of the foreground service that keeps downloads alive.
    pub fn background_service(&self) -> String {
        render(BACKGROUND_SERVICE, self)
    }
}

fn render(template: &str, names: &Names) -> String {
    let rendered = template
        .replace("@PACKAGE@", names.package)
        .replace("@PREFIX@", names.prefix)
        .replace("@CHANNEL_ID@", names.channel_id)
        .replace("@WAKE_LOCK_TAG@", names.wake_lock_tag);
    debug_assert!(
        !has_placeholder(&rendered),
        "template placeholder left unresolved"
    );
    rendered
}

fn has_placeholder(text: &str) -> bool {
    text.match_indices('@').any(|(start, _)| {
        let rest = &text[start + 1..];
        rest.find('@').is_some_and(|end| {
            end > 0
                && rest[..end]
                    .bytes()
                    .all(|b| b.is_ascii_uppercase() || b == b'_')
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAMES: Names = Names {
        package: "dev.example.app",
        prefix: "Example",
        channel_id: "example.background",
        wake_lock_tag: "example:downloads",
    };

    #[test]
    fn every_placeholder_is_replaced() {
        assert!(has_placeholder(BACKGROUND_SERVICE));
        let source = NAMES.background_service();
        assert!(!has_placeholder(&source));
        assert!(source.starts_with("package dev.example.app;\n"));
        assert!(source.contains("public class ExampleBackgroundService extends Service"));
        assert!(source.contains("ExampleActivity.requestNotificationPermission(0x53);"));
        assert!(source.contains("CHANNEL_ID = \"example.background\""));
        assert!(source.contains("\"example:downloads\""));
        assert_eq!(
            NAMES.background_service_file(),
            "ExampleBackgroundService.java"
        );
    }

    #[test]
    fn annotations_are_not_placeholders() {
        assert!(!has_placeholder("    @Override\n    public void f() {}\n"));
    }
}
