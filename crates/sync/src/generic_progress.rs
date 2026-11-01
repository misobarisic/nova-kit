//! No video progress policy is linked when `nova-progress` is disabled.
use crate::{Record, Version};
pub(crate) fn valid(_: &Record) -> bool {
    true
}
pub(crate) fn local_input(value: String) -> String {
    value
}
pub(crate) fn local(_: Option<&Record>, value: String, _: Version) -> String {
    value
}
pub(crate) fn merge(_: &Record, _: &Record) -> Option<String> {
    None
}
