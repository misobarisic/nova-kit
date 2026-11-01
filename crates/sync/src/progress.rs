//! Convergent progress registers. Action versions live inside JSON values so
//! reconciliation never invents a new watch/unwatch action from a projection.
use crate::merge::{Record, Version};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const META: &str = "__nova_progress_v1";

/// Local intent comes from the changed materialized baseline. Register state
/// belongs to the stored record; carrying it into the new payload can make a
/// same-value rewatch look like a no-op after another alias was unwatched.
pub(crate) fn local_input(value: String) -> String {
    let Ok(Value::Object(mut data)) = serde_json::from_str(&value) else {
        return value;
    };
    data.remove(META);
    Value::Object(data).to_string()
}

#[derive(Clone, Serialize, Deserialize)]
struct Candidate {
    version: Version,
    data: Value,
}
#[derive(Clone, Serialize, Deserialize)]
struct State {
    activity: Candidate,
    watched: Option<Candidate>,
    unwatch: Option<Candidate>,
    play_count: u64,
}
fn flag(v: &Value) -> bool {
    v.get("watched").and_then(Value::as_bool).unwrap_or(false)
}
fn count(v: &Value) -> u64 {
    v.get("play_count").and_then(Value::as_u64).unwrap_or(0)
}
fn stamp(v: &Value) -> u64 {
    v.get("unwatched_at_secs")
        .and_then(Value::as_u64)
        .unwrap_or(0)
}
fn manual_action(v: &Value) -> Option<&str> {
    v.get("manual_watch_action")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty() && id.len() <= 128)
}
fn candidate(mut data: Value, version: Version) -> Candidate {
    if let Some(map) = data.as_object_mut() {
        map.remove(META);
    }
    Candidate { version, data }
}
fn state(record: &Record) -> Option<State> {
    let data: Value = serde_json::from_str(record.value.as_ref()?).ok()?;
    if let Some(meta) = data.get(META)
        && let Ok(s) = serde_json::from_value(meta.clone())
    {
        return Some(s);
    }
    let c = candidate(data, record.version);
    Some(State {
        play_count: count(&c.data),
        watched: flag(&c.data).then(|| c.clone()),
        unwatch: (!flag(&c.data) && stamp(&c.data) > 0).then(|| c.clone()),
        activity: c,
    })
}
/// Read-only explanation of the same action registers used by `render`.
/// The versions identify actions, not a log of every playback event.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProgressInspection {
    pub watched: bool,
    pub activity: Version,
    pub watch_action: Option<Version>,
    pub unwatch_action: Option<Version>,
}

pub fn inspect_progress(record: &Record) -> Option<ProgressInspection> {
    if record.is_deleted() || !valid(record) {
        return None;
    }
    let state = state(record)?;
    let watched = state.watched.as_ref().is_some_and(|watch| {
        state
            .unwatch
            .as_ref()
            .is_none_or(|unwatch| watch.version.newer_than(&unwatch.version))
    });
    Some(ProgressInspection {
        watched,
        activity: state.activity.version,
        watch_action: state.watched.map(|action| action.version),
        unwatch_action: state.unwatch.map(|action| action.version),
    })
}

pub(crate) fn valid(record: &Record) -> bool {
    let Some(raw) = record.value.as_ref() else {
        return true;
    };
    let Ok(data) = serde_json::from_str::<Value>(raw) else {
        return true;
    };
    let Some(meta) = data.get(META) else {
        return true;
    };
    let Ok(s) = serde_json::from_value::<State>(meta.clone()) else {
        return false;
    };
    [Some(&s.activity), s.watched.as_ref(), s.unwatch.as_ref()]
        .into_iter()
        .flatten()
        .all(|c| {
            !c.version.newer_than(&record.version) && crate::store::valid_clock(c.version.hlc())
        })
}
fn max(a: Candidate, b: Candidate) -> Candidate {
    if b.version.newer_than(&a.version)
        || (b.version == a.version && data_cmp(&b.data, &a.data).is_gt())
    {
        b
    } else {
        a
    }
}

fn data_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    // JSON objects serialize with stable key order in this crate configuration;
    // byte comparison gives equal-version candidates a deterministic total tie.
    serde_json::to_vec(a)
        .expect("JSON value serialization")
        .cmp(&serde_json::to_vec(b).expect("JSON value serialization"))
}
fn union(a: Option<Candidate>, b: Option<Candidate>) -> Option<Candidate> {
    match (a, b) {
        (Some(a), Some(b)) => Some(max(a, b)),
        (a, b) => a.or(b),
    }
}
fn render(s: State) -> String {
    let watched = s.watched.as_ref().filter(|w| {
        s.unwatch
            .as_ref()
            .is_none_or(|u| w.version.newer_than(&u.version))
    });
    let mut data = watched.unwrap_or(&s.activity).data.clone();
    if let Some(map) = data.as_object_mut() {
        map.insert("watched".into(), Value::Bool(watched.is_some()));
        map.insert("play_count".into(), s.play_count.into());
        if let Some(at) = s.activity.data.get("updated_at_secs") {
            map.insert("updated_at_secs".into(), at.clone());
        }
        map.insert(
            "unwatched_at_secs".into(),
            if watched.is_some() {
                0.into()
            } else {
                s.unwatch
                    .as_ref()
                    .map(|u| stamp(&u.data))
                    .unwrap_or(0)
                    .into()
            },
        );
        map.insert(
            META.into(),
            serde_json::to_value(&s).expect("progress registers"),
        );
    }
    data.to_string()
}
pub(crate) fn local(old: Option<&Record>, value: String, version: Version) -> String {
    let Ok(data) = serde_json::from_str::<Value>(&value) else {
        return value;
    };
    let c = candidate(data, version);
    let mut s = old.and_then(state).unwrap_or_else(|| State {
        activity: c.clone(),
        watched: None,
        unwatch: None,
        play_count: 0,
    });
    s.play_count = s.play_count.max(count(&c.data));
    let prior_stamp = stamp(&s.activity.data);
    let was_watched = flag(&s.activity.data);
    let explicit =
        manual_action(&c.data).is_some_and(|id| Some(id) != manual_action(&s.activity.data));
    if flag(&c.data) {
        s.watched = Some(c.clone());
    }
    if !flag(&c.data)
        && stamp(&c.data) > 0
        && (old.is_none() || was_watched || stamp(&c.data) != prior_stamp || explicit)
    {
        s.unwatch = Some(c.clone());
    }
    s.activity = c;
    render(s)
}
pub(crate) fn merge(a: &Record, b: &Record) -> Option<String> {
    let a = state(a)?;
    let b = state(b)?;
    Some(render(State {
        activity: max(a.activity, b.activity),
        watched: union(a.watched, b.watched),
        unwatch: union(a.unwatch, b.unwatch),
        play_count: a.play_count.max(b.play_count),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn rec(dev: u64, watched: bool, unwatch: u64, pos: u64) -> Record {
        let version = Version::new(100, 0, dev, false);
        let data = serde_json::json!({"watched":watched,"unwatched_at_secs":unwatch,"position_secs":pos,"duration_secs":100,"updated_at_secs":1,"play_count":dev}).to_string();
        Record {
            value: Some(local(None, data, version)),
            version,
        }
    }
    fn merged(a: &Record, b: &Record) -> Record {
        Record {
            value: merge(a, b),
            version: if b.version.newer_than(&a.version) {
                b.version
            } else {
                a.version
            },
        }
    }
    #[test]
    fn inspection_explains_actions_without_turning_activity_into_watch_intent() {
        let watched = rec(1, true, 0, 100);
        let unwatched = rec(2, false, 1, 0);
        let activity = rec(3, false, 0, 12);
        let record = merged(&merged(&watched, &unwatched), &activity);
        let before = record.clone();
        let inspection = inspect_progress(&record).unwrap();
        assert!(!inspection.watched);
        assert_eq!(inspection.activity, activity.version);
        assert_eq!(inspection.watch_action, Some(watched.version));
        assert_eq!(inspection.unwatch_action, Some(unwatched.version));
        assert_eq!(record, before);
        assert!(inspect_progress(&Record::tombstone(record.version)).is_none());
    }

    #[test]
    fn inspection_matches_unwatch_tie_and_legacy_watch_rules() {
        let watched = rec(1, true, 0, 100);
        let unwatched = rec(1, false, 1, 0);
        assert!(
            !inspect_progress(&merged(&watched, &unwatched))
                .unwrap()
                .watched
        );
        let legacy = Record::present(
            serde_json::json!({"watched": true, "position_secs": 100}).to_string(),
            watched.version,
        );
        let inspection = inspect_progress(&legacy).unwrap();
        assert!(inspection.watched);
        assert_eq!(inspection.watch_action, Some(watched.version));
        assert_eq!(inspection.unwatch_action, None);
    }

    #[test]
    fn progress_registers_are_commutative_associative_and_idempotent() {
        let a = rec(1, true, 0, 100);
        let b = rec(2, false, 1, 0);
        let c = rec(3, false, 0, 12);
        assert_eq!(merge(&a, &b), merge(&b, &a));
        assert_eq!(merge(&a, &a), a.value);
        assert_eq!(
            merged(&merged(&a, &b), &c).value,
            merged(&a, &merged(&b, &c)).value
        );
        let data: Value =
            serde_json::from_str(merged(&merged(&a, &b), &c).value.as_ref().unwrap()).unwrap();
        assert_eq!(data["watched"], false);
        assert_eq!(data["position_secs"], 12);
        assert_eq!(data["play_count"], 3);
    }
}
