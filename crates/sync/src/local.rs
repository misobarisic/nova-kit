//! Socket-free mutation owner. App snapshots and their materialized baselines
//! commit atomically with changed record rows in the same redb transaction.
use crate::Store;
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

static LOCAL: Mutex<Option<Arc<Mutex<Store>>>> = Mutex::new(None);

pub fn local_store() -> Result<Arc<Mutex<Store>>> {
    let mut slot = LOCAL.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(store) = slot.as_ref() {
        return Ok(store.clone());
    }
    let mut store = Store::try_load()?;
    store.set_device(local_device()?);
    let store = Arc::new(Mutex::new(store));
    *slot = Some(store.clone());
    Ok(store)
}

pub fn local_device() -> Result<u64> {
    Ok(crate::fnv1a(
        crate::load_or_create_secret()?.public().as_bytes(),
    ))
}

/// Missing keys only imply deletion if present in our materialized baseline,
/// never merely because a remote update exists in the record owner.
pub fn commit_snapshot(
    domain: &str,
    records: &[(String, String, u64)],
    snapshot: Option<(&str, String)>,
    projection: bool,
    seed: bool,
) -> Result<()> {
    crate::flush_persistence()?;
    let owner = local_store()?;
    let dev = local_device()?;
    let mut store = owner.lock().unwrap_or_else(|e| e.into_inner());
    let changed = prepare_snapshot(&mut store, dev, domain, records, snapshot, projection, seed)?;
    store.save()?;
    drop(store);
    if changed && !projection && !seed {
        crate::request_local_sync();
    }
    Ok(())
}

pub fn prepare_snapshot(
    store: &mut Store,
    dev: u64,
    domain: &str,
    records: &[(String, String, u64)],
    snapshot: Option<(&str, String)>,
    projection: bool,
    seed: bool,
) -> Result<bool> {
    // Retired IDs are never reused. Delayed actors must not recreate either
    // records or credential-bearing baselines after profile cleanup.
    if let Some((profile, _)) = crate::parse_profile_domain(domain)
        && store
            .record("profile_deletions", profile)
            .is_some_and(|row| !row.is_deleted())
    {
        return Ok(false);
    }
    let mut changed = false;
    let baseline_key = format!("sync:baseline:{domain}");
    let baseline: BTreeMap<String, String> = store
        .extra_value(&baseline_key)?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()
        .context("materialized baseline")?
        .unwrap_or_default();
    let current: BTreeMap<String, String> = records
        .iter()
        .map(|(k, v, _)| (k.clone(), v.clone()))
        .collect();
    if !projection {
        for key in baseline.keys() {
            if !current.contains_key(key) && !seed {
                changed |= store.set(domain, key, None, 0, dev);
            }
        }
        for (key, value, ts) in records {
            let differs = baseline.get(key) != Some(value);
            if (seed && store.record(domain, key).is_none()) || (!seed && differs) {
                let value = preserve_unknown(
                    store.record(domain, key).and_then(|r| r.value.as_deref()),
                    value,
                );
                let value = if crate::is_progress_domain(domain) {
                    crate::progress::local_input(value)
                } else {
                    value
                };
                changed |= store.set(domain, key, Some(value), *ts, dev);
            }
        }
    }
    store.queue_extra(&baseline_key, Some(serde_json::to_string(&current)?));
    if let Some((key, raw)) = snapshot {
        store.queue_extra(key, Some(raw));
    }
    Ok(changed)
}

/// Replace `domain` with exactly `records`, as a restore rather than an edit.
///
/// The materialized baseline is first set to the store's current records, so a
/// live record absent from `records` is deleted (tombstoned) and a record that
/// already matches is left alone, whatever the previous baseline said. Records
/// that arrived while the owning profile was inactive are part of that
/// baseline too.
pub fn replace_domain(
    store: &mut Store,
    dev: u64,
    domain: &str,
    records: &[(String, String, u64)],
) -> Result<bool> {
    let live: BTreeMap<String, String> = store.records(domain).into_iter().collect();
    store.queue_extra(
        &format!("sync:baseline:{domain}"),
        Some(serde_json::to_string(&live)?),
    );
    prepare_snapshot(store, dev, domain, records, None, false, false)
}

/// Publish a staged copy of the live store: run `commit` (normally a durable
/// save) on `staged`, and only if it succeeds replace `live` with it. A failed
/// commit drops the staged copy, so a later unrelated save of `live` cannot
/// carry the abandoned change to disk or to peers.
///
/// Callers keep the store's owner lock and any write serializer held across
/// the whole stage, commit and publish.
pub fn publish<E>(
    live: &mut Store,
    mut staged: Store,
    commit: impl FnOnce(&mut Store) -> Result<(), E>,
) -> Result<(), E> {
    commit(&mut staged)?;
    *live = staged;
    Ok(())
}

/// Stage-and-swap with a caller-defined commit: clone `live`, let `mutate`
/// change the clone, [`publish`] it with `commit`, and return what `mutate`
/// produced. `live` is untouched unless both steps succeed.
pub fn stage_with<T, E>(
    live: &mut Store,
    mutate: impl FnOnce(&mut Store) -> Result<T, E>,
    commit: impl FnOnce(&mut Store) -> Result<(), E>,
) -> Result<T, E> {
    let mut staged = live.clone();
    let output = mutate(&mut staged)?;
    publish(live, staged, commit)?;
    Ok(output)
}

/// [`stage_with`] committing through [`Store::save`].
pub fn stage<T>(live: &mut Store, mutate: impl FnOnce(&mut Store) -> Result<T>) -> Result<T> {
    stage_with(live, mutate, |staged| staged.save())
}

/// Typed clients may update known object fields without erasing newer fields.
pub fn preserve_unknown(old: Option<&str>, new: &str) -> String {
    let old = old.and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok());
    let new_value = serde_json::from_str::<serde_json::Value>(new).ok();
    match (old, new_value) {
        (Some(serde_json::Value::Object(mut old)), Some(serde_json::Value::Object(new))) => {
            old.extend(new);
            serde_json::Value::Object(old).to_string()
        }
        _ => new.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retired_profile_snapshots_cannot_recreate_baselines_or_user_snapshots() {
        let mut store = Store::default();
        let domain = crate::profile_domain("deleted", "tracking");
        let baseline = format!("sync:baseline:{domain}");
        let snapshot = "profile:deleted:tracking:credentials";
        store.set("profile_deletions", "deleted", Some("1".into()), 0, 1);
        store.queue_extra(&baseline, None);
        store.queue_extra(snapshot, None);
        let records = [("credentials".into(), r#"{"token":"secret"}"#.into(), 0)];
        assert!(
            !prepare_snapshot(
                &mut store,
                1,
                &domain,
                &records,
                Some((snapshot, "secret".into())),
                false,
                false
            )
            .unwrap()
        );
        assert!(store.record(&domain, "credentials").is_none());
        assert!(store.extra_value(&baseline).unwrap().is_none());
        assert!(store.extra_value(snapshot).unwrap().is_none());
    }

    #[test]
    #[cfg(feature = "nova-progress")]
    fn alias_projection_does_not_suppress_a_same_value_rewatch() {
        let mut store = Store::default();
        store.queue_extra("sync:baseline:progress", Some("{}".into()));
        let value = |watched, unwatch| {
            serde_json::json!({
                "watched":watched, "unwatched_at_secs":unwatch, "position_secs":0,
                "duration_secs":100, "updated_at_secs":1, "play_count":1
            })
            .to_string()
        };
        let watched = [("canonical".into(), value(true, 0), 0)];
        prepare_snapshot(&mut store, 1, "progress", &watched, None, false, false).unwrap();
        let original = store.record("progress", "canonical").unwrap().clone();
        // A newer unwatch under another alias materializes this ID as false.
        // The underlying canonical record remains the older watched action.
        let projected = [("canonical".into(), value(false, 1), 0)];
        prepare_snapshot(&mut store, 1, "progress", &projected, None, true, false).unwrap();
        assert_eq!(store.record("progress", "canonical"), Some(&original));
        assert!(
            prepare_snapshot(&mut store, 1, "progress", &watched, None, false, false).unwrap(),
            "a changed materialized baseline represents a real rewatch intent"
        );
        assert!(
            store
                .record("progress", "canonical")
                .unwrap()
                .version
                .newer_than(&original.version)
        );
        assert!(
            !prepare_snapshot(&mut store, 1, "progress", &watched, None, false, false).unwrap(),
            "repeating the same snapshot remains idle"
        );
    }

    #[test]
    #[cfg(feature = "nova-progress")]
    fn explicit_same_value_actions_outrank_unsettled_aliases_without_projection_actions() {
        let mut store = Store::default();
        store.queue_extra("sync:baseline:progress", Some("{}".into()));
        let value = |watched, action: &str| {
            serde_json::json!({
                "watched":watched, "unwatched_at_secs":if watched {0} else {1},
                "position_secs":0, "duration_secs":0, "updated_at_secs":1,
                "manual_watch_action":action
            })
            .to_string()
        };
        let mut snapshot = vec![
            ("original".into(), value(true, "initial-watch"), 0),
            ("canonical".into(), value(true, "initial-watch"), 0),
        ];
        prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap();
        snapshot[1].1 = value(false, "canonical-unwatch");
        prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap();
        snapshot[0].1 = value(true, "original-reassert-watch");
        prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap();
        let resolved = store.resolved_progress(["original", "canonical"]).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&resolved).unwrap()["watched"],
            true
        );
        let before = store.digest();
        let projected = vec![
            ("original".into(), value(true, "original-reassert-watch"), 0),
            (
                "canonical".into(),
                value(true, "original-reassert-watch"),
                0,
            ),
        ];
        prepare_snapshot(&mut store, 1, "progress", &projected, None, true, false).unwrap();
        assert_eq!(store.digest(), before);
        // Canonical's own stored activity is already false with the same
        // second-level unwatch stamp. The explicit marker must reassert it.
        snapshot = projected;
        snapshot[0].1 = value(true, "original-new-watch");
        prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap();
        snapshot[1].1 = value(false, "canonical-reassert-unwatch");
        prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap();
        let resolved = store.resolved_progress(["original", "canonical"]).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&resolved).unwrap()["watched"],
            false
        );
        let before = store.digest();
        assert!(
            !prepare_snapshot(&mut store, 1, "progress", &snapshot, None, false, false).unwrap()
        );
        assert_eq!(
            store.digest(),
            before,
            "unchanged checkpoints do not reassert manual intent"
        );
    }

    #[test]
    fn snapshot_noops_and_remote_projections_do_not_request_publication() {
        let mut store = Store::default();
        store.queue_extra("sync:baseline:library", Some("{}".into()));
        let records = vec![("a".into(), r#"{"title":"a"}"#.into(), 0)];
        assert!(prepare_snapshot(&mut store, 1, "library", &records, None, false, false).unwrap());
        assert!(!prepare_snapshot(&mut store, 1, "library", &records, None, false, false).unwrap());
        let projected = vec![("b".into(), r#"{"title":"remote"}"#.into(), 0)];
        assert!(
            !prepare_snapshot(&mut store, 1, "library", &projected, None, true, false).unwrap()
        );
        assert!(store.record("library", "b").is_none());
        assert!(
            !prepare_snapshot(&mut store, 1, "library", &projected, None, false, false).unwrap()
        );
    }

    fn row(key: &str, value: &str) -> (String, String, u64) {
        (key.into(), value.into(), 0)
    }

    #[test]
    fn replace_domain_deletes_what_the_restore_omits_whatever_the_old_baseline_said() {
        let mut store = Store::default();
        store.set("library", "kept", Some("1".into()), 0, 1);
        store.set("library", "dropped", Some("2".into()), 0, 1);
        store.set("library", "changed", Some("3".into()), 0, 1);
        // A stale baseline that already lists a stranger would make a plain
        // snapshot treat the live records as unchanged edits.
        store.queue_extra("sync:baseline:library", Some(r#"{"ghost":"x"}"#.into()));
        let restore = [row("kept", "1"), row("changed", "30"), row("added", "4")];
        assert!(replace_domain(&mut store, 1, "library", &restore).unwrap());
        let live: BTreeMap<_, _> = store.records("library").into_iter().collect();
        assert_eq!(
            live,
            BTreeMap::from([
                ("added".into(), "4".into()),
                ("changed".into(), "30".into()),
                ("kept".into(), "1".into()),
            ])
        );
        assert!(store.record("library", "dropped").unwrap().is_deleted());
        // Restoring the same state again is a no-op.
        assert!(!replace_domain(&mut store, 1, "library", &restore).unwrap());
    }

    #[test]
    fn a_failed_stage_leaves_the_live_store_untouched() {
        let mut live = Store::default();
        live.set("library", "a", Some("1".into()), 0, 1);
        let before = live.digest();
        let failed: std::result::Result<(), String> = stage_with(
            &mut live,
            |staged| {
                staged.set("library", "a", Some("2".into()), 0, 1);
                staged.set("library", "b", Some("3".into()), 0, 1);
                Ok(())
            },
            |_| Err("disk full".into()),
        );
        assert_eq!(failed.unwrap_err(), "disk full");
        assert_eq!(live.digest(), before);
        assert!(live.record("library", "b").is_none());

        let aborted: std::result::Result<(), String> = stage_with(
            &mut live,
            |staged| {
                staged.set("library", "b", Some("3".into()), 0, 1);
                Err("rejected".into())
            },
            |_| Ok(()),
        );
        assert_eq!(aborted.unwrap_err(), "rejected");
        assert!(live.record("library", "b").is_none());
    }

    #[test]
    fn a_committed_stage_replaces_the_live_store_and_returns_the_result() {
        let mut live = Store::default();
        let changed: std::result::Result<bool, String> = stage_with(
            &mut live,
            |staged| Ok(staged.set("library", "a", Some("1".into()), 0, 1)),
            |_| Ok(()),
        );
        assert!(changed.unwrap());
        assert_eq!(
            live.record("library", "a").unwrap().value.as_deref(),
            Some("1")
        );
    }

    #[test]
    fn stage_commits_through_the_stores_own_save() {
        let mut live = Store::default();
        let changed = stage(&mut live, |staged| {
            Ok(staged.set("library", "a", Some("1".into()), 0, 1))
        })
        .unwrap();
        assert!(changed);
        assert_eq!(
            live.record("library", "a").unwrap().value.as_deref(),
            Some("1")
        );
        // A mutation error is passed through and nothing is published.
        let error = stage(&mut live, |staged| -> Result<()> {
            staged.set("library", "b", Some("2".into()), 0, 1);
            Err(anyhow::anyhow!("rejected"))
        })
        .unwrap_err();
        assert_eq!(error.to_string(), "rejected");
        assert!(live.record("library", "b").is_none());
    }
}
