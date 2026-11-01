# Nova sync engine extraction

The implementation and regression tests were relocated from Nova commit
[`c035d61af9b57e34b997b44e53a379652bf5654a`](https://github.com/misobarisic/nova/tree/c035d61af9b57e34b997b44e53a379652bf5654a),
`crates/sync/src/*.rs`, under GPL-3.0-or-later. The optional
`nova-playback-api` crate is the original `crates/playback-api` at that revision.
The clock and historical FNV hash helpers are copied exactly from
`crates/config/src/lib.rs`; the hash multiplier is deliberately unchanged.

The shared crate retains the actual HLC, wire framing, authenticated pairing,
peer membership tombstones, removal notices, retry/cadence, corruption quarantine,
atomic snapshots/baselines, persistence barriers, package distribution, transfer
status and foreground/background engine ownership. No wire structs, record keys,
storage schema, Nova ALPNs or NV1 invite encoding changed.

`playback` and `nova-progress` are explicit default features preserving existing
Nova APIs and watch/unwatch convergence. With `default-features = false`, the
engine does not depend on `nova-playback-api` or compile Nova's progress schema.
Reader selects `ProtocolConfig::reader()` before loading identity/store and
installs its own `RecordPolicy` to validate local, persisted and remote records.
Wire configuration is immutable after first use. All listeners and outbound
sync, pairing, removal, package and optional playback paths use that selection.
Store policy validation also covers corruption recovery and merged values.
Applications retain independent storage directories and one identity/engine
owner per process.

Run `cargo test -p nova-sync --locked` and the same command with
`--no-default-features`, plus `cargo clippy -p nova-sync --all-targets --locked -- -D warnings`.
