# Sync extraction assessment

Audited Nova `c035d61af9b57e34b997b44e53a379652bf5654a`, Reader
`63110af43a5ac909f972b2f8adf8e33cd90c1353` and Kit
`689f86852aaaf5b13f1f51178672ac5de6745795`. This is an implementation plan;
Kit does not provide `nova-sync` yet and Reader does not sync yet.

## Recommended boundary

Move the actual engine and its regressions from Nova's `crates/sync` into Kit,
then make both apps consume that published revision. Preserve Nova's existing
wire format, stored records, identities and pairings. Do not introduce a second
Reader-only sync implementation or copy the whole video application adapter.

| Ownership | Code and behavior |
| --- | --- |
| Kit `nova-sync` | Hybrid logical clocks, versioned records, tombstones, durable snapshots/baselines, delta exchange, authenticated peer membership/removal, invitations and confirmation codes, transfer status, retry/cadence and foreground/background engine ownership |
| Kit UI source components | Device/name/status controls, enable switch, sync-now action, auto-sync settings, local discovery, invite creation/join/cancel, pairing approval, peer list/removal and generic record inspector |
| Nova adapter | Movie/episode identities, watched/unwatched action registers, episode aliases, video remote control, profile directory and Nova-specific settings/materialization |
| Reader adapter | Source-local series/chapter/page identities, library/categories, chapter reading progress/completion/history/bookmarks, reader settings and source package policy |
| App platform layer | Storage directory, clipboard, QR scanner, lifecycle notifications and Android JobScheduler entry points |

The current engine is about 11,000 lines including tests. Nova additionally has
a roughly 5,600-line application adapter (`src/app/sync.rs`) and a 686-line
settings page (`crates/ui-settings-sync/sync-module.slint`). Moving the crate
alone would not give Reader either the data integration or settings UI.

## Couplings to remove first

1. `crates/sync/Cargo.toml` depends on `nova-config` for wall-clock/hash helpers
   and on `nova-playback-api`. Move the small deterministic helpers with their
   compatibility tests; retain playback as an explicit Nova extension, outside
   the generic engine. Reader must not pull in video-domain dependencies.
2. `store.rs` recognizes a domain called `progress` and invokes `progress.rs`.
   Its schema is specifically Nova's `watched`, `play_count`,
   `unwatched_at_secs` and manual watch actions, with `__nova_progress_v1`
   metadata. This is not Reader's `ReadingProgress`. Inject a validation/merge
   policy (including persisted-load and local-write paths), and keep Nova's
   current policy unchanged in its adapter. Plain last-write-wins must not
   silently replace Nova's existing convergent watch actions.
3. Engine setup hardcodes sync, pairing, package, removal and playback ALPNs.
   Use an immutable application protocol configuration through every listen
   and connect path. Preserve Nova's `nova/sync/6`, `nova/pair/5`,
   `nova/addon-package/1`, `nova/remove/1`, `nova/playback/5` and `NV1` ticket
   compatibility. Give Reader its own protocol/ticket namespace, so accidental
   cross-app pairing cannot import movie records into chapter state.
4. The settings page imports `SettingsBridge` and `SettingsContent` from
   Nova's compiled `@NovaUiShared`. Extract source components with explicit
   properties/callbacks into `nova-ui-base`; leave routing, measurements,
   settings search, persistence and domain-specific inspector text in each app.
   Use the existing Kit host contract to preserve one set of theme/input globals.
5. Several APIs use a process-global engine, package provider and database.
   Retain the one-app-directory-per-process contract; run isolated peer/restart
   tests in subprocesses or independently owned test engines. Extraction does
   not mean sharing Nova's and Reader's databases or endpoint identities.

## Reader data integration

`reader-domain::DomainStore::update` currently commits one validated JSON
snapshot under `reader:state`. Split changes into durable per-identity records
for exchange; do not sync the entire snapshot as one value. Otherwise unrelated
offline edits on two devices overwrite each other.

| Record | Identity and merge requirement |
| --- | --- |
| Library membership and manual reading status | Full serialized `SeriesId` (`source_id`, `local_id`); retained tombstones prevent a delayed metadata refresh from re-adding removed entries |
| Categories and membership | Stable category identity and separate membership records; changing a title does not replace the identity |
| Resume position | Full `ChapterId`, stable page ID plus `ResumeAnchor` fractional offset; latest intentional activity, rather than maximum offset, allows rereading |
| Chapter completion | Explicit read/unread action version separate from frequent resume checkpoints; a late old checkpoint must not undo a manual mark unread/read |
| History | Chapter identity and activity timestamp, with independent removals so hidden entries do not return after an old peer reconnects |
| Bookmarks | Requires a durable bookmark ID and deletion record: the current bookmark has only chapter, anchor and note, so editing its note must not create a new identity |
| Settings | Field-level shared values with local override baselines; keep paths, cache quotas, orientation/device policy and credentials local by default |
| Source packages and catalogue switches | Stable package/source IDs; reuse verified package blobs, but retain Reader's `nova-reader1` runtime gate and permission-expansion consent |

Categories already have an ID, but creation currently derives it only from a
timestamp (`category-{now()}` in `src/app.rs`); use collision-resistant IDs for
independent offline devices. Bookmarks lack an ID. Both changes need explicit
migration tests before enabling record sync. Source keys
must never be replaced by chapter numbers, title strings or expiring page URLs.
Fetched chapter lists and covers are metadata/cache, not membership authority.

Save the local materialized snapshot and corresponding sync changes atomically
using Kit storage batches. Apply validated remote records with an echo guard,
preserving pending local progress writes and checking the current generation
before publishing to Slint. Persistence barriers must drain queued saves before
exchange/projection. Preserve unknown future fields without accepting invalid
Reader identities or restoring removed entities.

Reader currently has one `ReaderState`, rather than Nova's full user-profile
directory. Establish a stable default profile namespace now; multi-profile UI
and profile retirement require an explicit Reader data migration, not merely
relabeling the inspector. Never manufacture a profile switch with no isolated
underlying library/settings.

Chapter image caches, downloaded bundles, local file paths, source cookies and
credentials stay device-local. Addon package distribution is separate from
chapter-byte transfer. Executable package installation still passes signature,
format and capability checks even when a paired device supplies the blob.

## UI and platform parity

Reuse Nova's actual settings component bodies, adapting the explicit bridge:
enable sync, device name/ID, status and Sync now; running-app cadence; Android
background toggle; local discovery; confirmation requirement; invite create,
copy, join and cancel; incoming Accept/Deny plus match code; peer removal; and
the advanced read-only inspector. Use chapter/read terminology in Reader's
inspector and omit Nova's remote video-control row.

Keep desktop keyboard focus, accessible actions, clipboard and responsive
phone/desktop layouts. Android background parity also needs the Java job
service, manifest declaration, JNI callback and lifecycle/foreground ownership
adapters; a timer in the Slint settings page is insufficient. Android's closed
app jobs remain best effort with the platform's approximately 15-minute floor;
the default running-app exchange interval is 60 seconds. Disabled sync must
bind no endpoint, and background execution must not open a reading window.

## Delivery order and acceptance gates

1. Extract the engine and tests, introduce explicit application/merge policy
   boundaries, and switch Nova to the published Kit revision first. Require
   compatibility tests for old tickets, ALPNs, records, profile retirement,
   watch/unwatch winners, peer removal and restart recovery.
2. Extract the shared UI source components and convert Nova's settings page
   without changing its behavior. Test narrow/wide layouts and pairing flows.
3. Add Reader's stable identities, per-record adapter, atomic persistence and
   settings overrides. Test two peers editing different titles offline,
   read/unread versus resume races, bookmark edits/deletions, library removal,
   source collisions, reconnect and restart without echoing remote edits.
4. Wire Reader's shared settings UI and desktop/Android lifecycle. Exercise
   actual pairing, rejection/cancel, expired tickets, removed peers, disabled
   networking, background job ownership and incompatible-app rejection.
5. Publish Kit before pinning the consumers. Run each workspace's required
   checks and platform builds, then release app tags once the adapters pass.

The key prerequisite is making merge behavior and protocol identity explicit.
That preserves Nova's mature synchronization while giving Reader the same
device workflow without inheriting video semantics.
