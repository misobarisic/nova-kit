# Nova Kit

Shared Rust crates for the Nova applications, under GPL-3.0-or-later.

| Package | Responsibility |
| --- | --- |
| `nova-storage` | redb JSON storage, atomic batches, collections, compressed metadata and durable sequences |
| `addons` | Stremio addon URL builders, protocol types and response parsing, with an optional blocking HTTP client |
| `nova-download` | Existing download jobs, HTTP resume, finite HLS bundles, cancellation, filesystem capacity and offline budgeting |
| `nova-ui-base` | Slint source components, theme/animation tokens and associated assets |
| `nova-cache` | Nova's processed image variants, decoded LRU, atomic encoded-page cache, batched usage index and incremental grouped eviction |

Storage was extracted from Nova commit
`7a0cb47d0977fdb64601af4f37a6e7d53b6f5e45`. Addons, downloads and UI components
come from `7c317b76fb468938101c6ed1190876212df9e30d`. The addon/download Rust
implementations and public models are unchanged. Download jobs retain their
existing media/episode fields; this extraction does not generalize their schema.

## Storage

Applications supply their own directory to `init_at`; the filename remains
`nova.redb`. The API owns one process-wide database and must be initialized once.
Sharing the crate does not require applications to share data directories.

## UI components

The source package contains scrolling, icons, touch/slider input, focus markers,
search fields, dropdowns, filter chips, menus and generic Settings controls.
Component bodies and behavior are preserved; image paths now point to packaged
assets. Lucide SVG attribution and license notices are retained beside the icons.

Components import the host's `@NovaUiShared` globals. The minimal compatible
contract is provided in `slint/host-globals.slint`: Theme, ThemePalette, Anim,
InterfaceStyle, SettingsStyle, ControlInput, SliderGesture and SheetItem.
Nova supplies those through its existing compiled shared library, preserving
native global identity, page factories and app-owned translation catalogs.
Other apps can provide a compatible source or compiled host contract.

Use `nova_ui_base::SLINT_SOURCE_DIR` with Slint's standard
`CompilerConfiguration::with_library_paths` to map `NovaUiBase` to the source
directory; map `NovaUiShared` to the host contract if it is not already a compiled
library. Components can then be imported from `@NovaUiBase/dropdown.slint`, etc.
The Cargo package also exposes `DEP_NOVAUIBASE_SLINT_SOURCE_DIR` metadata.
Nova's compiler extension reads that metadata for source-only imports, including
inline Slint fixtures. No second compiled global bundle is introduced.

App pages, navigation routing, domain models and Settings bridges remain in Nova.
Its old component paths are compatibility imports. App-specific SettingsRow
and SettingsSearchHighlight remain in Nova's settings-controls.
The shared `cache-settings-controls.slint` now contains Nova's actual numeric
and cache controls; app-specific settings rows, search highlighting and
persistence bridges remain application adapters.

## Image and chapter caches

`nova-cache` relocates the actual image cache from Nova revision
`b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda`; its original behavior tests and
file format move with it. See [provenance and adapter changes](crates/cache/PROVENANCE.md).
Applications install paths/transport with `config::configure` and publish only
the image-related `config::CacheSettings` subset. Profile and video settings
remain app-owned. Existing processed-poster functions retain their names.

`DecodedCache` provides independently budgeted pixel LRUs, protected-key
priority, recency queries and eviction reports. `blob::Cache` stores bounded
encoded bytes using the same atomic variant container and usage/GC index;
the application supplies validation and optional transforms. No resizing or
decoding happens in the blob path, so native-resolution chapter pages and
application-specific codecs remain intact. `get` plus `generation` and
`put_if_current` support displaying original bytes before background encoding;
an existing valid entry is never overwritten by that conditional publication.

Retain a `gc::Worker` from `gc::start(directory, backend)` through orderly
shutdown. `Backend::policy` supplies per-directory disk enable/budget settings;
`eviction_group` makes all pages of a chapter one eviction unit. `Cache::pin`
leases and backend protected keys protect whole groups, including synthetic
active-group markers. Keep chapters and artwork in separate directories with
separate budgets to preserve plentiful small posters when chapter pages turn
over. `touch` and decoded hits update usage in memory; persistence is batched.
Do not point cache roots at durable downloads or imported original files.

## Development

On Debian/Ubuntu, install `pkg-config libfontconfig1-dev` for the image cache's
Slint font dependency before running the workspace checks.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --locked -- -D warnings
cargo test --workspace --locked
```

Tests include storage migration/corruption/concurrency and cross-process reopen,
addon parsing/transport, HTTP/HLS transfers/rate limits, and standalone UI source
compilation against the minimal host contract. CI runs on Linux and Windows.

Consumers can select individual packages from a pinned Git revision:

```toml
[dependencies]
nova-storage = { git = "https://github.com/misobarisic/nova-kit", rev = "<commit-sha>" }
nova-download = { git = "https://github.com/misobarisic/nova-kit", rev = "<commit-sha>" }
```

Cargo discovers packages inside the workspace; Git dependencies need no `path`.
Publish a kit revision before updating consumers to pin new or changed packages.

## Local Nova development

Clone this repository beside Nova as `../nova-kit`. From the Nova checkout:

```sh
cp .cargo/local-kit.example.toml .cargo/local-kit.toml
cargo dev              # build with local kit sources
cargo dev-run          # build and run
cargo dev-release      # optimized build
cargo dev-release-run  # optimized build and run
```

The ignored config patches the Kit packages. Ordinary Cargo commands and
CI use the pinned Git dependencies once the extraction is published. No global
Cargo configuration is needed. Local patches can change Nova's lockfile; keep
those changes out of commits and restore its Git-based lockfile for normal
`--locked` CI commands. An initial Git resolution may still need network access.
