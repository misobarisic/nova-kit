# Nova Kit

Shared Rust crates for the Nova applications, under GPL-3.0-or-later.

See the [shared sync architecture](SYNC-PLAN.md) for the ownership boundary and
consumer integration contracts.

| Package | Responsibility |
| --- | --- |
| `nova-sync` | HLC record engine, durable snapshots, authenticated pairing/membership, package transport and optional Nova playback/progress policies |
| `nova-playback-api` | Optional Nova video remote-control models; omitted by Reader |
| `nova-storage` | redb JSON storage, atomic batches, collections, compressed metadata and durable sequences |
| `addons` | Stremio addon URL builders, protocol types and response parsing, with an optional blocking HTTP client |
| `nova-download` | Existing download jobs, HTTP resume, finite HLS bundles, cancellation, filesystem capacity and offline budgeting |
| `nova-ui-base` | Slint source components, theme/animation tokens and associated assets |
| `nova-cache` | Nova's processed image variants, decoded LRU, atomic encoded-page cache, batched usage index and incremental grouped eviction |
| `nova-image` | Shared bounded AVIF/JPEG XL still codecs and incremental JPEG XL previews, shared by Nova and Reader |

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

The image cache also supports AVIF through `nova-image`. JPEG/WebP recipe keys
and their quality setting are unchanged; AVIF has its own quality default of
85. The codec exposes caller-bounded `inspect_avif`, `decode_avif` and
`encode_avif`, preserves native dimensions and alpha, and rejects unsupported
animation/transforms/bit depths. It uses Reader's actual scalar portable decoder
and encoder code, with the complete BSD-licensed patched decoder vendored once
in Kit. See [codec provenance](crates/image/PROVENANCE.md). Existing files are
not reencoded merely by choosing AVIF; explicit maintenance remains app-owned.

AVIF encoding is **lossy**, including quality 100. rav1e 0.8.1 does not
implement lossless AV1; a pixel-roundtrip regression also checks RGB storage
at quality 100 rather than assuming that removing YCbCr conversion is enough.
Applications must not offer a lossless AVIF switch for this encoder.

`nova-image::ProgressiveJxlDecoder` accepts JPEG XL HTTP/file chunks and exposes
bounded provisional RGBA through an independently throttled `render_preview`.
Native header geometry stays stable. `validate_complete` is mandatory before
promoting a preview or committing a file; consumers can then drop this decoder
and use their existing final display path. `finish` instead returns validated
native pixels. See [the incremental codec contract](crates/image/PROVENANCE.md#incremental-jpeg-xl)
for admission, unsupported features and framing/EOF behavior.

`nova-image::is_lossless_input` conservatively identifies PNG and WebP VP8L
for input-aware cache policies. An AVIF/JPEG XL signature alone cannot prove
that a source is lossless. `lossless_input_requires_original` identifies 16-bit
PNG, which the 8-bit processing path must retain rather than silently reducing
precision when lossless compression is requested. These helpers classify source
encoding; normal bounded image admission is still required.

## Development

### Nix

Use the same pinned Fenix stable toolchain and native headers as Linux CI:

```sh
nix develop
```

The flake copies Nova/Reader's existing `flake.lock` pins and Nova's host Rust
component list. Its default shell supplies Cargo, rustc, rustfmt, Clippy,
pkg-config, fontconfig/FreeType, CMake and stdenv's C/C++ compiler. Bundled WebP
uses that compiler; the scalar AVIF path needs no assembler or system codec.
Shells are exposed for `x86_64-linux`, `aarch64-linux` and `aarch64-darwin`.
They are development shells, rather than Nix package or release outputs.

For individual checks without entering the interactive shell, use
`nix develop --command cargo test --workspace --locked` and the corresponding
format/Clippy commands below. Linux CI runs these commands through the same
shell. Native Windows CI runs independently with stable Rust and MSVC so its
filesystem/file-lock tests exercise Windows itself.

Without Nix, Debian/Ubuntu developers need `pkg-config libfontconfig1-dev`
and a C/C++ toolchain before running the same Cargo checks. Windows developers
use stable Rust with the native MSVC build tools.

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


## JPEG XL

`nova-image` exposes `is_jpeg_xl`, `inspect_jxl`, `decode_jxl` and `encode_jxl`.
Decoding uses Reader's actual jxl-oxide 0.12.6 adapter. Encoding uses jixel 0.5.2,
with explicit `JxlEncoding::Lossless` or `JxlEncoding::Lossy { quality }` (1–100).
Both are pure Rust and single-threaded; native libjxl and SIMD/experimental encoder
features are disabled. Lossy alpha stays lossless. Input/output bounds are checked;
encoder transient working memory is additional, as with AVIF. Decoder still-page
limits and unsupported metadata/color layouts are documented in
[codec provenance](crates/image/PROVENANCE.md). `nova-cache` can decode JPEG XL
sources and store either JPEG XL mode with an independent quality preference.


`transcode_jpeg_to_jxl` uses jixel's reversible JPEG coefficient path and retains
JPEG reconstruction boxes. It checks encoded size and dimensions before
transcoding and admits only the shared still-image decoder's supported subset.
The cache's opt-in `jxl_lossless_jpeg` defaults to false and applies only to JPEG
inputs that do not require resizing. Unsupported JPEGs and other images use the
selected normal encoder/quality. The false recipe omits the new field, preserving
existing keys. Tests reconstruct the original JPEG byte for byte using jxl-oxide.

## Cross-device sync

`nova-sync` contains Nova's actual engine and tests, extracted at
`c035d61af9b57e34b997b44e53a379652bf5654a`; see
[provenance and compatibility boundaries](crates/sync/PROVENANCE.md).
Nova's default features preserve its existing wire/storage behavior, watch
registers and remote playback API. Reader uses `default-features = false`, then
calls `configure_protocol(ProtocolConfig::reader())` before identity/store access
and `configure_record_policy` before record access. Reader tickets use `NR1`
and every Reader protocol uses the `nova-reader/` namespace. Namespace selection
is immutable; unrelated app records cannot enter through Nova pairing tickets
or ALPN negotiation. App adapters own domain identity, materialization, validation
and installation consent; shared package transport never grants install consent.
