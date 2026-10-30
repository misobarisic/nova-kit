# Nova Kit

Shared Rust crates for the Nova applications. This workspace currently contains
only `nova-storage`, extracted from `misobarisic/nova` without changing its Rust
implementation or storage formats. The license remains GPL-3.0-or-later.
The initial source comes from Nova commit
`7a0cb47d0977fdb64601af4f37a6e7d53b6f5e45`.

## Storage

`nova-storage` provides redb-backed JSON key/value storage, atomic batches,
registered collections, compressed metadata and durable local sequences.
It has no dependency on Nova's UI, configuration or application models.

Each application supplies its own data directory to `init_at`; the database
filename remains `nova.redb`. The current API owns one process-wide database and
must be initialized once, before use. Sharing this crate does not require apps
to share a database or their data directories.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --locked --all-targets -- -D warnings
cargo test --workspace --locked
```

Unit tests cover legacy migration, collection identity/order, corruption,
atomic writes, compression and concurrent sequence allocation. The public-API
integration test reopens data in a second process, including collections and
compressed metadata, and verifies sequence continuity.

During the local extraction draft, Nova expects this checkout beside its own:
`../nova-kit/crates/storage`. After publishing, consumers can select the package
with a pinned Git dependency (replace the placeholder with a real commit):

```toml
[dependencies]
nova-storage = { git = "https://github.com/misobarisic/nova-kit", rev = "<commit-sha>" }
```

Cargo discovers the package inside the Git workspace; no `path` is needed.

## Local development with a Git dependency

Keep the pinned Git dependency and its lockfile as the committed default. CI/CD
uses ordinary Cargo commands and fetches that revision from GitHub.

Nova includes `.cargo/local-kit.example.toml`: copy it to the ignored
`.cargo/local-kit.toml`, then opt into the sibling checkout with:

```sh
cargo dev          # build and run Nova with the local kit override
cargo dev-release  # same override, optimized dev-release profile
cargo --config .cargo/local-kit.toml build
cargo --config .cargo/local-kit.toml test -p nova-sync --lib
```

That file supplies a local `[patch."https://github.com/misobarisic/nova-kit"]`
override. For plain `cargo build` to apply the override automatically, put the
same patch in your personal `$CARGO_HOME/config.toml` (normally
`~/.cargo/config.toml`), using an absolute path to the storage crate. CI has
neither local override, so it continues using GitHub.

Local patches can change the consuming app's `Cargo.lock`; keep that local lock
change out of commits and restore the Git-based lockfile before running the
normal CI `--locked` commands. The first Git resolution may still need network
access; a local patch does not guarantee an uncached offline build. This setup
becomes usable once the initial shared crate revision has been published.
