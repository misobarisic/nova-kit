# Nova image cache extraction

The implementation was relocated from Nova commit
[`b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda`](https://github.com/misobarisic/nova/tree/b69a1e5765ecc65ac3b00acdfe2f16e2f46a5dda),
under GPL-3.0-or-later, the same license as Nova Kit.

| Original file | Shared file |
| --- | --- |
| `crates/media/src/cache.rs` | `src/lib.rs` |
| `crates/media/src/cache_gc.rs` | `src/cache_gc.rs` |
| `crates/media/src/cache_tests.rs` | `src/cache_tests.rs` |
| image settings/runtime helpers in `crates/config/src/lib.rs` | `src/config.rs` |
| `DISK_CACHE_LIMITS` in `src/app/settings.rs` | `src/config.rs` |

Original SHA-256 values (read using `git show` at that immutable revision):

```text
4dd87a2ba0db469ab948fd52143493d5375ef1ca026a1e79c9fe631f0bfb01c8  cache.rs
0c9014567f192733b23e16e16ff64e21855629b8e822c7281287fbac60ee3ca5  cache_gc.rs
16b641a79b95fb8e5480245f1f7e871bb3258dbe06388afed48795b9809608b1  cache_tests.rs
```

This is the actual recipe-keyed LRU, processed-variant encoding, NVIMAGE2
envelope, atomic file publication, source/publication sequencing, single-flight,
generation checks, maintenance queue, usage index and incremental GC code.
The original behavior tests moved with it. The application/profile/video
configuration was excluded; applications install their directory and fetch
callback and map their image settings into the shared subset. Recipe key strings,
FNV filename hashes, metadata and encoded image contents remain compatible with
existing Nova files. Nova's adapter regression reads a static fixture produced
by compiling the original, unmodified cache source at the pinned revision.

Extensions for Reader expose the same decoded LRU as an independently owned,
hard-budgeted object with protected-key priority and eviction reporting.
`blob::Cache` uses the existing variant and GC machinery for bounded encoded
originals without decoding or resizing them. Applications supply their bounded
codec/admission function; AVIF and tall-page decoding remain app responsibilities.
Asynchronous transforms can publish conditionally without rewriting existing
entries. Blob generations belong to their directory; maintenance clears cannot
cancel loads in another blob namespace.

The GC backend can supply per-directory policy, hard protection and eviction
groups. A chapter group is ordered by its newest member use and is evicted in
full; a pinned member or synthetic group marker protects the entire group.
Separate chapter/artwork directories and budgets keep chapter pressure away
from posters. The original batching, restart index reconciliation, active
publication checks, revision checks, time slicing and hysteresis remain shared.

Run `cargo test -p nova-cache --locked` and
`cargo clippy -p nova-cache --all-targets --locked -- -D warnings`.
