# Local changes to rav1d 1.1.0

Upstream: https://github.com/memorysafety/rav1d, revision
`782dab2135ea64a057c097088a13eb8ed3cc3320` (BSD-2-Clause; COPYING retained).
Source: the published crates.io archive, SHA-256
`1932f060d5e7bd49dc9f8b272c1dc5e9ce0ffe141c28be900265d3989b36c9ed`.
Modified files: src/decode.rs, src/cdf.rs, src/disjoint_mut.rs, src/internal.rs,
src/wrap_fn_ptr.rs, include/dav1d/picture.rs.

- `rav1d_submit_frame`'s `on_error` no longer unwraps the frame header. When
  `rav1d_decode_frame` fails with a single frame context, `rav1d_decode_frame_exit`
  has already cleared the header (and released the output CDF), so the unwrap
  panicked inside `extern "C" fn dav1d_send_data` and aborted the process on
  corrupt AV1 tile data. The decode error is now returned. Upstream made the same
  change in commit `6c6f463785a546dea18fd9ca0f14634c2ee0557c`, not yet released.
- Default features no longer enable assembly. The published 1.1.0 archive omits
  the aarch64 assembly headers (upstream issue #1449), so its default build fails
  on aarch64, and x86 assembly needs NASM. The renderer disables assembly anyway;
  the `asm*` features remain available.
- Compiler warnings are fixed as upstream `main` fixes them: the `wrap_fn_ptr!`
  newtype no longer derives `PartialEq`/`Eq` (function pointer comparison),
  `cdf_write`, `with_offset` and `lf_offsets` name their elided `'_` lifetimes,
  `Bounds::overlaps` compiles only with debug assertions or tests, and the unused
  `Rav1dFrameContextTaskThreadPendingTasks` struct is removed.
- The package excludes upstream's CI configuration, coverage configuration,
  retranspile script and nightly `rust-toolchain.toml`; none affect the build.

The crate keeps upstream's own `unsafe` code and lints; the renderer's
`unsafe_code = "forbid"` applies only to its own crate.

## Nova Reader: stable scalar ARMv7/RISC-V builds

This directory vendors the complete published `ferri-pdf-rav1d` 1.1.2 crate
from https://crates.io/crates/ferri-pdf-rav1d/1.1.2. Archive SHA-256:
`bb4072634f94938773523af4005a080ff3e2b3a0328eaa9bd222eb52d85e2cd3`.
The published fork identifies commit
`a3f6972aa5663772da254ff5abfc802e6b2e13cb` in
`https://github.com/apollohg/ferri-pdf`, path `vendor/rav1d`.
Its upstream rav1d provenance and earlier malformed-frame repair are retained
above and in `UPSTREAM_VCS.json`, `UPSTREAM_CARGO.toml`, and `COPYING`.
Only generated registry cache markers (`.cargo-ok`/`.cargo-checksum.json`) are
omitted; source files and license material are retained.

Nova Reader changes only two predicates in `lib.rs`:

```diff
-#![cfg_attr(target_arch = "arm", feature(stdarch_arm_feature_detection))]
+#![cfg_attr(all(feature = "asm", target_arch = "arm"), feature(stdarch_arm_feature_detection))]
 #![cfg_attr(
-    any(target_arch = "riscv32", target_arch = "riscv64"),
+    all(feature = "asm", any(target_arch = "riscv32", target_arch = "riscv64")),
     feature(stdarch_riscv_feature_detection)
 )]
```

The ARM runtime macros in `src/cpu.rs` already compile only inside
`#[cfg(feature = "asm")]`; scalar builds use the existing
`#[cfg(not(feature = "asm"))]` implementation returning empty runtime flags.
The RISC-V path has the same requirement. Reader enables only `bitdepth_8` and
disables default features and assembly. The old unconditional ARM attribute
made its otherwise portable scalar decoder fail on stable Rust with E0554.
There are no decoder, pixel, CPU-dispatch, malformed-input cleanup, or codec
admission algorithm changes here. ARM/RISC-V assembly builds still retain the
original feature requirements.

Verification (using stable Rust; no `RUSTC_BOOTSTRAP`):

```sh
rustup target add armv7-linux-androideabi
cargo check --locked -p ferri-pdf-rav1d -p ravif --target armv7-linux-androideabi
cargo test --locked -p reader-engine --test avif_portability
cargo test --locked -p reader-engine --lib
cargo test --locked -p reader-engine avif::tests::malformed_fixture_does_not_abort_decoder_process -- --ignored --exact
```

The codec-only ARMv7 check needs the Rust target standard library, without an
Android NDK. Checking the full engine/application additionally traverses native
storage/network dependencies and uses the normal Android NDK environment.
