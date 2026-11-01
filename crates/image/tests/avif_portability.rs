//! Regression for the scalar decoder's stable ARMv7 compilation profile.

#[test]
fn scalar_decoder_gates_arm_nightly_detection_behind_assembly() {
    let source = include_str!("../../../vendor/ferri-pdf-rav1d/lib.rs");
    let compact = source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    assert!(compact.contains("#![cfg_attr(all(feature=\"asm\",target_arch=\"arm\"),feature(stdarch_arm_feature_detection))]"));
    assert!(!compact.contains("#![cfg_attr(target_arch=\"arm\",feature("));
}

#[test]
fn scalar_decoder_gates_riscv32_and_riscv64_nightly_detection_behind_assembly() {
    let source = include_str!("../../../vendor/ferri-pdf-rav1d/lib.rs");
    let compact = source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect::<String>();
    assert!(compact.contains("#![cfg_attr(all(feature=\"asm\",any(target_arch=\"riscv32\",target_arch=\"riscv64\")),feature(stdarch_riscv_feature_detection))]"));
}

#[test]
fn shared_codec_uses_the_existing_scalar_runtime_detection_path() {
    // Detect accidental feature unification enabling assembly as well as the
    // source-level E0554 regression. Native-frame decoding remains tested by
    // the independent AVIF fixture and malformed-input engine regressions.
    assert!(rav1d::src::cpu::CpuFlags::run_time_detect().is_empty());
}
