fn main() {
    let root = std::env::var("CARGO_MANIFEST_DIR").expect("Cargo sets the manifest directory");
    println!("cargo::metadata=slint_source_dir={root}/slint");
}
