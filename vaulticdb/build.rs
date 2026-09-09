fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("CARGO_FEATURE_RADOS").is_some() {
        println!("cargo:rustc-link-lib=rados");
    }
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/vaulticdb/v1/daemon.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/vaulticdb/v1/daemon.proto");
    Ok(())
}
