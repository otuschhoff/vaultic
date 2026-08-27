fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/vaulticdb/v1/daemon.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/vaulticdb/v1/daemon.proto");
    Ok(())
}
