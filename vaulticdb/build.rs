use std::{collections::BTreeMap, fs, process::Command};

const IMPORTANT_DEPENDENCIES: &[&str] = &[
    "aes-gcm",
    "argon2",
    "aws-lc-rs",
    "cryptoki",
    "ctap-hid-fido2",
    "ed25519-dalek",
    "hkdf",
    "hmac",
    "hpke",
    "jsonwebtoken",
    "object_store",
    "p256",
    "reqwest",
    "rustls",
    "sha2",
    "sharks",
    "slatedb",
    "tonic",
    "zeroize",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    embed_dependency_versions()?;
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

fn embed_dependency_versions() -> Result<(), Box<dyn std::error::Error>> {
    let lockfile = fs::read_to_string("Cargo.lock")?.parse::<toml::Value>()?;
    let packages = lockfile
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or("Cargo.lock contains no package list")?;
    let mut versions = BTreeMap::<&str, Vec<String>>::new();
    for package in packages {
        let Some(name) = package.get("name").and_then(toml::Value::as_str) else {
            continue;
        };
        if !IMPORTANT_DEPENDENCIES.contains(&name) {
            continue;
        }
        let version = package
            .get("version")
            .and_then(toml::Value::as_str)
            .ok_or("Cargo.lock package contains no version")?;
        let mut identity = version.to_owned();
        if name == "slatedb" {
            if let Some(revision) = package
                .get("source")
                .and_then(toml::Value::as_str)
                .and_then(|source| source.rsplit_once('#').map(|(_, revision)| revision))
            {
                identity.push_str(" (git ");
                identity.push_str(revision);
                identity.push(')');
            }
        }
        versions.entry(name).or_default().push(identity);
    }
    for name in IMPORTANT_DEPENDENCIES {
        if !versions.contains_key(name) {
            return Err(format!("important dependency {name} is missing from Cargo.lock").into());
        }
    }
    let report = versions
        .into_iter()
        .map(|(name, mut versions)| {
            versions.sort();
            versions.dedup();
            format!("{name} {}", versions.join(", "))
        })
        .collect::<Vec<_>>()
        .join(";");
    let rustc = std::env::var("RUSTC")?;
    let rustc_version = String::from_utf8(Command::new(rustc).arg("--version").output()?.stdout)?;
    println!("cargo:rustc-env=VAULTICDB_IMPORTANT_DEPENDENCIES={report}");
    println!(
        "cargo:rustc-env=VAULTICDB_RUSTC_VERSION={}",
        rustc_version.trim()
    );
    println!("cargo:rerun-if-changed=Cargo.lock");
    Ok(())
}
