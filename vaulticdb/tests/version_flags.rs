use std::process::Command;

#[test]
fn version_flags_do_not_require_runtime_configuration() {
    for (name, executable) in [
        ("vaulticdb", env!("CARGO_BIN_EXE_vaulticdb")),
        (
            "vaultic-key-broker",
            env!("CARGO_BIN_EXE_vaultic-key-broker"),
        ),
        (
            "vaultic-key-custodian",
            env!("CARGO_BIN_EXE_vaultic-key-custodian"),
        ),
    ] {
        let output = Command::new(executable)
            .arg("--version")
            .env_remove("VAULTICDB_TOPOLOGY_SOURCE")
            .env_remove("VAULTIC_KEY_BROKER_CONFIG")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{name} --version failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let report = String::from_utf8(output.stdout).unwrap();
        assert!(report.starts_with(&format!("{name} {}\n", env!("CARGO_PKG_VERSION"))));
        assert!(report.contains("slatedb 0.15.0 (git "));
        assert!(report.contains("aes-gcm 0.10.3"));
    }
}
