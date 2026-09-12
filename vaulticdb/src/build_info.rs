//! Compiled application and security-relevant dependency identity.

/// Formats a version report without consulting runtime configuration or external services.
#[must_use]
pub fn version_report(binary_name: &str) -> String {
    let dependencies = env!("VAULTICDB_IMPORTANT_DEPENDENCIES").replace(';', "\n");
    format!(
        "{binary_name} {}\nrust {}\ntarget {}/{}\nimportant dependencies:\n{}\n",
        env!("CARGO_PKG_VERSION"),
        env!("VAULTICDB_RUSTC_VERSION"),
        std::env::consts::OS,
        std::env::consts::ARCH,
        dependencies,
    )
}

/// Prints the compiled version report.
pub fn print_version(binary_name: &str) {
    print!("{}", version_report(binary_name));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_contains_application_storage_and_crypto_versions() {
        let report = version_report("vaulticdb");
        assert!(report.starts_with(&format!("vaulticdb {}\n", env!("CARGO_PKG_VERSION"))));
        assert!(report.contains("slatedb 0.15.0 (git "));
        assert!(report.contains("object_store 0.14.1"));
        assert!(report.contains("aes-gcm 0.10.3"));
        assert!(report.contains("rustls "));
    }
}
