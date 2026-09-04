use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::Serialize;

use super::{unix_time_ms, ContributionRejection};

#[derive(Serialize)]
struct BrokerSecurityEvent<'a> {
    timestamp_unix_ms: u64,
    severity: &'a str,
    category: &'a str,
    component: &'static str,
    event: &'a str,
    fields: BTreeMap<&'a str, String>,
}

pub fn emit_security_event(severity: &str, category: &str, event: &str, fields: &[(&str, String)]) {
    match security_event_json(severity, category, event, fields) {
        Ok(encoded) => eprintln!("{encoded}"),
        Err(error) => eprintln!("vaultic-key-broker: security event rejected: {error}"),
    }
}

pub fn emit_rejection_event(error: &anyhow::Error, connection_id: &str) {
    let (severity, category, event, fields) = rejection_event(error, connection_id);
    emit_security_event(severity, category, event, &fields);
}

pub fn rejection_event(
    error: &anyhow::Error,
    connection_id: &str,
) -> (
    &'static str,
    &'static str,
    &'static str,
    Vec<(&'static str, String)>,
) {
    match error.downcast_ref::<ContributionRejection>() {
        Some(ContributionRejection::PayloadAuthentication) => (
            "warning",
            "integrity",
            "contribution_payload_authentication_failed",
            vec![("connection_id", connection_id.to_owned())],
        ),
        Some(ContributionRejection::PayloadInvalid) => (
            "warning",
            "integrity",
            "contribution_payload_invalid",
            vec![("connection_id", connection_id.to_owned())],
        ),
        Some(ContributionRejection::Rollback {
            last_seen_generation,
            current_generation,
        }) => (
            "critical",
            "integrity",
            "capsule_rollback_rejected",
            vec![
                ("connection_id", connection_id.to_owned()),
                ("last_seen_generation", last_seen_generation.to_string()),
                ("current_generation", current_generation.to_string()),
            ],
        ),
        None => (
            "warning",
            "auth",
            "request_rejected",
            vec![("connection_id", connection_id.to_owned())],
        ),
    }
}

pub fn security_event_json(
    severity: &str,
    category: &str,
    event: &str,
    fields: &[(&str, String)],
) -> Result<String> {
    const ALLOWED_FIELDS: &[&str] = &[
        "repository_id",
        "capsule_generation",
        "capsule_logical_id",
        "session_id",
        "member_id",
        "unlocked",
        "connection_id",
        "component",
        "version",
        "release_identity",
        "capability",
        "lease_id",
        "expires_unix_ms",
        "identity_recovery",
        "capsule_sha256",
        "expired_sessions",
        "expired_leases",
        "session_count",
        "lease_count",
        "last_seen_generation",
        "current_generation",
    ];
    let mut encoded_fields = BTreeMap::new();
    for (name, value) in fields {
        if !ALLOWED_FIELDS.contains(name) {
            bail!("security event field {name:?} is not allowlisted");
        }
        encoded_fields.insert(*name, value.clone());
    }
    Ok(serde_json::to_string(&BrokerSecurityEvent {
        timestamp_unix_ms: unix_time_ms()?,
        severity,
        category,
        component: "vaultic-key-broker",
        event,
        fields: encoded_fields,
    })?)
}
