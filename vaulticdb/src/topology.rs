//! Canonical sealed topology shared by recovery capsules and storage consumers.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const TOPOLOGY_FORMAT: u32 = 1;
pub const MAX_TOPOLOGY_BYTES: usize = 1 << 20;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TopologyDocument {
    pub format: u32,
    pub repository_id: String,
    pub topology_generation: u64,
    pub pack_backends: Vec<PackBackend>,
    pub placement_policy: PlacementPolicy,
    pub staging_backends: Vec<String>,
    pub metadata_replicas: MetadataReplicas,
    pub credentials: BTreeMap<String, Credential>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PackBackend {
    pub id: String,
    pub provider: Provider,
    pub endpoint: BTreeMap<String, Value>,
    pub role: BackendRole,
    pub offsite: bool,
    pub failure_domain: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Provider {
    Local,
    S3,
    Azure,
    Gcs,
    GoogleDrive,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum BackendRole {
    Primary,
    Staging,
    Archival,
    Replica,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PlacementPolicy {
    pub min_copies: u32,
    pub min_domains: u32,
    pub min_offsite: u32,
    pub offsite_deadline_seconds: u64,
    pub promotion_crossover_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReplicas {
    pub mode: ReplicaMode,
    pub order: Vec<String>,
    pub fencing: String,
    pub replicas: BTreeMap<String, MetadataReplica>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ReplicaMode {
    Local,
    Replicated,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct MetadataReplica {
    pub provider: Provider,
    pub endpoint: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    #[serde(default)]
    pub read_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Zeroize, ZeroizeOnDrop)]
#[serde(deny_unknown_fields)]
pub struct Credential {
    pub kind: CredentialKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_key_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_access_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub scopes: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub issued_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation_due: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub may_issue: Option<MayIssue>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Zeroize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialKind {
    AwsStatic,
    S3Static,
    AzureSharedKey,
    AzureSas,
    GcpServiceAccountJson,
    Oauth2RefreshToken,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Zeroize)]
#[serde(deny_unknown_fields)]
pub struct MayIssue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TopologyMutation {
    SetBackend {
        backend: PackBackend,
    },
    SetBackendCredential {
        backend: PackBackend,
        reference: String,
        credential: Credential,
    },
    SetReplica {
        id: String,
        replica: MetadataReplica,
    },
    SetCredential {
        reference: String,
        credential: Credential,
    },
    RotateCredential {
        reference: String,
        credential: Credential,
    },
    RemoveCredential {
        reference: String,
    },
}

impl TopologyDocument {
    pub fn decode(encoded: &[u8]) -> Result<Self> {
        if encoded.len() > MAX_TOPOLOGY_BYTES {
            bail!("sealed topology exceeds {MAX_TOPOLOGY_BYTES} bytes");
        }
        let mut deserializer = serde_json::Deserializer::from_slice(encoded);
        let topology = Self::deserialize(&mut deserializer).context("decode sealed topology")?;
        deserializer.end().context("decode sealed topology")?;
        topology.validate()?;
        if topology.canonical_json()?.as_slice() != encoded {
            bail!("sealed topology is not canonical JSON");
        }
        Ok(topology)
    }

    pub fn canonical_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let encoded = serde_json::to_vec(self).context("encode sealed topology")?;
        if encoded.len() > MAX_TOPOLOGY_BYTES {
            bail!("sealed topology exceeds {MAX_TOPOLOGY_BYTES} bytes");
        }
        Ok(encoded)
    }

    pub fn decode_redacted(encoded: &[u8]) -> Result<Self> {
        if encoded.len() > MAX_TOPOLOGY_BYTES {
            bail!("redacted topology exceeds {MAX_TOPOLOGY_BYTES} bytes");
        }
        let mut deserializer = serde_json::Deserializer::from_slice(encoded);
        let topology = Self::deserialize(&mut deserializer).context("decode redacted topology")?;
        deserializer.end().context("decode redacted topology")?;
        topology.validate_shape(true)?;
        if serde_json::to_vec(&topology)?.as_slice() != encoded {
            bail!("redacted topology is not canonical JSON");
        }
        Ok(topology)
    }

    pub fn sha256(&self) -> Result<String> {
        Ok(format!("{:x}", Sha256::digest(self.canonical_json()?)))
    }

    pub fn redacted(&self) -> Self {
        let mut redacted = self.clone();
        redacted.credentials.clear();
        redacted
    }

    pub fn redacted_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(&self.redacted()).context("encode redacted topology")
    }

    pub fn credential_json(&self, reference: &str) -> Result<Vec<u8>> {
        let credential = self
            .credentials
            .get(reference)
            .with_context(|| format!("topology has no credential {reference:?}"))?;
        serde_json::to_vec(credential).context("encode credential lease")
    }

    pub fn validate(&self) -> Result<()> {
        self.validate_shape(false)
    }

    fn validate_shape(&self, redacted: bool) -> Result<()> {
        if self.format != TOPOLOGY_FORMAT
            || self.repository_id.is_empty()
            || self.topology_generation == 0
            || self.pack_backends.is_empty()
        {
            bail!("invalid sealed topology identity or version");
        }
        let mut backend_ids = BTreeSet::new();
        let mut domains = BTreeSet::new();
        let mut offsite = 0_u32;
        let mut references = BTreeSet::new();
        for (index, backend) in self.pack_backends.iter().enumerate() {
            if backend.id.is_empty()
                || backend.failure_domain.is_empty()
                || !backend_ids.insert(backend.id.clone())
                || index > 0 && self.pack_backends[index - 1].id >= backend.id
            {
                bail!("pack backends require unique IDs in canonical order");
            }
            validate_endpoint(&backend.provider, &backend.endpoint)?;
            validate_reference(
                &backend.provider,
                backend.credential_ref.as_deref(),
                &mut references,
            )?;
            domains.insert(backend.failure_domain.clone());
            if backend.offsite {
                offsite += 1;
            }
        }
        let policy = &self.placement_policy;
        if policy.min_copies == 0
            || policy.min_domains == 0
            || self.pack_backends.len() < policy.min_copies as usize
            || domains.len() < policy.min_domains as usize
            || offsite < policy.min_offsite
        {
            bail!("pack backends cannot satisfy placement policy");
        }
        let mut staging = BTreeSet::new();
        for (index, id) in self.staging_backends.iter().enumerate() {
            if !backend_ids.contains(id)
                || !staging.insert(id)
                || index > 0 && self.staging_backends[index - 1] >= *id
            {
                bail!("staging backends must be known, unique, and canonically ordered");
            }
        }
        self.metadata_replicas.validate(&mut references)?;
        for (reference, credential) in &self.credentials {
            if !valid_reference(reference) || !references.contains(reference) {
                bail!("credential {reference:?} is invalid or unused");
            }
            credential.validate()?;
        }
        if !redacted
            && (references.len() != self.credentials.len()
                || references
                    .iter()
                    .any(|reference| !self.credentials.contains_key(reference)))
        {
            bail!("topology contains dangling credential references");
        }
        for backend in &self.pack_backends {
            if let Some(reference) = &backend.credential_ref {
                if let Some(credential) = self.credentials.get(reference) {
                    validate_credential_provider(&credential.kind, &backend.provider, reference)?;
                }
            }
        }
        for replica in self.metadata_replicas.replicas.values() {
            if let Some(reference) = &replica.credential_ref {
                if let Some(credential) = self.credentials.get(reference) {
                    validate_credential_provider(&credential.kind, &replica.provider, reference)?;
                }
            }
        }
        if redacted && !self.credentials.is_empty() {
            bail!("redacted topology contains credentials");
        }
        Ok(())
    }

    pub fn apply_mutation(&mut self, mutation: TopologyMutation, generation: u64) -> Result<()> {
        match mutation {
            TopologyMutation::SetBackend { backend } => {
                if let Some(existing) = self
                    .pack_backends
                    .iter_mut()
                    .find(|existing| existing.id == backend.id)
                {
                    *existing = backend;
                } else {
                    self.pack_backends.push(backend);
                }
                self.pack_backends
                    .sort_by(|left, right| left.id.cmp(&right.id));
            }
            TopologyMutation::SetBackendCredential {
                backend,
                reference,
                credential,
            } => {
                if backend.credential_ref.as_deref() != Some(reference.as_str()) {
                    bail!("backend credential reference does not match enrolled credential");
                }
                if self.credentials.contains_key(&reference) {
                    bail!("credential already exists");
                }
                self.credentials.insert(reference, credential);
                if let Some(existing) = self
                    .pack_backends
                    .iter_mut()
                    .find(|existing| existing.id == backend.id)
                {
                    *existing = backend;
                } else {
                    self.pack_backends.push(backend);
                }
                self.pack_backends
                    .sort_by(|left, right| left.id.cmp(&right.id));
            }
            TopologyMutation::SetReplica { id, replica } => {
                if id.is_empty() {
                    bail!("metadata replica ID must not be empty");
                }
                self.metadata_replicas.replicas.insert(id.clone(), replica);
                if !self.metadata_replicas.order.contains(&id) {
                    self.metadata_replicas.order.push(id);
                    self.metadata_replicas.order.sort();
                }
            }
            TopologyMutation::SetCredential {
                reference,
                credential,
            } => {
                if self.credentials.contains_key(&reference) {
                    bail!("credential already exists; use rotate-credential");
                }
                self.credentials.insert(reference, credential);
            }
            TopologyMutation::RotateCredential {
                reference,
                credential,
            } => {
                if !self.credentials.contains_key(&reference) {
                    bail!("credential does not exist; use set-credential");
                }
                self.credentials.insert(reference, credential);
            }
            TopologyMutation::RemoveCredential { reference } => {
                self.credentials
                    .remove(&reference)
                    .context("credential does not exist")?;
            }
        }
        self.topology_generation = generation;
        self.validate()
    }
}

impl MetadataReplicas {
    fn validate(&self, references: &mut BTreeSet<String>) -> Result<()> {
        if self.order.is_empty() || self.replicas.is_empty() || self.fencing.is_empty() {
            bail!("metadata replica topology is incomplete");
        }
        let ordered = self.order.iter().cloned().collect::<BTreeSet<_>>();
        if ordered.len() != self.order.len()
            || ordered.len() != self.replicas.len()
            || !ordered.contains(&self.fencing)
            || self.replicas.keys().any(|id| !ordered.contains(id))
        {
            bail!("metadata replica order or fencing reference is invalid");
        }
        if self.mode == ReplicaMode::Local && self.replicas.len() != 1 {
            bail!("local metadata mode requires exactly one replica");
        }
        for replica in self.replicas.values() {
            validate_endpoint(&replica.provider, &replica.endpoint)?;
            validate_reference(
                &replica.provider,
                replica.credential_ref.as_deref(),
                references,
            )?;
        }
        Ok(())
    }
}

impl Credential {
    pub fn validate(&self) -> Result<()> {
        let present =
            |value: &Option<String>| value.as_ref().is_some_and(|value| !value.is_empty());
        let valid = match self.kind {
            CredentialKind::AwsStatic | CredentialKind::S3Static => {
                present(&self.access_key_id) && present(&self.secret_access_key)
            }
            CredentialKind::AzureSharedKey => {
                present(&self.account_name) && present(&self.account_key)
            }
            CredentialKind::AzureSas => present(&self.sas_token),
            CredentialKind::GcpServiceAccountJson => self
                .service_account_json
                .as_ref()
                .is_some_and(|value| serde_json::from_str::<Value>(value).is_ok()),
            CredentialKind::Oauth2RefreshToken => {
                present(&self.client_id)
                    && present(&self.client_secret)
                    && present(&self.refresh_token)
                    && present(&self.token_uri)
                    && !self.scopes.is_empty()
                    && self.scopes.iter().all(|scope| !scope.is_empty())
            }
            CredentialKind::None => !self.has_secret(),
        };
        if !valid {
            bail!("credential fields do not match kind");
        }
        for value in [self.issued_at.as_deref(), self.rotation_due.as_deref()]
            .into_iter()
            .flatten()
        {
            OffsetDateTime::parse(value, &Rfc3339)
                .context("credential timestamp is not RFC3339")?;
        }
        Ok(())
    }

    pub fn rotation_overdue(&self, now: OffsetDateTime) -> bool {
        self.rotation_due
            .as_deref()
            .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
            .is_some_and(|due| due < now)
    }

    pub fn has_secret(&self) -> bool {
        self.secret_access_key.is_some()
            || self.account_key.is_some()
            || self.sas_token.is_some()
            || self.service_account_json.is_some()
            || self.client_secret.is_some()
            || self.refresh_token.is_some()
    }
}

fn validate_reference(
    provider: &Provider,
    reference: Option<&str>,
    references: &mut BTreeSet<String>,
) -> Result<()> {
    if *provider == Provider::Local {
        if reference.is_some() {
            bail!("local endpoints must not reference credentials");
        }
        return Ok(());
    }
    let reference = reference.context("remote endpoint requires a credential reference")?;
    if !valid_reference(reference) {
        bail!("invalid credential reference");
    }
    references.insert(reference.to_owned());
    Ok(())
}

fn validate_credential_provider(
    kind: &CredentialKind,
    provider: &Provider,
    reference: &str,
) -> Result<()> {
    let supported = match provider {
        Provider::S3 => matches!(
            kind,
            CredentialKind::AwsStatic | CredentialKind::S3Static | CredentialKind::None
        ),
        Provider::Azure => matches!(
            kind,
            CredentialKind::AzureSharedKey | CredentialKind::AzureSas | CredentialKind::None
        ),
        Provider::Gcs => matches!(
            kind,
            CredentialKind::GcpServiceAccountJson | CredentialKind::None
        ),
        Provider::GoogleDrive => matches!(
            kind,
            CredentialKind::Oauth2RefreshToken | CredentialKind::GcpServiceAccountJson
        ),
        Provider::Local => false,
    };
    if !supported {
        bail!("credential {reference:?} cannot authenticate provider {provider:?}");
    }
    Ok(())
}

fn valid_reference(reference: &str) -> bool {
    reference.strip_prefix("cred:").is_some_and(|name| {
        !name.is_empty()
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    })
}

fn validate_endpoint(provider: &Provider, endpoint: &BTreeMap<String, Value>) -> Result<()> {
    let required: &[&str] = match provider {
        Provider::Local => &["data_dir"],
        Provider::S3 => &["url", "bucket", "region"],
        Provider::Azure => &["url", "container"],
        Provider::Gcs => &["bucket"],
        Provider::GoogleDrive => &["drive_id", "root_folder_id", "path"],
    };
    for field in required {
        if !endpoint
            .get(*field)
            .and_then(Value::as_str)
            .is_some_and(|value| !value.is_empty())
        {
            bail!("{provider:?} endpoint requires non-empty {field}");
        }
    }
    let allowed: &[&str] = match provider {
        Provider::Local => &["data_dir"],
        Provider::S3 => &[
            "url",
            "bucket",
            "prefix",
            "region",
            "storage_class",
            "tls_sha256",
        ],
        Provider::Azure => &["url", "container", "prefix", "tls_sha256"],
        Provider::Gcs => &["bucket", "prefix"],
        Provider::GoogleDrive => &["drive_id", "root_folder_id", "path"],
    };
    for (field, value) in endpoint {
        if !allowed.contains(&field.as_str()) {
            bail!("{provider:?} endpoint contains unsupported field {field:?}");
        }
        if !value.is_string() && !(field == "tls_sha256" && value.is_null()) {
            bail!("{provider:?} endpoint field {field:?} must be a string");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> TopologyDocument {
        serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v1.json"
        )))
        .unwrap()
    }

    #[test]
    fn shared_fixture_is_canonical_and_valid() {
        let encoded = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v1.json"
        ));
        let topology = TopologyDocument::decode(encoded).unwrap();
        assert_eq!(topology.canonical_json().unwrap(), encoded);
        assert_eq!(topology.sha256().unwrap().len(), 64);
    }

    #[test]
    fn references_and_policy_fail_closed() {
        let mut topology = topology();
        topology.credentials.insert(
            "cred:unused".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        assert!(topology.validate().is_err());
        topology.credentials.remove("cred:unused");
        topology.pack_backends[0].credential_ref = Some("cred:missing".to_owned());
        assert!(topology.validate().is_err());
        topology.pack_backends[0].credential_ref = Some("cred:drive".to_owned());
        topology.placement_policy.min_domains = 3;
        assert!(topology.validate().is_err());
    }

    #[test]
    fn redaction_removes_every_credential_value() {
        let topology = topology();
        let encoded = String::from_utf8(topology.redacted_json().unwrap()).unwrap();
        assert!(encoded.contains("cred:drive"));
        assert!(!encoded.contains("refresh-secret"));
    }

    #[test]
    fn credential_kind_must_match_provider() {
        let mut topology = topology();
        topology.credentials.get_mut("cred:drive").unwrap().kind = CredentialKind::S3Static;
        assert!(topology.validate().is_err());
    }

    #[test]
    fn endpoint_schema_rejects_embedded_credential() {
        let mut topology = topology();
        topology.pack_backends[0].endpoint.insert(
            "refresh_token".to_owned(),
            Value::String("must-not-appear-here".to_owned()),
        );
        assert!(topology.validate().is_err());
    }
}
