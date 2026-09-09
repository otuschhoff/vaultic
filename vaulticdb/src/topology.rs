//! Canonical sealed topology shared by recovery capsules and storage consumers.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use time::{format_description::well_known::Rfc3339, Date, OffsetDateTime};
use zeroize::{Zeroize, ZeroizeOnDrop};

pub const TOPOLOGY_FORMAT: u32 = 2;
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
    pub credential_policy: Option<CredentialPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CredentialBindings {
    #[serde(
        default,
        rename = "storage-read",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_read: Option<String>,
    #[serde(
        default,
        rename = "storage-append",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_append: Option<String>,
    #[serde(
        default,
        rename = "storage-maintain",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_maintain: Option<String>,
    #[serde(
        default,
        rename = "storage-lock",
        skip_serializing_if = "Option::is_none"
    )]
    pub storage_lock: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CredentialPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sts: Option<S3StsPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub azure_user_delegation: Option<AzureUserDelegationPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcp_downscope: Option<GcpDownscopePolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#static: Option<StaticCredentialPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct S3StsPolicy {
    pub issuer_ref: String,
    pub endpoint: String,
    pub region: String,
    pub session_name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_id: Option<String>,
    pub roles: CredentialBindings,
    pub fallback: StsFallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AzureUserDelegationPolicy {
    pub issuer_ref: String,
    pub service_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_ip: Option<String>,
    pub hierarchical_namespace: bool,
    pub tiers: Vec<StorageCredentialTier>,
    pub fallback: StsFallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GcpDownscopePolicy {
    pub issuer_ref: String,
    pub service_account: String,
    pub iam_credentials_endpoint: String,
    pub token_exchange_endpoint: String,
    pub tiers: Vec<StorageCredentialTier>,
    pub fallback: StsFallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StaticCredentialPolicy {
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub revoked_generations: Vec<u64>,
    pub bindings: CredentialBindings,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum StsFallback {
    Disabled,
    StaticOnUnavailable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "kebab-case")]
pub enum StorageCredentialTier {
    Read,
    Append,
    Maintain,
    Lock,
}

impl StorageCredentialTier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "storage-read",
            Self::Append => "storage-append",
            Self::Maintain => "storage-maintain",
            Self::Lock => "storage-lock",
        }
    }
}

impl CredentialBindings {
    pub fn reference(&self, tier: StorageCredentialTier) -> Option<&str> {
        match tier {
            StorageCredentialTier::Read => self.storage_read.as_deref(),
            StorageCredentialTier::Append => self.storage_append.as_deref(),
            StorageCredentialTier::Maintain => self.storage_maintain.as_deref(),
            StorageCredentialTier::Lock => self.storage_lock.as_deref(),
        }
    }

    fn references(&self) -> [Option<&str>; 4] {
        [
            self.storage_read.as_deref(),
            self.storage_append.as_deref(),
            self.storage_maintain.as_deref(),
            self.storage_lock.as_deref(),
        ]
    }
}

pub fn select_static_credential_reference(
    policy: Option<&CredentialPolicy>,
    tier: StorageCredentialTier,
) -> Result<&str> {
    policy
        .and_then(|configured| configured.r#static.as_ref())
        .and_then(|configured| configured.bindings.reference(tier))
        .with_context(|| format!("credential binding {:?} is not configured", tier.as_str()))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum Provider {
    Local,
    S3,
    Rados,
    Azure,
    Gcs,
    GoogleDrive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Provider {
    Generic,
    Backblaze,
    Ceph,
    Wasabi,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Profile {
    pub provider: S3Provider,
    pub endpoint: String,
    pub region: String,
    pub bucket_lookup: String,
}

pub fn normalize_s3_endpoint(endpoint: &BTreeMap<String, Value>) -> Result<S3Profile> {
    let value = |name: &str| {
        endpoint
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_default()
    };
    let raw_url = value("url");
    let parsed = reqwest::Url::parse(raw_url).context("S3 endpoint has invalid URL")?;
    if !matches!(parsed.scheme(), "https" | "http")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
        || !matches!(parsed.path(), "" | "/")
    {
        bail!("S3 endpoint has invalid URL");
    }
    let host = parsed
        .host_str()
        .context("S3 endpoint URL requires a host")?
        .trim_end_matches('.')
        .to_ascii_lowercase();
    let (inferred, inferred_region) = infer_s3_provider(&host);
    let explicit = match value("provider") {
        "" => None,
        "generic" => Some(S3Provider::Generic),
        "backblaze" => Some(S3Provider::Backblaze),
        "ceph" => Some(S3Provider::Ceph),
        "wasabi" => Some(S3Provider::Wasabi),
        other => bail!("bad S3 provider {other:?}: must be generic, backblaze, ceph, or wasabi"),
    };
    let provider = explicit.unwrap_or(inferred);
    if explicit.is_some()
        && provider != S3Provider::Generic
        && inferred != S3Provider::Generic
        && inferred != provider
    {
        bail!("S3 provider does not match endpoint host {host:?}");
    }
    if matches!(provider, S3Provider::Backblaze | S3Provider::Wasabi)
        && inferred == S3Provider::Generic
        && provider_domain_lookalike(&host)
    {
        bail!("S3 provider endpoint host {host:?} is not a valid provider endpoint");
    }
    let mut region = value("region").to_owned();
    if inferred == provider && !inferred_region.is_empty() {
        if !region.is_empty() && region != inferred_region {
            bail!(
                "S3 endpoint region {inferred_region:?} does not match configured region {region:?}"
            );
        }
        region = inferred_region;
    }
    if provider != S3Provider::Generic {
        if parsed.scheme() != "https" && inferred == provider {
            bail!("S3 provider production endpoint requires HTTPS");
        }
        if inferred == S3Provider::Generic && region.is_empty() {
            bail!("S3 provider custom endpoint requires an explicit region");
        }
        let storage_class = value("storage_class");
        if !storage_class.is_empty() && !storage_class.eq_ignore_ascii_case("STANDARD") {
            bail!("S3 provider does not support storage class {storage_class:?}");
        }
    }
    let bucket_lookup = match value("bucket_lookup") {
        "" | "auto" if provider == S3Provider::Generic => "auto",
        "" | "auto" => "dns",
        "dns" => "dns",
        "path" => "path",
        other => bail!("bad S3 bucket lookup {other:?}: must be auto, dns, or path"),
    };
    if bucket_lookup == "dns" && !dns_compatible_bucket(value("bucket")) {
        bail!("S3 DNS bucket lookup requires a DNS-compatible bucket name");
    }
    Ok(S3Profile {
        provider,
        endpoint: raw_url.trim_end_matches('/').to_owned(),
        region,
        bucket_lookup: bucket_lookup.to_owned(),
    })
}

fn infer_s3_provider(host: &str) -> (S3Provider, String) {
    for (suffix, provider) in [
        (".backblazeb2.com", S3Provider::Backblaze),
        (".wasabisys.com", S3Provider::Wasabi),
    ] {
        if let Some(region) = host
            .strip_prefix("s3.")
            .and_then(|value| value.strip_suffix(suffix))
            .filter(|value| !value.is_empty())
        {
            return (provider, region.to_owned());
        }
    }
    if host == "s3.wasabisys.com" {
        return (S3Provider::Wasabi, String::new());
    }
    (S3Provider::Generic, String::new())
}

fn provider_domain_lookalike(host: &str) -> bool {
    host.ends_with(".backblazeb2.com") || host.ends_with(".wasabisys.com")
}

fn dns_compatible_bucket(bucket: &str) -> bool {
    (3..=63).contains(&bucket.len())
        && !bucket.contains("..")
        && !bucket.starts_with(['-', '.'])
        && !bucket.ends_with(['-', '.'])
        && bucket.parse::<std::net::IpAddr>().is_err()
        && bucket.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
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
    pub credential_policy: Option<CredentialPolicy>,
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
    pub session_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sas_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_account_json: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_token: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Zeroize)]
#[serde(rename_all = "kebab-case")]
pub enum CredentialKind {
    AwsStatic,
    S3Static,
    S3Session,
    CephxStatic,
    AzureSharedKey,
    AzureSas,
    AzureEntraClientSecret,
    GcpServiceAccountJson,
    GcpAccessToken,
    Oauth2RefreshToken,
    None,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TopologyMutation {
    SetBackend {
        backend: PackBackend,
    },
    SetBackendPolicy {
        backend: PackBackend,
        credentials: BTreeMap<String, Credential>,
    },
    SetReplica {
        id: String,
        replica: MetadataReplica,
    },
    SetReplicaPolicy {
        id: String,
        replica: MetadataReplica,
        credentials: BTreeMap<String, Credential>,
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
            validate_credential_policy(
                &backend.provider,
                &backend.endpoint,
                backend.credential_policy.as_ref(),
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
            if matches!(
                credential.kind,
                CredentialKind::S3Session | CredentialKind::GcpAccessToken
            ) {
                bail!("credential {reference:?}: temporary credentials must not be stored in topology");
            }
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
            if let Some(sts) = backend
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.sts.as_ref())
            {
                if let Some(credential) = self.credentials.get(&sts.issuer_ref) {
                    validate_sts_issuer_kind(&credential.kind, &sts.issuer_ref)?;
                }
            }
            if let Some(azure) = backend
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.azure_user_delegation.as_ref())
            {
                if let Some(credential) = self.credentials.get(&azure.issuer_ref) {
                    validate_azure_issuer_kind(&credential.kind, &azure.issuer_ref)?;
                }
            }
            if let Some(gcp) = backend
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.gcp_downscope.as_ref())
            {
                if let Some(credential) = self.credentials.get(&gcp.issuer_ref) {
                    validate_gcp_issuer_kind(&credential.kind, &gcp.issuer_ref)?;
                }
            }
            for reference in credential_references(backend.credential_policy.as_ref()) {
                if let Some(credential) = self.credentials.get(reference) {
                    if !is_dynamic_issuer(backend.credential_policy.as_ref(), reference) {
                        validate_credential_provider(
                            &credential.kind,
                            &backend.provider,
                            reference,
                        )?;
                    }
                }
            }
        }
        for replica in self.metadata_replicas.replicas.values() {
            if let Some(sts) = replica
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.sts.as_ref())
            {
                if let Some(credential) = self.credentials.get(&sts.issuer_ref) {
                    validate_sts_issuer_kind(&credential.kind, &sts.issuer_ref)?;
                }
            }
            if let Some(azure) = replica
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.azure_user_delegation.as_ref())
            {
                if let Some(credential) = self.credentials.get(&azure.issuer_ref) {
                    validate_azure_issuer_kind(&credential.kind, &azure.issuer_ref)?;
                }
            }
            if let Some(gcp) = replica
                .credential_policy
                .as_ref()
                .and_then(|policy| policy.gcp_downscope.as_ref())
            {
                if let Some(credential) = self.credentials.get(&gcp.issuer_ref) {
                    validate_gcp_issuer_kind(&credential.kind, &gcp.issuer_ref)?;
                }
            }
            for reference in credential_references(replica.credential_policy.as_ref()) {
                if let Some(credential) = self.credentials.get(reference) {
                    if !is_dynamic_issuer(replica.credential_policy.as_ref(), reference) {
                        validate_credential_provider(
                            &credential.kind,
                            &replica.provider,
                            reference,
                        )?;
                    }
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
            TopologyMutation::SetBackendPolicy {
                backend,
                credentials,
            } => {
                if backend.credential_policy.is_none() {
                    bail!("backend requires credential_policy");
                }
                if credentials
                    .keys()
                    .any(|reference| self.credentials.contains_key(reference))
                {
                    bail!("credential already exists; use rotate-credential");
                }
                let old_references = self
                    .pack_backends
                    .iter()
                    .find(|existing| existing.id == backend.id)
                    .map(|existing| {
                        credential_references(existing.credential_policy.as_ref())
                            .into_iter()
                            .map(ToOwned::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
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
                self.credentials.extend(credentials);
                for reference in old_references {
                    let still_used =
                        self.pack_backends.iter().any(|configured| {
                            credential_references(configured.credential_policy.as_ref())
                                .contains(&reference.as_str())
                        }) || self.metadata_replicas.replicas.values().any(|configured| {
                            credential_references(configured.credential_policy.as_ref())
                                .contains(&reference.as_str())
                        });
                    if !still_used {
                        self.credentials.remove(&reference);
                    }
                }
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
            TopologyMutation::SetReplicaPolicy {
                id,
                replica,
                credentials,
            } => {
                if id.is_empty() {
                    bail!("metadata replica ID must not be empty");
                }
                if replica.credential_policy.is_none() {
                    bail!("metadata replica requires credential_policy");
                }
                if credentials
                    .keys()
                    .any(|reference| self.credentials.contains_key(reference))
                {
                    bail!("credential already exists; use rotate-credential");
                }
                let old_references = self
                    .metadata_replicas
                    .replicas
                    .get(&id)
                    .map(|existing| {
                        credential_references(existing.credential_policy.as_ref())
                            .into_iter()
                            .map(ToOwned::to_owned)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                self.metadata_replicas.replicas.insert(id.clone(), replica);
                if !self.metadata_replicas.order.contains(&id) {
                    self.metadata_replicas.order.push(id);
                    self.metadata_replicas.order.sort();
                }
                self.credentials.extend(credentials);
                for reference in old_references {
                    let still_used =
                        self.pack_backends.iter().any(|configured| {
                            credential_references(configured.credential_policy.as_ref())
                                .contains(&reference.as_str())
                        }) || self.metadata_replicas.replicas.values().any(|configured| {
                            credential_references(configured.credential_policy.as_ref())
                                .contains(&reference.as_str())
                        });
                    if !still_used {
                        self.credentials.remove(&reference);
                    }
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
            validate_credential_policy(
                &replica.provider,
                &replica.endpoint,
                replica.credential_policy.as_ref(),
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
            CredentialKind::S3Session => {
                present(&self.access_key_id)
                    && present(&self.secret_access_key)
                    && present(&self.session_token)
                    && present(&self.expires_at)
            }
            CredentialKind::CephxStatic => {
                self.client_id.as_deref().is_some_and(|client| {
                    client.starts_with("client.") && client.len() > "client.".len()
                }) && present(&self.client_secret)
            }
            CredentialKind::AzureSharedKey => {
                present(&self.account_name) && present(&self.account_key)
            }
            CredentialKind::AzureSas => present(&self.sas_token),
            CredentialKind::AzureEntraClientSecret => {
                present(&self.tenant_id)
                    && present(&self.client_id)
                    && present(&self.client_secret)
                    && present(&self.token_uri)
                    && self.scopes.len() == 1
                    && self.scopes.iter().all(|scope| !scope.is_empty())
            }
            CredentialKind::GcpServiceAccountJson => self
                .service_account_json
                .as_ref()
                .is_some_and(|value| serde_json::from_str::<Value>(value).is_ok()),
            CredentialKind::GcpAccessToken => {
                present(&self.access_token) && present(&self.expires_at)
            }
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
        for value in [
            self.issued_at.as_deref(),
            self.rotation_due.as_deref(),
            self.expires_at.as_deref(),
        ]
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
            || self.access_token.is_some()
            || self.client_secret.is_some()
            || self.refresh_token.is_some()
            || self.session_token.is_some()
    }
}

fn credential_references(policy: Option<&CredentialPolicy>) -> Vec<&str> {
    let Some(policy) = policy else {
        return Vec::new();
    };
    let mut references = Vec::new();
    if let Some(sts) = &policy.sts {
        references.push(sts.issuer_ref.as_str());
    }
    if let Some(azure) = &policy.azure_user_delegation {
        references.push(azure.issuer_ref.as_str());
    }
    if let Some(gcp) = &policy.gcp_downscope {
        references.push(gcp.issuer_ref.as_str());
    }
    if let Some(configured) = &policy.r#static {
        references.extend(configured.bindings.references().into_iter().flatten());
    }
    references
}

fn is_dynamic_issuer(policy: Option<&CredentialPolicy>, reference: &str) -> bool {
    policy.is_some_and(|policy| {
        policy
            .sts
            .as_ref()
            .is_some_and(|sts| sts.issuer_ref == reference)
            || policy
                .azure_user_delegation
                .as_ref()
                .is_some_and(|azure| azure.issuer_ref == reference)
            || policy
                .gcp_downscope
                .as_ref()
                .is_some_and(|gcp| gcp.issuer_ref == reference)
    })
}

fn validate_credential_policy(
    provider: &Provider,
    endpoint: &BTreeMap<String, Value>,
    policy: Option<&CredentialPolicy>,
    references: &mut BTreeSet<String>,
) -> Result<()> {
    if *provider == Provider::Local {
        if policy.is_some() {
            bail!("local endpoints must not reference credentials");
        }
        return Ok(());
    }
    let policy = policy.context("remote endpoint requires a credential policy")?;
    if policy.sts.is_none()
        && policy.azure_user_delegation.is_none()
        && policy.gcp_downscope.is_none()
        && policy.r#static.is_none()
    {
        bail!("remote endpoint requires a credential source");
    }
    if [
        policy.sts.is_some(),
        policy.azure_user_delegation.is_some(),
        policy.gcp_downscope.is_some(),
    ]
    .into_iter()
    .filter(|configured| *configured)
    .count()
        > 1
    {
        bail!("credential policy must configure only one dynamic issuer");
    }
    if let Some(sts) = &policy.sts {
        validate_sts_policy(provider, endpoint, sts)?;
        if sts.fallback == StsFallback::StaticOnUnavailable {
            let configured = policy
                .r#static
                .as_ref()
                .context("static-on-unavailable requires a static credential policy")?;
            if sts
                .roles
                .references()
                .into_iter()
                .zip(configured.bindings.references())
                .any(|(role, binding)| role.is_some() && binding.is_none())
            {
                bail!("static-on-unavailable requires an exact static binding for every STS role");
            }
        }
        if policy.r#static.as_ref().is_some_and(|configured| {
            configured
                .bindings
                .references()
                .into_iter()
                .flatten()
                .any(|reference| reference == sts.issuer_ref)
        }) {
            bail!("STS issuer credential must not be a consumer static binding");
        }
    }
    if let Some(azure) = &policy.azure_user_delegation {
        validate_azure_user_delegation_policy(provider, endpoint, azure)?;
        if azure.fallback == StsFallback::StaticOnUnavailable {
            let configured = policy
                .r#static
                .as_ref()
                .context("static-on-unavailable requires a static credential policy")?;
            if azure
                .tiers
                .iter()
                .any(|tier| configured.bindings.reference(*tier).is_none())
            {
                bail!(
                    "static-on-unavailable requires an exact static binding for every dynamic tier"
                );
            }
        }
        if policy.r#static.as_ref().is_some_and(|configured| {
            configured
                .bindings
                .references()
                .into_iter()
                .flatten()
                .any(|reference| reference == azure.issuer_ref)
        }) {
            bail!("dynamic issuer credential must not be a consumer static binding");
        }
    }
    if let Some(gcp) = &policy.gcp_downscope {
        validate_gcp_downscope_policy(provider, gcp)?;
        if gcp.fallback == StsFallback::StaticOnUnavailable {
            let configured = policy
                .r#static
                .as_ref()
                .context("static-on-unavailable requires a static credential policy")?;
            if gcp
                .tiers
                .iter()
                .any(|tier| configured.bindings.reference(*tier).is_none())
            {
                bail!(
                    "static-on-unavailable requires an exact static binding for every dynamic tier"
                );
            }
        }
        if policy.r#static.as_ref().is_some_and(|configured| {
            configured
                .bindings
                .references()
                .into_iter()
                .flatten()
                .any(|reference| reference == gcp.issuer_ref)
        }) {
            bail!("dynamic issuer credential must not be a consumer static binding");
        }
    }
    if let Some(configured) = &policy.r#static {
        if configured.generation == 0 {
            bail!("static credential policy requires a non-zero generation");
        }
        if configured
            .bindings
            .references()
            .into_iter()
            .flatten()
            .next()
            .is_none()
        {
            bail!("static credential policy requires at least one binding");
        }
        if configured
            .revoked_generations
            .iter()
            .enumerate()
            .any(|(index, generation)| {
                *generation == 0
                    || *generation >= configured.generation
                    || index > 0 && configured.revoked_generations[index - 1] >= *generation
            })
        {
            bail!("revoked static credential generations must be non-zero, older, unique, and ordered");
        }
    }
    let configured = credential_references(Some(policy));
    if configured.is_empty() {
        bail!("remote endpoint requires a credential reference");
    }
    for reference in configured {
        if !valid_reference(reference) {
            bail!("invalid credential reference");
        }
        references.insert(reference.to_owned());
    }
    Ok(())
}

fn validate_azure_user_delegation_policy(
    provider: &Provider,
    endpoint: &BTreeMap<String, Value>,
    policy: &AzureUserDelegationPolicy,
) -> Result<()> {
    if *provider != Provider::Azure {
        bail!("Azure user delegation policy requires an Azure endpoint");
    }
    if !valid_reference(&policy.issuer_ref)
        || !valid_service_version(&policy.service_version)
        || policy.tiers.is_empty()
    {
        bail!("Azure user delegation policy is incomplete");
    }
    if endpoint
        .get("prefix")
        .and_then(Value::as_str)
        .is_some_and(|prefix| !prefix.is_empty())
    {
        bail!("Azure user delegation requires an empty prefix and dedicated container");
    }
    if policy
        .tiers
        .iter()
        .enumerate()
        .any(|(index, tier)| index > 0 && policy.tiers[index - 1] >= *tier)
    {
        bail!("Azure user delegation tiers must be unique and canonically ordered");
    }
    if policy.tiers.contains(&StorageCredentialTier::Lock) && !policy.hierarchical_namespace {
        bail!("Azure storage-lock user delegation requires hierarchical namespace");
    }
    if let Some(signed_ip) = &policy.signed_ip {
        let addresses = signed_ip
            .split('-')
            .map(str::parse::<std::net::Ipv4Addr>)
            .collect::<std::result::Result<Vec<_>, _>>();
        let valid = addresses.is_ok_and(|addresses| match addresses.as_slice() {
            [_] => true,
            [start, end] => start <= end,
            _ => false,
        });
        if !valid {
            bail!("Azure user delegation signed_ip must be an IPv4 address or range");
        }
    }
    Ok(())
}

fn valid_service_version(value: &str) -> bool {
    const FORMAT: &[time::format_description::BorrowedFormatItem<'_>] =
        time::macros::format_description!("[year]-[month]-[day]");
    ("2020-12-06"..="2026-04-06").contains(&value) && Date::parse(value, FORMAT).is_ok()
}

fn validate_gcp_downscope_policy(provider: &Provider, policy: &GcpDownscopePolicy) -> Result<()> {
    if *provider != Provider::Gcs {
        bail!("GCP downscope policy requires a GCS endpoint");
    }
    if !valid_reference(&policy.issuer_ref)
        || !valid_gcp_service_account(&policy.service_account)
        || policy.tiers.is_empty()
    {
        bail!("GCP downscope policy is incomplete");
    }
    if policy
        .tiers
        .iter()
        .enumerate()
        .any(|(index, tier)| index > 0 && policy.tiers[index - 1] >= *tier)
    {
        bail!("GCP downscope tiers must be unique and canonically ordered");
    }
    for (name, raw, production) in [
        (
            "IAM credentials",
            policy.iam_credentials_endpoint.as_str(),
            "https://iamcredentials.googleapis.com",
        ),
        (
            "token exchange",
            policy.token_exchange_endpoint.as_str(),
            "https://sts.googleapis.com/v1/token",
        ),
    ] {
        let endpoint =
            reqwest::Url::parse(raw).with_context(|| format!("GCP {name} endpoint is invalid"))?;
        let loopback_http = endpoint.scheme() == "http"
            && endpoint
                .host_str()
                .is_some_and(|host| matches!(host, "127.0.0.1" | "::1" | "localhost"));
        if endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || raw != production && !loopback_http
        {
            bail!("GCP {name} endpoint is invalid");
        }
    }
    Ok(())
}

fn valid_gcp_service_account(value: &str) -> bool {
    value
        .strip_suffix(".iam.gserviceaccount.com")
        .and_then(|name| {
            (name.matches('@').count() == 1 && !name.starts_with('@') && !name.ends_with('@'))
                .then_some(name)
        })
        .is_some_and(|name| {
            name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'@')
            })
        })
}

fn validate_sts_policy(
    provider: &Provider,
    storage_endpoint: &BTreeMap<String, Value>,
    policy: &S3StsPolicy,
) -> Result<()> {
    if *provider != Provider::S3 {
        bail!("STS credential policy requires an S3 endpoint");
    }
    if !valid_reference(&policy.issuer_ref)
        || policy.endpoint.is_empty()
        || policy.region.is_empty()
        || policy.session_name.is_empty()
    {
        bail!("STS credential policy is incomplete");
    }
    let roles = policy.roles.references();
    if roles.iter().flatten().next().is_none() {
        bail!("STS credential policy requires at least one role binding");
    }
    if roles.iter().flatten().any(|role| !role.starts_with("arn:")) {
        bail!("STS role binding requires a role ARN");
    }
    let profile = normalize_s3_endpoint(storage_endpoint)?;
    let endpoint = reqwest::Url::parse(&policy.endpoint).context("STS endpoint has invalid URL")?;
    if endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
        || !matches!(endpoint.path(), "" | "/")
    {
        bail!("STS endpoint has invalid URL");
    }
    match profile.provider {
        S3Provider::Backblaze => bail!("Backblaze S3 does not support STS"),
        S3Provider::Wasabi if policy.endpoint != "https://sts.wasabisys.com" => {
            bail!("Wasabi STS endpoint must be https://sts.wasabisys.com")
        }
        _ => {}
    }
    if endpoint.scheme() != "https"
        && !(endpoint.scheme() == "http" && profile.endpoint.starts_with("http://"))
    {
        bail!("STS endpoint requires HTTPS unless the S3 endpoint explicitly uses HTTP");
    }
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
            CredentialKind::AwsStatic
                | CredentialKind::S3Static
                | CredentialKind::S3Session
                | CredentialKind::None
        ),
        Provider::Rados => matches!(kind, CredentialKind::CephxStatic),
        Provider::Azure => matches!(
            kind,
            CredentialKind::AzureSharedKey | CredentialKind::AzureSas | CredentialKind::None
        ),
        Provider::Gcs => matches!(
            kind,
            CredentialKind::GcpServiceAccountJson
                | CredentialKind::GcpAccessToken
                | CredentialKind::None
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

fn validate_sts_issuer_kind(kind: &CredentialKind, reference: &str) -> Result<()> {
    if !matches!(kind, CredentialKind::AwsStatic | CredentialKind::S3Static) {
        bail!("STS issuer credential {reference:?} must be a static S3 credential");
    }
    Ok(())
}

fn validate_azure_issuer_kind(kind: &CredentialKind, reference: &str) -> Result<()> {
    if *kind != CredentialKind::AzureEntraClientSecret {
        bail!(
            "Azure user delegation issuer credential {reference:?} must be an Entra client secret"
        );
    }
    Ok(())
}

fn validate_gcp_issuer_kind(kind: &CredentialKind, reference: &str) -> Result<()> {
    if *kind != CredentialKind::GcpServiceAccountJson {
        bail!(
            "GCP downscope issuer credential {reference:?} must be a service account JSON credential"
        );
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
        Provider::Rados => &["monitors", "cluster_fsid", "pool", "namespace", "prefix"],
        Provider::Azure => &["url", "account", "container"],
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
            "provider",
            "bucket_lookup",
            "storage_class",
            "tls_sha256",
        ],
        Provider::Rados => &["monitors", "cluster_fsid", "pool", "namespace", "prefix"],
        Provider::Azure => &["url", "account", "container", "prefix", "tls_sha256"],
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
    if provider == &Provider::S3 {
        normalize_s3_endpoint(endpoint)?;
    }
    if provider == &Provider::Rados {
        validate_rados_endpoint(endpoint)?;
    }
    if provider == &Provider::Azure {
        let raw_url = endpoint["url"].as_str().unwrap_or_default();
        let parsed = reqwest::Url::parse(raw_url).context("Azure endpoint has invalid URL")?;
        let loopback_http = parsed.scheme() == "http"
            && parsed
                .host_str()
                .is_some_and(|host| matches!(host, "127.0.0.1" | "::1" | "localhost"));
        if parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
            || !matches!(parsed.path(), "" | "/")
            || parsed.scheme() != "https" && !loopback_http
        {
            bail!("Azure endpoint has invalid URL");
        }
        if let Some((account, suffix)) = parsed.host_str().unwrap_or_default().split_once(".blob.")
        {
            if account.is_empty()
                || suffix.is_empty()
                || endpoint["account"].as_str() != Some(account)
            {
                bail!("Azure account does not match endpoint host");
            }
        }
    }
    Ok(())
}

fn validate_rados_endpoint(endpoint: &BTreeMap<String, Value>) -> Result<()> {
    let value = |field: &str| endpoint[field].as_str().unwrap_or_default();
    let fsid = value("cluster_fsid");
    let valid_fsid = fsid.len() == 36
        && fsid.bytes().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_hexdigit(),
        });
    if !valid_fsid {
        bail!("RADOS endpoint cluster_fsid must be a UUID");
    }
    for monitor in value("monitors").split(',').map(str::trim) {
        let valid = if monitor.starts_with('[') {
            monitor
                .split_once("]:")
                .is_some_and(|(host, port)| host.len() > 1 && valid_port(port))
        } else {
            monitor.rsplit_once(':').is_some_and(|(host, port)| {
                !host.is_empty() && !host.contains(':') && valid_port(port)
            })
        };
        if !valid {
            bail!("RADOS endpoint monitors must be comma-separated host:port addresses");
        }
    }
    for field in ["pool", "namespace"] {
        let configured = value(field);
        if configured.contains(['/', '\0', '\r', '\n']) || matches!(configured, "." | "..") {
            bail!("RADOS endpoint {field} is invalid");
        }
    }
    let prefix = value("prefix");
    if prefix.starts_with('/')
        || !prefix.ends_with('/')
        || prefix.contains("..")
        || prefix.contains(['\0', '\r', '\n'])
    {
        bail!("RADOS endpoint prefix must be a relative directory prefix ending in slash");
    }
    Ok(())
}

fn valid_port(port: &str) -> bool {
    port.parse::<u16>().is_ok_and(|port| port != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topology() -> TopologyDocument {
        serde_json::from_slice(include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
        )))
        .unwrap()
    }

    #[test]
    fn shared_fixture_is_canonical_and_valid() {
        let encoded = include_bytes!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../testdata/topology-v2.json"
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
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_maintain = Some("cred:missing".to_owned());
        assert!(topology.validate().is_err());
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_maintain = Some("cred:archive".to_owned());
        topology.placement_policy.min_domains = 3;
        assert!(topology.validate().is_err());
    }

    #[test]
    fn static_credential_policy_is_exact_and_fail_closed() {
        let mut topology = topology();
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        let configured = policy.r#static.as_mut().unwrap();
        let legacy_reference = configured.bindings.storage_maintain.clone().unwrap();
        configured.bindings = CredentialBindings {
            storage_read: Some("cred:archive-read".to_owned()),
            storage_append: Some("cred:archive-append".to_owned()),
            storage_maintain: Some(legacy_reference.clone()),
            storage_lock: Some("cred:archive-lock".to_owned()),
        };
        for reference in [
            "cred:archive-read",
            "cred:archive-append",
            "cred:archive-lock",
        ] {
            topology.credentials.insert(
                reference.to_owned(),
                topology.credentials[&legacy_reference].clone(),
            );
        }
        topology.validate().unwrap();
        assert_eq!(
            select_static_credential_reference(
                topology.pack_backends[0].credential_policy.as_ref(),
                StorageCredentialTier::Read
            )
            .unwrap(),
            "cred:archive-read"
        );
        assert_eq!(
            select_static_credential_reference(
                topology.pack_backends[0].credential_policy.as_ref(),
                StorageCredentialTier::Maintain
            )
            .unwrap(),
            legacy_reference
        );
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_append = None;
        assert!(select_static_credential_reference(
            topology.pack_backends[0].credential_policy.as_ref(),
            StorageCredentialTier::Append
        )
        .is_err());
    }

    #[test]
    fn sts_policy_requires_role_and_static_generation() {
        let mut topology = topology();
        topology.credentials.insert(
            "cred:issuer".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .sts = Some(S3StsPolicy {
            issuer_ref: "cred:issuer".to_owned(),
            endpoint: "https://sts.example.com".to_owned(),
            region: "us-east-1".to_owned(),
            session_name: "vaultic".to_owned(),
            external_id: None,
            roles: CredentialBindings {
                storage_read: Some("not-an-arn".to_owned()),
                storage_append: None,
                storage_maintain: None,
                storage_lock: None,
            },
            fallback: StsFallback::StaticOnUnavailable,
        });
        assert!(topology.validate().is_err());
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.sts.as_mut().unwrap().roles.storage_read =
            Some("arn:aws:iam::123456789012:role/vaultic-read".to_owned());
        policy.r#static.as_mut().unwrap().bindings.storage_read = Some("cred:archive".to_owned());
        policy.r#static.as_mut().unwrap().generation = 0;
        assert!(topology.validate().is_err());
    }

    #[test]
    fn azure_user_delegation_policy_is_exact_and_fail_closed() {
        let mut topology = topology();
        let mut static_credential = topology.credentials["cred:archive"].clone();
        static_credential.kind = CredentialKind::AzureSas;
        static_credential.access_key_id = None;
        static_credential.secret_access_key = None;
        static_credential.account_name = Some("account-a".to_owned());
        static_credential.sas_token = Some("sp=rl&sig=static".to_owned());
        topology
            .credentials
            .insert("cred:archive".to_owned(), static_credential);

        let mut issuer = topology.credentials["cred:archive"].clone();
        issuer.kind = CredentialKind::AzureEntraClientSecret;
        issuer.account_name = None;
        issuer.sas_token = None;
        issuer.tenant_id = Some("tenant-a".to_owned());
        issuer.client_id = Some("client-a".to_owned());
        issuer.client_secret = Some("issuer-secret".to_owned());
        issuer.scopes = vec!["https://storage.azure.com/.default".to_owned()];
        issuer.token_uri =
            Some("https://login.microsoftonline.com/tenant-a/oauth2/v2.0/token".to_owned());
        topology
            .credentials
            .insert("cred:azure-issuer".to_owned(), issuer);

        let backend = &mut topology.pack_backends[0];
        backend.provider = Provider::Azure;
        backend.endpoint = BTreeMap::from([
            (
                "url".to_owned(),
                Value::String("https://account-a.blob.core.windows.net".to_owned()),
            ),
            ("account".to_owned(), Value::String("account-a".to_owned())),
            ("container".to_owned(), Value::String("repo-a".to_owned())),
            ("prefix".to_owned(), Value::String(String::new())),
        ]);
        backend.credential_policy = Some(CredentialPolicy {
            sts: None,
            azure_user_delegation: Some(AzureUserDelegationPolicy {
                issuer_ref: "cred:azure-issuer".to_owned(),
                service_version: "2026-04-06".to_owned(),
                signed_ip: None,
                hierarchical_namespace: false,
                tiers: vec![StorageCredentialTier::Read, StorageCredentialTier::Maintain],
                fallback: StsFallback::StaticOnUnavailable,
            }),
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 1,
                revoked_generations: Vec::new(),
                bindings: CredentialBindings {
                    storage_read: Some("cred:archive".to_owned()),
                    storage_append: None,
                    storage_maintain: Some("cred:archive".to_owned()),
                    storage_lock: None,
                },
            }),
        });
        topology.validate().unwrap();

        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .azure_user_delegation
            .as_mut()
            .unwrap()
            .tiers
            .push(StorageCredentialTier::Lock);
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("hierarchical namespace"));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .azure_user_delegation
            .as_mut()
            .unwrap()
            .tiers = vec![StorageCredentialTier::Read, StorageCredentialTier::Maintain];
        let entra = topology.credentials["cred:azure-issuer"].clone();
        topology
            .credentials
            .insert("cred:consumer-issuer".to_owned(), entra);
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_read = Some("cred:consumer-issuer".to_owned());
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("cannot authenticate"));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_read = Some("cred:archive".to_owned());

        topology.pack_backends[0].endpoint.insert(
            "prefix".to_owned(),
            Value::String("shared-prefix".to_owned()),
        );
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("dedicated container"));
        topology.pack_backends[0]
            .endpoint
            .insert("prefix".to_owned(), Value::String(String::new()));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_read = None;
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exact static binding"));
    }

    #[test]
    fn provider_specific_sts_policy_and_revocation_validation() {
        let mut topology = topology();
        topology.credentials.insert(
            "cred:issuer".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        let backend = &mut topology.pack_backends[0];
        backend.credential_policy.as_mut().unwrap().sts = Some(S3StsPolicy {
            issuer_ref: "cred:issuer".to_owned(),
            endpoint: "https://sts.wasabisys.com".to_owned(),
            region: "us-east-1".to_owned(),
            session_name: "vaultic".to_owned(),
            external_id: None,
            roles: CredentialBindings {
                storage_read: Some("arn:aws:iam::123456789012:role/read".to_owned()),
                storage_append: None,
                storage_maintain: None,
                storage_lock: None,
            },
            fallback: StsFallback::StaticOnUnavailable,
        });
        backend.endpoint.insert(
            "url".to_owned(),
            Value::String("https://s3.us-west-004.backblazeb2.com".to_owned()),
        );
        backend
            .endpoint
            .insert("region".to_owned(), Value::String("us-west-004".to_owned()));
        backend
            .endpoint
            .insert("provider".to_owned(), Value::String("backblaze".to_owned()));
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("does not support STS"));

        let backend = &mut topology.pack_backends[0];
        backend.endpoint.insert(
            "url".to_owned(),
            Value::String("https://s3.eu-central-2.wasabisys.com".to_owned()),
        );
        backend.endpoint.insert(
            "region".to_owned(),
            Value::String("eu-central-2".to_owned()),
        );
        backend
            .endpoint
            .insert("provider".to_owned(), Value::String("wasabi".to_owned()));
        let policy = backend.credential_policy.as_mut().unwrap();
        policy.sts.as_mut().unwrap().region = "eu-central-2".to_owned();
        policy.sts.as_mut().unwrap().endpoint = "https://sts.example.com".to_owned();
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("sts.wasabisys.com"));

        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.sts.as_mut().unwrap().endpoint = "https://sts.wasabisys.com".to_owned();
        policy.r#static.as_mut().unwrap().bindings.storage_read = Some("cred:archive".to_owned());
        policy.r#static.as_mut().unwrap().generation = 3;
        policy.r#static.as_mut().unwrap().revoked_generations = vec![2, 1];
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("unique, and ordered"));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .revoked_generations = vec![1, 2];
        topology.validate().unwrap();
    }

    #[test]
    fn gcp_downscope_policy_is_exact_and_fail_closed() {
        let mut topology = topology();
        let gcp_key = || {
            serde_json::from_value::<Credential>(serde_json::json!({
                "kind": "gcp-service-account-json",
                "service_account_json": "{\"type\":\"service_account\",\"client_email\":\"issuer@example.test\"}"
            }))
            .unwrap()
        };
        topology
            .credentials
            .insert("cred:gcp-issuer".to_owned(), gcp_key());
        topology
            .credentials
            .insert("cred:gcp-fallback".to_owned(), gcp_key());
        topology.credentials.remove("cred:archive");
        let backend = &mut topology.pack_backends[0];
        backend.provider = Provider::Gcs;
        backend.endpoint = BTreeMap::from([
            ("bucket".to_owned(), Value::String("repo-bucket".to_owned())),
            ("prefix".to_owned(), Value::String("repo".to_owned())),
        ]);
        backend.credential_policy = Some(CredentialPolicy {
            sts: None,
            azure_user_delegation: None,
            gcp_downscope: Some(GcpDownscopePolicy {
                issuer_ref: "cred:gcp-issuer".to_owned(),
                service_account: "target@example.iam.gserviceaccount.com".to_owned(),
                iam_credentials_endpoint: "https://iamcredentials.googleapis.com".to_owned(),
                token_exchange_endpoint: "https://sts.googleapis.com/v1/token".to_owned(),
                tiers: vec![StorageCredentialTier::Read, StorageCredentialTier::Maintain],
                fallback: StsFallback::StaticOnUnavailable,
            }),
            r#static: Some(StaticCredentialPolicy {
                generation: 1,
                revoked_generations: Vec::new(),
                bindings: CredentialBindings {
                    storage_read: Some("cred:gcp-fallback".to_owned()),
                    storage_append: None,
                    storage_maintain: Some("cred:gcp-fallback".to_owned()),
                    storage_lock: None,
                },
            }),
        });
        topology.validate().unwrap();

        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy
            .gcp_downscope
            .as_mut()
            .unwrap()
            .token_exchange_endpoint = "https://attacker.example/token".to_owned();
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("token exchange endpoint is invalid"));
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy
            .gcp_downscope
            .as_mut()
            .unwrap()
            .token_exchange_endpoint = "https://sts.googleapis.com/v1/token".to_owned();
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.gcp_downscope.as_mut().unwrap().tiers =
            vec![StorageCredentialTier::Maintain, StorageCredentialTier::Read];
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("canonically ordered"));
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.gcp_downscope.as_mut().unwrap().tiers =
            vec![StorageCredentialTier::Read, StorageCredentialTier::Maintain];
        policy.r#static.as_mut().unwrap().bindings.storage_read =
            Some("cred:gcp-issuer".to_owned());
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("issuer credential"));
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.r#static.as_mut().unwrap().bindings.storage_read =
            Some("cred:gcp-fallback".to_owned());
        policy.r#static.as_mut().unwrap().bindings.storage_maintain = None;
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exact static binding"));
    }

    #[test]
    fn static_fallback_requires_exact_binding_and_separate_issuer() {
        let mut topology = topology();
        topology.credentials.insert(
            "cred:issuer".to_owned(),
            topology.credentials["cred:archive"].clone(),
        );
        let policy = topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap();
        policy.sts = Some(S3StsPolicy {
            issuer_ref: "cred:issuer".to_owned(),
            endpoint: "https://sts.example.com".to_owned(),
            region: "us-east-1".to_owned(),
            session_name: "vaultic".to_owned(),
            external_id: None,
            roles: CredentialBindings {
                storage_read: Some("arn:aws:iam::123456789012:role/read".to_owned()),
                storage_append: None,
                storage_maintain: None,
                storage_lock: None,
            },
            fallback: StsFallback::StaticOnUnavailable,
        });
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("exact static binding"));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_read = Some("cred:issuer".to_owned());
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("issuer credential"));
        topology.pack_backends[0]
            .credential_policy
            .as_mut()
            .unwrap()
            .r#static
            .as_mut()
            .unwrap()
            .bindings
            .storage_read = Some("cred:archive".to_owned());
        topology.validate().unwrap();
    }

    #[test]
    fn credential_tiers_have_shared_canonical_json() {
        let bindings = CredentialBindings {
            storage_read: Some("cred:read".to_owned()),
            storage_append: Some("cred:append".to_owned()),
            storage_maintain: Some("cred:maintain".to_owned()),
            storage_lock: Some("cred:lock".to_owned()),
        };
        assert_eq!(
            serde_json::to_string(&bindings).unwrap(),
            r#"{"storage-read":"cred:read","storage-append":"cred:append","storage-maintain":"cred:maintain","storage-lock":"cred:lock"}"#
        );
    }

    #[test]
    fn backend_policy_mutation_is_atomic() {
        let mut topology = topology();
        let mut backend = topology.pack_backends[0].clone();
        backend.credential_policy = Some(CredentialPolicy {
            sts: None,
            azure_user_delegation: None,
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 2,
                revoked_generations: vec![1],
                bindings: CredentialBindings {
                    storage_read: Some("cred:archive-read".to_owned()),
                    storage_append: Some("cred:archive-append".to_owned()),
                    storage_maintain: Some("cred:archive-maintain".to_owned()),
                    storage_lock: Some("cred:archive-lock".to_owned()),
                },
            }),
        });
        let credential = topology.credentials["cred:archive"].clone();
        let credentials = [
            "cred:archive-read",
            "cred:archive-append",
            "cred:archive-maintain",
            "cred:archive-lock",
        ]
        .into_iter()
        .map(|reference| (reference.to_owned(), credential.clone()))
        .collect();
        topology
            .apply_mutation(
                TopologyMutation::SetBackendPolicy {
                    backend,
                    credentials,
                },
                8,
            )
            .unwrap();
        assert_eq!(topology.topology_generation, 8);
        assert!(!topology.credentials.contains_key("cred:archive"));
        assert_eq!(
            topology.pack_backends[0]
                .credential_policy
                .as_ref()
                .unwrap()
                .r#static
                .as_ref()
                .unwrap()
                .bindings
                .storage_maintain
                .as_deref(),
            Some("cred:archive-maintain")
        );
    }

    #[test]
    fn replica_policy_mutation_is_atomic() {
        let mut topology = topology();
        let mut replica = topology.metadata_replicas.replicas["remote"].clone();
        replica.credential_policy = Some(CredentialPolicy {
            sts: None,
            azure_user_delegation: None,
            gcp_downscope: None,
            r#static: Some(StaticCredentialPolicy {
                generation: 2,
                revoked_generations: vec![1],
                bindings: CredentialBindings {
                    storage_read: Some("cred:metadata-read".to_owned()),
                    storage_append: None,
                    storage_maintain: Some("cred:metadata-maintain".to_owned()),
                    storage_lock: None,
                },
            }),
        });
        let credential = topology.credentials["cred:metadata"].clone();
        let credentials = ["cred:metadata-read", "cred:metadata-maintain"]
            .into_iter()
            .map(|reference| (reference.to_owned(), credential.clone()))
            .collect();
        topology
            .apply_mutation(
                TopologyMutation::SetReplicaPolicy {
                    id: "remote".to_owned(),
                    replica,
                    credentials,
                },
                8,
            )
            .unwrap();
        assert!(!topology.credentials.contains_key("cred:metadata"));
        assert_eq!(
            topology.metadata_replicas.replicas["remote"]
                .credential_policy
                .as_ref()
                .unwrap()
                .r#static
                .as_ref()
                .unwrap()
                .bindings
                .storage_maintain
                .as_deref(),
            Some("cred:metadata-maintain")
        );
    }

    #[test]
    fn s3_provider_profiles_validate_endpoint_and_region() {
        let mut topology = topology();
        {
            let endpoint = &mut topology.pack_backends[0].endpoint;
            endpoint.insert(
                "url".to_owned(),
                Value::String("https://s3.us-west-004.backblazeb2.com".to_owned()),
            );
            endpoint.insert("region".to_owned(), Value::String("us-west-004".to_owned()));
            endpoint.insert("provider".to_owned(), Value::String("backblaze".to_owned()));
            endpoint.insert("bucket_lookup".to_owned(), Value::String("dns".to_owned()));
        }
        topology.validate().unwrap();

        topology.pack_backends[0].endpoint.insert(
            "region".to_owned(),
            Value::String("eu-central-2".to_owned()),
        );
        assert!(topology
            .validate()
            .unwrap_err()
            .to_string()
            .contains("does not match"));
    }

    #[test]
    fn explicit_generic_s3_profile_suppresses_inference() {
        let mut topology = topology();
        let endpoint = &mut topology.pack_backends[0].endpoint;
        endpoint.insert(
            "url".to_owned(),
            Value::String("https://s3.us-west-004.backblazeb2.com".to_owned()),
        );
        endpoint.insert("region".to_owned(), Value::String("custom".to_owned()));
        endpoint.insert("provider".to_owned(), Value::String("generic".to_owned()));
        assert_eq!(
            normalize_s3_endpoint(endpoint).unwrap().provider,
            S3Provider::Generic
        );
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

    #[test]
    fn rados_endpoint_and_credential_validate() {
        let mut topology = topology();
        let backend = &mut topology.pack_backends[0];
        backend.provider = Provider::Rados;
        backend.endpoint = BTreeMap::from([
            (
                "monitors".to_owned(),
                Value::String("ceph-mon-a.example:3300,[2001:db8::1]:3300".to_owned()),
            ),
            (
                "cluster_fsid".to_owned(),
                Value::String("2f525d6a-8f31-4f79-b731-82a6acb235f5".to_owned()),
            ),
            ("pool".to_owned(), Value::String("vaultic-data".to_owned())),
            ("namespace".to_owned(), Value::String("repo-2".to_owned())),
            ("prefix".to_owned(), Value::String("packs/".to_owned())),
        ]);
        let credential = topology.credentials.get_mut("cred:archive").unwrap();
        credential.kind = CredentialKind::CephxStatic;
        credential.access_key_id = None;
        credential.secret_access_key = None;
        credential.client_id = Some("client.vaultic-repo-2".to_owned());
        credential.client_secret = Some("AQB-secret".to_owned());
        assert!(topology.validate().is_ok());

        topology.pack_backends[0].endpoint.insert(
            "monitors".to_owned(),
            Value::String("https://ceph-mon-a.example:3300".to_owned()),
        );
        assert!(topology.validate().is_err());
    }
}
