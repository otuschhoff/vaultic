//! Broker request/response wire protocol and connection negotiation.

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio::sync::Mutex;
use zeroize::Zeroizing;

use crate::{
    broker::{
        audit::emit_security_event, unix_time_ms, Capability, ClientIdentity,
        EncryptedContribution, KeyBroker, SignedSession,
    },
    encryption::recovery_capsule::{
        ExternalMemberProtection, HardwareBinding, MemberCredential, MemberProtection,
        MemberProvider, PrincipalBinding, RecoveryCapsule, UnlockPolicy,
    },
    ids::MemberId,
};

pub const PROTOCOL_VERSION: &str = "vaultic-key-broker.v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthorizedOperation {
    pub component: String,
    pub version: u64,
    pub release_identity: String,
    pub release_signature: String,
    pub challenge_response: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OfflinePolicyMember {
    pub member_id: MemberId,
    pub provider: MemberProvider,
    pub credential: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalPolicyMember {
    pub member_id: MemberId,
    pub provider: MemberProvider,
    pub key_reference: String,
    pub principal: Option<PrincipalBinding>,
    pub hardware: Option<HardwareBinding>,
    pub bearer_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum BrokerRequest {
    Negotiate {
        protocols: Vec<String>,
    },
    Status,
    CreateSession {
        ttl_seconds: u64,
    },
    SubmitContribution {
        contribution: EncryptedContribution,
    },
    AcquireLease {
        component: String,
        version: u64,
        release_identity: String,
        release_signature: String,
        capability: Capability,
        ttl_seconds: u64,
        challenge_response: String,
    },
    ReleaseLease {
        lease_id: String,
    },
    PreparePolicyMutation {
        authorization: AuthorizedOperation,
        policy: UnlockPolicy,
        members: Vec<OfflinePolicyMember>,
        #[serde(default)]
        external_members: Vec<ExternalPolicyMember>,
        acknowledge_downgrade: bool,
    },
    ActivatePolicyMutation {
        authorization: AuthorizedOperation,
        capsule_sha256: String,
    },
    PendingPolicyMutation {
        authorization: AuthorizedOperation,
    },
    CancelPolicyMutation {
        authorization: AuthorizedOperation,
    },
    Lock,
}

impl BrokerRequest {
    pub fn requests_lock(&self) -> bool {
        matches!(self, Self::Lock | Self::ActivatePolicyMutation { .. })
    }
}

pub async fn handle_request(
    request: BrokerRequest,
    broker: &Mutex<KeyBroker>,
    connection_id: &str,
    peer: &PeerProcess,
    endpoint_binding: &str,
    protocol: &mut ConnectionProtocol,
) -> Result<BrokerResponse> {
    let now = unix_time_ms()?;
    let mut broker = broker.lock().await;
    let expiration = broker.expire_state(now);
    if expiration.expired_sessions != 0 {
        emit_security_event(
            "notice",
            "lifecycle",
            "sessions_expired",
            &[("expired_sessions", expiration.expired_sessions.to_string())],
        );
    }
    if expiration.expired_leases != 0 {
        emit_security_event(
            "warning",
            "lifecycle",
            "leases_expired_or_revoked",
            &[("expired_leases", expiration.expired_leases.to_string())],
        );
    }
    if expiration.automatic_lock {
        emit_security_event("critical", "lifecycle", "maximum_epoch_lifetime_lock", &[]);
    }
    if let BrokerRequest::Negotiate { protocols } = &request {
        if protocol.negotiated {
            bail!("broker protocol is already negotiated");
        }
        if !protocols.iter().any(|value| value == PROTOCOL_VERSION) {
            bail!("no mutually supported broker protocol");
        }
        let challenge = random_id();
        protocol.negotiated = true;
        protocol.lease_challenge = Some(challenge.clone());
        return Ok(BrokerResponse::Negotiated {
            protocol: PROTOCOL_VERSION,
            challenge,
        });
    }
    if !protocol.negotiated {
        bail!("broker protocol negotiation is required");
    }
    match request {
        BrokerRequest::Negotiate { .. } => unreachable!(),
        BrokerRequest::Status => {
            let status = broker.status(now);
            Ok(BrokerResponse::Status {
                protocol: PROTOCOL_VERSION,
                locked: status.locked,
                repository_id: status.repository_id.into_string(),
                capsule_generation: status.capsule_generation,
                capsule_logical_id: status.capsule_logical_id,
                policy_hash: status.policy_hash,
                epoch_id: status.epoch_id,
                active_sessions: status.active_sessions,
                active_leases: status.active_leases,
                minimum_custodians: status.minimum_custodians,
                principal_verified: status.principal_verified,
                hardware_verified: status.hardware_verified,
                custody_assumed: status.custody_assumed,
                compliant: status.compliant,
                findings: status.findings,
                policy_mutation_pending: status.policy_mutation_pending,
                pending_capsule_generation: status.pending_capsule_generation,
                pending_capsule_sha256: status.pending_capsule_sha256,
                identity_recovery: status.identity_recovery,
            })
        }
        BrokerRequest::CreateSession { ttl_seconds } => {
            let session =
                broker.create_session(endpoint_binding, Duration::from_secs(ttl_seconds), now)?;
            emit_security_event(
                "notice",
                "auth",
                "session_created",
                &[
                    ("session_id", session.transcript.session_id.to_string()),
                    (
                        "repository_id",
                        session.transcript.repository_id.to_string(),
                    ),
                    (
                        "capsule_generation",
                        session.transcript.capsule_generation.to_string(),
                    ),
                    (
                        "expires_unix_ms",
                        session.transcript.expires_unix_ms.to_string(),
                    ),
                    (
                        "identity_recovery",
                        session.transcript.identity_recovery.to_string(),
                    ),
                ],
            );
            Ok(BrokerResponse::Session { session })
        }
        BrokerRequest::SubmitContribution { contribution } => {
            let session_id = contribution.session_id.clone();
            let unlocked = broker.submit_contribution(contribution, now)?;
            emit_security_event(
                if unlocked { "critical" } else { "notice" },
                "auth",
                if unlocked {
                    "quorum_completed"
                } else {
                    "contribution_accepted"
                },
                &[
                    ("session_id", session_id.to_string()),
                    ("unlocked", unlocked.to_string()),
                ],
            );
            Ok(BrokerResponse::Contribution { unlocked })
        }
        BrokerRequest::AcquireLease {
            component,
            version,
            release_identity,
            release_signature,
            capability,
            ttl_seconds,
            challenge_response,
        } => {
            let challenge = protocol
                .lease_challenge
                .take()
                .context("lease challenge is missing or already consumed")?;
            let expected = lease_challenge_response(&challenge, &peer.executable_sha256);
            if challenge_response != expected {
                bail!("lease challenge response is invalid");
            }
            let client = ClientIdentity {
                connection_id: connection_id.to_owned(),
                component,
                version,
                release_identity,
                executable_sha256: peer.executable_sha256.clone(),
                release_signature,
                peer_uid: peer.uid,
                executable_owned_by_root: peer.owned_by_root,
                installation_path_read_only: peer.installation_path_read_only,
            };
            let lease =
                broker.acquire_lease(&client, capability, Duration::from_secs(ttl_seconds), now)?;
            emit_security_event(
                "notice",
                "auth",
                "lease_granted",
                &[
                    ("connection_id", connection_id.to_owned()),
                    ("component", client.component.clone()),
                    ("version", client.version.to_string()),
                    ("release_identity", client.release_identity.clone()),
                    ("capability", format!("{:?}", lease.capability)),
                    ("lease_id", lease.lease_id.clone()),
                    ("expires_unix_ms", lease.expires_unix_ms.to_string()),
                ],
            );
            Ok(BrokerResponse::Lease {
                lease_id: lease.lease_id,
                epoch_id: lease.epoch_id,
                capability: lease.capability,
                expires_unix_ms: lease.expires_unix_ms,
                key_version: lease.key_version,
                capsule_generation: lease.capsule_generation,
                key: BASE64.encode(lease.key.as_slice()),
            })
        }
        BrokerRequest::ReleaseLease { lease_id } => {
            broker.release_lease(&lease_id, connection_id)?;
            emit_security_event(
                "notice",
                "lifecycle",
                "lease_released",
                &[
                    ("connection_id", connection_id.to_owned()),
                    ("lease_id", lease_id),
                ],
            );
            Ok(BrokerResponse::Ok)
        }
        BrokerRequest::PreparePolicyMutation {
            authorization,
            policy,
            members,
            external_members,
            acknowledge_downgrade,
        } => {
            let client = authorized_client(protocol, peer, connection_id, authorization)?;
            let credentials = members
                .into_iter()
                .map(|member| {
                    if member.member_id.is_empty() || member.credential.is_empty() {
                        bail!("policy member ID and credential must not be empty");
                    }
                    let credential = BASE64
                        .decode(member.credential)
                        .context("decode policy member credential")?;
                    if credential.is_empty() {
                        bail!("policy member credential must not be empty");
                    }
                    match member.provider {
                        MemberProvider::OfflineArgon2id | MemberProvider::OfflineKeyfile => Ok((
                            member.member_id,
                            member.provider,
                            Zeroizing::new(credential),
                        )),
                        _ => bail!("offline policy member has an external provider"),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            let mut external_providers = Vec::with_capacity(external_members.len());
            for member in external_members {
                if member.member_id.is_empty() || member.key_reference.is_empty() {
                    bail!("external policy member ID and key reference must not be empty");
                }
                let provider_name = match member.provider {
                    MemberProvider::AzureKeyVault => "azure-key-vault",
                    MemberProvider::AwsKms | MemberProvider::AwsCloudhsm => "aws-kms",
                    MemberProvider::GcpKms | MemberProvider::GcpCloudHsm => "gcp-kms",
                    MemberProvider::YubikeyPiv => "yubikey-piv",
                    MemberProvider::Fido2HmacSecret => "fido2-hmac-secret",
                    MemberProvider::MacosSecureEnclave => "macos-secure-enclave",
                    _ => bail!("unsupported external policy member provider"),
                };
                let provider: Box<dyn crate::encryption::envelope::providers::KeyProvider> =
                    if member.provider == MemberProvider::MacosSecureEnclave {
                        Box::new(
                            crate::encryption::envelope::providers::MacosSecureEnclaveProvider::from_key_reference(
                                &member.key_reference,
                            )?,
                        )
                    } else {
                        crate::encryption::envelope::providers::for_management(
                            provider_name,
                            member.bearer_token.clone(),
                        )
                        .await?
                    };
                external_providers.push((member, provider));
            }
            let mut protections = credentials
                .iter()
                .map(|(member_id, provider, credential)| {
                    let credential = match provider {
                        MemberProvider::OfflineArgon2id => {
                            MemberCredential::Passphrase(credential.as_slice())
                        }
                        MemberProvider::OfflineKeyfile => {
                            MemberCredential::Keyfile(credential.as_slice())
                        }
                        _ => unreachable!("validated above"),
                    };
                    (member_id.as_str(), MemberProtection::Offline(credential))
                })
                .collect::<Vec<_>>();
            protections.extend(external_providers.iter().map(|(member, provider)| {
                (
                    member.member_id.as_str(),
                    MemberProtection::External(ExternalMemberProtection {
                        provider: member.provider.clone(),
                        key_reference: &member.key_reference,
                        principal: member.principal.clone(),
                        hardware: member.hardware.clone(),
                        key_provider: provider.as_ref(),
                    }),
                )
            }));
            let (capsule, capsule_sha256) = broker
                .prepare_policy_mutation(&client, policy, &protections, acknowledge_downgrade, now)
                .await?;
            emit_security_event(
                if acknowledge_downgrade {
                    "critical"
                } else {
                    "notice"
                },
                "lifecycle",
                "policy_mutation_prepared",
                &[
                    ("repository_id", capsule.header.repository_id.to_string()),
                    ("capsule_generation", capsule.header.generation.to_string()),
                    ("capsule_sha256", capsule_sha256.clone()),
                ],
            );
            Ok(BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            })
        }
        BrokerRequest::ActivatePolicyMutation {
            authorization,
            capsule_sha256,
        } => {
            let client = authorized_client(protocol, peer, connection_id, authorization)?;
            broker.activate_policy_mutation(&client, &capsule_sha256)?;
            emit_security_event(
                "critical",
                "lifecycle",
                "policy_mutation_activated_and_locked",
                &[("capsule_sha256", capsule_sha256)],
            );
            Ok(BrokerResponse::Ok)
        }
        BrokerRequest::PendingPolicyMutation { authorization } => {
            let client = authorized_client(protocol, peer, connection_id, authorization)?;
            let (capsule, capsule_sha256) = broker.pending_policy_mutation(&client)?;
            Ok(BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            })
        }
        BrokerRequest::CancelPolicyMutation { authorization } => {
            let client = authorized_client(protocol, peer, connection_id, authorization)?;
            broker.authorize_client(&client, Capability::PolicyMutation)?;
            broker.cancel_policy_mutation()?;
            emit_security_event(
                "warning",
                "lifecycle",
                "policy_mutation_cancelled",
                &[
                    ("component", client.component),
                    ("release_identity", client.release_identity),
                ],
            );
            Ok(BrokerResponse::Ok)
        }
        BrokerRequest::Lock => {
            let status = broker.status(now);
            broker.lock();
            emit_security_event(
                "notice",
                "lifecycle",
                "sessions_closed_by_explicit_lock",
                &[
                    ("connection_id", connection_id.to_owned()),
                    ("session_count", status.active_sessions.to_string()),
                    ("lease_count", status.active_leases.to_string()),
                ],
            );
            emit_security_event(
                "critical",
                "lifecycle",
                "broker_locked",
                &[
                    ("connection_id", connection_id.to_owned()),
                    ("session_count", status.active_sessions.to_string()),
                    ("lease_count", status.active_leases.to_string()),
                ],
            );
            Ok(BrokerResponse::Ok)
        }
    }
}

fn authorized_client(
    protocol: &mut ConnectionProtocol,
    peer: &PeerProcess,
    connection_id: &str,
    authorization: AuthorizedOperation,
) -> Result<ClientIdentity> {
    let challenge = protocol
        .lease_challenge
        .take()
        .context("authorization challenge is missing or already consumed")?;
    let expected = lease_challenge_response(&challenge, &peer.executable_sha256);
    if authorization.challenge_response != expected {
        bail!("authorization challenge response is invalid");
    }
    Ok(ClientIdentity {
        connection_id: connection_id.to_owned(),
        component: authorization.component,
        version: authorization.version,
        release_identity: authorization.release_identity,
        executable_sha256: peer.executable_sha256.clone(),
        release_signature: authorization.release_signature,
        peer_uid: peer.uid,
        executable_owned_by_root: peer.owned_by_root,
        installation_path_read_only: peer.installation_path_read_only,
    })
}

pub fn random_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}

#[derive(Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum BrokerResponse {
    Negotiated {
        protocol: &'static str,
        challenge: String,
    },
    Status {
        protocol: &'static str,
        locked: bool,
        repository_id: String,
        capsule_generation: u64,
        capsule_logical_id: String,
        policy_hash: String,
        epoch_id: Option<String>,
        active_sessions: usize,
        active_leases: usize,
        minimum_custodians: usize,
        principal_verified: bool,
        hardware_verified: bool,
        custody_assumed: bool,
        compliant: bool,
        findings: Vec<String>,
        policy_mutation_pending: bool,
        pending_capsule_generation: Option<u64>,
        pending_capsule_sha256: Option<String>,
        identity_recovery: bool,
    },
    Session {
        session: SignedSession,
    },
    Contribution {
        unlocked: bool,
    },
    Lease {
        lease_id: String,
        epoch_id: String,
        capability: Capability,
        expires_unix_ms: u64,
        key_version: u32,
        capsule_generation: u64,
        key: String,
    },
    PolicyMutationPrepared {
        capsule: RecoveryCapsule,
        capsule_sha256: String,
    },
    Ok,
    Error {
        code: &'static str,
        message: String,
    },
}

pub struct PeerProcess {
    pub uid: u32,
    pub executable_sha256: String,
    pub owned_by_root: bool,
    pub installation_path_read_only: bool,
}

#[derive(Default)]
pub struct ConnectionProtocol {
    pub negotiated: bool,
    pub lease_challenge: Option<String>,
}

pub fn lease_challenge_response(challenge: &str, executable_sha256: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!(
            "vaultic-broker-lease-challenge-v1\0{PROTOCOL_VERSION}\0{challenge}\0{executable_sha256}"
        ))
    )
}

pub fn encode_key(key: &[u8]) -> String {
    BASE64.encode(key)
}
