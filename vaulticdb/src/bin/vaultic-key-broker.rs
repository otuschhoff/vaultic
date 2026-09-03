#![allow(clippy::result_large_err)]

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    os::{
        fd::AsRawFd,
        unix::fs::{FileTypeExt, MetadataExt},
    },
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use ed25519_dalek::{Signer, SigningKey};
use rand::RngCore;
use rand08::rngs::OsRng as LegacyOsRng;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{UnixListener, UnixStream},
    sync::Mutex,
    sync::Notify,
};
use vaulticdb::{
    broker::{
        unix_time_ms, Capability, ClientAuthorization, ClientIdentity, EncryptedContribution,
        KeyBroker,
    },
    encryption::recovery_capsule::{
        ExternalMemberProtection, HardwareBinding, MemberCredential, MemberProtection,
        MemberProvider, PrincipalBinding, RecoveryCapsule, UnlockPolicy,
    },
};
use zeroize::Zeroizing;

const PROTOCOL_VERSION: &str = "vaultic-key-broker.v1";
const MAX_REQUEST_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BrokerConfig {
    format: u32,
    capsule_directory: PathBuf,
    repository_id: String,
    identity_key_path: PathBuf,
    socket_path: PathBuf,
    #[serde(default)]
    maximum_unlocked_seconds: Option<u64>,
    #[serde(default)]
    identity_recovery: bool,
    authorizations: Vec<FileAuthorization>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileAuthorization {
    component: String,
    minimum_version: u64,
    maximum_version: u64,
    release_identity: String,
    release_public_key: String,
    peer_uid: u32,
    capabilities: BTreeSet<Capability>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuthorizedOperation {
    component: String,
    version: u64,
    release_identity: String,
    release_signature: String,
    challenge_response: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OfflinePolicyMember {
    member_id: String,
    provider: MemberProvider,
    credential: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExternalPolicyMember {
    member_id: String,
    provider: MemberProvider,
    key_reference: String,
    principal: Option<PrincipalBinding>,
    hardware: Option<HardwareBinding>,
    bearer_token: Option<String>,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum BrokerRequest {
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

#[derive(Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
enum BrokerResponse {
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
        session: vaulticdb::broker::SignedSession,
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

struct PeerProcess {
    uid: u32,
    executable_sha256: String,
    owned_by_root: bool,
    installation_path_read_only: bool,
}

#[derive(Default)]
struct ConnectionProtocol {
    negotiated: bool,
    lease_challenge: Option<String>,
}

#[derive(Serialize)]
struct BrokerSecurityEvent<'a> {
    timestamp_unix_ms: u64,
    severity: &'a str,
    category: &'a str,
    component: &'static str,
    event: &'a str,
    fields: BTreeMap<&'a str, String>,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    disable_core_dumps();
    let arguments = env::args_os().skip(1).collect::<Vec<_>>();
    if arguments
        .first()
        .is_some_and(|argument| argument == "identity-init")
    {
        return identity_init(&arguments[1..]);
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "release-sign")
    {
        return release_sign(&arguments[1..]);
    }
    let config_path = arguments
        .first()
        .map(PathBuf::from)
        .or_else(|| env::var_os("VAULTIC_KEY_BROKER_CONFIG").map(PathBuf::from))
        .context("usage: vaultic-key-broker CONFIG.json | identity-init PRIVATE PUBLIC | release-sign PRIVATE EXECUTABLE COMPONENT VERSION IDENTITY OUTPUT")?;
    require_private_regular_file(&config_path)?;
    let config: BrokerConfig =
        serde_json::from_slice(&fs::read(&config_path)?).context("decode broker config")?;
    if config.format != 1 || config.authorizations.is_empty() {
        bail!("unsupported broker config or empty authorization policy");
    }
    require_private_regular_file(&config.identity_key_path)?;
    let (_, capsule): (_, RecoveryCapsule) =
        vaulticdb::encryption::recovery_capsule::discover_latest(
            &config.capsule_directory,
            &config.repository_id,
        )?
        .context("no recovery capsule generation found")?;
    emit_security_event(
        "notice",
        "integrity",
        "capsule_discovered",
        &[
            ("repository_id", config.repository_id.clone()),
            ("capsule_generation", capsule.header.generation.to_string()),
            ("capsule_logical_id", capsule.header.logical_id.clone()),
        ],
    );
    let identity_bytes = fs::read(&config.identity_key_path)?;
    let identity = SigningKey::from_bytes(
        &identity_bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("broker identity key must contain exactly 32 bytes"))?,
    );
    let authorizations = config
        .authorizations
        .into_iter()
        .map(|authorization| {
            let public_key = BASE64
                .decode(&authorization.release_public_key)
                .context("decode release public key")?;
            Ok(ClientAuthorization {
                component: authorization.component,
                minimum_version: authorization.minimum_version,
                maximum_version: authorization.maximum_version,
                release_identity: authorization.release_identity,
                release_public_key: public_key
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("release public key must be 32 bytes"))?,
                peer_uid: authorization.peer_uid,
                capabilities: authorization.capabilities,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let maximum_lifetime = config.maximum_unlocked_seconds.map(Duration::from_secs);
    let broker = Arc::new(Mutex::new(if config.identity_recovery {
        KeyBroker::new_identity_recovery(capsule, identity, authorizations, maximum_lifetime)?
    } else {
        KeyBroker::new(capsule, identity, authorizations, maximum_lifetime)?
    }));
    let lock_notification = Arc::new(Notify::new());

    prepare_socket_parent(&config.socket_path)?;
    remove_stale_socket(&config.socket_path).await?;
    let listener = UnixListener::bind(&config.socket_path)
        .with_context(|| format!("bind broker socket {}", config.socket_path.display()))?;
    set_mode(&config.socket_path, 0o600)?;
    let endpoint_binding = format!("unix:{}", config.socket_path.display());

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let broker = broker.clone();
                let endpoint_binding = endpoint_binding.clone();
                let lock_notification = lock_notification.clone();
                tokio::spawn(async move {
                    if let Err(error) = serve_connection(stream, broker, endpoint_binding, lock_notification).await {
                        eprintln!("vaultic-key-broker: connection rejected: {error:#}");
                    }
                });
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                broker.lock().await.lock();
                emit_security_event("critical", "lifecycle", "broker_locked_for_shutdown", &[]);
                break;
            }
        }
    }
    drop(listener);
    let _ = fs::remove_file(&config.socket_path);
    Ok(())
}

fn identity_init(arguments: &[std::ffi::OsString]) -> Result<()> {
    if arguments.len() != 2 {
        bail!("usage: vaultic-key-broker identity-init PRIVATE PUBLIC");
    }
    let private_path = PathBuf::from(&arguments[0]);
    let public_path = PathBuf::from(&arguments[1]);
    let identity = SigningKey::generate(&mut LegacyOsRng);
    write_new_file(&private_path, identity.as_bytes(), 0o600)?;
    let mut public = BASE64
        .encode(identity.verifying_key().as_bytes())
        .into_bytes();
    public.push(b'\n');
    if let Err(error) = write_new_file(&public_path, &public, 0o644) {
        let _ = fs::remove_file(private_path);
        return Err(error);
    }
    Ok(())
}

fn release_sign(arguments: &[std::ffi::OsString]) -> Result<()> {
    if arguments.len() != 6 {
        bail!("usage: vaultic-key-broker release-sign PRIVATE EXECUTABLE COMPONENT VERSION IDENTITY OUTPUT");
    }
    let private_path = PathBuf::from(&arguments[0]);
    let executable = PathBuf::from(&arguments[1]);
    let component = arguments[2].to_string_lossy().into_owned();
    let version = arguments[3]
        .to_string_lossy()
        .parse::<u64>()
        .context("release version must be an unsigned integer")?;
    let release_identity = arguments[4].to_string_lossy().into_owned();
    let output = PathBuf::from(&arguments[5]);
    if component.is_empty() || release_identity.is_empty() || version == 0 {
        bail!("component, non-zero version, and release identity are required");
    }
    require_private_regular_file(&private_path)?;
    let mut private = zeroize::Zeroizing::new(fs::read(&private_path)?);
    let signing_key = SigningKey::from_bytes(
        &private
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("release signing key must contain exactly 32 bytes"))?,
    );
    private.fill(0);
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable)?));
    let manifest_bytes = serde_json::to_vec(&(
        "vaultic-client-release-v1",
        &component,
        version,
        &executable_sha256,
        &release_identity,
    ))?;
    let signature = BASE64.encode(signing_key.sign(&manifest_bytes).to_bytes());
    let output_value = serde_json::json!({
        "component": component,
        "version": version,
        "release_identity": release_identity,
        "executable_sha256": executable_sha256,
        "signature": signature,
    });
    let mut encoded = serde_json::to_vec_pretty(&output_value)?;
    encoded.push(b'\n');
    write_new_file(&output, &encoded, 0o644)
}

fn write_new_file(path: &Path, contents: &[u8], mode: u32) -> Result<()> {
    use std::io::Write;
    use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(mode)
        .open(path)
        .with_context(|| format!("create {}", path.display()))?;
    if let Err(error) = file.write_all(contents).and_then(|_| file.sync_all()) {
        let _ = fs::remove_file(path);
        return Err(error).with_context(|| format!("write {}", path.display()));
    }
    file.set_permissions(fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn serve_connection(
    stream: UnixStream,
    broker: Arc<Mutex<KeyBroker>>,
    endpoint_binding: String,
    lock_notification: Arc<Notify>,
) -> Result<()> {
    let peer = inspect_peer(&stream)?;
    let connection_id = random_id();
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut request = Vec::new();
    let mut protocol = ConnectionProtocol::default();
    loop {
        request.clear();
        let read = tokio::select! {
            read = reader.read_until(b'\n', &mut request) => read.context("read broker request")?,
            _ = lock_notification.notified() => break,
        };
        if read == 0 {
            break;
        }
        let response = if request.len() > MAX_REQUEST_BYTES {
            BrokerResponse::Error {
                code: "request_too_large",
                message: "broker request exceeds size limit".to_owned(),
            }
        } else {
            match serde_json::from_slice::<BrokerRequest>(&request) {
                Ok(request) => {
                    let requests_lock = matches!(
                        &request,
                        BrokerRequest::Lock | BrokerRequest::ActivatePolicyMutation { .. }
                    );
                    let response = handle_request(
                        request,
                        &broker,
                        &connection_id,
                        &peer,
                        &endpoint_binding,
                        &mut protocol,
                    )
                    .await
                    .unwrap_or_else(|error| {
                        emit_security_event(
                            "warning",
                            "auth",
                            "request_rejected",
                            &[("connection_id", connection_id.clone())],
                        );
                        BrokerResponse::Error {
                            code: "request_rejected",
                            message: error.to_string(),
                        }
                    });
                    if requests_lock {
                        lock_notification.notify_waiters();
                    }
                    response
                }
                Err(error) => BrokerResponse::Error {
                    code: "invalid_request",
                    message: error.to_string(),
                },
            }
        };
        let mut encoded = serde_json::to_vec(&response)?;
        encoded.push(b'\n');
        writer.write_all(&encoded).await?;
        writer.flush().await?;
    }
    broker.lock().await.disconnect(&connection_id);
    emit_security_event(
        "notice",
        "lifecycle",
        "connection_closed_leases_revoked",
        &[("connection_id", connection_id)],
    );
    Ok(())
}

async fn handle_request(
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
                repository_id: status.repository_id,
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
                    ("session_id", session.transcript.session_id.clone()),
                    ("repository_id", session.transcript.repository_id.clone()),
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
                    ("session_id", session_id),
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
                    _ => bail!("unsupported external policy member provider"),
                };
                let provider = vaulticdb::encryption::envelope::providers::for_management(
                    provider_name,
                    member.bearer_token.clone(),
                )
                .await?;
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
                    ("repository_id", capsule.header.repository_id.clone()),
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
            broker.lock();
            emit_security_event(
                "critical",
                "lifecycle",
                "broker_locked",
                &[("connection_id", connection_id.to_owned())],
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

fn inspect_peer(stream: &UnixStream) -> Result<PeerProcess> {
    let credentials = stream.peer_cred().context("read Unix peer credentials")?;
    let uid = credentials.uid();
    let pid = peer_pid(stream, &credentials)?;
    let executable = peer_executable(pid)?;
    let metadata = fs::metadata(&executable)
        .with_context(|| format!("inspect peer executable {}", executable.display()))?;
    let owned_by_root = metadata.uid() == 0;
    let installation_path_read_only = trusted_installation_path(&executable)?;
    let executable_sha256 = format!("{:x}", Sha256::digest(fs::read(&executable)?));
    Ok(PeerProcess {
        uid,
        executable_sha256,
        owned_by_root,
        installation_path_read_only,
    })
}

#[cfg(target_os = "linux")]
fn peer_pid(_stream: &UnixStream, credentials: &tokio::net::unix::UCred) -> Result<u32> {
    credentials
        .pid()
        .map(|pid| pid as u32)
        .context("Unix peer did not expose a process ID")
}

#[cfg(target_os = "macos")]
fn peer_pid(stream: &UnixStream, _credentials: &tokio::net::unix::UCred) -> Result<u32> {
    let mut pid: libc::pid_t = 0;
    let mut length = std::mem::size_of::<libc::pid_t>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            (&mut pid as *mut libc::pid_t).cast(),
            &mut length,
        )
    };
    if result != 0 || pid <= 0 {
        return Err(std::io::Error::last_os_error()).context("read Unix peer process ID");
    }
    Ok(pid as u32)
}

#[cfg(target_os = "linux")]
fn peer_executable(pid: u32) -> Result<PathBuf> {
    fs::read_link(format!("/proc/{pid}/exe")).context("resolve peer executable")
}

#[cfg(target_os = "macos")]
fn peer_executable(pid: u32) -> Result<PathBuf> {
    let mut buffer = vec![0_u8; libc::PROC_PIDPATHINFO_MAXSIZE as usize];
    let length = unsafe {
        libc::proc_pidpath(
            pid as libc::pid_t,
            buffer.as_mut_ptr().cast(),
            buffer.len() as u32,
        )
    };
    if length <= 0 {
        return Err(std::io::Error::last_os_error()).context("resolve peer executable");
    }
    buffer.truncate(length as usize);
    Ok(PathBuf::from(std::ffi::OsString::from_vec(buffer)))
}

#[cfg(target_os = "macos")]
use std::os::unix::ffi::OsStringExt;

fn trusted_installation_path(executable: &Path) -> Result<bool> {
    let canonical = executable.canonicalize()?;
    for path in canonical.ancestors() {
        let metadata = fs::symlink_metadata(path)?;
        if metadata.file_type().is_symlink() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0
        {
            return Ok(false);
        }
    }
    Ok(true)
}

fn require_private_regular_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("inspect private file {}", path.display()))?;
    if !metadata.file_type().is_file()
        || metadata.file_type().is_symlink()
        || metadata.mode() & 0o077 != 0
    {
        bail!(
            "{} must be a non-symlink regular file with mode 0600 or stricter",
            path.display()
        );
    }
    Ok(())
}

fn prepare_socket_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("broker socket has no parent directory")?;
    fs::create_dir_all(parent)?;
    set_mode(parent, 0o700)
}

fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

async fn remove_stale_socket(path: &Path) -> Result<()> {
    match UnixStream::connect(path).await {
        Ok(_) => bail!("broker socket {} is already active", path.display()),
        Err(_) if path.exists() => {
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.file_type().is_socket() {
                bail!("refusing to replace non-socket path {}", path.display());
            }
            fs::remove_file(path)?;
        }
        Err(_) => {}
    }
    Ok(())
}

fn random_id() -> String {
    let mut bytes = [0_u8; 16];
    rand::rng().fill_bytes(&mut bytes);
    BASE64.encode(bytes)
}

fn lease_challenge_response(challenge: &str, executable_sha256: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!(
            "vaultic-broker-lease-challenge-v1\0{PROTOCOL_VERSION}\0{challenge}\0{executable_sha256}"
        ))
    )
}

fn emit_security_event(severity: &str, category: &str, event: &str, fields: &[(&str, String)]) {
    match security_event_json(severity, category, event, fields) {
        Ok(encoded) => eprintln!("{encoded}"),
        Err(error) => eprintln!("vaultic-key-broker: security event rejected: {error}"),
    }
}

fn security_event_json(
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

fn disable_core_dumps() {
    #[cfg(unix)]
    unsafe {
        let limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        libc::setrlimit(libc::RLIMIT_CORE, &limit);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signature, Verifier};
    use std::ffi::OsString;
    use vaulticdb::{
        broker::{encrypt_offline_contribution, ClientAuthorization},
        encryption::recovery_capsule::{CapsuleBuilder, MemberCredential},
    };

    async fn exchange(stream: &mut UnixStream, request: serde_json::Value) -> serde_json::Value {
        let mut encoded = serde_json::to_vec(&request).unwrap();
        encoded.push(b'\n');
        stream.write_all(&encoded).await.unwrap();
        let mut response = Vec::new();
        BufReader::new(stream)
            .read_until(b'\n', &mut response)
            .await
            .unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[tokio::test]
    async fn peer_inspection_uses_kernel_identity_and_running_executable() {
        let (peer, _other) = UnixStream::pair().unwrap();
        let inspected = inspect_peer(&peer).unwrap();
        let executable = env::current_exe().unwrap();
        assert_eq!(inspected.uid, unsafe { libc::geteuid() });
        assert_eq!(
            inspected.executable_sha256,
            format!("{:x}", Sha256::digest(fs::read(&executable).unwrap()))
        );
        assert_eq!(
            inspected.owned_by_root,
            fs::metadata(&executable).unwrap().uid() == 0
        );
        assert_eq!(
            inspected.installation_path_read_only,
            trusted_installation_path(&executable).unwrap()
        );
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn installation_path_rejects_mutable_ancestors_and_accepts_system_binary() {
        let root = env::temp_dir().join(format!(
            "vaultic-broker-untrusted-path-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let executable = root.join("vaulticdb");
        fs::write(&executable, b"not an executable").unwrap();
        assert!(!trusted_installation_path(&executable).unwrap());
        fs::remove_dir_all(root).unwrap();

        assert!(trusted_installation_path(Path::new("/bin/sh")).unwrap());
    }

    #[test]
    fn provisioning_creates_pinned_identity_and_verifiable_release() {
        let root = env::temp_dir().join(format!(
            "vaultic-broker-provisioning-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        fs::create_dir(&root).unwrap();
        let identity_private = root.join("identity.key");
        let identity_public = root.join("identity.pub");
        identity_init(&[
            identity_private.clone().into_os_string(),
            identity_public.clone().into_os_string(),
        ])
        .unwrap();
        assert_eq!(fs::read(&identity_private).unwrap().len(), 32);
        assert_eq!(
            BASE64
                .decode(
                    String::from_utf8(fs::read(&identity_public).unwrap())
                        .unwrap()
                        .trim()
                )
                .unwrap()
                .len(),
            32
        );

        let release_private = root.join("release.key");
        let release_key = SigningKey::from_bytes(&[9; 32]);
        write_new_file(&release_private, release_key.as_bytes(), 0o600).unwrap();
        let executable = env::current_exe().unwrap();
        let manifest_path = root.join("release.json");
        release_sign(&[
            release_private.into_os_string(),
            executable.clone().into_os_string(),
            OsString::from("vaulticdb"),
            OsString::from("7"),
            OsString::from("release-a"),
            manifest_path.clone().into_os_string(),
        ])
        .unwrap();
        let manifest: vaulticdb::broker::ReleaseManifest =
            serde_json::from_slice(&fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(
            manifest.executable_sha256,
            format!("{:x}", Sha256::digest(fs::read(executable).unwrap()))
        );
        let message = serde_json::to_vec(&(
            "vaultic-client-release-v1",
            &manifest.component,
            manifest.version,
            &manifest.executable_sha256,
            &manifest.release_identity,
        ))
        .unwrap();
        let signature = Signature::from_slice(&BASE64.decode(manifest.signature).unwrap()).unwrap();
        release_key
            .verifying_key()
            .verify(&message, &signature)
            .unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn lease_challenge_is_protocol_and_executable_bound() {
        let challenge = "challenge-a";
        let digest = "ab".repeat(32);
        let response = lease_challenge_response(challenge, &digest);
        assert_eq!(response.len(), 64);
        assert_eq!(response, lease_challenge_response(challenge, &digest));
        assert_ne!(response, lease_challenge_response("challenge-b", &digest));
        assert_ne!(
            response,
            lease_challenge_response(challenge, &"cd".repeat(32))
        );
    }

    #[test]
    fn connection_protocol_consumes_lease_challenge_once() {
        let mut protocol = ConnectionProtocol {
            negotiated: true,
            lease_challenge: Some("challenge-a".to_owned()),
        };
        assert_eq!(
            protocol.lease_challenge.take().as_deref(),
            Some("challenge-a")
        );
        assert!(protocol.lease_challenge.take().is_none());
    }

    #[test]
    fn security_events_reject_secret_bearing_fields() {
        assert!(security_event_json(
            "notice",
            "auth",
            "lease_granted",
            &[
                ("component", "vaultic".to_owned()),
                ("release_identity", "release-a".to_owned())
            ],
        )
        .unwrap()
        .contains("\"component\":\"vaultic-key-broker\""));
        assert!(security_event_json(
            "warning",
            "auth",
            "request_rejected",
            &[("bearer_token", "secret-token".to_owned())],
        )
        .is_err());
    }

    #[tokio::test]
    async fn unix_service_negotiates_connections_independently_and_broadcasts_lock() {
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let capsule = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(identity.verifying_key().as_bytes())
            .create_offline_threshold(
                "operators",
                1,
                &[("alice", MemberCredential::Passphrase(b"alice passphrase"))],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        let broker = Arc::new(Mutex::new(
            KeyBroker::new(capsule, identity, Vec::new(), None).unwrap(),
        ));
        let notification = Arc::new(Notify::new());
        let (mut client_a, server_a) = UnixStream::pair().unwrap();
        let (mut client_b, server_b) = UnixStream::pair().unwrap();
        let task_a = tokio::spawn(serve_connection(
            server_a,
            broker.clone(),
            "unix:/test/broker.sock".to_owned(),
            notification.clone(),
        ));
        let task_b = tokio::spawn(serve_connection(
            server_b,
            broker.clone(),
            "unix:/test/broker.sock".to_owned(),
            notification,
        ));

        for stream in [&mut client_a, &mut client_b] {
            let response = exchange(
                stream,
                serde_json::json!({"operation":"negotiate","protocols":[PROTOCOL_VERSION]}),
            )
            .await;
            assert_eq!(response["result"], "negotiated");
            assert_eq!(response["protocol"], PROTOCOL_VERSION);
            assert!(response["challenge"]
                .as_str()
                .is_some_and(|value| !value.is_empty()));
        }
        let status = exchange(&mut client_b, serde_json::json!({"operation":"status"})).await;
        assert_eq!(status["result"], "status");
        assert_eq!(status["repository_id"], "repo-a");

        let locked = exchange(&mut client_a, serde_json::json!({"operation":"lock"})).await;
        assert_eq!(locked["result"], "ok");
        let mut closed = Vec::new();
        assert_eq!(
            client_b
                .readable()
                .await
                .and_then(|_| client_b.try_read_buf(&mut closed))
                .unwrap(),
            0
        );

        drop(client_a);
        task_a.await.unwrap().unwrap();
        task_b.await.unwrap().unwrap();
        assert!(broker.lock().await.status(unix_time_ms().unwrap()).locked);
    }

    #[tokio::test]
    async fn protocol_policy_mutation_retains_candidate_until_exact_activation() {
        let identity = SigningKey::generate(&mut LegacyOsRng);
        let release_key = SigningKey::from_bytes(&[6; 32]);
        let capsule = CapsuleBuilder::new("repo-a", 1)
            .broker_identity_public_key(identity.verifying_key().as_bytes())
            .create_offline_threshold(
                "operators",
                1,
                &[("alice", MemberCredential::Passphrase(b"alice passphrase"))],
                &[7; 32],
                b"repository-master-key",
            )
            .unwrap();
        let authorizations = vec![ClientAuthorization {
            component: "vaultic".to_owned(),
            minimum_version: 20,
            maximum_version: 20,
            release_identity: "release-a".to_owned(),
            release_public_key: release_key.verifying_key().to_bytes(),
            peer_uid: 42,
            capabilities: BTreeSet::from([Capability::PolicyMutation]),
        }];
        let mut state = KeyBroker::new(capsule.clone(), identity, authorizations, None).unwrap();
        let session = state
            .create_session("unix:/test/broker.sock", Duration::from_secs(60), 1_000)
            .unwrap();
        let contribution = encrypt_offline_contribution(
            &capsule,
            &session,
            "unix:/test/broker.sock",
            "alice",
            &MemberCredential::Passphrase(b"alice passphrase"),
            1,
            None,
            1_001,
        )
        .unwrap();
        assert!(state.submit_contribution(contribution, 1_002).unwrap());
        let broker = Mutex::new(state);
        let peer = PeerProcess {
            uid: 42,
            executable_sha256: "ab".repeat(32),
            owned_by_root: true,
            installation_path_read_only: true,
        };
        let authorize = |challenge: &str| {
            let manifest = serde_json::to_vec(&(
                "vaultic-client-release-v1",
                "vaultic",
                20_u64,
                &peer.executable_sha256,
                "release-a",
            ))
            .unwrap();
            AuthorizedOperation {
                component: "vaultic".to_owned(),
                version: 20,
                release_identity: "release-a".to_owned(),
                release_signature: BASE64.encode(release_key.sign(&manifest).to_bytes()),
                challenge_response: lease_challenge_response(challenge, &peer.executable_sha256),
            }
        };
        let mut protocol = ConnectionProtocol {
            negotiated: true,
            lease_challenge: Some("prepare-challenge".to_owned()),
        };
        let prepared = handle_request(
            BrokerRequest::PreparePolicyMutation {
                authorization: authorize("prepare-challenge"),
                policy: UnlockPolicy::Threshold {
                    group_id: "operators".to_owned(),
                    required: 1,
                    members: vec!["bob".to_owned()],
                },
                members: vec![OfflinePolicyMember {
                    member_id: "bob".to_owned(),
                    provider: MemberProvider::OfflineArgon2id,
                    credential: BASE64.encode(b"bob passphrase"),
                }],
                external_members: Vec::new(),
                acknowledge_downgrade: false,
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        let digest = match prepared {
            BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            } => {
                assert_eq!(capsule.header.generation, 2);
                capsule_sha256
            }
            _ => panic!("unexpected mutation response"),
        };
        protocol.lease_challenge = Some("pending-challenge".to_owned());
        let pending = handle_request(
            BrokerRequest::PendingPolicyMutation {
                authorization: authorize("pending-challenge"),
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        match pending {
            BrokerResponse::PolicyMutationPrepared {
                capsule,
                capsule_sha256,
            } => {
                assert_eq!(capsule.header.generation, 2);
                assert_eq!(capsule_sha256, digest);
            }
            _ => panic!("unexpected pending mutation response"),
        }
        protocol.lease_challenge = Some("wrong-challenge".to_owned());
        assert!(handle_request(
            BrokerRequest::ActivatePolicyMutation {
                authorization: authorize("wrong-challenge"),
                capsule_sha256: "00".repeat(32),
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .is_err());
        assert!(broker.lock().await.status(1_003).policy_mutation_pending);
        protocol.lease_challenge = Some("activate-challenge".to_owned());
        handle_request(
            BrokerRequest::ActivatePolicyMutation {
                authorization: authorize("activate-challenge"),
                capsule_sha256: digest,
            },
            &broker,
            "connection-a",
            &peer,
            "unix:/test/broker.sock",
            &mut protocol,
        )
        .await
        .unwrap();
        let status = broker.lock().await.status(1_004);
        assert!(status.locked);
        assert_eq!(status.capsule_generation, 2);
        assert!(!status.policy_mutation_pending);
    }
}
