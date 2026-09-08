# Phase 23: Ephemeral cloud storage credentials, remote principals, and brokered VaulticDB access tickets

[← Back to roadmap index](00-overview.md)

[← Phase 22](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md)

[Broker implementation and state machines](../02-architecture/08-quorum-key-broker.md) · [Cloud object storage operator guide](../../051_cloud_object_storage.rst)

**Status: design specification, not yet implemented.**

**Goal:** remove long-lived cloud object-storage credentials from every `vaultic` and `vaulticdb` process. The Phase 20 `vaultic-key-broker` becomes the sole holder of the *issuer credential* for each S3-compatible, Azure Blob, or Google Cloud Storage location, and hands out short-lived, least-privilege *storage credentials* scoped to what a specific job needs: read only, read plus create, or read plus delete for maintenance. Credential lifetime and proactive renewal margin are configurable so operators can decide how long consumers keep working when the broker is down, locked, or waiting for a custodian ceremony. Extend the same brokered model across hosts: a `vaultic` backup worker or a read-only `vaulticdb` on a second host authenticates to the main host's broker with an enrolled key, receives scoped storage credentials, and receives a short-lived *access ticket* that authorizes an authenticated connection to the main host's read-write `vaulticdb`. A compromised worker host must gain at most time-limited read access to data it was already allowed to read and time-limited, attributable, additive metadata writes; it must never gain the ability to delete or overwrite existing repository data, delete metadata, change key policy, or mint credentials for other principals.

## Threat model and trust boundaries

Three trust zones exist:

- **Main host.** Runs the unlocked `vaultic-key-broker`, the read-write `vaulticdb`, and ordinary local `vaultic` jobs. Compromise of this host during an unlock epoch is out of scope, exactly as in Phase 20.
- **Worker host.** Runs `vaultic` backup, restore, or check jobs and optionally a read-only `vaulticdb`. It holds no cloud credential, no capsule, no broker identity, and no key material at rest except its own principal authentication key. It is assumed to be compromisable.
- **Cloud provider.** Enforces credential scope and expiry. The design relies on provider-enforced scoping; Vaultic-side checks are defense in depth, not the security boundary.

Damage bound for a fully compromised worker host while its principal remains enrolled:

| Attacker capability | Bounded by |
|---|---|
| Read repository objects the principal may read | Storage-credential scope and TTL; repository master-key lease TTL; principal revocation. |
| Decrypt those objects | The repository master-key lease that a backup job legitimately needs (Phase 20 limitation; the Restic-compatible format has no per-job data key). |
| Create new objects | Create-only scope, bucket versioning and immutability, staging quotas (Phase 22). Cost and noise, not data loss. |
| Overwrite or delete existing objects | **Not possible** with correctly scoped credentials; overwrite of versioned objects only creates a new version and is detectable because objects are content-addressed. |
| Write metadata to read-write VaulticDB | `metadata-append` ticket scope: additive keys only, no deletes, no lifecycle transitions toward deletion, attributed to the principal, revertible per principal. |
| Delete or prune metadata, change writer role, rotate keys | **Not possible**; `metadata-maintain` and `admin` scopes are never issuable to remote principals by default. |
| Mint credentials for another principal or repository | **Not possible**; issuance is bound to the authenticated principal's enrollment record. |
| Persist access after operator revocation | Bounded by the shortest of credential TTL, ticket TTL, and provider revocation latency. |

The residual accepted risk is exactly the one stated in the goal: time-limited read access to data the worker could already read. Everything else fails closed.

## Design

### Storage capability tiers

Every brokered storage credential carries one tier. Tiers are ordered; a request may be granted a tier equal to or lower than the principal's enrollment allows.

| Tier | Vaultic operations | Provider permissions (conceptual) |
|---|---|---|
| `storage-read` | restore, check, ls, read-only `vaulticdb`, `read-only-assisted` crawl | list, get |
| `storage-append` | backup, deferred staging journal publication, placement copy | list, get, create-new-object; no overwrite, no delete |
| `storage-maintain` | prune, gc, forget, placement eviction, journal abandonment, metadata rebuild | list, get, create, delete (and version delete only where policy explicitly allows) |
| `storage-admin` | bucket policy, lifecycle, Object Lock changes | never issued by the broker; operator tooling only |

"Append" means create-only. Vaultic clients always write with a create-only precondition (`If-None-Match: *` on S3 and compatible stores, `If-None-Match: *` on Azure, `ifGenerationMatch=0` on GCS). The broker additionally scopes the credential so the provider itself rejects overwrite where the provider can express it, and the operator guide requires bucket versioning with immutability so that a store that cannot express "create-only" still preserves the prior version.

Provider mapping:

| Provider | Issuance mechanism | Scope expression | Practical TTL bounds (verify against current provider limits) |
|---|---|---|---|
| AWS S3 | `sts:AssumeRole` with an inline session policy | IAM actions and prefix conditions in the session policy; `s3:DeleteObject*` absent for `read`/`append` | 15 minutes to the role's maximum session duration (up to 12 hours; 1 hour when role chaining) |
| S3-compatible with STS (MinIO, Ceph RGW, others) | `AssumeRole` or `AssumeRoleWithWebIdentity` with session policy | Same policy language subset | Provider-specific; MinIO permits up to 7 days |
| S3-compatible without STS | Not supported for brokered credentials | — | Operators keep a rotated static credential per tier on the main host only; remote principals get no storage credential for that location and the location is reported non-compliant |
| Azure Blob Storage | User delegation SAS derived from a user delegation key obtained with the broker's Entra identity | Permission letters: `rl` for read, `rlc` for append (create without write), `rlcd` for maintain; container or prefix scoped | Up to 7 days for the delegation key; SAS expiry bounded by it |
| Google Cloud Storage | Service-account impersonation `generateAccessToken` followed by a Credential Access Boundary (downscoped token) | `roles/storage.objectViewer` for read, plus `roles/storage.objectCreator` for append; delete roles only for maintain; availability condition on the object prefix | 1 hour by default, up to 12 hours where the organization policy permits |

The broker refuses to issue a tier the provider cannot enforce for that location and reports why. Vaultic never silently downgrades a request from `append` to `maintain` or widens a prefix.

### Issuer credential custody

The issuer credential is the cloud identity able to mint scoped credentials. It is never present on a worker host and never present in a `vaultic` or `vaulticdb` process on any host. Two custody modes are supported:

1. **Sealed static issuer credential.** A long-lived access key, client secret, or service-account key is stored in a versioned issuer definition encrypted under a purpose-separated key derived from the recovered capsule material (`HKDF(purpose = "cloud-issuer-kek", repository, generation)`). It is decryptable only while the broker is unlocked, so cold theft of the broker host yields ciphertext only. Rotating the cloud secret rewrites the issuer definition without publishing a new capsule generation.
2. **Ambient issuer identity.** The broker host's own workload identity (instance profile, managed identity, attached service account) holds only `sts:AssumeRole`, `generateUserDelegationKey`, or `iam.serviceAccounts.getAccessToken` on the issuer role. No secret is stored anywhere. Issuance remains gated on an unlocked epoch by policy, so that a locked broker cannot be used as an oracle even though the ambient identity is always available.

Issuer definitions are owner-only, root-owned files in an `issuers.d/` directory, one per storage location, binding location ID, provider, issuer role or key reference, tenant/account/project, maximum tier, maximum TTL, permitted prefixes, and the immutable repository IDs they may serve. Vaultic and VaulticDB reference locations by ID and never see issuer material. Status reports every location's custody mode and flags any location whose only path is a static credential outside the broker as non-compliant.

### Credential issuance and lifetime semantics

A consumer requests `(location, tier, requested_ttl)` from the broker through an authenticated channel (local Unix socket with Phase 20 peer authorization, or the remote channel below). The broker validates the principal's enrollment, clamps `requested_ttl` to the minimum of the principal maximum, issuer maximum, provider maximum, and remaining unlock-epoch lifetime if a maximum unlocked lifetime is configured, mints the credential, and returns `{credential, not_before, not_after, tier, location, issue_id}`. Credentials are returned only over the authenticated channel, are never logged, and are held by the consumer in zeroizing memory.

Three consumer-side durations govern behavior, each configurable per component and per job:

- **`storage-token-ttl`** — requested credential lifetime. Default 1 hour.
- **`storage-token-renew-margin`** — how long before `not_after` the consumer starts proactive renewal. Default 20 minutes or one third of the TTL, whichever is larger. Renewal issues a *new* credential; provider tokens cannot be extended. The old credential stays valid until its own expiry, which gives in-flight multipart uploads and long reads a natural overlap window.
- **`broker-outage-grace`** — maximum time a consumer keeps operating after its last successful broker contact, independent of the current credential's remaining validity. Default equals the TTL, meaning "run until the credential expires." Operators who treat an unreachable or locked broker as a security signal set it lower.

The margin must cover the realistic worst case for the broker being unavailable: process restart, host reboot, and the time custodians need to complete a Phase 20 unlock ceremony. The documented rule is `renew_margin ≥ expected_ceremony_time + retry_backoff`, and `ttl ≥ renew_margin + minimum_useful_work_window`. A locked broker answers renewal with a typed `locked` response; the consumer keeps its current credential, emits a warning event containing time-to-expiry, and retries with bounded jittered backoff until success, expiry, or `broker-outage-grace`.

Expiry and grace exhaustion have component-specific, fail-closed behavior:

- **Read-only `vaulticdb`** stops admitting new reads at the earlier of `not_after` and grace exhaustion, drains in-flight RPCs, and reports `credential-expired` in health. It does not fall back to any cached or ambient credential.
- **`vaultic` backup** stops starting new pack uploads at `not_after - upload_safety_margin`, finishes or aborts in-flight uploads before expiry, and then either completes normally if metadata commit is still possible or exits with the Phase 22 `data_durable_metadata_pending` result if the sealed journal is durable. It never claims completion for data it could not publish.
- **`vaultic` restore or check** finishes the current object and exits with a distinct `credential-expired` status.
- **Maintenance jobs** (`prune`, `gc`) never begin a deletion batch they cannot finish within the current credential validity, and abort cleanly between batches.

Revocation is coarse on every provider and is documented as such: AWS supports denying all sessions issued before a timestamp on the role; Azure supports revoking user delegation keys, invalidating every SAS derived from them; GCS offers no per-token revocation short of disabling the service account. Short TTL is therefore the primary control, and principal revocation on the broker stops *new* issuance immediately.

### Remote principals

A remote principal is a `vaultic` or `vaulticdb` deployment on another host that is allowed to ask the main host's broker for scoped storage credentials, repository key leases, and VaulticDB access tickets. The design deliberately avoids DNS, PKI, and Kerberos realm coupling: identities are key fingerprints, the broker is identified by its capsule-pinned Ed25519 identity, and neither side verifies hostnames.

#### Authentication decision: SSH keys, with cloud workload identity as an optional second authenticator

**Recommendation: use SSH key material and formats as the primary remote-principal authenticator.** Specifically:

- The worker holds an Ed25519 private key in OpenSSH format, optionally hardware-backed (`sk-ssh-ed25519@openssh.com` on a FIDO2 token, or a TPM/PKCS#11 key exposed through `ssh-agent`). The broker enrolls the public key by fingerprint (`SHA256:...`), exactly like `authorized_keys`.
- Client proof is an `sshsig` detached signature (`ssh-keygen -Y sign` format) over a broker-issued challenge transcript, in the dedicated namespace `vaultic-broker-principal@vaultic.dev`. `sshsig` already provides domain separation and rejects cross-protocol reuse of the signature; Go's `golang.org/x/crypto/ssh` and Rust's `ssh-key` crate implement the key formats, agent protocol, and signature verification without an `sshd` on either side.
- For fleets, the broker may trust an SSH certificate authority instead of individual keys: a worker presents an OpenSSH certificate whose principals, validity window, and critical options the broker checks, and revocation uses a broker-maintained fingerprint denylist (OpenSSH KRL import is optional).

Why SSH keys fit: they are not bound to DNS names, operators already know how to generate, rotate, fingerprint, and hardware-back them, `ssh-agent` gives private-key isolation from the `vaultic` process, and the trust decision is a single, auditable registration on the main host. Why not literal SSH transport: tunnelling the broker's Unix socket through `sshd` would make the broker see `sshd`'s peer credentials instead of the real principal and would couple authorization to Unix accounts; the design uses SSH *keys and signature formats*, not the SSH protocol.

**Cloud workload identity (OIDC) is supported as an optional authenticator, not the default.** A worker running in a cloud can present an OIDC ID token from Entra (managed identity), Google (service-account identity token), AWS (via IAM Roles Anywhere or an OIDC-capable identity provider), or SPIFFE/SPIRE, with an audience naming the broker identity. The broker validates issuer, audience, expiry, and the enrolled `subject`/`azp` claim. Benefits: no long-lived private key on the worker at all, and central lifecycle in the identity provider. Costs: the broker needs egress to the provider's JWKS endpoint and therefore inherits DNS/PKI trust at authentication time, and an attacker on the worker can mint tokens as long as the host holds the identity, so the damage bound is unchanged. Both authenticators map to the same enrollment record and the same capability limits; a deployment may require both (`all_of`) for higher assurance.

#### Enrollment record

Each principal is a root-owned, mode-0600 JSON definition in `principals.d/`, binding: principal ID; authenticator(s) — SSH public key fingerprint or certificate CA plus allowed certificate principals, and/or OIDC issuer/audience/subject; allowed repositories; allowed storage locations and maximum tier per location; allowed broker capabilities (`storage-credential`, `repository-master-key`, `metadata-dek`, `vaulticdb-ticket`) and their maximum TTLs; allowed VaulticDB ticket scopes; optional source address allowlist; required assurance (for example, hardware-backed key only); and an `enabled` flag. Enrollment and revocation are administrative operations that run on the main host: `vaultic-key-broker principal enroll|list|disable|remove`. Changes are audited but do not require a capsule generation, because they change authorization, not key custody.

#### Remote channel

The broker core remains a local-only, network-free process, preserving the Phase 20 hardening claim. Remote connectivity is provided by a separate, minimal `vaultic-key-broker-gateway` process running as a different unprivileged account:

- The gateway terminates TLS 1.3 on a configured address. Its server identity is a key pair whose public key the broker signs; workers pin the broker identity fingerprint (already distributed out of band for Phase 20 unlock fingerprints) and verify the gateway's signed key, so no CA and no hostname check is required.
- The gateway connects to the broker's Unix socket as an authorized Phase 20 client with a single new capability, `remote-gateway`, which lets it relay opaque principal-session frames and nothing else. It cannot request leases or credentials for itself.
- Every remote request is end-to-end authenticated and encrypted between the worker and the broker core using the existing Phase 20 HPKE construction: the broker's session key exchange, the challenge transcript, and the returned credential or key are opaque to the gateway. The challenge transcript includes a TLS exporter value so a relayed session cannot be spliced onto a different TLS connection.
- The gateway enforces connection limits, per-principal rate limits, and idle timeouts, and emits `auth` events for TLS failures. A compromised gateway can deny service and observe metadata (principal ID, request type, timing) but cannot read keys, forge principal proofs, or escalate scope.

Deployments that do not need cross-host operation do not run the gateway, and the broker configuration defaults to refusing `remote-gateway` clients.

### VaulticDB access tickets

Remote `vaultic` jobs need to read and, for backups, write metadata in the main host's read-write `vaulticdb`. Today the daemon's TCP transport is opt-in and protected by an address allowlist plus a static bearer token, which is unsuitable for multiple principals. This phase replaces that with a Kerberos-style ticket flow in which the broker is the ticket-granting authority and VaulticDB verifies tickets offline:

1. The worker authenticates to the broker (above) and requests `vaulticdb-ticket` for `(repository, vaulticdb instance, scope, ttl)`.
2. The broker checks enrollment, generates a fresh random 32-byte session key, and returns an **access ticket** and the session key. The ticket is a compact signed record — Ed25519 signature by the capsule-pinned broker identity — containing repository ID, target instance ID, principal ID, scope, unlock epoch, `not_before`, `not_after`, ticket ID, the session key wrapped with HPKE to the instance's registered public key, and the instance's expected TLS public key fingerprint.
3. The worker connects to `vaulticdb` over TLS 1.3, verifies the instance's TLS key against the fingerprint carried in the ticket (mutual authentication without PKI or DNS), and sends the ticket plus an **authenticator**: an HMAC under the session key over the TLS exporter value, a fresh nonce, and a timestamp.
4. `vaulticdb` verifies the ticket signature using the broker identity it already pinned when it validated the capsule at startup, checks instance ID, repository, epoch, validity window, and scope, unwraps the session key with its own instance private key, and verifies the authenticator. It admits the connection with the ticket's scope and principal attribution. No callback to the broker is needed on the request path.
5. Before `not_after - renew_margin`, the worker obtains a new ticket and presents it on the same connection through a `RefreshTicket` RPC bound to the same TLS session. RPCs are admitted only while the current ticket is valid; a transaction that outlives its ticket is aborted, never silently committed.

VaulticDB registers its instance key pair with the broker when it acquires its metadata-DEK lease, so the broker always knows the current instance it may issue tickets for. Broker lock ends the epoch; `vaulticdb` loses its own lease and shuts down (Phase 20), and any ticket from a previous epoch is rejected by the restarted instance, keeping epoch semantics consistent. The broker may additionally push ticket-ID and principal revocations to `vaulticdb` over the existing lease connection for immediate effect within an epoch.

#### Ticket scopes and additive-only remote writes

| Scope | Admitted RPCs | Issuable to remote principals |
|---|---|---|
| `metadata-read` | `Health`, `Capabilities`, `Get`, `MultiGet`, `Scan`, read-only status | yes |
| `metadata-append` | `metadata-read` plus `Begin`, `WriteBatch`, `Commit`, `Rollback` restricted to additive mutations | yes, per enrollment |
| `metadata-maintain` | deletes, pack lifecycle transitions toward `delete-pending`, GC, journal abandonment | no by default; requires explicit enrollment override and emits a critical event |
| `metadata-admin` | key slots, generation activation, writer role, encryption audit, capsule migration | never remote |

`metadata-append` is enforced by `vaulticdb` with a schema-version-pinned policy table: `Delete` operations are rejected outright; `Put` is rejected for key prefixes that encode deletion, retirement, or lifecycle regression (for example a pack record moving to `delete-pending`, a snapshot removal marker, a generation or writer-role record); and every accepted transaction is stamped with the principal ID and ticket ID in the transaction record and the Phase 10 event log. Because remote writes are additive and attributed, an operator can list and revert everything a specific principal wrote after a given time with `vaultic index principal-writes list|revert`, which is the recovery procedure after a worker compromise.

The read-write `vaulticdb` remains the single SlateDB writer; remote principals never receive a writer role. A read-only `vaulticdb` on a worker host uses `storage-read` credentials to follow the metadata object store and a `metadata-dek` lease from the broker; it never needs a ticket because it never connects to the main instance.

### Configuration surface

Broker (main host):

| Setting | Meaning |
|---|---|
| `issuers.d/<location>.json` | Provider, custody mode, issuer role or sealed credential reference, max tier, max TTL, prefixes, repositories |
| `principals.d/<principal>.json` | Authenticators, repositories, locations and tiers, capabilities, ticket scopes, TTL caps, source constraints, assurance, enabled |
| `VAULTIC_BROKER_REMOTE_GATEWAY=deny\|allow` | Whether a `remote-gateway` client may connect; default `deny` |
| `vaultic-key-broker issuer add\|list\|rotate\|remove` | Issuer lifecycle |
| `vaultic-key-broker principal enroll\|list\|disable\|remove` | Principal lifecycle |
| `vaultic-key-broker-gateway --listen ADDR --broker-socket PATH` | Optional remote channel |

Consumers (either host):

| Setting | Component | Meaning |
|---|---|---|
| `--storage-credentials broker` / `VAULTICDB_STORAGE_CREDENTIALS=broker` | `vaultic`, `vaulticdb` | Obtain storage credentials from the broker instead of the ambient provider chain |
| `--storage-token-ttl` / `VAULTICDB_STORAGE_TOKEN_TTL` | both | Requested credential lifetime (default `1h`) |
| `--storage-token-renew-margin` / `VAULTICDB_STORAGE_TOKEN_RENEW_MARGIN` | both | Proactive renewal margin (default `max(20m, ttl/3)`) |
| `--broker-outage-grace` / `VAULTICDB_BROKER_OUTAGE_GRACE` | both | Maximum runtime after last broker contact (default = TTL) |
| `--broker-remote HOST:PORT --broker-fingerprint SHA256:...` | `vaultic`, read-only `vaulticdb` | Use the gateway instead of the local socket; fingerprint is mandatory |
| `--broker-principal-key PATH` or `SSH_AUTH_SOCK` | remote consumers | Principal private key, or agent-held key |
| `--broker-principal-oidc-token-file PATH` | remote consumers | Optional OIDC authenticator, mode 0600, never on the command line |
| `--vaulticdb-remote HOST:PORT` | remote `vaultic` | Connect to the main read-write instance with a brokered ticket |
| `--vaulticdb-ticket-ttl`, `--vaulticdb-ticket-renew-margin` | remote `vaultic` | Ticket lifetime (default `1h`) and renewal margin |
| `VAULTICDB_REMOTE_ACCESS=ticket` | read-write `vaulticdb` | Enable the TLS listener that admits only broker-issued tickets; replaces the static bearer token |

All durations accept the existing `ms`, `s`, `m`, `h` suffixes and are validated against provider and enrollment maxima at request time; a request outside the permitted range fails with a typed error rather than being clamped silently.

### Observability

New `auth` events: principal authentication success and failure with authenticator type and fingerprint, gateway TLS failures, issuance grant and denial with location, tier, TTL, and issue ID, renewal attempts and `locked` responses with remaining validity, grace exhaustion, ticket grant, ticket verification failure by reason, scope violation attempts with the rejected key prefix class, revocation pushes. New `lifecycle` events: consumer stop due to credential expiry, backup transition to staged-pending because of expiry. Events carry principal IDs, fingerprints, issue and ticket IDs, and durations, never credentials, session keys, SAS strings, or tokens. Broker status lists every active issuance and ticket with expiry; `vaultic index keys status` and `index check` report locations still using non-brokered credentials and principals with `metadata-maintain` overrides as non-compliant findings.

## Implementation steps

1. Define the storage capability tiers, the provider scope mapping, and the create-only write contract; make every `vaultic` object write use the provider's create-only precondition and treat a precondition failure on an existing identical object as success.
2. Add issuer definitions and both custody modes to the broker: sealed static credentials under a capsule-derived KEK and ambient identity with an unlocked-epoch policy gate. Implement AWS STS session-policy issuance, MinIO/Ceph STS issuance with capability probing, Azure user delegation SAS issuance, and GCS impersonation plus Credential Access Boundary downscoping. Refuse tiers a location cannot enforce.
3. Add the `storage-credential` broker capability with request validation, TTL clamping, issue IDs, zeroizing return paths, and a per-principal, per-location active-issuance view in `status`.
4. Implement the consumer credential manager shared by `vaultic` and `vaulticdb`: TTL, renewal margin, outage grace, overlap handling for in-flight multipart uploads, typed `locked` handling, backoff, and the component-specific fail-closed stop behaviors including the Phase 22 staged-pending exit for backups.
5. Implement principal enrollment records and the `principal` CLI. Implement the SSH authenticator (OpenSSH public keys, `ssh-agent`, hardware `sk-` keys, certificates with CA trust and denylist) using `sshsig` over the broker challenge transcript, and the optional OIDC authenticator with pinned issuer, audience, and subject.
6. Implement `vaultic-key-broker-gateway` with TLS 1.3, broker-signed server key, fingerprint pinning on the client, the `remote-gateway` capability, end-to-end HPKE framing so the gateway is an opaque relay, TLS exporter binding in the challenge transcript, and rate limiting.
7. Extend Phase 20 leases so an authenticated remote principal may obtain short-lived `repository-master-key` and `metadata-dek` leases subject to enrollment, delivered end-to-end encrypted through the gateway.
8. Add VaulticDB instance key registration to the metadata-DEK lease handshake, the `vaulticdb-ticket` capability, ticket format and signature, HPKE-wrapped session keys, and revocation push over the lease connection.
9. Replace the static-bearer TCP transport in `vaulticdb` with a TLS listener that admits connections only with a valid ticket and authenticator, implements `RefreshTicket`, enforces scope per RPC, applies the schema-pinned additive-only policy table for `metadata-append`, and stamps principal and ticket IDs into transaction records and the event log.
10. Implement `vaultic index principal-writes list|revert` for attributed, principal-scoped rollback of additive remote writes, with the same safety delays and rechecks as other destructive metadata operations.
11. Add compliance findings for non-brokered locations, remote `metadata-maintain` overrides, principals without hardware or OIDC assurance where policy requires it, and stale issuer credentials.
12. Document setup in `doc/051_cloud_object_storage.rst` (issuer roles and minimal IAM for each provider, bucket versioning and immutability requirements, TTL and margin sizing against ceremony time) and in `doc/070_encryption.rst` (principal enrollment, SSH key and agent guidance, gateway deployment, ticket model, damage bounds, compromise response with principal disable plus attributed revert).

## Tests

Provider issuance tests against mocked STS, Azure, and GCS APIs and live MinIO: each tier receives exactly the intended permissions; `append` credentials cannot overwrite or delete, including versioned buckets; over-TTL requests are rejected, not clamped; unsupported tiers for a location fail closed. Consumer lifecycle tests: renewal at the margin, overlap during multipart upload, `locked` broker responses, broker restart mid-job, grace exhaustion, expiry during backup producing the staged-pending result with no snapshot published, read-only `vaulticdb` draining at expiry, maintenance jobs never starting an unfinishable batch. Principal tests: SSH key, agent, hardware `sk-` key (mocked and physical), certificate with CA and denylist, OIDC with wrong issuer/audience/subject/expiry, disabled principal, source-address denial, `all_of` dual authentication, replayed and cross-connection challenge responses rejected by TLS exporter binding. Gateway tests: opaque relay cannot read or alter credential frames, cannot request capabilities for itself, rate limits and idle timeouts hold, fingerprint mismatch aborts before any principal proof is sent. Ticket tests: signature, instance, repository, epoch, window, and scope checks; authenticator replay; `RefreshTicket` on the same session; transaction aborted at ticket expiry; every `metadata-append` deny rule against real schema prefixes; attribution recorded; `principal-writes revert` restores the pre-compromise state and is idempotent; tickets from a previous epoch rejected after broker relock. Multi-host integration: worker host with backup and read-only `vaulticdb` against a main host with broker, gateway, and read-write `vaulticdb`; simulated worker compromise proves the damage table above, including inability to delete or overwrite objects and metadata and successful operator recovery through principal disable and attributed revert. Secret hygiene: no credential, SAS, token, session key, or ticket session material in events, errors, environment, command lines, or core dumps.

## Exit criterion

No `vaultic` or `vaulticdb` process on any host holds a long-lived cloud storage credential; the broker is the only holder of issuer credentials, sealed under capsule-derived keys or delegated to ambient host identity gated on an unlocked epoch. Every storage credential a consumer uses is provider-enforced to its tier and expires within the configured TTL; consumers renew proactively at the configured margin, tolerate broker unavailability up to the configured grace, and stop fail-closed at expiry with component-appropriate results. A remote worker host authenticates with an enrolled SSH key or OIDC identity over a fingerprint-pinned, DNS-independent channel, receives only the tiers, leases, and ticket scopes its enrollment permits, and can back up to cloud storage and write additive, attributed metadata to the main host's read-write `vaulticdb` through broker-issued tickets verified offline by VaulticDB. Compromise of that worker host yields time-limited read access and additive, revertible metadata writes only; it cannot delete or overwrite repository objects, delete metadata, change key policy, or obtain credentials for other principals or repositories. Operators can disable a principal on the main host, observe every active issuance and ticket, and revert a principal's writes from documented commands.
