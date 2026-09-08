# Phase 27: Remote principals and brokered VaulticDB access tickets

[← Back to roadmap index](00-overview.md)

[← Phase 26](phase-26-ephemeral-cloud-storage-credentials.md) · [Phase 28 →](phase-28-read-only-nfsv3-snapshot-server.md)

[Broker implementation and state machines](../02-architecture/08-quorum-key-broker.md) · [Cloud object storage operator guide](../../051_cloud_object_storage.rst)

**Status: design specification, not yet implemented.**

**Goal:** extend Phase 26's ephemeral storage credentials and Phase 20's key leases safely across hosts. A `vaultic` worker or read-only `vaulticdb` on a second host authenticates to the main host's broker with an enrolled key or workload identity, receives only its allowed storage tier and key leases, and receives a short-lived access ticket for the main host's read-write `vaulticdb`. A compromised worker must gain at most time-limited read access to data it was already allowed to read and time-limited, attributable, additive metadata writes. It must never gain authority to delete or overwrite repository data, delete metadata, change key policy, or mint credentials for another principal.

## Prerequisites

- Phase 20 provides the local broker, unlock epochs, client authorization, HPKE lease delivery, and capsule-pinned broker identity.
- Phase 24 provides authoritative capsule topology and credential custody.
- Phase 26 provides provider-scoped, expiring storage credential issuance and consumer renewal semantics. This phase reuses that API; it does not redefine provider tiers or issuer custody.

## Threat model and trust boundaries

Three trust zones exist:

- **Main host.** Runs the unlocked broker and read-write `vaulticdb`. Compromise during an unlock epoch remains out of scope under the Phase 20 model.
- **Worker host.** Runs backup, restore, or check jobs and optionally a read-only `vaulticdb`. It stores no capsule, broker identity private key, repository key, metadata key, or cloud issuer credential. Its only persistent secret is its own principal authentication key when workload identity or hardware-backed SSH is unavailable. The worker is assumed to be compromisable.
- **Cloud provider.** Enforces Phase 26 credential scope and expiry.

Damage bound for a fully compromised worker while its principal remains enrolled:

| Attacker capability | Bound |
|---|---|
| Read allowed repository objects | Storage credential and repository-key lease TTL, plus principal revocation |
| Decrypt allowed objects | Repository master-key lease TTL; the Restic-compatible format has no per-job data key |
| Create objects | Create-only scope, versioning and immutability, and Phase 22 staging quotas; cost and noise, not data loss |
| Overwrite or delete objects | Not permitted by correctly scoped Phase 26 credentials |
| Write metadata | `metadata-append` permits additive keys only and attributes every transaction to principal and ticket |
| Delete metadata, prune, change writer role, or rotate keys | Not permitted; maintain and admin scopes are not remotely issuable by default |
| Mint authority for another principal or repository | Not permitted; issuance is bound to the authenticated enrollment |
| Persist after revocation | Bounded by the shortest credential, lease, or ticket TTL and provider revocation latency |

## Design

### Remote principal authentication

SSH key material and formats are the primary authenticator:

- The worker holds an Ed25519 private key in OpenSSH format, optionally hardware-backed as `sk-ssh-ed25519@openssh.com` or exposed by `ssh-agent` from a TPM or PKCS#11 device. The broker enrolls the public-key fingerprint exactly like `authorized_keys`.
- Client proof is an `sshsig` detached signature over a broker challenge transcript in namespace `vaultic-broker-principal@vaultic.dev`. Domain separation prevents cross-protocol signature reuse.
- Fleets may enroll an SSH certificate authority instead of individual keys. The broker validates certificate principals, validity, critical options, and a fingerprint denylist. OpenSSH KRL import is optional.

The design uses SSH key and signature formats, not SSH transport. Tunneling through `sshd` would hide the actual principal behind the daemon's Unix credentials and couple authorization to host accounts.

Cloud workload identity through OIDC is an optional authenticator. The broker validates issuer, audience naming the broker identity, expiry, and enrolled subject and authorized-party claims. Entra managed identity, Google service-account identity tokens, AWS deployments with an OIDC-capable identity provider, and SPIFFE/SPIRE can map to the same enrollment record. This avoids a persistent worker key but adds issuer JWKS, DNS, and PKI dependencies. Policy may require SSH and OIDC together through `all_of`.

### Enrollment records

Each root-owned, mode-0600 principal definition binds:

- principal ID and enabled state;
- SSH fingerprint or certificate CA and allowed principals;
- optional OIDC issuer, audience, subject, and authorized party;
- allowed repositories;
- allowed storage locations and maximum Phase 26 tier per location;
- allowed broker capabilities and maximum TTL for `storage-credential`, `repository-master-key`, `metadata-dek`, and `vaulticdb-ticket`;
- allowed VaulticDB ticket scopes;
- optional source-address constraints and required assurance.

Enrollment and revocation are local administrative operations: `vaultic-key-broker principal enroll|list|disable|remove`. They are audited but do not create capsule generations because they alter authorization, not custody or repository topology.

### Remote gateway

The broker core remains local-only and network-free. A separate minimal `vaultic-key-broker-gateway`, running as another unprivileged account, provides remote connectivity:

- The gateway terminates TLS 1.3. The broker signs its server public key; workers pin the capsule-pinned broker fingerprint and verify the signed gateway key, avoiding DNS and CA identity dependencies.
- The gateway connects to the broker Unix socket with only `remote-gateway`, allowing opaque principal-session relay and no lease or credential requests for itself.
- Requests are authenticated and encrypted end to end between worker and broker core using Phase 20 HPKE. The gateway cannot read credentials or keys.
- The challenge transcript includes the TLS exporter so a relayed session cannot be spliced onto another connection.
- The gateway enforces connection limits, per-principal rate limits, source constraints, and idle timeouts, and emits secret-free authentication events.

A compromised gateway can deny service and observe principal IDs, request types, and timing, but cannot read leased material, forge proofs, or widen scope. Deployments that do not need remote access omit it; brokers reject `remote-gateway` by default.

### Remote leases and storage credentials

After authentication, the broker maps every request to the enrollment record. A remote worker may receive:

- Phase 26 storage credentials at or below its per-location tier and TTL;
- a repository master-key lease for jobs that must decrypt repository objects;
- a metadata-DEK lease for a read-only worker-side `vaulticdb`;
- a VaulticDB access ticket as described below.

All material remains end-to-end encrypted through the gateway and bound to principal, repository, connection, unlock epoch, and expiry. Broker lock prevents renewal. Disabling a principal blocks new sessions and issuance immediately; existing material remains usable only until its independent expiry or an available revocation mechanism takes effect.

### VaulticDB access tickets

Remote jobs need read access and backups need constrained writes to the main read-write `vaulticdb`. Static shared bearer tokens are replaced by a Kerberos-style flow whose tickets VaulticDB verifies offline:

1. The worker authenticates to the broker and requests `vaulticdb-ticket` for repository, target instance, scope, and TTL.
2. The broker checks enrollment, generates a random 32-byte session key, and returns it with a signed ticket containing repository ID, target instance ID, principal ID, scope, unlock epoch, validity window, ticket ID, session key wrapped with HPKE to the instance key, and expected instance TLS-key fingerprint.
3. The worker connects over TLS 1.3, pins the instance key from the ticket, and sends the ticket plus an HMAC authenticator over the TLS exporter, a fresh nonce, and a timestamp.
4. VaulticDB verifies broker signature, instance, repository, epoch, validity, and scope; unwraps the session key with its instance private key; verifies the authenticator; and attributes the connection to principal and ticket. No broker callback is needed on the request path.
5. Before the renewal margin, the worker obtains a new ticket and presents it through `RefreshTicket` bound to the same TLS session. RPCs are admitted only while the active ticket is valid; an overlong transaction aborts rather than committing after expiry.

VaulticDB registers its instance key with the broker when acquiring its metadata-DEK lease. Broker relock ends the epoch, shuts down the leased daemon under Phase 20, and causes a restarted instance to reject old tickets. The broker may push principal and ticket revocations over the existing lease connection for immediate within-epoch enforcement.

### Ticket scopes and additive-only writes

| Scope | Admitted RPCs | Remotely issuable |
|---|---|---|
| `metadata-read` | health, capabilities, get, multiget, scan, read-only status | yes |
| `metadata-append` | reads plus begin, write batch, commit, and rollback restricted to additive mutations | yes, per enrollment |
| `metadata-maintain` | deletes, lifecycle transitions toward deletion, GC, journal abandonment | no by default; explicit override emits a critical event |
| `metadata-admin` | key slots, generation activation, writer role, encryption audit, capsule migration | never |

For `metadata-append`, VaulticDB rejects deletes and puts to schema-prefix classes representing deletion, retirement, lifecycle regression, generation control, or writer role. It stamps principal ID and ticket ID into transaction records and the Phase 10 event log. `vaultic index principal-writes list|revert` enumerates and safely reverses one principal's additive writes after a specified time.

The main VaulticDB remains the only SlateDB writer. A worker-side read-only VaulticDB follows metadata storage with a Phase 26 `storage-read` credential and a remote metadata-DEK lease; because it never connects to the main instance, it needs no ticket.

### Configuration surface

Broker and gateway:

| Setting | Meaning |
|---|---|
| `principals.d/<principal>.json` | Authenticators, repositories, location tiers, capabilities, ticket scopes, TTL caps, source constraints, assurance, enabled state |
| `VAULTIC_BROKER_REMOTE_GATEWAY=deny|allow` | Permit the gateway broker client; default `deny` |
| `vaultic-key-broker principal enroll|list|disable|remove` | Principal lifecycle |
| `vaultic-key-broker-gateway --listen ADDR --broker-socket PATH` | Optional remote relay |

Remote consumers and VaulticDB:

| Setting | Component | Meaning |
|---|---|---|
| `--broker-remote HOST:PORT --broker-fingerprint SHA256:...` | worker clients | Use the gateway and pin broker identity |
| `--broker-principal-key PATH` or `SSH_AUTH_SOCK` | worker clients | Principal private key or agent-held key |
| `--broker-principal-oidc-token-file PATH` | worker clients | Optional mode-0600 OIDC token file, never a command-line value |
| `--vaulticdb-remote HOST:PORT` | remote `vaultic` | Connect to the main instance with a broker ticket |
| `--vaulticdb-ticket-ttl`, `--vaulticdb-ticket-renew-margin` | remote `vaultic` | Ticket lifetime and proactive renewal margin |
| `VAULTICDB_REMOTE_ACCESS=ticket` | main VaulticDB | Enable TLS transport admitting broker tickets only |

### Observability and compliance

Authentication events record principal success and failure, authenticator type and fingerprint, gateway TLS failures, issuance decisions, ticket grants, ticket verification failures by reason, scope violations by key-prefix class, renewal, and revocation. Lifecycle events record remote connection drain and transaction abort at expiry. Events carry principal, issue, and ticket IDs and durations, never credentials, repository keys, metadata keys, session keys, OIDC tokens, or ticket session material.

Status lists active remote sessions, issuances, leases, and tickets with expiry. Compliance reports remote maintain overrides, principals missing required hardware or OIDC assurance, stale credentials, and an enabled gateway without source or rate constraints.

## Implementation steps

1. Define principal enrollment records and implement `principal enroll|list|disable|remove` with secure file ownership, validation, and audit events.
2. Implement SSH public-key, agent, hardware `sk-` key, and certificate authentication using `sshsig`; add optional OIDC validation and `all_of` assurance policy.
3. Implement `vaultic-key-broker-gateway` with TLS 1.3, broker-signed server keys, client fingerprint pinning, opaque HPKE relay, TLS-exporter challenge binding, connection controls, and rate limiting.
4. Extend Phase 20 leases and Phase 26 storage credential requests to authenticated remote principals subject to enrollment, with end-to-end encrypted delivery through the gateway.
5. Register VaulticDB instance keys during metadata-DEK lease acquisition and add the `vaulticdb-ticket` broker capability, signed ticket format, HPKE-wrapped session key, and revocation push.
6. Replace static-bearer VaulticDB TCP authentication with a ticket-only TLS listener, authenticator verification, `RefreshTicket`, per-RPC scope enforcement, and transaction expiry checks.
7. Implement the schema-version-pinned additive-only policy for `metadata-append` and stamp principal and ticket attribution into transactions and events.
8. Implement `vaultic index principal-writes list|revert` with safety delays, precondition rechecks, audit output, and idempotent recovery.
9. Add compliance findings for remote maintain overrides, inadequate assurance, unsafe gateway configuration, and stale principal enrollment.
10. Document enrollment, SSH agent and hardware guidance, OIDC tradeoffs, gateway deployment, ticket operation, damage bounds, principal disable, and attributed-write recovery in `doc/070_encryption.rst` and the cloud operator guide.

## Tests

Principal tests cover direct SSH keys, agents, mocked and physical hardware keys, certificates with CA trust and denylist, wrong OIDC issuer/audience/subject/expiry, disabled principals, source-address denial, dual authentication, and replayed or cross-connection challenge responses.

Gateway tests prove it cannot read or alter credential frames or request capabilities for itself; rate limits and idle timeouts hold; fingerprint mismatch aborts before principal proof; TLS exporter binding rejects session splicing.

Ticket tests cover signature, instance, repository, epoch, window, and scope; authenticator replay; same-session refresh; transaction abort at expiry; every additive-policy denial against real schema prefixes; attribution; idempotent principal-write reversion; and rejection of tickets from prior unlock epochs.

A multi-host integration runs worker backup and read-only VaulticDB against a main host's broker, gateway, and read-write VaulticDB. A simulated worker compromise verifies the damage table, including inability to overwrite or delete objects and metadata, inability to obtain another principal's authority, expiry of all access, and successful recovery by disabling the principal and reverting attributed writes. Secret-hygiene tests assert no credential, key, token, or session material appears in events, status, errors, process state, or crash artifacts.

## Exit criterion

A remote worker authenticates with an enrolled SSH key, OIDC identity, or required combination over a fingerprint-pinned channel and receives only the Phase 26 storage tiers, Phase 20 key leases, and VaulticDB ticket scopes its enrollment permits. It can back up to cloud storage and make additive, attributable metadata writes through tickets verified offline by VaulticDB. Compromise yields only time-limited read access and additive, revertible writes; it cannot overwrite or delete repository objects, delete metadata, change key policy, acquire administrative scope, or act for another principal or repository. Operators can disable a principal, observe every active remote issuance, lease, and ticket, and revert that principal's writes through documented commands.
