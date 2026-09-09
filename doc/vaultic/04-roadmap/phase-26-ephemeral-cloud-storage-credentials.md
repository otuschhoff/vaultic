# Phase 26: Ephemeral cloud storage credentials

[← Back to roadmap index](00-overview.md)

[← Phase 25](phase-25-backblaze-and-wasabi-s3-compatible-backends.md) · [Phase 27 →](phase-27-native-ceph-librados-object-store-backend.md)

[Broker implementation and state machines](../02-architecture/08-quorum-key-broker.md) · [Cloud object storage operator guide](../../051_cloud_object_storage.rst)

**Status: Complete. S3 (including Wasabi and Ceph), Azure, and GCS issuance, exact-tier static fallback, proactive consumer renewal, outage grace, fail-closed expiry, and lifecycle observability are implemented.**

**Goal:** remove long-lived cloud object-storage credentials from every local `vaultic` and `vaulticdb` process. The Phase 20 `vaultic-key-broker` becomes the sole holder of the *issuer credential* for each S3-compatible, Azure Blob, or Google Cloud Storage location and hands out short-lived, least-privilege *storage credentials* scoped to what a specific job needs: read only, read plus create, or read plus delete for maintenance. Credential lifetime and proactive renewal margin are configurable so operators can decide how long consumers keep working when the broker is down, locked, or waiting for a custodian ceremony. This phase establishes the provider-enforced issuance and consumer-lifecycle foundation that Phase 33 extends to remote principals.

## Threat model and trust boundaries

This phase has two trust zones:

- **Local host.** Runs the unlocked broker, `vaulticdb`, and ordinary `vaultic` jobs. Compromise during an unlock epoch remains the Phase 20 accepted risk, but consumers receive only short-lived credentials with the minimum tier needed by the current operation.
- **Cloud provider.** Enforces credential scope and expiry. Vaultic-side checks are defense in depth, not the security boundary.

A compromised local consumer can use its current credential only until the shortest of credential expiry, broker outage grace, and provider revocation. Read consumers cannot modify repository data. Append consumers cannot delete data and cannot overwrite it where the provider can enforce create-only writes. Long-lived issuer credentials never enter consumer memory, environment variables, command lines, logs, or runtime profiles. A provider without delegation, such as Backblaze B2, is an explicit degraded case: the broker may lease a pre-provisioned, least-privilege application key, but lease expiry only makes a conforming client discard that key and does not revoke an exfiltrated copy.

Remote authentication, cross-host credential delivery, repository-key leases, and VaulticDB access tickets are deliberately deferred to Phase 33.

## Design

### Storage capability tiers

Every brokered storage credential carries one tier. Tiers are ordered; a request may be granted a tier equal to or lower than the requesting client's authorization.

| Tier | Vaultic operations | Provider permissions (conceptual) |
|---|---|---|
| `storage-read` | restore, lock-free check/ls/mount, read-only `vaulticdb`, `read-only-assisted` crawl | list, get |
| `storage-append` | backup, deferred staging journal publication, placement copy | list, get, create; no delete, and no overwrite where the provider can enforce it |
| `storage-maintain` | prune, gc, forget, placement eviction, journal abandonment, metadata rebuild | list, get, create, delete (and version delete only where policy explicitly allows) |
| `storage-admin` | bucket policy, lifecycle, Object Lock changes | never issued by the broker; operator tooling only |

"Append" means create-only when the provider can enforce that distinction. Vaultic clients always write with a create-only precondition (`If-None-Match: *` on S3-compatible stores and Azure, `ifGenerationMatch=0` on GCS). The broker also scopes credentials so the provider rejects overwrite where it can express that distinction. Backblaze's `writeFiles` application-key capability can create or replace an object version and therefore is not provider-enforced append-only; conditional requests, versioning, Object Lock, verification, and an explicit compliance finding are required rather than claiming a stronger boundary.

Repository locks are a separate coordination permission. Most read commands create, refresh, and remove objects in the lock namespace unless ``--no-lock`` is explicitly selected. A strictly read-only job therefore receives `storage-read` for repository data plus a second `storage-lock` credential restricted to the repository lock prefix. `storage-lock` permits list, get, create, and delete only in that prefix and is never valid for packs, indexes, snapshots, configuration, or metadata. Backends route lock-file requests through that credential; they must not solve the problem by upgrading the whole job to `storage-maintain`.

Provider mapping:

| Provider | Issuance mechanism | Scope expression | Practical TTL bounds (verify against current provider limits) |
|---|---|---|---|
| AWS S3 | `sts:AssumeRole` with an inline session policy | IAM actions and prefix conditions; `s3:DeleteObject*` absent for `read` and `append` | 15 minutes to the role maximum, up to 12 hours; 1 hour for role chaining |
| Wasabi S3 | native AWS-compatible `AssumeRole` at `https://sts.wasabisys.com` with an inline session policy; issuer must be a sub-user | Wasabi IAM role actions and the supported policy subset | Up to 12 hours; probe account policy and reject unsupported requested TTLs |
| Backblaze B2 S3 | no STS; select a pre-provisioned application key registered for the exact requested tier | bucket- and prefix-restricted `listFiles`/`readFiles`, plus `writeFiles` for append and `deleteFiles` for maintain | Long-lived until key expiry or revocation; broker lease expiry is not provider revocation |
| S3-compatible with STS | AWS-compatible `AssumeRole` with session policy, including Ceph RGW | Provider-supported IAM policy subset | Provider-specific; validate limits against the deployed service |
| Other S3-compatible without STS | Optional exact-tier static credential bindings when independently validated | Provider-specific static permissions | Long-lived until provider expiry or revocation; reported as non-ephemeral |
| Azure Blob Storage | User delegation SAS derived from a user delegation key obtained with the broker's capsule-sealed Entra client identity | `rl` for read, `rcwl` for append, and `rcwdl` for maintain; dedicated-container scope | Up to 7 days, bounded by the delegation key and broker lease |
| Google Cloud Storage | Service-account impersonation followed by a Credential Access Boundary downscoped token | viewer for read, object creator for append, delete roles only for maintain, with prefix conditions | 1 hour by default, up to 12 hours where organization policy permits |

Azure `storage-lock` is a special case: on a hierarchical-namespace account the broker signs a directory SAS for `locks/` with `sr=d` and `sdd=1`. Topology must explicitly declare `hierarchical_namespace: true`; otherwise the broker rejects the lock tier instead of issuing container-wide lock authority. Azure's `w` permission can overwrite, so its append tier also relies on Vaultic's create-only request precondition and is not reported as provider-enforced append-only.

The broker refuses a tier the provider cannot enforce and never silently widens a scope or substitutes `maintain` for `append`. Static credential selection is exact, not an ordered privilege fallback. AWS, Wasabi, MinIO, Ceph, another compatible S3 provider, and Azure may configure an exact-tier static credential as an availability fallback for the corresponding dynamic tier.

### Static credential bundles and dynamic fallback

Topology format 2 gives each backend and metadata replica one canonical `credential_policy`. Its optional `sts` member contains the sealed issuer reference, endpoint, region, session name, optional external ID, and exact per-tier role ARNs. Its optional `azure_user_delegation` member contains the Entra issuer reference, service version, optional IPv4 restriction, explicit HNS declaration, exact tiers, and fallback mode. Its optional `static` member contains a monotonically increasing generation, acknowledged revoked generations, and exact per-tier credential references. There is no legacy singular credential reference. Backblaze supports only the static member; S3 and Azure policies may use dynamic-only, static-only, or unavailable-only exact-tier fallback.

| Binding | Backblaze application-key capabilities | Use |
|---|---|---|
| `storage-read` | bucket-scoped listing plus `listFiles` and `readFiles` | Repository reads and read-only VaulticDB |
| `storage-append` | `storage-read` capabilities plus `writeFiles` | Backup and placement destinations; workflow-append only, not provider-enforced create-only |
| `storage-maintain` | `storage-append` capabilities plus `deleteFiles` | Prune, repack, garbage collection, eviction, and read-write VaulticDB |
| `storage-lock` | list/read/write/delete restricted to the repository lock prefix | Lock acquisition, refresh, and release for otherwise read-only or append jobs |

There is deliberately no brokered `storage-admin` or account-master binding. "Full access" for ordinary Vaultic operation means one bucket- and repository-prefix-restricted data key with read, write, and delete capabilities. An operator who does not want separate data keys registers that same credential reference explicitly for `storage-read`, `storage-append`, and `storage-maintain`; the broker still records the requested tier but never guesses that a missing narrow binding should fall back to a stronger one. A separate lock-prefix key remains recommended.

For S3, the broker generates the inline session policy from the sealed bucket, prefix, and requested tier; callers cannot supply it. For Azure, it requests an OAuth token, obtains a user delegation key, and locally signs an HTTPS-only SAS. Only network failures, timeouts, HTTP 408/429, and provider 5xx responses are fallback-eligible. Authentication or authorization denial, malformed or oversized responses, invalid roles, invalid policies, and configuration errors fail closed. Status reports active fallback provider authority. When dynamic issuance succeeds after a fallback checkout, the broker retires every outstanding fallback generation for that target and refuses to issue it again. Status remains non-compliant until the operator revokes those credentials at the provider, installs a fresh generation, and records the old generation in `revoked_generations`.

Fallback checkout and retirement are synchronously persisted before lease return in an atomically replaced lifecycle journal signed by the capsule-pinned broker identity and bound to repository and capsule logical IDs. Broker startup verifies and restores that journal, so restart cannot make a checked-out or retired generation eligible again. The journal contains no credentials; capsule topology remains the authority for configured keys and the operator's provider-revocation acknowledgement.

### Issuer credential custody

The issuer credential is the cloud identity able to mint scoped credentials. It never leaves the broker. Topology format 2 binds a sealed issuer reference to exact per-tier roles inside each location's `credential_policy`. The broader design supports these issuer modes:

1. **Capsule-sealed issuer credential.** The implemented S3 path uses an `aws-static` or `s3-static` issuer. The implemented Azure path uses an `azure-entra-client-secret` containing tenant ID, client ID, client secret, tenant token URI, and exactly one Storage OAuth scope. Rotation remains a capsule topology mutation and therefore publishes a new generation. The broker rejects direct leases and static consumer bindings for the Entra issuer kind.
2. **Ambient issuer identity.** A future broker host workload identity may hold only `sts:AssumeRole`, `generateUserDelegationKey`, or `iam.serviceAccounts.getAccessToken` permission on the issuer role. This mode is not yet implemented.
3. **Pre-provisioned static credential bundle.** For every S3 provider, the capsule may seal separate bucket/prefix-restricted keys for the supported bindings. The broker releases only the exact binding requested by the authorized job. These keys are consumer credentials, not issuer credentials, and status marks fallback generations for provider-side revocation after STS recovery.

The topology's issuer policy binds location ID, provider, role or key reference, tenant/account/project, maximum tier, maximum TTL, permitted prefixes, and repository IDs. Status reports each location's custody mode and flags static credentials that cannot mint scoped, expiring credentials.

### Credential issuance and lifetime semantics

A local consumer requests `(location, tier, purpose, requested_ttl)` through the authenticated Phase 20 Unix channel. The broker validates client authorization and location policy. `purpose` is normally `data` or `lock`; the latter can select only `storage-lock`. Requests beyond any configured maximum fail with a typed error rather than being silently clamped. Successful responses contain `{credential, not_before, not_after, tier, purpose, location, issue_id, provider_expiry}`. Credential values travel only over the authenticated channel and remain in zeroizing memory. For a static binding, `not_after` limits client possession but must not be presented as provider-side expiry.

Three consumer-side durations govern behavior, configurable per component and job:

- **`storage-token-ttl`**: requested lifetime, default 1 hour.
- **`storage-token-renew-margin`**: proactive renewal margin, default 20 minutes or one third of TTL, whichever is larger. Renewal issues a new credential and leaves the old credential valid for in-flight work until its expiry.
- **`broker-outage-grace`**: maximum runtime after the last successful broker contact, default equal to TTL.

The documented sizing rule is `renew_margin >= expected_ceremony_time + retry_backoff` and `ttl >= renew_margin + minimum_useful_work_window`. A locked broker returns a typed `locked` response. The consumer retains its still-valid credential, emits a secret-free warning with time to expiry, and retries with bounded jitter until renewal, credential expiry, or grace exhaustion.

Failure remains component-specific and fail-closed:

- Read-only `vaulticdb` stops admitting new reads at the earlier of expiry and grace exhaustion, drains in-flight RPCs, and reports `credential-expired`.
- Backup stops starting uploads at `not_after - upload_safety_margin`, completes or aborts in-flight uploads, and either commits normally or returns Phase 22 `data_durable_metadata_pending` when the sealed journal is durable.
- Restore and check finish the current object and exit with a distinct `credential-expired` status.
- Maintenance jobs never start a deletion batch they cannot finish before expiry and abort cleanly between batches.

Provider revocation is coarse: AWS and Wasabi can stop new role sessions, Azure can revoke all user delegation keys, and GCS generally requires disabling the service account. Short TTL is the primary control for provider-issued credentials. A Backblaze application key must be deleted or expire at Backblaze to revoke a copied key; locking the broker prevents new leases but cannot invalidate an existing copy.

### Configuration surface

Broker and topology:

| Setting | Meaning |
|---|---|
| capsule issuer policy per location | Provider, custody mode, role or sealed credential reference, maximum tier and TTL, prefixes, repositories |
| backend `credential_policy` | Optional per-tier STS roles plus generation-tagged exact `storage-read`, `storage-append`, `storage-maintain`, and `storage-lock` static references |
| `vaultic-key-broker issuer list|test` | Inspect and test issuer capability without exposing material |
| topology credential mutation commands | Add, rotate, or remove capsule-sealed issuer credentials through a new capsule generation |

Consumers:

| Setting | Component | Meaning |
|---|---|---|
| `--storage-credentials broker` / `VAULTICDB_STORAGE_CREDENTIALS=broker` | `vaultic`, `vaulticdb` | Obtain ephemeral credentials from the local broker |
| `--storage-token-ttl` / `VAULTICDB_STORAGE_TOKEN_TTL` | both | Requested lifetime, default `1h` |
| `--storage-token-renew-margin` / `VAULTICDB_STORAGE_TOKEN_RENEW_MARGIN` | both | Proactive renewal margin, default `max(20m, ttl/3)` |
| `--broker-outage-grace` / `VAULTICDB_BROKER_OUTAGE_GRACE` | both | Maximum runtime since last broker contact, default equal to TTL |

Durations use existing `ms`, `s`, `m`, and `h` suffixes.

### Observability and compliance

New `auth` events cover issuance grants and denials with location, tier, TTL, and issue ID; renewal attempts; typed locked responses with remaining validity; and grace exhaustion. New `lifecycle` events cover consumer shutdown at expiry and backup transition to staged-pending. Events never include credentials, SAS strings, tokens, or issuer material.

Broker status lists active issuances, requested tier and purpose, broker lease expiry, provider expiry, and whether overwrite prevention is provider-enforced. `vaultic index keys status` and `index check` report non-brokered cloud locations, missing exact-tier bindings, a broad key reused across tiers, unsupported provider tiers, stale issuer credentials, client-only append enforcement, and locations that can supply only a long-lived static credential.

## Implementation steps

1. Define storage capability tiers, provider mappings, and the create-only write contract. Make object creation use provider preconditions and treat a precondition failure for an identical existing object as success.
2. Extend capsule topology format 2 with one canonical per-location `credential_policy`; require exact tier selection and validate provider, STS endpoint, bucket, prefix, and lock-prefix scope while retaining generation-bound rotation.
3. Implement AWS-compatible STS session-policy issuance, including Ceph RGW, Azure Entra user delegation SAS issuance, and GCS service-account impersonation with Credential Access Boundary downscoping. Refuse unenforceable tiers.
4. Add the `storage-credential` broker capability for authorized local clients with exact `(location, tier, purpose)` selection, issue IDs, strict TTL rejection, zeroizing responses, and per-location active issuance status. Add Backblaze static-binding selection without representing broker lease expiry as token expiry.
5. Implement the shared `vaultic` and `vaulticdb` credential manager with renewal margin, outage grace, overlap for in-flight work, typed locked handling, bounded backoff, component-specific fail-closed behavior, and separate routing for lock-file credentials.
6. Wire every cloud backend to refresh credentials in memory without rebuilding unrelated repository state or falling back to ambient consumer credentials.
7. Add compliance findings for non-brokered locations, unsupported scope tiers, and stale issuer credentials.
8. Document provider issuer roles, minimum IAM, create-only behavior, versioning and immutability requirements, and TTL sizing in `doc/051_cloud_object_storage.rst`; document issuer custody and unlock behavior in `doc/070_encryption.rst`.

## Tests

Provider issuance tests mock AWS-compatible STS, Azure Entra/Blob APIs, and GCS OAuth/IAM/STS APIs. Each dynamically issued tier receives exactly its intended permissions; append credentials cannot delete and use create-only client requests where the provider cannot prohibit overwrite; requests above TTL limits are rejected; prefixes and repositories cannot be widened; unsupported tiers fail closed. Azure tests cover version-specific strings to sign, HTTPS-only SAS, HNS directory scope for `locks/`, terminal versus availability failures, broker-only issuer custody, fallback retirement, and the absence of issuer secrets from leases. Backblaze tests register independently restricted application keys, prove exact tier selection and no automatic fallback to a stronger binding, report that append can overwrite, and verify maintain can repack before deleting.

Consumer lifecycle tests cover renewal at the margin, overlap during multipart upload, locked responses, broker restart during a job, grace exhaustion, expiry during backup producing staged-pending without publishing a snapshot, read-only VaulticDB draining at expiry, and maintenance batches refusing to begin without enough remaining lifetime.

Integration tests run local `vaultic` and `vaulticdb` with no cloud credentials in their environment, obtain scoped credentials from the broker, exercise read, append, and maintain jobs, and verify provider-side denial of forbidden operations. Secret-hygiene tests assert that issuer credentials and ephemeral tokens never appear in events, status, errors, process arguments or environment, core dumps, or runtime profiles.

## Exit criterion

For the implemented S3, Azure, and GCS providers with delegation, no local `vaultic` or `vaulticdb` consumer holds a long-lived cloud storage credential: the broker alone holds issuer identity, and every dynamic consumer credential is provider-enforced to its location, scope, tier, and configured TTL. Azure data scope is a dedicated container; Azure lock scope is the HNS `locks/` directory. Backblaze is supported through exact least-privilege static bindings but is explicitly non-ephemeral; a consumer receives only the key bound to its requested tier and purpose, never an automatic stronger fallback. Read credentials cannot modify objects, append credentials cannot delete, lack of provider-enforced overwrite prevention is explicit, and unsupported or static-only locations remain compliance findings. Consumers renew before the configured margin without rebuilding unrelated repository or SlateDB state, tolerate broker outages only for the configured grace, retain old clients for in-flight operations, stop new writes before the safety margin, and fail closed at hard expiry. Security events report issuance and renewal lifecycle metadata without secrets. The complete phase exit criterion is met.
