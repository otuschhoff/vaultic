# Phase 26: Ephemeral cloud storage credentials

[← Back to roadmap index](00-overview.md)

[← Phase 25](phase-25-backblaze-and-wasabi-s3-compatible-backends.md) · [Phase 27 →](phase-27-remote-principals-and-brokered-vaulticdb-access-tickets.md)

[Broker implementation and state machines](../02-architecture/08-quorum-key-broker.md) · [Cloud object storage operator guide](../../051_cloud_object_storage.rst)

**Status: design specification, not yet implemented.**

**Goal:** remove long-lived cloud object-storage credentials from every local `vaultic` and `vaulticdb` process. The Phase 20 `vaultic-key-broker` becomes the sole holder of the *issuer credential* for each S3-compatible, Azure Blob, or Google Cloud Storage location and hands out short-lived, least-privilege *storage credentials* scoped to what a specific job needs: read only, read plus create, or read plus delete for maintenance. Credential lifetime and proactive renewal margin are configurable so operators can decide how long consumers keep working when the broker is down, locked, or waiting for a custodian ceremony. This phase establishes the provider-enforced issuance and consumer-lifecycle foundation that Phase 27 extends to remote principals.

## Threat model and trust boundaries

This phase has two trust zones:

- **Local host.** Runs the unlocked broker, `vaulticdb`, and ordinary `vaultic` jobs. Compromise during an unlock epoch remains the Phase 20 accepted risk, but consumers receive only short-lived credentials with the minimum tier needed by the current operation.
- **Cloud provider.** Enforces credential scope and expiry. Vaultic-side checks are defense in depth, not the security boundary.

A compromised local consumer can use its current credential only until the shortest of credential expiry, broker outage grace, and provider revocation. Read and append consumers cannot delete or overwrite existing repository data. Long-lived issuer credentials never enter consumer memory, environment variables, command lines, logs, or runtime profiles.

Remote authentication, cross-host credential delivery, repository-key leases, and VaulticDB access tickets are deliberately deferred to Phase 27.

## Design

### Storage capability tiers

Every brokered storage credential carries one tier. Tiers are ordered; a request may be granted a tier equal to or lower than the requesting client's authorization.

| Tier | Vaultic operations | Provider permissions (conceptual) |
|---|---|---|
| `storage-read` | restore, check, ls, read-only `vaulticdb`, `read-only-assisted` crawl | list, get |
| `storage-append` | backup, deferred staging journal publication, placement copy | list, get, create-new-object; no overwrite, no delete |
| `storage-maintain` | prune, gc, forget, placement eviction, journal abandonment, metadata rebuild | list, get, create, delete (and version delete only where policy explicitly allows) |
| `storage-admin` | bucket policy, lifecycle, Object Lock changes | never issued by the broker; operator tooling only |

"Append" means create-only. Vaultic clients always write with a create-only precondition (`If-None-Match: *` on S3-compatible stores and Azure, `ifGenerationMatch=0` on GCS). The broker also scopes credentials so the provider rejects overwrite where it can express that distinction. The operator guide requires bucket versioning with immutability when the provider cannot express create-only access directly.

Provider mapping:

| Provider | Issuance mechanism | Scope expression | Practical TTL bounds (verify against current provider limits) |
|---|---|---|---|
| AWS S3 | `sts:AssumeRole` with an inline session policy | IAM actions and prefix conditions; `s3:DeleteObject*` absent for `read` and `append` | 15 minutes to the role maximum, up to 12 hours; 1 hour for role chaining |
| S3-compatible with STS | `AssumeRole` or `AssumeRoleWithWebIdentity` with session policy | Provider-supported IAM policy subset | Provider-specific; MinIO permits up to 7 days |
| S3-compatible without STS | Not supported for brokered credentials | none | Static credential leases remain available from Phase 24, but the location is non-compliant with this phase |
| Azure Blob Storage | User delegation SAS derived from a user delegation key obtained with the broker's Entra identity | `rl` for read, `rlc` for append, `rlcd` for maintain; container or prefix scoped | Up to 7 days, bounded by the delegation key |
| Google Cloud Storage | Service-account impersonation followed by a Credential Access Boundary downscoped token | viewer for read, object creator for append, delete roles only for maintain, with prefix conditions | 1 hour by default, up to 12 hours where organization policy permits |

The broker refuses a tier the provider cannot enforce and never silently widens a prefix or substitutes `maintain` for `append`.

### Issuer credential custody

The issuer credential is the cloud identity able to mint scoped credentials. It never leaves the broker. Phase 24 already seals long-lived credentials in capsule topology; this phase interprets credentials whose `may_issue` policy authorizes delegation and supports two issuer modes:

1. **Capsule-sealed issuer credential.** The broker uses an `aws-static`, `gcp-service-account-json`, or `azure-shared-key` credential recovered from the capsule. Rotation remains a capsule topology mutation and therefore publishes a new generation.
2. **Ambient issuer identity.** The broker host's workload identity holds only `sts:AssumeRole`, `generateUserDelegationKey`, or `iam.serviceAccounts.getAccessToken` permission on the issuer role. No secret is stored. Issuance remains gated on an unlocked epoch so a locked broker cannot act as an issuance oracle.

The topology's issuer policy binds location ID, provider, role or key reference, tenant/account/project, maximum tier, maximum TTL, permitted prefixes, and repository IDs. Status reports each location's custody mode and flags static credentials that cannot mint scoped, expiring credentials.

### Credential issuance and lifetime semantics

A local consumer requests `(location, tier, requested_ttl)` through the authenticated Phase 20 Unix channel. The broker validates client authorization and location policy. Requests beyond any configured maximum fail with a typed error rather than being silently clamped. Successful responses contain `{credential, not_before, not_after, tier, location, issue_id}`. Credential values travel only over the authenticated channel and remain in zeroizing memory.

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

Provider revocation is coarse: AWS can deny sessions issued before a role timestamp, Azure can revoke all user delegation keys, and GCS generally requires disabling the service account. Short TTL is the primary control. Broker policy changes stop new issuance immediately.

### Configuration surface

Broker and topology:

| Setting | Meaning |
|---|---|
| capsule issuer policy per location | Provider, custody mode, role or sealed credential reference, maximum tier and TTL, prefixes, repositories |
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

Broker status lists active issuances and expiry. `vaultic index keys status` and `index check` report non-brokered cloud locations, unsupported provider tiers, stale issuer credentials, and locations that can supply only a long-lived static credential.

## Implementation steps

1. Define storage capability tiers, provider mappings, and the create-only write contract. Make object creation use provider preconditions and treat a precondition failure for an identical existing object as success.
2. Extend capsule topology issuer policy with custody mode, delegation target, maximum tier and TTL, prefixes, and repository constraints while retaining Phase 24 generation-bound credential rotation.
3. Implement AWS STS session-policy issuance, capability-probed MinIO/Ceph STS issuance, Azure user delegation SAS issuance, and GCS impersonation plus Credential Access Boundary downscoping. Refuse unenforceable tiers.
4. Add the `storage-credential` broker capability for authorized local clients with request validation, issue IDs, strict TTL rejection, zeroizing responses, and per-location active issuance status.
5. Implement the shared `vaultic` and `vaulticdb` credential manager with renewal margin, outage grace, overlap for in-flight work, typed locked handling, bounded backoff, and component-specific fail-closed behavior.
6. Wire every cloud backend to refresh credentials in memory without rebuilding unrelated repository state or falling back to ambient consumer credentials.
7. Add compliance findings for non-brokered locations, unsupported scope tiers, and stale issuer credentials.
8. Document provider issuer roles, minimum IAM, create-only behavior, versioning and immutability requirements, and TTL sizing in `doc/051_cloud_object_storage.rst`; document issuer custody and unlock behavior in `doc/070_encryption.rst`.

## Tests

Provider issuance tests use mocked AWS, Azure, and GCS APIs plus live MinIO. Each tier receives exactly its intended permissions; append credentials cannot overwrite or delete, including on versioned buckets; requests above TTL limits are rejected; prefixes and repositories cannot be widened; unsupported tiers fail closed.

Consumer lifecycle tests cover renewal at the margin, overlap during multipart upload, locked responses, broker restart during a job, grace exhaustion, expiry during backup producing staged-pending without publishing a snapshot, read-only VaulticDB draining at expiry, and maintenance batches refusing to begin without enough remaining lifetime.

Integration tests run local `vaultic` and `vaulticdb` with no cloud credentials in their environment, obtain scoped credentials from the broker, exercise read, append, and maintain jobs, and verify provider-side denial of forbidden operations. Secret-hygiene tests assert that issuer credentials and ephemeral tokens never appear in events, status, errors, process arguments or environment, core dumps, or runtime profiles.

## Exit criterion

No local `vaultic` or `vaulticdb` process holds a long-lived cloud storage credential. The broker is the only process able to use capsule-sealed or ambient issuer identity, and every consumer credential is provider-enforced to its location, prefix, repository, tier, and configured TTL. Consumers renew proactively, tolerate broker unavailability only within the configured grace, and stop with component-appropriate fail-closed behavior at expiry. Read and append credentials cannot overwrite or delete existing repository objects, status exposes every active issuance without secret material, and unsupported or static-only locations are explicit compliance findings. Phase 27 can use this issuance API without changing provider credential semantics.
