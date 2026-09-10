# Phase 24: Sealed topology and credentials in the recovery capsule

[← Back to roadmap index](00-overview.md)

[← Phase 23](phase-23-macos-fsevents-change-detection-and-apfs-snapshot-backup-source.md) · [Phase 25 →](phase-25-backblaze-and-wasabi-s3-compatible-backends.md)

[Phase 20 quorum unlock](phase-20-quorum-based-encryption-unlock.md) · [Phase 22 bootstrap topology](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md) · [Broker implementation](../02-architecture/08-quorum-key-broker.md) · [macOS takeover runbook](../../055_takeover_rustic_google_drive_macos.rst)

**Status: implemented.**

**Goal:** make the quorum-protected recovery capsule the *single* sealed source of everything a fresh host needs to reach and open a repository: the pack-placement topology, the VaulticDB metadata-replica topology, every endpoint identity (URL, bucket/container, prefix, region, expected TLS fingerprint where pinning is required), and the long-lived credentials for those endpoints. Today these live in three places with three trust levels — the encrypted repository `config` and the sealed bootstrap-topology manifest (both keyed from the repository master key, both explicitly credential-free), `VAULTICDB_*` environment variables and `issuers.d/`-style files on the host (plaintext, outside any custody boundary), and, for Google Drive, an `rclone` configuration file owned by a separate program. After this phase, a 2-of-3 ceremony against the capsule alone is sufficient to reconstruct pack backends, metadata replicas, and their credentials; nothing outside the capsule is a prerequisite except the capsule file itself and the custodian credentials. Long-lived credentials never reach a `vaultic` or `vaulticdb` process as environment variables or command-line arguments; they are released by the broker as leased material, exactly as the master key is today. Google Drive is supported natively so its OAuth refresh token is sealed like any other credential instead of living in an `rclone.conf`.

## Why the current split is a problem

The Phase 22 design deliberately kept credentials out of the bootstrap-topology manifest so a manifest copy on a public-ish bucket could not leak a key. That was correct for a manifest whose confidentiality depends only on the repository master key. It leaves two gaps:

1. **Recovery is not self-contained.** The takeover runbook shows the consequence: after finalizing quorum custody, an operator still needs the `VAULTICDB_REPLICATED_*` bucket names, endpoints, and `AWS_*` keys, plus an `rclone.conf` with a Google refresh token, to bring the repository back on a new Mac. Those are the pieces most likely to be lost with the old host and least likely to be in the custodians' possession.
2. **Credentials are the weakest link.** The master key is behind a 2-of-3 ceremony; the S3 secret that lets anyone *delete* every pack is in a launchd environment file, a shell profile, or `~/.config/rclone/rclone.conf` with mode 0600. An attacker who cannot open the capsule can still destroy the repository.

Phase 26 addresses the second gap for cloud providers that can mint short-lived scoped credentials from an issuer credential. This phase is the prerequisite it needs: the issuer credential itself, and every endpoint the issuer is for, must live inside the capsule rather than beside it. It also covers what Phase 26 cannot: providers with no delegation mechanism (S3-compatible stores without STS, Google Drive) whose only credential is long-lived. Phase 38 then extends those scoped credentials and the existing key leases to enrolled remote principals.

## Design

### A mandatory wrapped payload: the sealed topology

`RecoveryCapsule` format 1 carries all three purpose-separated `WrappedPayload`s under the quorum-recovered root key:

```text
RecoveryCapsule {
  header, policy, members,
  metadata_dek:            WrappedPayload,
  repository_master_key:   WrappedPayload,
  sealed_topology:         WrappedPayload,
}
```

`sealed_topology` wraps a canonical-JSON **topology document** with its own purpose-separated key (`HKDF(root, repository_id, "sealed-topology-v1")`) and AEAD associated data binding capsule `logical_id`, `generation`, and `repository_id`, so a payload cannot be transplanted between capsules or generations. Capsules without this payload, or with any format other than 1, are rejected.

The topology document:

```json
{
  "format": 2,
  "repository_id": "…",
  "topology_generation": 7,
  "pack_backends": [
    {
      "id": "drive",
      "provider": "google-drive",
      "endpoint": { "drive_id": "0AL…", "root_folder_id": "1xY…", "path": "Backup/Hosts/mbp" },
      "role": "primary", "offsite": true, "failure_domain": "google-drive",
      "credential_policy": { "static": { "generation": 1, "bindings": { "storage-read": "cred:drive-oauth", "storage-append": "cred:drive-oauth", "storage-maintain": "cred:drive-oauth", "storage-lock": "cred:drive-oauth" } } }
    },
    {
      "id": "archive",
      "provider": "s3",
      "endpoint": { "url": "https://s3.us-east-1.amazonaws.com", "bucket": "alice-deep-archive", "prefix": "", "region": "us-east-1", "storage_class": "DEEP_ARCHIVE", "tls_sha256": null },
      "role": "archival", "offsite": true, "failure_domain": "aws-archive",
      "credential_policy": { "static": { "generation": 1, "bindings": { "storage-maintain": "cred:aws-archive" } } }
    }
  ],
  "placement_policy": { "min_copies": 2, "min_domains": 2, "min_offsite": 1, "offsite_deadline_seconds": 14400, "promotion_crossover_seconds": 2592000 },
  "staging_backends": ["drive"],
  "metadata_replicas": {
    "mode": "replicated",
    "order": ["local", "gcp"],
    "fencing": "gcp",
    "replicas": {
      "local": { "provider": "local", "endpoint": { "data_dir": "/Users/oli/Library/Application Support/vaultic/vaulticdb" } },
      "gcp":   { "provider": "s3", "endpoint": { "url": "https://storage.googleapis.com", "bucket": "alice-vaulticdb", "prefix": "mbp", "region": "auto" }, "credential_policy": { "static": { "generation": 1, "bindings": { "storage-maintain": "cred:gcp-hmac" } } } }
    }
  },
  "credentials": {
    "cred:drive-oauth":        { "kind": "oauth2-refresh-token", "client_id": "…apps.googleusercontent.com", "client_secret": "…", "refresh_token": "1//0g…", "scopes": ["https://www.googleapis.com/auth/drive.file"], "token_uri": "https://oauth2.googleapis.com/token", "issued_at": "…", "rotation_due": "…" },
    "cred:aws-archive":        { "kind": "aws-static", "access_key_id": "AKIA…", "secret_access_key": "…" },
    "cred:gcp-hmac":           { "kind": "s3-static", "access_key_id": "GOOG1E…", "secret_access_key": "…" }
  }
}
```

Rules:

- `pack_backends`, `placement_policy`, and `staging_backends` are the same shapes the encrypted repository `config` and the Phase 22 bootstrap manifest already carry. The document adds a structured `endpoint` object per backend that replaces free-form locator strings, so there is nothing to parse a secret out of and `containsCredential` becomes unnecessary for capsule-sourced topology.
- `metadata_replicas` carries what `VAULTICDB_OBJECT_STORE`, `VAULTICDB_REPLICATED_REPLICAS`, `VAULTICDB_FENCING_REPLICA`, and the per-replica `VAULTICDB_REPLICATED_<ID>_*` variables carry today, with credentials factored out by reference.
- Every issuer and static binding in `credential_policy` points into `credentials`; dangling or unused references fail validation. The closed bindings are `storage-read`, `storage-append`, `storage-maintain`, and `storage-lock`. Static policies carry a rotation generation and acknowledged provider-revoked generations. Credential kinds are a closed enum with per-kind validation (`aws-static`, `s3-static`, `azure-shared-key`, `azure-sas`, `gcp-service-account-json`, `oauth2-refresh-token`, `none` for workload identity); generated `s3-session` values are lease-only and cannot be sealed into topology.
- The document is canonical JSON (sorted keys, no insignificant whitespace) so its SHA-256 is a stable `topology_sha256` for anchoring and status.
- Size is bounded (1 MiB, same as `MaxManifestBytes`); the capsule stays a single small object.

### Precedence and reconciliation with existing sources

The capsule topology becomes the *authority* for reachability; the encrypted repository `config` remains the authority for repository semantics (chunker, pack sizes, and so on). Reconciliation on open:

| Source | Contains after this phase | Role |
|---|---|---|
| Capsule `sealed_topology` | pack endpoints, replica endpoints, placement policy, credentials | authoritative for *where to connect and with what* |
| Encrypted repository `config` | `placement_backends` (IDs, roles, domains, policy) without endpoint credentials | authoritative for repository-level policy; must agree with the capsule on backend IDs, roles, domains, and policy, or open fails with a typed conflict |
| Phase 22 bootstrap-topology manifest | unchanged, credential-free | retained as a metadata-outage discovery aid and verified against the authoritative capsule topology |
| `VAULTICDB_*` env, `AWS_*` env, `rclone.conf` | nothing required | permitted only in `--topology-source external` mode (see below), which is reported as non-compliant |

Placement mutations (`vaultic config --set-placement-*`, `index placement`, Phase 22 topology mutation) already require an unlocked broker. This phase routes them through one path: the mutation produces a new topology document, the broker validates it, wraps it, publishes a new capsule generation (the existing policy-mutation publication path: repository mirror first, local immutable generation second, then activate), and *then* the repository config projection is updated. A topology change is therefore a capsule generation, auditable and rollback-protected by the same generation anchors custodians already hold. Credentials-only rotation (new secret, same endpoints) is also a generation; there is no side channel that changes a credential without a custodian-visible generation bump.

### Releasing topology and credentials to consumers

The broker never hands the whole topology document to a client. Two capabilities are added to the `Capability` enum:

- `topology-read` — returns the document with the `credentials` map removed. Replaces the Phase 22 `topology-discovery` HKDF key for recovery capsules. Granted to `vaultic` and `vaulticdb` alongside their existing leases.
- `credential-lease` — returns one credential by broker-side `(storage_target, storage_tier)` selection. Storage consumers never select sealed references directly. The lease uses the same TTL, connection binding, epoch binding, and revocation semantics as key leases and includes source, target, tier, provider expiry, and static generation metadata. A read-only client may request only `storage-read`.

Consumers hold credentials in zeroizing memory for the lease duration. For static kinds a repeated lease returns the exact configured tier key; broker expiry does not revoke a copied provider key. Phase 26 adds broker-owned STS issuance and generation-tagged static fallback through `credential_policy`; issuer credentials remain sealed and are never leased to consumers.

`vaulticdb` startup changes from "read `VAULTICDB_*` and connect" to "acquire `metadata-dek` lease, acquire `topology-read`, acquire `credential-lease` for each replica reference, build the object-store stack." The `VAULTICDB_*` variables remain accepted only when `VAULTICDB_TOPOLOGY_SOURCE=external`, which status reports as a finding. The same applies to `vaultic`: `--topology-source capsule|external`, default `capsule` when the broker is configured.

### Native Google Drive backend

Today Google Drive is reached through `internal/backend/rclone`, which spawns `rclone serve restic --stdio` and speaks HTTP/2 over the child's stdin/stdout. Credentials live in rclone's config, outside Vaultic's custody boundary, and Vaultic cannot seal what it does not hold. Sealing an entire `rclone.conf` blob as an opaque credential and materializing it to a temp file for the child was considered and rejected: it writes a plaintext refresh token to disk on every run, it couples the capsule format to rclone's configuration format, and it leaves rclone (not Vaultic) responsible for token refresh and scope.

Feasibility of a native backend (`internal/backend/gdrive`):

| Concern | Assessment |
|---|---|
| Client library | `google.golang.org/api` (Drive v3) and `golang.org/x/oauth2` are already dependencies for the GCS backend. No new module. |
| Backend contract | `backend.Backend` needs `Save`, `Load` (with offset/length), `Stat`, `Remove`, `List`, `Delete`, `IsNotExist`, `Hasher`, `HasAtomicReplace`. Drive v3 supports: resumable and simple multipart upload; `alt=media` download with HTTP `Range` (offset/length reads work natively); `files.get` for stat with `md5Checksum` and `size`; `files.delete`; `files.list` with `q` and pagination. Atomic replace does not exist, but Vaultic's content-addressed layout never overwrites, and `HasAtomicReplace=false` is the same answer S3 gives. |
| Layout | The default `layout.Default` maps to a folder tree `data/<prefix>/…`, `index/`, `keys/`, `snapshots/`, `locks/`, `config`. Drive identifies files by ID, not path; the backend keeps a per-process path→ID cache warmed by a single recursive `files.list` at open and invalidated on `NotFound`. Folder creation is idempotent by listing before creating with a per-folder mutex, because Drive permits duplicate names. |
| Consistency | Drive is eventually consistent for `files.list` after `create` on the order of seconds. Vaultic's backend tests already model this (`orderedListOnceBackend`); the backend is marked non-strongly-consistent so lock-free and list-twice optimizations stay disabled, exactly as for the existing rclone path. |
| Rate limits | Per-user and per-project quota; 403 `userRateLimitExceeded` / `rateLimitExceeded` map to retriable errors with the existing `backend/retry` exponential backoff. Default connection limit 4 (Drive's practical write concurrency), matching the runbook's `--transfers 2`–style tuning through `-o gdrive.connections`. |
| Credential | OAuth 2.0 refresh token for an installed-app client, scope `drive` so the native backend can inspect an existing tree created by rclone, plus `client_id`/`client_secret`. `oauth2.TokenSource` refreshes access tokens in memory; nothing is written to disk. Service accounts with domain-wide delegation and an explicit sealed scope list are also supported (`gcp-service-account-json` kind with a `subject`) for Workspace tenants. |
| Enrollment | `vaultic backend enroll gdrive --client-id … --client-secret-file …` runs the loopback OAuth flow once on a workstation, obtains the refresh token, and hands it to the unlocked broker to seal into the topology as `cred:<id>`. No plaintext token is printed or stored. |
| Migration from `rclone:` | The runbook's `rclone:biz-drive:Backup/Hosts/mbp` becomes `gdrive:<drive_id>/Backup/Hosts/mbp` with the same folder layout; the native backend reads the tree rclone wrote. A `vaultic backend verify --compare rclone:… gdrive:…` lists both and confirms identical object sets before the topology switch. |
| Effort | Roughly the size of `internal/backend/gs` plus the ID-cache layer: a few thousand lines including tests against a recorded-response fake and an opt-in live test behind `VAULTIC_TEST_GDRIVE_*`. Medium difficulty; no unknowns that block it. |

The `rclone` backend is retained for other remotes, but a Google Drive location reached through `rclone:` is reported as `credential-outside-capsule` non-compliant once this phase lands.

### Bootstrapping a fresh host

The recovery sequence the runbook needs becomes:

1. Install Vaultic and the broker; place the capsule file (from the repository mirror `_vaultic/recovery-capsules/`, a local copy, or the offline export) in the capsule directory.
2. Start the broker; complete a 2-of-3 ceremony.
3. `vaultic bootstrap --from-capsule` acquires `topology-read` and the pack-backend credential leases, opens the pack backends, reads the encrypted repository `config`, verifies it agrees with the capsule topology, and writes a credential-free local runtime profile (repository ID, capsule directory, broker socket) — nothing else.
4. `vaulticdb` starts with `VAULTICDB_TOPOLOGY_SOURCE=capsule`, acquires its leases, and connects to the replicas the capsule names. If the local replica is empty and a remote replica exists, the existing Phase 19 remote-candidate rebuild procedure applies, now without any hand-typed bucket names.

The only artifacts that must survive the old host are the capsule generation (already mirrored into the repository and exportable offline) and two of three custodian credentials.

### What is deliberately not sealed

- Custodian material (Secure Enclave key, YubiKey PIV key, recovery passphrase) — it unlocks the capsule and cannot be inside it.
- The broker identity key and release-signing keys — they authenticate the capsule and the clients; sealing them would be circular.
- Ephemeral tokens minted by Phase 26 — they are leases, not custody.
- Local paths that are host-specific (`data_dir`, socket paths) *are* sealed, because they are topology, but a `--topology-override local.data_dir=…` is allowed at bootstrap and is recorded as a deviation in status.

### Threat model delta

| Before | After |
|---|---|
| S3 secret for the archive bucket in a launchd environment file; anyone with local user access can delete every pack. | Secret is inside the capsule; released only to an authorized, signed `vaultic` binary during an unlock epoch, as a lease. Cold theft of the host yields ciphertext. |
| Google refresh token in `~/.config/rclone/rclone.conf`; readable by any process running as the user. | Token is inside the capsule; the native backend refreshes in memory; nothing on disk. |
| VaulticDB replica endpoints and HMAC keys in environment variables visible in `ps`, crash logs, and launchd plists. | Endpoints and keys are leased from the broker per epoch. |
| Recovering on a new host needs the capsule + custodians + a separately kept list of buckets, endpoints, and keys. | Capsule + custodians. |
| Unchanged: compromise of the host *during* an unlock epoch exposes leased credentials for the lease duration. | Unchanged; Phase 26 narrows it with short-lived scoped tokens. |

## Implementation steps

1. Define capsule format 1 with a mandatory `sealed_topology: WrappedPayload`; `recover_from_shares` and `RecoveredKeys` always unwrap it under `sealed-topology-v1`, and all other capsule formats are rejected.
2. Define the topology document schema in `internal/topology` (Go) and `vaulticdb/src/topology.rs` (Rust) with canonical JSON encoding, the credential-kind enum, reference validation, and size bounds; share fixtures across both languages (the same pattern as the existing cross-language capsule fixture test).
3. Add `Capability::TopologyRead` and `Capability::CredentialLease` to the broker; implement redaction for `topology-read` and target/tier selection for storage credential leases with existing lease semantics.
4. Route all topology and placement mutations through a broker `topology-mutate` operation that validates, seals, and publishes a new capsule generation before updating the repository config projection; add `vaultic index keys quorum topology show|set-backend|set-replica|set-credential|rotate-credential|remove-credential` with `--acknowledge-policy-downgrade`-style confirmation when a change reduces `min_copies`, `min_domains`, or `min_offsite`.
5. Teach `vaultic` repository opening to prefer capsule topology (`--topology-source capsule`), build backends from structured endpoints plus leased credentials, and verify agreement with the encrypted `config`.
6. Teach `vaulticdb` to build its object-store stack from `topology-read` plus `credential-lease` (`VAULTICDB_TOPOLOGY_SOURCE=capsule`), keeping the environment path behind `external` with a compliance finding.
7. Implement `internal/backend/gdrive` with the ID cache, idempotent folder creation, ranged reads, resumable uploads, quota-aware retries, and OAuth/service-account token sources fed from a leased credential; add `vaultic backend enroll gdrive` and `vaultic backend verify --compare`.
8. Implement `vaultic bootstrap --from-capsule` and the credential-free runtime profile writer.
9. Add compliance findings: `topology: external`, any backend reached via `rclone:` whose remote is Google Drive, credentials with `rotation_due` in the past, `--topology-override` deviations.
10. Update `doc/070_encryption.rst` (format-1 capsule, capabilities, topology mutation as a generation), `doc/051_cloud_object_storage.rst` (capsule-sourced replica configuration, retirement of `VAULTICDB_*` endpoint variables), and the macOS takeover runbook (enroll Google Drive natively, drop `rclone.conf` and `VAULTICDB_REPLICATED_*` from the LaunchAgent environment, fresh-host recovery from capsule only).

## Tests

Capsule: format-1 round trip with all three mandatory payloads; every other capsule format and every missing topology payload is rejected; a `sealed_topology` payload transplanted to another capsule or generation fails authentication; the format-2 topology cross-language fixture proves Go and Rust produce identical canonical JSON and digests. Topology schema: every credential kind's validation; dangling and unused policy references; duplicate backend IDs; policy evaluation identical to `bootstrap.EvaluatePolicy`; size bound. Broker: `topology-read` never contains a credential value (byte-scan assertion); target/tier credential leases fail closed and read-only clients cannot request write-capable tiers; leases end at lock, epoch expiry, and connection close; a topology mutation publishes a new generation in the required order and refuses to activate on mirror-publication failure; policy-downgrade acknowledgement required. Consumers: `vaultic` opens all backends from capsule topology with no `AWS_*`, `RCLONE_*`, or `VAULTICDB_*` in the environment (asserted by a clean-environment integration harness); config/capsule disagreement fails with a typed conflict; `vaulticdb` builds local+S3 replicated storage from leases and refuses to start on `external` without the explicit flag. Google Drive: recorded-response fake covering upload, ranged download, stat, list pagination, delete, duplicate-folder races, `rateLimitExceeded` retry, and token refresh; live test behind `VAULTIC_TEST_GDRIVE_*`; `backend verify --compare` against a tree written by `rclone serve restic`; `orderedListOnceBackend` conformance. Bootstrap: fresh-host recovery with only the capsule and two custodian credentials reaches a readable repository and a running replicated `vaulticdb`; `--topology-override` recorded as a deviation. Secret hygiene: no credential value in events, status output, errors, `ps` environment, core dumps, or the runtime profile.

## Exit criterion

A format-1 recovery capsule containing format-2 topology plus any two of three custodian credentials is sufficient, with no other file or environment variable, to bring a fresh host to a fully opened repository: all pack backends (including Google Drive natively), the replicated VaulticDB metadata store, placement policy, and every long-lived credential are recovered from the capsule and released to signed `vaultic` and `vaulticdb` binaries only as epoch-bound leases. Every change to endpoints, policy, or credentials is a custodian-visible capsule generation with mirror-first publication and rollback protection. Google Drive credentials are held only in memory by a native backend; `rclone.conf`, `VAULTICDB_REPLICATED_*`, and `AWS_*` are no longer required and their use is reported as non-compliant. Earlier topology formats are deliberately unsupported because none were initialized.
