# Phase 25: Backblaze and Wasabi S3-compatible backends

[← Back to roadmap index](00-overview.md)

[← Phase 24](phase-24-sealed-topology-and-credentials-in-the-recovery-capsule.md) · [Phase 26 →](phase-26-ephemeral-cloud-storage-credentials.md)

[Cloud object storage operator guide](../../051_cloud_object_storage.rst) · [Storage placement architecture](../02-architecture/05-storage-placement.md)

**Status: design specification, not yet implemented.**

**Goal:** make Backblaze B2 Cloud Storage and Wasabi dependable, explicitly supported S3-compatible targets for repository data, Phase 12 placement backends, and Phase 19 VaulticDB metadata replicas. Both providers use the existing `s3:` backend and AWS Signature Version 4 rather than new storage implementations. Add provider-aware endpoint and region handling, capability validation, safe defaults, diagnostics, documentation, and conformance coverage so operators do not have to infer provider-specific behavior from generic S3 options. Preserve the native `b2:` backend for compatibility, but recommend B2's S3-compatible API for new deployments.

## Scope and constraints

This phase supports:

- Backblaze B2 through its S3-compatible endpoint and S3-compatible application keys;
- Wasabi through its regional S3-compatible service endpoints and access keys;
- repository backends, pack-placement backends, and VaulticDB object-store replicas;
- static credentials from Phase 24 sealed topology as well as explicit external mode;
- provider-native versioning, immutability, and lifecycle settings without attempting to administer bucket policy from Vaultic.

This phase does not add provider-specific REST clients, create cloud accounts or buckets, manage billing controls, or emulate unsupported AWS services. Backblaze documents IAM roles as unsupported, so Phase 26 must report it as static-only unless the provider adds a delegation mechanism. Wasabi's temporary-credential capabilities and account-level availability must be probed against its current API before Phase 26 enables ephemeral issuance; absence of a verified mechanism is reported honestly rather than treated as AWS STS compatibility.

## Design

### Provider profiles

Add an optional S3 provider profile selected explicitly or inferred only from a recognized hostname:

| Profile | Endpoint form | Required region behavior | Default bucket lookup |
|---|---|---|---|
| `backblaze` | `s3.<region>.backblazeb2.com` | region is parsed from or must match the endpoint | `dns` |
| `wasabi` | `s3.<region>.wasabisys.com` | explicit region or an unambiguous regional endpoint is required | `dns` |
| `generic` | operator supplied | existing S3 behavior | `auto` |

The profile is available as `-o s3.provider=backblaze|wasabi|generic`. Existing generic URLs continue to work unchanged. Hostname inference may improve diagnostics and defaults but must never override an explicit profile, region, endpoint, or bucket-lookup option. A claimed provider whose endpoint does not belong to that provider fails before credentials are sent, preventing accidental credential disclosure to a mistyped host.

Canonical repository examples:

```text
s3:https://s3.us-west-004.backblazeb2.com/example-bucket/repository
s3:https://s3.eu-central-2.wasabisys.com/example-bucket/repository
```

Topology endpoints retain the common structured S3 shape and add the provider profile. No provider credential is embedded in a URL or endpoint object.

### Authentication and secret handling

Backblaze uses an S3-compatible application key ID as the access-key ID and the corresponding application key as the secret. The key must be restricted to the target bucket where possible. Wasabi uses a dedicated access key and secret restricted by IAM policy to the repository bucket and prefix.

For quorum repositories, credentials are stored in Phase 24 capsule topology and released by broker lease. External topology mode continues to support the standard `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN` variables. Provider-branded aliases are not added because duplicate variable names make precedence and secret auditing harder.

Credentials are never accepted in repository URLs, logged configuration, diagnostics, or generated runtime profiles. Endpoint validation occurs before the credential provider performs a signed request.

### S3 compatibility contract

Both profiles use the existing MinIO-based S3 backend and must satisfy the full `backend.Backend` contract:

- create and open a repository;
- save with length and hash verification;
- ranged and full loads;
- stat, list with pagination, and remove;
- multipart upload and abort cleanup;
- idempotent handling of an already-present identical object;
- retry classification for throttling and transient server failures;
- repository layout compatibility with AWS S3 and existing generic S3 endpoints.

ListObjectsV2 is the default. `s3.list-objects-v1=true` remains an explicit compatibility escape hatch and is never enabled silently by a provider profile. HTTPS is mandatory unless the existing explicit HTTP form is used for a local test endpoint; recognized Backblaze and Wasabi production hostnames reject plaintext HTTP.

Provider-specific request behavior is limited to values required for interoperability: endpoint, signing region, addressing style, checksum negotiation, and capability reporting. Unsupported headers or AWS-only storage classes are omitted rather than retried indefinitely.

### Capabilities and operational differences

At open, the backend records a provider capability set used by validation and status:

| Capability | Backblaze B2 S3 | Wasabi |
|---|---|---|
| Signature V4 | required | required |
| Multipart upload | supported | supported |
| Range reads | supported | supported |
| ListObjectsV2 | supported | supported |
| Conditional create | probe and report exact behavior | probe and report exact behavior |
| Version retention | provider bucket lifecycle/version rules | bucket versioning and retention rules |
| Object immutability | B2 Object Lock when enabled | Wasabi Object Lock when enabled |
| AWS STS role assumption | unsupported; IAM roles are not available | capability-probed and disabled unless the account and endpoint prove support |
| Glacier restore API | unsupported | unsupported |

Selecting `s3.enable-restore=true` or AWS Glacier storage classes for either profile fails during configuration. Unknown storage classes produce a typed provider-compatibility error. Vaultic does not infer durability from a successful write: storage verification still performs independent read and hash checks.

Because both services can charge for retention, minimum storage duration, API operations, or egress, prune and placement estimates identify the provider and warn when deletion may incur charges. The warning does not weaken retention or skip requested deletion.

### Addressing and endpoint safety

Buckets use DNS-style addressing by default. Path-style access remains available only as an explicit override for documented provider or network compatibility. Validation covers:

- HTTPS and recognized provider hostname suffix;
- endpoint region agreeing with `s3.region`;
- nonempty bucket and normalized prefix;
- DNS-compatible bucket names when DNS lookup is selected;
- rejection of endpoint userinfo, query strings, fragments, and credential-bearing URLs;
- TLS certificate and optional Phase 24 `tls_sha256` pin validation;
- redirects never forwarding authorization to an unvalidated host.

A custom CNAME or private endpoint cannot be inferred safely and therefore requires `s3.provider`, an explicit region, and the existing endpoint trust configuration.

### Configuration and topology

The S3 options gain:

| Option | Meaning |
|---|---|
| `s3.provider=generic|backblaze|wasabi` | Select provider validation, defaults, and capability reporting |
| `s3.region=REGION` | Signing region; required when it cannot be derived safely |
| `s3.bucket-lookup=dns|path|auto` | Existing addressing control; provider profiles default to `dns` |

The Phase 24 topology S3 endpoint gains an optional `provider` field with the same closed values. Canonicalization includes it, and Go and Rust topology validation agree on endpoint and region rules. Existing topology documents without the field remain `generic`; adding a provider profile is a topology mutation and publishes a new capsule generation.

VaulticDB receives the same profile through structured capsule topology. External mode exposes an equivalent daemon provider setting and retains existing endpoint, region, bucket, and prefix configuration. Repository, placement, and metadata code paths must all construct the same normalized S3 configuration rather than implementing provider rules independently.

### Migration and compatibility

The native `b2:` backend remains readable and writable. No repository rewrite is required to keep using it. New Backblaze documentation recommends `s3:` because that path shares the same placement, metadata-replica, credential-leasing, and conformance behavior as other object stores.

Moving an existing repository from `b2:` to Backblaze's S3 API is a backend migration, not a URL edit: operators copy or verify every object through existing migration tooling, compare object names, lengths, and hashes, then switch topology through the broker. Vaultic must not assume native B2 object naming and S3 object naming are interchangeable without verification.

Existing generic S3 configurations for Wasabi or Backblaze remain valid. Adding `s3.provider` opts into stricter checks and clearer status; hostname inference may report a provider but does not mutate persisted configuration.

### Observability and documentation

Status and events identify provider, endpoint host, region, bucket, prefix hash, addressing mode, and capability results without exposing credentials or full sensitive prefixes. Errors distinguish endpoint mismatch, signing-region mismatch, authentication denial, unsupported API, throttling, quota, retention denial, and TLS failure.

Update `doc/030_preparing_a_new_repo.rst` and `doc/051_cloud_object_storage.rst` with current Backblaze and Wasabi endpoint examples, key creation guidance, minimum permissions, versioning/Object Lock considerations, lifecycle and deletion-cost warnings, and configurations for repository, placement, and VaulticDB use. Clearly distinguish Backblaze S3-compatible application keys from credentials for the native `b2:` backend.

## Implementation steps

1. Add the closed `s3.provider` option and one normalized provider-profile abstraction in `internal/backend/s3`, preserving generic defaults.
2. Implement explicit and hostname-derived Backblaze and Wasabi profiles with endpoint suffix, signing-region, addressing, HTTPS, redirect, storage-class, and restore validation.
3. Extend Phase 24's Go and Rust structured S3 endpoint schema with the optional provider field and matching canonical validation.
4. Route repository, placement, and VaulticDB S3 construction through the same normalized profile and expose provider capabilities in secret-free status.
5. Add provider-aware errors and retry classification for authorization, throttling, quota, retention, and unsupported API responses.
6. Verify conditional-create behavior and preserve create-only semantics where supported; report any weaker provider behavior so placement and Phase 26 credential policy can fail closed.
7. Add migration verification coverage between native `b2:` and Backblaze S3 layouts without removing or changing the native backend.
8. Update setup, cloud-storage, encryption, and migration documentation with provider-specific examples and limitations.

## Tests

Unit tests cover explicit profile selection, safe hostname inference, explicit-option precedence, endpoint suffix validation, custom endpoints, region mismatch, DNS and path lookup, production HTTP rejection, redirect credential isolation, unsupported storage classes, Glacier-option rejection, and stable topology canonicalization in Go and Rust.

Backend conformance runs against provider-faithful local fakes and opt-in live buckets for both Backblaze and Wasabi. It covers create/open, save, stat, ranged load, paginated list, remove, multipart upload and abort, retries, duplicate writes, wrong region, wrong credentials, TLS failure, throttling, retention denial, and cleanup under interrupted operations. Every live test uses a unique prefix and refuses to run without an explicit destructive-test acknowledgement.

Integration tests exercise each provider as the primary repository, a placement target, and a VaulticDB metadata replica using capsule-leased static credentials and clean consumer environments. Migration tests compare a native `b2:` tree with the same Backblaze bucket reached through `s3:` before topology activation. Secret-hygiene tests assert that access keys and secrets never appear in URLs, logs, errors, events, status, process arguments, or runtime profiles.

## Exit criterion

Backblaze B2 and Wasabi are documented and continuously tested S3-compatible providers for repository storage, pack placement, and VaulticDB metadata. Operators can select a provider profile that validates endpoint, signing region, addressing, TLS, and unsupported features before use while existing generic S3 configurations remain compatible. Both providers pass the backend contract and clean-environment capsule-credential integration tests, native `b2:` remains supported, provider limitations are visible in status, and no credential appears outside the Phase 24 custody and lease boundary.
