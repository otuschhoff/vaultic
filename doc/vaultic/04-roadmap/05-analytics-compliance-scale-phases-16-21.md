# Analytics, Compliance, and Scale-Out: Phases 16–21

[← Back to roadmap index](00-overview.md)

### Phase 16: Growth, churn, per-user/group attribution, and GDPR audit CLI

**Goal:** expose growth/churn and GDPR views plus exact, high-dimensional file
creation and live-versus-archive residency analytics across ownership, time,
source topology, custom path groups, and file size. Keep common queries fast
without precomputing every possible aggregate tuple.

**Implementation steps:**

1. Define versioned analytics configuration for source-root/LIF topology mapping,
  default SVM/volume extraction, custom `first-subdir`/depth/prefix path groups,
  history retention, query concurrency, and memory/persistent cache budgets.
2. Add dictionary-coded `af:` columnar creation segments, `am:` segment
  metadata, `ai:` compressed dimension bitmaps, and the mutable `ar:` residency
  overlay. Record UID, GID, exact logical size, decimal magnitude bucket,
  trustworthy birth time or first-seen basis, calendar year, ISO week-year/workweek,
  SVM, volume, custom group, and identity generation.
3. Emit creation facts only for new verified file identities. Update source
  state only after complete deletion proofs and update retained-snapshot
  reachability in snapshot publish/forget transactions. Atomically append
  idempotent `ae:` deltas with authoritative changes, consume them in commit
  order, and advance `aw:` only after a whole analytics commit is durable.
  Preserve `unknown` for incomplete evidence and expose metadata-head lag.
4. Implement a vectorized query engine that prunes segments, intersects bitmap
  predicates, applies exact range checks, joins residency, and computes count
  and logical-byte metrics for arbitrary filters/groupings. Add bounded
  concurrency, progress, cancellation, explain plans, snapshot consistency,
  and checkpointed asynchronous query jobs.
5. Add canonical query hashing, bounded in-memory and persistent result caches,
  scan-cost/frequency telemetry, watermark/epoch invalidation, TinyLFU-style
  admission, asynchronous cleanup, and adaptive promotion/demotion of popular
  expensive query shapes into partial `aq:view:` cuboids.
6. Implement `g:time:*`, `g:path:*`, `u:summary:*`, `g:summary:*`, and
  `u:churn:*` as rebuildable common materialized views. Implement `u:inodes:*`
  and `u:blobs:*` for exact GDPR lookup during reconciliation and snapshot
  purge transactions.
7. Implement `vaultic index analytics` with all ownership, creation-time,
  topology, custom-path, size, and residency predicates; arbitrary grouping;
  async query lifecycle; cache controls; `--explain`; and stable JSON.
8. Implement `vaultic index growth`, `index user-stats`, and
  `index gdpr audit --uid` on the same facts/views, including active/archive-only
  paths, blob hashes, pack locations, and retention expiry dates.
9. Add checkpointed `index analytics rebuild`, cache/view inspection and purge,
  classification-rule migration, and `index check` validation for fact/index,
  residency, dictionary, rollup, and adaptive-view consistency.
10. Benchmark 10M and 100M representative facts and extrapolate to 1.4B before
   general enablement. Measure compressed bytes/fact, write amplification,
   reconciliation overhead, bitmap cardinality, cold/cached latency, cache hit
  ratio, outbox catch-up lag, and rebuild duration. Any failed storage,
  overhead, lag, or query-timeout gate blocks general enablement and requires
  redesign plus a complete benchmark rerun.

**Tests:** creation-time fallback and inode-reuse identity tests; calendar-year/ISO
week boundary and zero/unknown/decimal/exact-size tests; trusted birth-time,
clock-skew fallback, concurrent basis selection, and source-generation tests;
SVM/volume and qtree/custom-rule
classification tests; complete-crawl deletion versus inaccessible/unknown tests;
archive-only transitions across backup, forget, and prune; randomized
multidimensional queries checked against a brute-force fact scan; broad async
query cancellation/resume; outbox crash/replay, whole-commit visibility, lag,
and concurrent snapshot-membership tests; cache key canonicalization, watermark invalidation,
admission/eviction, stale-read labeling, and adaptive-view fallback tests;
growth/user/GDPR rollup and rebuild tests; 10M/100M storage and latency
benchmarks using the reference feasibility profile and documented conservative
1.4B extrapolation.

**Exit criterion:** operators can correctly answer arbitrary conjunctions and
groupings over UID, GID, creation year/ISO workweek, SVM, volume, configured
path group, exact/decimal-magnitude file size, and live/archive residency, including
the UID 600 / 2024 / 1-10 MiB / archive-only example. Common and popular queries
use verified materialized or cached results; broad cold queries are bounded,
observable, cancellable, and resumable rather than falsely promised as instant.
At the 1.4B extrapolation, core analytics storage is at most 175 GB (250 GB
temporary planning ceiling), reconciliation overhead meets the measured 5% CPU
and 10% metadata-write targets, analytics catches up within one normal backup
interval, and the representative broad query meets its configured timeout.
Failure of any gate leaves analytics experimental and off by default. Disabling
or rebuilding it never affects backup correctness or the authoritative index.

The numeric gates above are evaluated exactly by the [analytics feasibility](../02-architecture/07-analytics-engine.md) reference
profile: 1,000/1,000 brute-force comparisons match, the named broad query meets
120 seconds at 100M and projects below 30 minutes at 1.4B, popular cache p95 is
at most two seconds, and catch-up meets the smaller-of-24-hours interval rule.
The real-daemon reconciliation profile runs seven alternating identical
baseline/analytics-enabled workloads through `SchemaStore` transactions. Its
paired median authoritative wall-time overhead is 0.0239% (paired p95 0.1555%)
against the 5% CPU/time target, and same-transaction encoded metadata grows from
316,000 to 336,400 bytes, or 6.4557%, against the 10% authoritative-write target.
Post-commit derived writes are measured separately; physical SlateDB compaction
amplification is not claimed by that logical encoded-byte ratio. All Phase 16
feasibility gates pass on the recorded reference environment.
Rebuild now checkpoints the exact authoritative source key and retains at most
`segment_rows` facts, while catch-up publishes bounded parent-linked delta
generations and compacts at eight layers. Production-path results expose peak
buffer and working-set estimates. The final 100M profile was rerun against the
current production segment codec, bitmap planner, and query path. Builder-memory
evidence is reported separately by the production rebuild/catch-up paths because
the direct large profile intentionally avoids duplicating authoritative input.
The reproducible harness, profile settings, and compact run artifacts are under
[the Phase 16 analytics benchmark](../benchmarks/phase16-analytics/README.md).

**Current implementation state (2026-08-30): complete.** Analytics is optional,
disabled until explicitly enabled, purgeable, and fully rebuildable from the
authoritative index. Rebuild streams authoritative source keys into bounded,
zstd-compressed columnar `af:` segments; dictionary records, segment metadata,
per-value bitmap indexes, residency overlays, common rollups, ownership
summaries, churn views, and GDPR inode/blob mappings are all derived state.
Candidate generations remain invisible until a constant-size transaction
publishes their manifest, completion marker, watermark, and active metadata.
Checkpoint validation prevents a corrupt or stale rebuild cursor from skipping
facts.

Authoritative inode, snapshot-membership, and proven source-state changes append
ordered `ae:` deltas in the same serializable transaction as the source change.
Retained-reference deltas carry explicit increment/decrement semantics while
remaining compatible with the original encoded form. Bounded catch-up creates
parent-linked immutable generations without rescanning the inode namespace,
uses tombstones for replaced derived records, compacts at eight layers, and
reclaims outbox records only after durable watermark publication. Complete,
debt-free crawl proofs are the only evidence that can turn absence into
deletion; incomplete or scoped-out evidence remains `unknown`. Snapshot forget
changes logical retained-reference state independently of physical GC.

The query engine pins a complete manifest/watermark, prunes segments, intersects
zstd or raw bitmap indexes, applies exact range predicates, joins residency,
and supports arbitrary filter/grouping subsets. Missing or malformed optional
indexes fall back to exact scans and appear in explain output. Persistent jobs
checkpoint at segment boundaries and support start, resume, wait, cancellation,
and generation-safe cleanup. Generation-scoped result caches and bounded
adaptive `aq:view:` cuboids use canonical predicates, TTL and admission/eviction
budgets, and exact rollup compatibility checks.

`vaultic index analytics`, `growth`, `user-stats`, and `gdpr audit` expose the
facts and materialized views with stable JSON, explicit stale/current behavior,
cache/view/job inspection, and bounded catch-up controls. `vaultic index check`
exactly validates active manifests, parent chains, segments, dictionaries,
indexes, overlays, rollups, ownership/churn/GDPR views, outbox ordering, cached
results, adaptive cuboids, and pinned jobs.

**Verification performed:** deterministic query tests compare 1,000 arbitrary
queries with an independent brute-force oracle; crash tests cover candidate
writes, checkpoints, pointer publication, outbox replay, and job resume; tests
cover identity reuse, complete/incomplete crawl evidence, snapshot publish and
forget, archive-only/expired transitions, malformed-index fallback, cuboid
promotion/eviction, exact materialized-view equivalence, bounded buffers,
generation compaction, and concurrent rebuild/query/catch-up. Focused package
tests and analytics/index race tests pass. Exact 10M and 100M profiles and the
real-daemon reconciliation profile pass every stated Phase 16 numeric gate. The
recorded write gate measures same-transaction encoded authoritative metadata;
physical SlateDB WAL, block-compression, and compaction amplification remains
explicitly unclaimed because it is storage-engine deployment evidence, not a
different hidden interpretation of the passed 10% gate.

### Phase 17: ISO27001 & GDPR compliance, Azure Key Vault, Syslog, and Storage Verification

**Goal:** provide enterprise compliance features including Azure Key Vault Option A passphrase vaulting, multi-target syslog event routing, advanced GDPR "Right to be forgotten" erasure & chunk survival analysis, UID backup exclusion policies, and attribute/sample-based storage verification across hot and cold tiers.

**Implementation steps:**

1. Implement Azure Key Vault Option A (Secret Store) integration using Azure Arc Managed Identity / `DefaultAzureCredential` to fetch repository key passphrases at startup (`SecretGet`) without modifying restic keyfile formats or requiring mid-job WAN connections.
2. Implement multi-target syslog exporter supporting UDP, TCP, TLS (syslog-over-TLS RFC 5424/3164), and local Unix domain sockets, with event routing filters by category (`auth`, `integrity`, `gdpr`, `restore`, `lifecycle`) and severity.
3. Expand `vaultic index gdpr audit` with `--explain-surviving-chunks` to report per-chunk deduplication survival and external non-scoped file references.
4. Implement `vaultic index gdpr execute-forget --uid <uid>` to purge user file references/inodes, re-evaluate blob reference counts, enqueue unreferenced packs into the deletion queue (`dq:`), and issue a cryptographic deletion certificate.
5. Implement `vaultic index gdpr set-policy --exclude-uid <uid>` writing persistent blocklist rules (`u:policy:blocklist:<uid>`) enforced by archiver/reconciliation during backup crawls.
6. Implement `vaultic index verify-storage` / `vaultic verify-packs` supporting
  attribute filters (tier, backend, age, retention, size, pack type), sampling
  controls (`--all`, `--sample-count`, `--sample-percent`), verification levels
  (header, checksum, full unpack), and automatic cold pack warm-up. Persist
  level-specific successful verification timestamps and current health per
  `(pack, backend)` in mutable `vr:` records. Persist only failure-state
  transitions (`detected`, classification `changed`, and `resolved`) in a
  dedicated append-only `ve:` error event log that survives pack deletion;
  coalesce repeated identical failures in `vr:` to avoid event storms. Add
  `--not-verified-since`, `--verification-level`, `--errors-only`, and
  `--error-history` selection/reporting, with integrity failures explicitly
  distinguished from transient operational failures.

**Tests:** Azure Key Vault SecretGet mock integration test verifying single startup fetch and restic keyfile decryption; syslog multi-target exporter test verifying TLS/UDP socket sending and category filter routing; GDPR audit `--explain-surviving-chunks` test confirming accurate identification of exclusive vs shared chunks and external file reference listings; GDPR `execute-forget` end-to-end test verifying inode reference removal, blob reference count decrements, `dq:` enqueuing, and deletion certificate generation; UID blocklist policy test verifying archiver skips files owned by blocked UIDs during subsequent backup crawls; sampled storage verification test verifying uniform random percentage selection, attribute filtering, level 1/2/3 checks, and automated cold pack warm-up invocation. Verification-state tests cover independent placements, stronger-level success advancing weaker timestamps without the reverse, age-based selection, atomic `vr:`/`ve:` updates, crash/replay idempotency, first detection, repeated-failure coalescing, classification changes, successful resolution, operational-versus-integrity classification, event retention after pack deletion, and `index check` reconstruction/drift reporting for the coarse `pl:` projection.

**Exit criterion:** enterprise operators can manage keys via Azure Key Vault, route structured audit events to SIEM/syslog targets, execute verifiable GDPR erasure with survival analysis and UID exclusion policies, and run sampled integrity checks across hot and cold storage tiers. Operators can prove when every placement last passed each verification level, select stale placements for re-verification, identify current integrity and operational failures without scanning event history, and retain an append-only detected/changed/resolved audit trail without logging every successful check or repeated retry.

### Phase 18: SlateDB metadata encryption and unified key envelope

**Goal:** encrypt every `vaulticdb` SlateDB object (SSTs, WALs, manifests, checkpoints) client-side at the `ObjectStore` boundary with a per-repository metadata key, manage that key through a versioned multi-slot envelope wrapping it independently in Azure Key Vault / AWS KMS / Google Cloud KMS and an offline local recovery key, and optionally hold the repository data master key inside the encrypted database with cloud-vault escrow so a single startup unlock opens both the metadata database and the restic-format data repository.

**Design:**

The metadata data-encryption key (DEK) is a random 256-bit AES key generated once per repository and distinct from the repository pack master key, the keyfile password, and every Phase 17 key: metadata and payload must have separate compromise boundaries, IAM permissions, and rotation schedules. The DEK is never persisted in plaintext. It is unwrapped exactly once at daemon startup and held only in daemon memory (best-effort `mlock`, core dumps disabled), so a temporary key-provider outage never interrupts a running daemon.

The DEK is stored in a versioned key envelope holding one wrapped copy per configured key-encryption-key (KEK) slot. Cloud slots use non-exportable keys through wrap/unwrap-style operations only — Azure Key Vault or Managed HSM `wrapKey`/`unwrapKey`, AWS KMS `Encrypt`/`Decrypt` with an encryption context binding the repository UUID and purpose, Google Cloud KMS `encrypt`/`decrypt`, and HashiCorp Vault Transit or PKCS#11 for self-hosted deployments. The offline recovery slot wraps the same DEK under an Argon2id-derived KEK from an operator passphrase (random salt and calibrated cost parameters stored in the envelope) or under an OS-keystore/TPM/PKCS#11-held key. Any single slot suffices to unlock, so a repository can be initialized and used with only the local recovery slot while cloud vault provisioning is still in progress, and cloud slots can be added later by rewrapping without touching any data. Slot use is policy-ordered: unlocking through the recovery slot while a higher-priority cloud slot is configured requires an explicit operator acknowledgement and emits a high-severity `auth` event, so a provider outage can never silently downgrade key handling. Envelopes are published as new versions beside the database and mirrored into the repository backend; the only decryptable envelope is never overwritten in place.

Encryption happens below SlateDB in an `EncryptedObjectStore` wrapper rather than per schema value, because value-level encryption would still leak key prefixes, paths, record distribution, WAL structure, and manifest contents. Each object carries an authenticated header (magic, format version, algorithm, DEK version, random object nonce, chunk size, plaintext length) and a body of independently authenticated AES-256-GCM chunks (default 256 KiB) whose nonces derive from the object nonce plus chunk counter and whose associated data binds repository UUID, object path, DEK version, chunk index, and plaintext length. Chunking keeps SlateDB ranged reads efficient — a range request maps to the covering ciphertext chunks instead of forcing a whole-SST download — and the associated-data binding rejects truncation, chunk reordering, cross-repository object copies, and header or algorithm downgrades as authentication failures surfaced as `data_loss`, never as `not found`.

In key-in-DB mode the repository master key (the same JSON `cat masterkey` prints) is stored in a `meta:` record inside the encrypted database, and vaultic opens the data repository through the existing direct master-key path after the daemon unlocks: one unlock opens metadata and data. Classic password keyfiles remain valid in parallel. Independently, wrapped escrow copies of the master key are stored with one or more cloud vault providers and mirrored into the repository backend, so if the metadata database is ever corrupted beyond repair the escrowed master key alone recovers direct read access to the restic/rustic-format data repository and the legacy JSON rollback path.

**Implementation steps:**

1. Implement `EncryptedObjectStore` in the `vaulticdb` Rust crate wrapping any inner `ObjectStore` (local, S3, and Phase 19's `ReplicatedObjectStore`, so ciphertext is replicated identically to all providers): chunked AES-256-GCM with the header, nonce derivation, and associated-data binding above; exact plaintext-range to ciphertext-chunk translation for `get_range`; strict fail-closed decode rejecting authentication failures, truncation, reordering, foreign-repository objects, nonce reuse, unknown DEK versions, and downgraded headers.
2. Implement the key hierarchy: per-repository random 256-bit metadata DEK generated at initialization, unwrapped once at startup, never persisted or logged in plaintext, held with best-effort locked memory and disabled core dumps.
3. Implement the versioned multi-slot key envelope with atomic new-version publication beside the database and a mirror in the repository backend; implement Azure Key Vault/Managed HSM, AWS KMS (with encryption context), Google Cloud KMS, Vault Transit, and PKCS#11 KEK slot providers behind one wrap/unwrap interface with mockable clients.
4. Implement the offline local recovery slot (Argon2id passphrase KEK with stored salt/parameters, or OS keystore/TPM/PKCS#11 key) so repositories are fully usable for testing and production before any cloud slot exists; adding or removing slots only rewraps the DEK.
5. Implement the unlock policy: slots tried in configured priority order, explicit `--recovery-unlock` acknowledgement plus high-severity `auth` syslog event when a lower-priority recovery slot unlocks a repository that has cloud slots configured, and fail-closed refusal to open a repository whose persistent `meta:encryption` policy requires encryption when no slot can unwrap.
6. Implement key-in-DB mode: store the repository master key in a `meta:` record inside the encrypted database, open the data repository via the existing master-key path when the daemon unlocks, and keep password keyfiles as a supported parallel unlock; never write the master key to logs, environment, or command lines.
7. Implement master-key escrow: wrap copies of the repository master key under one or more cloud vault KEKs with escrow-specific purpose binding, store the escrow records in the repository backend (and optionally the provider secret store), and provide a documented recovery command that retrieves an escrowed key and opens the restic-format repository directly after total metadata loss.
8. Implement rotation: KEK rotation rewraps the DEK without any data rewrite; DEK rotation adds a new DEK version, switches new writes to it, rewrites live objects in bounded checkpointed background batches keyed by each object's header DEK version, verifies zero old-version objects remain, and only then retires the old version; the same rewrap flow rotates escrowed master-key wrappings.
9. Implement migration and policy pinning: `vaultic index encrypt` initializes the envelope and rewrites an existing plaintext database in bounded batches with crash-resumable checkpoints; the persistent `meta:encryption` policy pins algorithm, format version, and required slots so plaintext substitution or downgrade is detected; `index check` validates envelope integrity and slot metadata (without unwrapping), per-object header/DEK-version consistency, and policy pinning.
10. Implement CLI and observability: `vaultic index keys status|add-slot|remove-slot|rotate-kek|rotate-dek|escrow` with stable JSON; `auth` syslog events for unwrap, recovery-slot use, rotation, and failed unwrap attempts; `integrity` events for object authentication failures; log only key identifiers and versions, never key material or wrapped blobs.

**Tests:** encrypt/decrypt round-trip and ranged-read equivalence against a plaintext `ObjectStore` across chunk boundaries and empty/oversized objects; tamper tests (bit flips, truncation, chunk reorder, cross-repository copy, header downgrade, wrong DEK version, nonce reuse) all failing closed as `data_loss` with `integrity` events; envelope tests with mocked Azure/AWS/GCP wrap APIs verifying any single slot unlocks, slot add/remove/rewrap, and atomic envelope versioning; recovery-slot Argon2id unlock, cloud-precedence acknowledgement, and no-silent-downgrade tests; KEK and DEK rotation tests verifying mixed-version reads during rewrite and zero old-version objects afterward; plaintext-to-encrypted migration with crash/resume; key-in-DB open test and escrow recovery test opening the data repository with only the escrowed master key after simulated metadata destruction; fail-closed tests for plaintext substitution, missing envelopes, and all-slots-failing; syslog `auth`/`integrity` routing tests.

**Exit criterion:** every SlateDB object at rest is authenticated-encrypted with a per-repository key that never touches disk in plaintext; the DEK unlocks through any configured slot across Azure Key Vault, AWS KMS, and Google Cloud KMS plus an offline recovery key usable before or without cloud provisioning; one startup unlock opens the metadata database and, in key-in-DB mode, the data repository; the escrowed master key alone suffices to read the restic-format repository after total metadata loss; KEKs and the DEK rotate online without downtime; encrypted repositories refuse plaintext or downgraded operation; and unwrap, recovery, rotation, and authentication-failure events are auditable through the Phase 17 syslog exporter. This closes the metadata-at-rest encryption gaps recorded in the GDPR (Art. 32), ISO/IEC 27001 (A.8.24), and NIS2 (Art. 21(2)(h)) assessments.

**Phase 20 supersession:** Phase 18 key-in-DB and standalone master-key escrow are the migration baseline, not the final custody model. Phase 20 moves the repository master key out of SlateDB and replaces both mechanisms with one pre-database envelope/recovery capsule containing separately wrapped metadata and repository keys under a unified configurable unlock policy.

### Phase 19: Multi-provider cold storage pool and replicated metadata store

**Goal:** implement multi-backend cold storage pools ($K$-of-$M$ durability quorums across arbitrary active cold providers with read-only legacy backends) and multi-cloud replicated metadata stores for `vaulticdb`.

**Implementation steps:**

1. Implement `ingest` and `read_enabled` flag evaluation in the backend registry (`placement_backends`): mark legacy cold backends (`ingest: false`, `read_enabled: true`) as read-only pools, and route all new pack allocations exclusively to active ingesting backends (`ingest: true`).
2. Implement $K$-of-$M$ multi-provider cold placement scheduling: evaluate the durability predicate (`min_copies`, `min_domains`, `min_offsite`) over the active ingesting backend pool (e.g. 2-of-3 active cold backends), writing parallel placements (`pl:<pack-id>:<backend-id>`) during backup jobs.
3. Implement `ReplicatedObjectStore` wrapper in `vaulticdb` Rust layer for synchronous parallel writes of SlateDB metadata (SSTs, WALs, manifests) across multiple cloud providers (e.g. AWS S3 + Azure Blob / Cloudflare R2) with primary provider read routing, transparent failover, and epoch-based fencing.
4. Implement zero-egress natural drain for legacy backends (`ingest: false`): allow old cold packs to linger on legacy backends until expired by retention policy (`min_retention_until`), purge unreachable packs directly via deletion queue (`dq:`), and route defragmentation repacks into new packs written to the active ingesting pool.
5. Expose CLI backend management commands: `vaultic index backends` status showing `ingest`/`read_enabled` flags per pool, `vaultic index placement` showing $K$-of-$M$ quorum compliance per pack, and `vaultic index placement migrate-pool` options.

**Tests:** multi-provider cold placement test verifying new packs are placed on $K$ of $M$ active backends while legacy backends receive 0 new writes; legacy backend read and warm-up test confirming restore requests route to old backends via `pl:` records; zero-egress natural drain test verifying old packs are deleted from legacy backends when retention expires and repacked blobs write to active ingesting backends; `ReplicatedObjectStore` unit test verifying synchronous multi-cloud write, transient provider outage handling, and primary-to-secondary read failover.

**Exit criterion:** new data packs are durably multi-homed across $K$-of-$M$ active cold providers, legacy cold backends receive zero new writes and naturally drain as retention expires, and `vaulticdb` metadata is synchronously replicated across multi-cloud storage.

### Phase 20: Quorum-based encryption unlock (threshold key custody with offline and Azure principals)

**Goal:** replace Phase 18 key-in-DB and independently protected master-key escrow with one pre-database, immutable envelope/recovery capsule that contains authenticated wrapped copies of both the SlateDB metadata DEK and the repository data master key. Protect that capsule with explicit, configurable unlock policies ranging from a single offline bootstrap key to cryptographically enforced $K$-of-$N$ groups containing any supported combination of offline keys and Azure Key Vault / Managed HSM keys authorized to individual Entra users or groups. A successful configured unlock must recover the data repository even when SlateDB is missing or cannot initialize, while complete operator documentation covers setup, migration, custody, policy changes, and disaster recovery.

**Design:**

A versioned envelope/recovery capsule is stored outside SlateDB at a deterministic bootstrap location beside the database object store, mirrored into the repository backend, and exportable as ciphertext for offline custody. It is readable before SlateDB starts and is the sole managed authoritative copy of two secrets: the metadata DEK used by `EncryptedObjectStore` and the repository master key used to authenticate and decrypt data packs. The repository master key is no longer stored in a SlateDB `meta:` record. Quorum-compliant mode removes ordinary repository password keyfiles, standalone single-provider master-key escrow, and unattended `--key`, `--key-file`, or `--key-command` paths; explicit emergency export remains a non-compliant, high-severity audited action because any plaintext master-key copy bypasses the configured quorum.

Each capsule is encrypted under a random per-generation root wrapping secret. Independent wrapping keys are derived with HKDF using the repository UUID, capsule format and generation, root-key version, and distinct purposes (`metadata-dek` and `repository-master-key`), then used with authenticated encryption to wrap the two secrets separately. This domain separation prevents one wrapped payload or implementation path from being substituted for the other. The complete capsule header and both ciphertexts are authenticated and bind repository UUID, envelope generation, algorithms, key versions, unlock-policy hash, and immutable object location. Unlock succeeds only if both payloads authenticate; partial recovery must never start SlateDB or open the data repository.

An unlock policy is an explicit `any_of` list of alternatives. An alternative may be a single offline bootstrap member or a threshold group. A threshold group protects the capsule root secret with Shamir secret sharing from a reviewed implementation over a suitable finite field: the root secret is split into $N$ shares with reconstruction threshold $K$, and each share is wrapped independently under one member's protection. Offline members protect a share with an Argon2id-derived KEK from a per-custodian passphrase or with a keyfile on removable media. Azure members protect a share with a dedicated versioned Key Vault / Managed HSM key through `wrapKey`/`unwrapKey`; Azure RBAC scopes each key to one Entra principal, either a named user or a group acting as a duty pool for that single quorum seat, while the daemon's standing identity can unwrap no Azure share. Each share binds the repository, capsule generation, policy and group IDs, share index, threshold, share count, root-key version, member ID, provider, and key reference, so shares from different repositories, generations, policies, or resharings cannot be combined.

Policies support one or many alternatives such as normal `2-of-4` Azure custodians and break-glass `2-of-3` offline custodians. Every membership, threshold, member-key rotation, or policy change requires an already unlocked daemon, generates a fresh root wrapping secret and fresh shares, rewraps both capsule payloads, and publishes and mirrors a new immutable generation without changing either underlying secret. Status reports the minimum effective threshold across every current access path; a retained 1-of-1 bootstrap member, password keyfile, direct master-key copy, single-key escrow, overlapping Entra authorization, or other complete-key bypass makes the repository non-compliant and must never be presented as four-eyes protected.

Because a locked daemon cannot call custodian-scoped Azure keys itself, unlock uses an interactive contribution session. The daemon reads the capsule without opening SlateDB and creates a one-time session using an authenticated, reviewed HPKE construction with a fresh ephemeral key, nonce, transcript, and expiry. Each custodian runs `vaultic index unlock contribute`, authenticates directly with Entra or opens an offline share locally, unwraps only their member share, encrypts it to the session, submits it, and zeroizes local plaintext. The daemon validates the Entra tenant and immutable principal object ID where applicable and rejects duplicate share indexes, duplicate contributing principals, foreign transcripts, expired or replayed contributions, and contributions for another repository or generation. After $K$ valid shares it reconstructs the root secret in locked memory, authenticates and unwraps both the metadata DEK and repository master key, zeroizes all intermediate material, and permanently closes the session.

The recovered repository master key is available independently of SlateDB health. If metadata opens, normal operation continues with both keys held only in protected daemon memory. If SlateDB is absent, corrupt, or fails authentication, Vaultic can use the already recovered repository master key to enter explicit read-only metadata-loss recovery, inspect or restore packs through the legacy index, and rebuild metadata without writing a plaintext key file. Use of an alternative marked recovery/break-glass requires explicit acknowledgement and emits a high-severity `auth` event. Capsule ciphertext is not secret without a quorum and is replicated aggressively, but loss of every capsule copy is unrecoverable even if all custodians retain their shares.

Ordinary policy changes are online custody operations and do not re-encrypt SlateDB objects or data packs. They do not, however, revoke an old capsule generation already copied together with enough old member credentials. Immutable-generation rollback protection pins the current generation in all available external anchors and rejects older generations during normal startup. Retiring a historical 1-of-1 metadata path with cryptographic assurance requires metadata-DEK rotation and authenticated object rewrite after that path is removed; suspected exposure of the repository master key requires repository rekeying or pack rewrite and cannot be repaired by resharing alone.

**Implementation steps:**

1. Define and implement the versioned envelope/recovery-capsule format at a deterministic pre-SlateDB location: authenticated header and policy, independently HKDF-derived AEAD wrappings of the metadata DEK and repository master key, immutable local publication, automatic repository-backend mirroring, protected offline export, generation discovery, and external anti-rollback anchors.
2. Migrate Phase 18 key-in-DB repositories transactionally: while unlocked, copy and verify the repository master key in a new capsule generation, prove direct pack access from the capsule, publish all mirrors, then delete the SlateDB master-key record and retire standalone escrow records. Never remove the last verified recovery path, and retain no plaintext intermediate file.
3. Implement versioned Shamir sharing with a reviewed library and zeroizing buffers: split and reconstruct the capsule root secret, wrap each share with all repository/generation/policy/group/member/index/threshold bindings, and validate reconstruction by authenticated decryption of both capsule payloads before releasing either key.
4. Extend the envelope with `any_of` alternatives containing a single offline bootstrap member or threshold groups. Support one or many `offline-argon2id`, `offline-keyfile`, and `azure-key-vault` members in any configured grouping; preserve Phase 18 envelopes only as an explicit migration input, not as a parallel quorum-compliant access path.
5. Implement locked startup and policy evaluation before SlateDB initialization. Read and validate the current capsule, attempt only explicitly non-interactive alternatives, otherwise expose the unlock-session API while keeping storage closed; after unlock, pass the metadata DEK to `EncryptedObjectStore` and the repository master key directly to the repository opener.
6. Implement authenticated one-time unlock sessions in the daemon gRPC surface using a reviewed HPKE construction: session creation, transcript and endpoint binding, Entra token validation, contribution submission, duplicate-share and duplicate-principal rejection, replay/expiry rejection, progress reporting, permanent closure, and comprehensive secret zeroization.
7. Implement `vaultic index unlock status|contribute` for interactive Entra, offline passphrase, and offline keyfile members, plus `vaultic index keys quorum create-group|add-member|remove-member|set-threshold|replace-member|verify`. Every mutation requires an unlocked daemon, generates a fresh capsule root secret and shares, rewraps both payloads, and publishes all immutable mirrors before activation.
8. Implement effective-policy validation in `index keys status` and `index check`: enumerate every metadata and repository-master-key access route, compute the minimum number of independent custodians, reject or flag legacy password keys, direct exports, standalone escrow, stale current-generation anchors, bootstrap bypasses, duplicated Azure keys, and overlapping principal/group authorization.
9. Extend metadata-loss recovery so a successfully unlocked capsule opens the data repository read-only even when every SlateDB object is deleted or corrupt, without materializing the master key on disk. Add authenticated metadata rebuild handoff and require explicit acknowledgement and critical audit events before any recovery-mode operation.
10. Emit `auth`, `integrity`, and `lifecycle` events for capsule discovery and rollback rejection, session lifecycle, accepted/rejected contributions, quorum completion, both-payload authentication, break-glass use, policy changes, generation publication/mirroring, threshold downgrade, plaintext key export, and migration. Log identifiers and versions only.
11. Document the complete model in `doc/070_encryption.rst`: capsule availability versus secrecy, master-key and metadata-key separation, effective-policy semantics, Azure per-key RBAC for Entra users/groups, one-seat-per-group and non-overlap rules, offline custody, bootstrap → offline $2$-of-$3$ → Azure $2$-of-$4$ migration, removal of password/key-in-DB/standalone-escrow bypasses, online policy changes, old-generation limitations, break-glass recovery, total SlateDB destruction, rebuild, rotation, and periodic recovery exercises.

**Tests:** capsule-format round trips and independent purpose-binding tests for both payloads; malformed, partial, swapped, foreign-repository, foreign-location, stale-generation, and rollback capsules failing closed before SlateDB initialization; Shamir split/reconstruct across thresholds ($1$-of-$1$ through $3$-of-$5$), every $K$-subset succeeding and every $(K{-}1)$-subset failing; wrong, corrupt, duplicate, re-indexed, and mixed-policy/generation shares failing closed; unlock-session endpoint/transcript binding, Entra tenant/object-ID validation, duplicate-principal, replay, expiry, concurrent-session, and zeroization tests; offline Argon2id/keyfile and mocked Azure per-member isolation tests; effective-policy tests detecting every password, direct-key, escrow, bootstrap, stale-anchor, duplicate-key, and overlapping-principal bypass; online membership/threshold/KEK changes proving fresh roots/shares and byte-identical SlateDB/data-pack ciphertext; key-in-DB migration crash tests at every publication/deletion boundary; full migration (single offline bootstrap → offline $2$-of-$3$ → Azure $2$-of-$4$ → bootstrap removal); physical deletion and authenticated corruption of all SlateDB objects followed by quorum unlock, in-memory master-key recovery, pack listing and restore in read-only mode without a plaintext key file; metadata rebuild handoff; old-generation threat and metadata-DEK retirement tests; audit/syslog routing tests; documentation build and example-command validation.

**Exit criterion:** the current immutable envelope/recovery capsule, available before and independently of SlateDB, is the sole managed authoritative holder of authenticated wrapped copies of both the metadata DEK and repository master key; the database contains no repository master key. A configured single-member or $K$-of-$N$ policy can combine any supported offline and Azure members, with Azure seats individually RBAC-scoped to Entra users or groups, and status/`index check` report the minimum effective threshold across every access route. A quorum-compliant repository has no password-keyfile, direct-key, standalone-escrow, bootstrap, daemon-credential, or principal-overlap bypass. One successful unlock releases both keys only into protected memory; complete loss or corruption of SlateDB still permits read-only pack listing and restore from a surviving capsule without writing a plaintext master key. Policy and member changes publish fresh immutable capsule generations online without rewriting metadata or packs, while documented rotation rules accurately distinguish resharing from cryptographic revocation of copied old generations or exposed underlying keys. Operators can perform bootstrap, Azure and offline quorum setup, migration, verification, replacement, rollback response, total-metadata-loss recovery, rebuild, and periodic recovery exercises from the documentation alone.

### Phase 21: Crawl optimization with `cwalk` and `pathdiff`

**Goal:** accelerate backup crawls across 1.5+ billion inode storage targets using `cwalk` concurrent directory traversal and `pathdiff` selective change-path crawling with guaranteed event coverage and SVM/volume topology mapping.

**Implementation steps:**

1. Integrate `cwalk` (`github.com/otuschhoff/cwalk`) into the archiver and reconciliation scanner pipeline, replacing sequential traversal with parallel, multi-threaded directory walking with configurable concurrency (`--cwalk-concurrency N`) and queue capacity bounds.
2. Import `pathdiff` (`github.com/otuschhoff/pathdiff`) into `vaultic` (e.g. `internal/pathdiff`), making in-scope enhancements to support volume ID-to-name resolution, target host LIF (Logical Interface) $\rightarrow$ SVM (Storage Virtual Machine) $\rightarrow$ volume mapping, and event sequence continuity checks.
3. Implement 100% event-coverage verification: query `pathdiff` for contiguous change events since the last snapshot timestamp of the source path; verify zero sequence gaps, buffer overflows, or unmonitored windows.
4. Implement selective change-path crawl execution: if 100% event coverage is verified, crawl only the modified subtrees identified by `pathdiff`; if event coverage is incomplete or unverified, fall back automatically to a full `cwalk` traversal.
5. Expose CLI crawl options: `--use-cwalk`, `--cwalk-concurrency N`, `--use-pathdiff`, `--pathdiff-endpoint`, `--pathdiff-require-coverage`, and `--pathdiff-svm-map`.

**Tests:** `cwalk` high-concurrency directory traversal correctness test comparing results against standard traversal; `pathdiff` volume ID resolution and target host LIF $\rightarrow$ SVM $\rightarrow$ volume topology matching test; event coverage gap detection test verifying automatic fallback to full `cwalk` scan when event logs are truncated; selective change-path crawl integration benchmark demonstrating subtree skipping when changes are sparse; imported `pathdiff` module unit tests.

**Exit criterion:** backup crawls achieve linear scaling with `cwalk` concurrency, selective `pathdiff` crawls skip unchanged subtrees when 100% event coverage is verified, and any coverage gap or topology mismatch falls back safely to a full `cwalk` crawl.

