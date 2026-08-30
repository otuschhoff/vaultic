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

**Goal:** extend the Phase 18 key envelope from independent 1-of-N slots to explicit unlock policies with cryptographically enforced $K$-of-$N$ threshold groups, so unlocking the metadata database (and, in key-in-DB mode, the data repository) can require multiple custodians. Support one or many offline key members (passphrase- or keyfile-protected shares for bootstrap and break-glass) and one or many Azure members, where each Azure member is a dedicated Azure Key Vault / Managed HSM key whose `unwrapKey` permission is RBAC-scoped to a single Entra user or an Entra group, and deliver complete operator documentation for setup, custody, migration, and disaster recovery.

**Design:**

A quorum unlock group protects the metadata root key with Shamir secret sharing over a suitable finite field: the root key is split into $N$ shares with reconstruction threshold $K$, and each share is wrapped independently under one member's key protection. Offline members wrap their share under an Argon2id-derived KEK from a per-custodian passphrase (salt and cost parameters stored per member) or under a keyfile on removable media; Azure members wrap their share with a dedicated versioned Key Vault / Managed HSM key through `wrapKey`/`unwrapKey`, with Azure RBAC scoped so exactly one Entra principal — a named user or an Entra group acting as a duty pool for that single seat — can unwrap that member's share and the daemon identity can unwrap none of them. Shamir reconstruction alone proves nothing, so the reconstructed root key is validated by authenticated decryption of the wrapped metadata DEK; wrong or corrupted shares fail closed. Each wrapped share binds repository UUID, envelope generation, group ID, share index, threshold, share count, and root-key version in authenticated associated data, so shares from different repositories, generations, or resharings can never be combined.

The unlock policy is an explicit `any_of` list of alternatives evaluated instead of the Phase 18 "first successful slot wins" order: each alternative is either a legacy single slot (retained for compatibility and bootstrap) or a threshold group (e.g. normal operation `2-of-4` Azure custodians, break-glass `2-of-3` offline custodians). Policies live in the same immutable, mirrored envelope generations as Phase 18 slots; every membership, threshold, or policy change requires an already unlocked daemon, produces a fresh resharing (never reuse of old shares), publishes a new envelope generation, and never overwrites the previous one. Status output always reports the *effective* threshold: if a 1-of-1 bootstrap or testing slot still exists beside a quorum group, status must state that the effective protection is one key and flag the configuration as not quorum-compliant, so a leftover bootstrap path can never silently masquerade as four-eyes protection.

Because a locked daemon cannot call custodian-scoped Azure keys itself, unlock is an interactive contribution session: the locked daemon opens a one-time session with a fresh ephemeral public key, session nonce, and expiry; each custodian runs `vaultic index unlock contribute`, authenticates directly with Entra (never lending credentials to the daemon), unwraps their own share through their own Azure key or decrypts their offline share locally, immediately encrypts the share to the daemon's ephemeral session key, submits it, and zeroizes local plaintext. The daemon rejects duplicate share indexes, contributions bound to another session/repository/generation, expired or replayed contributions, and — as defense in depth where the contributor identity is authenticated — two shares contributed by the same principal. On reaching $K$ valid distinct shares it reconstructs the root key in locked memory, unwraps the DEK, zeroizes shares and root key, and permanently closes the session. Unlocking through a group marked as recovery/break-glass (e.g. the offline group while an Azure group is configured) reuses the Phase 18 explicit-acknowledgement rule and emits a high-severity `auth` event.

Quorum changes are pure key-custody operations layered on the Phase 18 hierarchy: adding, removing, or replacing members, changing thresholds, migrating 1-of-1 bootstrap → offline $2$-of-$3$ → Azure $2$-of-$4$, and rotating a member's Azure wrapping key all rewrap or reshare the root key online, without re-encrypting any SlateDB object and without daemon downtime; only metadata DEK rotation (Phase 18) rewrites objects.

**Implementation steps:**

1. Implement versioned Shamir secret sharing in the `vaulticdb` crate (constant-time field arithmetic, reviewed implementation, zeroizing buffers): split/reconstruct of the metadata root key, per-share authenticated wrapping with the repository/generation/group/index/threshold binding above, and validation of every reconstruction by authenticated decryption of the wrapped DEK before use.
2. Extend the envelope format with a new generation-compatible `unlock_policy`: `any_of` alternatives containing legacy single slots and threshold groups; each group carries scheme, threshold, share count, and members with provider (`offline-argon2id`, `offline-keyfile`, `azure-key-vault`), versioned key reference or Argon2id parameters, share index, and optional expected Entra principal/group object ID. Envelopes without a policy keep exact Phase 18 1-of-N semantics.
3. Implement policy evaluation at startup replacing priority-order slot trial: non-interactive alternatives (legacy environment-unlockable slots) are attempted first; if none succeeds and a threshold group exists, the daemon enters a locked state and exposes an unlock-session API instead of exiting, with fail-closed behavior for required encryption preserved.
4. Implement one-time unlock sessions in the daemon gRPC surface: session creation with ephemeral X25519 key, nonce, and expiry; contribution submission with full binding checks, duplicate-share and duplicate-principal rejection, replay and expiry rejection; progress reporting (`k received / K required`) that never reveals share material; permanent session closure after success, failure, or expiry.
5. Implement `vaultic index unlock` CLI: `status` (locked/unlocked, session, progress, which members have contributed), `contribute --member <id>` performing interactive Entra authentication for Azure members (device code or Azure CLI credential) or passphrase/keyfile entry for offline members, encrypting the contribution to the session key, and submitting it; `--recovery-unlock` acknowledgement required when contributing to a break-glass group while a normal group exists.
6. Implement quorum management CLI under `vaultic index keys quorum`: `create-group`, `add-member`, `remove-member`, `set-threshold`, `replace-member`, and `verify` (a challenge-based test reconstruction proving a group can unlock without exposing or replacing the live DEK); every mutation requires an unlocked daemon, performs a fresh resharing, publishes and mirrors a new immutable envelope generation, and refuses to remove the last alternative capable of unlocking.
7. Implement effective-threshold reporting in `index keys status` and `index check`: enumerate all alternatives, compute the minimum number of independent custodians required by any alternative, flag 1-of-1 bypass paths beside quorum groups as non-compliant, and detect member definitions whose Azure key references or principal bindings overlap such that one principal could satisfy two seats.
8. Implement the documented migration path: bootstrap with a single offline slot, add and `verify` an offline threshold group, add and `verify` an Azure threshold group (each Azure member a dedicated versioned Key Vault key RBAC-scoped to its user or Entra group), then remove the bootstrap slot in a final generation — with `verify` required before any removal and status flagging incomplete migrations.
9. Emit observability: `auth` events for session open/close/expiry, each accepted or rejected contribution (member ID and principal identifier only, never share material), quorum reached, break-glass acknowledgement, resharing/membership changes, and effective-threshold downgrades; `lifecycle` events for envelope generation publication and mirroring.
10. Write operator documentation in `doc/070_encryption.rst`: quorum concepts and threat model (what $K$-of-$N$ does and does not protect against), Azure setup (per-member Key Vault keys, minimal RBAC role scoped per key, Entra user versus group seats, the one-seat-per-group rule, non-overlap requirements, Conditional Access/MFA and audit-log export recommendations), offline custodian runbooks (share creation, storage, periodic verification), the bootstrap→offline→Azure migration walkthrough, the break-glass procedure with its audit consequences, and custodian replacement/resharing.

**Tests:** Shamir split/reconstruct round-trips across thresholds ($1$-of-$1$ through $3$-of-$5$) with every $K$-subset succeeding and every $(K{-}1)$-subset failing; wrong-share, corrupted-share, and mixed-generation reconstruction failing closed via DEK authentication; share binding tests rejecting cross-repository, cross-group, cross-generation, and re-indexed shares; unlock-session tests covering duplicate share submission, duplicate principal submission, replayed and expired contributions, session closure after success, and concurrent session rejection; offline-member Argon2id and keyfile unwrap tests; Azure-member tests with mocked `wrapKey`/`unwrapKey` verifying per-member key isolation and that the daemon identity holds no share access; policy evaluation tests for legacy-slot compatibility, locked-state entry, and fail-closed required encryption; quorum management tests for resharing on every membership/threshold change, refusal to strand the envelope without an unlockable alternative, and `verify` challenge reconstruction without live-DEK replacement; effective-threshold tests flagging bootstrap bypass paths and overlapping principal bindings; full migration test (bootstrap → offline $2$-of-$3$ → Azure $2$-of-$4$ → bootstrap removal) across daemon restarts with no object re-encryption; break-glass acknowledgement and high-severity `auth` event tests; syslog routing tests for all new event types; documentation build and example-command validation.

**Exit criterion:** a repository can require $K$-of-$N$ custodians to unlock, with any mix of offline members and Azure members whose seats are individually RBAC-scoped to Entra users or groups; no single custodian, Azure key, daemon credential, or leftover bootstrap slot can unlock a quorum-compliant repository, and status/`index check` prove it by reporting the effective threshold; unlock happens through auditable one-time contribution sessions that never expose share material to logs, the daemon's standing credentials, or other custodians; membership, threshold, and policy changes reshare online in immutable envelope generations without re-encrypting any SlateDB object or interrupting a running daemon; a documented break-glass path with explicit acknowledgement and high-severity audit events restores availability when the normal quorum is unavailable; and operators can execute setup, migration, verification, custodian replacement, and disaster recovery end-to-end from the published documentation alone.

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

