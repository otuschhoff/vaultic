# Security & Verification Architecture

[← Back to compliance index](00-overview.md)

This document covers the design sections of the
[Analytics Engine](../02-architecture/07-analytics-engine.md) that exist specifically
to support the GDPR, ISO 27001, NIS2, NIST CSF 2.0, CIS Controls v8.1, and
COBIT 2019 assessments in this section.

### 14.5 Azure Key Vault Option A (Secret Store Integration)

#### Architecture & Rationale

To comply with **ISO 27001 Control A.8.24** (Cryptography and Key Management), `vaultic` supports fetching repository key passphrases directly from **Azure Key Vault** (`https://<vault-name>.vault.azure.net/secrets/<secret-name>`) using Azure Arc Managed Identities or `DefaultAzureCredential`.

Option A (Secret Store / Passphrase Vaulting) is selected over Option B (Managed HSM Key Unwrap) because:
1. **Unmodified Cryptographic Format:** `vaultic` retains restic-compatible keyfile format (`/keys/<key_id>`), decrypted via Argon2id/Scrypt and AES-256-CTR + Poly1305.
2. **SIEM Audit Logging:** Every key fetch generates an immutable `SecretGet` audit event in Azure Key Vault diagnostic logs, forwarded directly to Microsoft Sentinel.
3. **WAN & Network Fault Tolerance:** The passphrase is fetched **once** at process startup (1 API request). Long-running 500 TB backups or multi-hour crawls are completely immune to mid-job Azure WAN disconnects or rate limits.

```text
vaultic process startup
  │
  ├─► Authenticate via Azure Arc Managed Identity (http://localhost:40342/...)
  ├─► GET https://<vault>.vault.azure.net/secrets/<secret-name> (SecretGet)
  ├─► Decrypt /keys/<key_id> in host memory
  └─► Proceed with local backup/index operations (no further WAN calls)
```

Configure the one-shot fetch with `--azure-key-vault-url`,
`--azure-key-vault-secret`, optional `--azure-key-vault-secret-version`, and
`--azure-key-vault-timeout`. The corresponding
`VAULTIC_AZURE_KEY_VAULT_*` environment variables are also supported. Key
Vault is mutually exclusive with password files, commands, and password
environment variables. The fetched value is never included in structured
events.

### 14.6 Multi-Target Syslog Exporter & Filtered Event Routing

#### Architecture & Rationale

To comply with **ISO 27001 (A.8.15 & A.8.16)** and **GDPR (Art. 30 & Art. 32)**, `vaultic` provides a multi-target syslog exporter supporting:
- **Transports:** UDP, TCP, TLS (syslog-over-TLS per RFC 5424 / RFC 3164), and local Unix domain socket (`/dev/log`, `/var/run/syslog`).
- **Granular Event Routing Rules:** Filter and route structured JSON/syslog events based on severity, facility, component, and category:
  - `auth`: Authentication attempts, mTLS drops, CIDR rejections, key vault fetches.
  - `integrity`: Index check failures, hash mismatches, corrupt packs, SlateDB fencing/epoch mismatches.
  - `gdpr`: `gdpr audit` queries, `gdpr execute-forget` executions, UID blocklist matches.
  - `restore`: Dataset restore start/complete, warm-up executions.
  - `lifecycle`: Pack creation, promotion, deletion-queue enqueuing, physical deletion.

```text
[ vaultic ]
  │
  ├── Filter: category == "auth" | "integrity" ──► Target 1: TLS Syslog (Microsoft Sentinel / SIEM)
  ├── Filter: category == "gdpr"              ──► Target 2: TLS Syslog (Compliance Audit Log)
  └── Filter: category == "lifecycle"         ──► Target 3: Local Unix Socket / Syslog (Ops Monitoring)
```

Each repeatable `--syslog-target` is a URL such as
`tls://siem.example:6514?format=rfc5424&categories=auth,gdpr&min-severity=notice&ca=/etc/siem-ca.pem`.
Supported schemes are `udp`, `tcp`, `tls`, `unix`, and `unixgram`; stream
transports use RFC 6587 octet-count framing. TLS defaults to certificate and
hostname verification and can use `cert` plus `key` query parameters for
mutual TLS.

### 14.7 Advanced GDPR "Right to be Forgotten" & Per-Chunk Deduplication Analysis

#### Per-Chunk Survival Analysis

When responding to a GDPR Article 17 erasure request, Data Protection Officers (DPOs) need to know not only which files belong to a user, but also **which underlying data chunks will survive** a deletion request due to content-defined deduplication (CDC) references from other files.

`vaultic index gdpr audit --uid <uid> --explain-surviving-chunks` analyzes all chunks referenced by `<uid>`'s files and categorizes them into two sets:
1. **Exclusive Chunks:** Chunks referenced *only* by `<uid>`'s files. These chunks will be enqueued for physical deletion immediately upon executing `vaultic index gdpr execute-forget`.
2. **Surviving Chunks:** Chunks referenced by `<uid>`'s files *and* by other files outside the GDPR audit scope. For each surviving chunk, the report details:
   - **External File Sample:** Paths, immutable inode revisions, and owner UIDs of non-scoped files referencing the chunk (e.g. shared OS binaries, zero-blocks, common templates, or multi-user documents).
   - **External Reference Count:** Total reference count outside `<uid>`'s scope.
   - **Assessment Hint:** Classifies whether the chunk is likely generic/shared data (e.g., high ref-count, common OS file) or potential residual user PII.

For a UID audit, the scoped set is every retained inode generation whose owner
UID at the referenced immutable revision equals the requested UID, including all
hardlink paths of that inode. A non-scoped reference is any retained manifest
edge from an inode generation outside that set, regardless of first-seen order
or owner. Samples are repository-local and report an immutable inode revision,
one known path, and UID; cross-repository deduplication is
out of scope unless repositories share an explicit global reference catalog.

#### Erasure Execution & Future Backup Exclusion Policy

1. **Erasure Execution (`vaultic index gdpr execute-forget --uid <uid>`):**
   - Atomically replaces `<uid>`'s inode revisions with structurally valid redacted placeholders, removes directory edges and path/hardlink bindings, and purges rebuildable analytics. Retained snapshot graphs remain well formed while UID, path, size, content IDs, and file hashes are removed.
   - Decrements reference counts on associated blobs (`rc:`).
   - Enqueues a pack's placements only when every blob in that physical pack is unreferenced, using each placement's retention deadline in `dq:`.
   - Generates a cryptographically signed deletion certificate containing purged reference hashes, timestamp, and retention expiry schedule. `--signing-key` requires an operator-held Ed25519 PKCS#8 PEM private key; `gdpr verify-certificate --public-key <pem>` verifies persisted certificates against the operator-held Ed25519 PKIX trust anchor rather than trusting the embedded key.
   - Requires `--confirm`. Supplying a stable `--run-id` makes an interrupted operator invocation replay-safe even when its wall-clock timestamp changes.
2. **Future Backup Exclusion (`--exclude-uid <uid>`):**
   - Writes persistent policy rule `u:policy:blocklist:<uid>`.
   - Archiver and backup reconciliation pipeline check file ownership (`lstat.uid`) against the blocklist; files owned by blocked UIDs are skipped automatically during future backup crawls.

### 14.8 Attribute-Based & Sampled Storage Verification

#### Architecture & Rationale

Verifying storage integrity across 500+ TB and cold Glacier archives requires targeted, sample-based verification to balance security confidence against API and egress costs.

`vaultic index verify-storage` / `vaultic verify-packs` provides query attribute filters and statistical sampling controls:

1. **Query Attribute Filters:**
   - `--tier hot|cold|mirrored|archival`: Filter candidate packs by storage tier.
   - `--backend <id>` / `--storage-class <class>`: Filter by specific backend or cloud storage class.
   - `--created-after <date>` / `--created-before <date>`: Filter by pack creation time range.
   - `--retention-status expired|active`: Filter by Glacier retention status.
   - `--pack-type data|tree`: Filter by pack content type.
   - `--min-size <bytes>` / `--max-size <bytes>`: Filter by known physical pack size.
2. **Sampling Controls:**
   - `--all`: Verify 100% of matched candidate packs.
   - `--sample-count N`: Uniformly sample exactly $N$ candidate packs.
   - `--sample-percent P%`: Uniformly sample $P\%$ of matched candidate packs using pseudo-random seed selection.
3. **Verification Levels & Cold Warm-up:**
   - **Level 1 (`header`):** Verify backend object existence, size, and pack header decryption.
   - **Level 2 (`checksum`):** Read the full encrypted object and verify its SHA-256 pack ID.
   - **Level 3 (`full` / `unpack`):** Full payload decryption and chunk inflation validation.
   - **Cold Warm-up Integration:** For cold/Glacier packs requiring Level 2 or 3 checks, automatically triggers `--warm-up-command` with batching, wait timeouts, and worker concurrency controls (`--concurrency N`).

#### Verification State and Sparse Error History

Verification freshness is tracked per physical placement, not only per logical
pack. A successful check of one backend does not prove that another backend's
copy is intact. Keep compact mutable state separate from the pack lifecycle
history:

```text
key:   vr:<32-byte pack ID>:<8-byte backend ID>
value: schema version
   last attempted time and level
   last successful header/checksum/full timestamps
   current result (healthy, operational-error, integrity-error, unknown)
   open finding ID, first/last error time, consecutive failure count
   last verification run ID
```

A stronger successful level also advances the weaker-level timestamps: a full
unpack proves checksum/header verification at the same instant, and a checksum
verification proves the header level. A weaker later check must not erase an
older stronger-level timestamp. The existing `pl:` `LastVerifiedAt` field is a
backward-compatible coarse projection of the latest success at any level;
`vr:` is authoritative for level-specific scheduling and reporting.

Successful checks are common and overwrite only `vr:`. Recording every success
as an immutable event would create high-volume history with little forensic
value. Verification failures and their resolution are rare, durable events in
a dedicated namespace:

```text
key:   ve:<8-byte event unix seconds>:<8-byte event seq>:
      <32-byte pack ID>:<8-byte backend ID>
value: schema version, event type (detected, changed, resolved), finding ID,
   verification run ID, verification level and stage,
   classification (missing, size-mismatch, checksum-mismatch,
   header-authentication, payload-decrypt, decompression,
   warm-up-timeout, transport, cancelled), expected/observed size or digest,
   first-detected time, occurrence count, resolution reason
```

The error log is separate from `ph:` because verification findings are not pack
lifecycle transitions, require different fields and retention, and must remain
available after a pack or placement is deleted. It is append-only and retained
indefinitely by this phase; any future audit-retention purge must be an explicit
operator policy. It is never used as the current health state.
`vr:` references the open finding. Repeated failures with the same
classification update `vr:` and its consecutive count without appending one
event per retry. Append a new `ve:` event only when a finding is first detected,
its classification changes, or a later successful verification at the failed
level (or stronger) resolves it. Operational failures such as a timeout are
kept distinct from integrity failures and must not label data corrupt.
Success below an open finding's level updates that weaker success timestamp but
does not clear the finding or make the placement healthy. `never-verified` and
`stale` are derived from the requested level's timestamp and configured
freshness policy rather than persisted as states that can drift with time.

Updating `vr:` and appending a `ve:` transition occurs in one metadata
transaction after the verification attempt finishes. Interruption before that
transaction leaves the previous state intact and the run resumable; replay uses
the run ID plus pack/backend identity as an idempotency key. Verification state
and events are advisory evidence: corruption or absence remains a finding for
operators and repair workflows, never direct authorization for deletion.

Candidate selection can use `--not-verified-since <time>` and
`--verification-level <header|checksum|full>` against `vr:` without backend
scans. Status output reports last success at each level, current/open finding,
first and last detection times, and consecutive failures. `--errors-only` reads
current unhealthy `vr:` records; `--current-status` reports state without
reading pack bytes; `--error-history` range-scans `ve:` and includes resolved
findings, bounded by `--history-limit`. `index check` reports malformed/orphan state and drift between `vr:`
and the coarse `pl:` projection.

