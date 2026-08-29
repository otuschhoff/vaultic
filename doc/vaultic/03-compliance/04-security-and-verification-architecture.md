# Security & Verification Architecture

[← Back to compliance index](00-overview.md)

This document covers the design sections of the
[Analytics Engine](../02-architecture/07-analytics-engine.md) that exist specifically
to satisfy the GDPR, ISO 27001, and NIS2 assessments in this section.

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

### 14.6 Multi-Target Syslog Exporter & Filtered Event Routing

#### Architecture & Rationale

To comply with **ISO 27001 (A.8.15 & A.8.16)** and **GDPR (Art. 30 & Art. 32)**, `vaultic` and `vaulticdb` provide a multi-target syslog exporter supporting:
- **Transports:** UDP, TCP, TLS (syslog-over-TLS per RFC 5424 / RFC 3164), and local Unix domain socket (`/dev/log`, `/var/run/syslog`).
- **Granular Event Routing Rules:** Filter and route structured JSON/syslog events based on severity, facility, component, and category:
  - `auth`: Authentication attempts, mTLS drops, CIDR rejections, key vault fetches.
  - `integrity`: Index check failures, hash mismatches, corrupt packs, SlateDB fencing/epoch mismatches.
  - `gdpr`: `gdpr audit` queries, `gdpr execute-forget` executions, UID blocklist matches.
  - `restore`: Dataset restore start/complete, warm-up executions.
  - `lifecycle`: Pack creation, promotion, deletion-queue enqueuing, physical deletion.

```text
[ vaultic / vaulticdb ]
  │
  ├── Filter: category == "auth" | "integrity" ──► Target 1: TLS Syslog (Microsoft Sentinel / SIEM)
  ├── Filter: category == "gdpr"              ──► Target 2: TLS Syslog (Compliance Audit Log)
  └── Filter: category == "lifecycle"         ──► Target 3: Local Unix Socket / Syslog (Ops Monitoring)
```

### 14.7 Advanced GDPR "Right to be Forgotten" & Per-Chunk Deduplication Analysis

#### Per-Chunk Survival Analysis

When responding to a GDPR Article 17 erasure request, Data Protection Officers (DPOs) need to know not only which files belong to a user, but also **which underlying data chunks will survive** a deletion request due to content-defined deduplication (CDC) references from other files.

`vaultic index gdpr audit --uid <uid> --explain-surviving-chunks` analyzes all chunks referenced by `<uid>`'s files and categorizes them into two sets:
1. **Exclusive Chunks:** Chunks referenced *only* by `<uid>`'s files. These chunks will be enqueued for physical deletion immediately upon executing `vaultic index gdpr execute-forget`.
2. **Surviving Chunks:** Chunks referenced by `<uid>`'s files *and* by other files outside the GDPR audit scope. For each surviving chunk, the report details:
   - **External File Sample:** Paths, snapshot IDs, and owner UIDs of non-scoped files referencing the chunk (e.g. shared OS binaries, zero-blocks, common templates, or multi-user documents).
   - **External Reference Count:** Total reference count outside `<uid>`'s scope.
   - **Assessment Hint:** Classifies whether the chunk is likely generic/shared data (e.g., high ref-count, common OS file) or potential residual user PII.

For a UID audit, the scoped set is every retained inode generation whose owner
UID at the referenced immutable revision equals the requested UID, including all
hardlink paths of that inode. A non-scoped reference is any retained manifest
edge from an inode generation outside that set, regardless of first-seen order
or owner. Samples are repository-local and report snapshot, immutable inode
revision, all known hardlink paths, and UID; cross-repository deduplication is
out of scope unless repositories share an explicit global reference catalog.

#### Erasure Execution & Future Backup Exclusion Policy

1. **Erasure Execution (`vaultic index gdpr execute-forget --uid <uid>`):**
   - Atomically removes `<uid>`'s inode revisions (`iv:`) and directory edges (`dv:`).
   - Decrements reference counts on associated blobs (`rc:`).
   - Enqueues zero-reference packs into the retention deletion queue (`dq:`).
   - Generates a cryptographically signed deletion certificate containing purged reference hashes, timestamp, and retention expiry schedule.
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
2. **Sampling Controls:**
   - `--all`: Verify 100% of matched candidate packs.
   - `--sample-count N`: Uniformly sample exactly $N$ candidate packs.
   - `--sample-percent P%`: Uniformly sample $P\%$ of matched candidate packs using pseudo-random seed selection.
3. **Verification Levels & Cold Warm-up:**
   - **Level 1 (`header`):** Verify backend object existence, size, and pack header decryption.
   - **Level 2 (`checksum`):** Verify backend byte checksum / ETag / SHA-256 against pack catalog records.
   - **Level 3 (`full` / `unpack`):** Full payload decryption and chunk inflation validation.
   - **Cold Warm-up Integration:** For cold/Glacier packs requiring Level 2 or 3 checks, automatically triggers `--warm-up-command` with batching, wait timeouts, and worker concurrency controls (`--concurrency N`).

