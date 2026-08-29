# CIS Critical Security Controls v8.1 Assessment: Vaultic & VaulticDB

[← Back to compliance index](00-overview.md)

## 1. Executive Summary

This document assesses **vaultic** and **`vaulticdb`** against the **CIS
Critical Security Controls v8.1**. CIS Controls are prioritized organizational
safeguards, not a product-certification scheme. A backup platform directly
supports only a subset of the 18 Controls; many safeguards concern enterprise
endpoint, identity, network, awareness, application-development, and service-
provider processes that remain outside vaultic.

The assessment therefore distinguishes:

- **Direct:** vaultic implements a relevant safeguard or supplies primary
  evidence.
- **Shared:** vaultic supplies a technical capability, but configuration,
  monitoring, or process ownership belongs to the operator.
- **External:** the Control is chiefly outside a backup product's scope.

Implementation Groups (IG1/IG2/IG3) are an organizational prioritization model.
Vaultic can support safeguards used by any group, but it does not assign an
organization to an IG.

### Control Coverage Matrix

| # | CIS Control | Vaultic contribution | Responsibility | Score |
|---:|---|---|---|---:|
| 1 | Inventory and Control of Enterprise Assets | Repository backend/pack inventory only | Shared | **3.2 / 5.0** |
| 2 | Inventory and Control of Software Assets | Build metadata and pinned dependencies; no SBOM | Shared / Gap | **2.8 / 5.0** |
| 3 | Data Protection | Encryption, retention, placement, erasure analysis | Direct | **4.8 / 5.0** |
| 4 | Secure Configuration of Enterprise Assets and Software | Secure daemon defaults and validated transport config | Shared | **4.1 / 5.0** |
| 5 | Account Management | No native enterprise account lifecycle | External | **1.5 / 5.0** |
| 6 | Access Control Management | Repository keys, socket permissions, mTLS/allowlist | Shared | **3.7 / 5.0** |
| 7 | Continuous Vulnerability Management | No built-in vulnerability scanner/SBOM pipeline | External / Gap | **2.0 / 5.0** |
| 8 | Audit Log Management | Categorized syslog and append-only lifecycle events | Direct / Shared | **4.5 / 5.0** |
| 9 | Email and Web Browser Protections | Not applicable | External | **N/A** |
| 10 | Malware Defenses | Integrity detection but no anti-malware engine | External | **2.2 / 5.0** |
| 11 | Data Recovery | Open format, multi-tier copies, tested restore paths | Direct | **5.0 / 5.0** |
| 12 | Network Infrastructure Management | Secured daemon transport only | Shared | **3.0 / 5.0** |
| 13 | Network Monitoring and Defense | Emits security telemetry; no NDR | External / Shared | **2.6 / 5.0** |
| 14 | Security Awareness and Skills Training | Operator documentation only | External | **2.0 / 5.0** |
| 15 | Service Provider Management | Backend/provider inventory and placement evidence | Shared | **3.5 / 5.0** |
| 16 | Application Software Security | Test/feature-gate discipline; no formal SSDLC/CVD | Shared / Gap | **3.4 / 5.0** |
| 17 | Incident Response Management | Forensic evidence and repair/recovery tools | Shared | **3.8 / 5.0** |
| 18 | Penetration Testing | No built-in or documented penetration-testing program | External / Gap | **1.5 / 5.0** |

**Overall assessment:** Vaultic is a **strong enabling control for CIS Controls
3, 8, and 11**, a supporting component for Controls 1, 2, 4, 6, 12, 15, 16,
and 17, and not a substitute for the organization's identity, endpoint,
network, awareness, vulnerability-management, or penetration-testing program.
An aggregate score would be misleading because several Controls are not
applicable to a backup product.

---

## 2. Controls 1–2: Asset and Software Inventory

### Control 1 — Inventory and Control of Enterprise Assets

#### Vaultic Contribution

- The backend registry identifies configured storage endpoints and their
  capabilities, cost, failure domain, retention, and storage class.
- Pack (`p:`), placement (`pl:`), and backend-pack (`bp:`) records inventory
  every backup object and where it physically resides.
- `vaultic index backends`, placement queries, and pack introspection provide
  machine-readable evidence suitable for reconciliation with a CMDB.

#### Boundary

Vaultic does not discover laptops, servers, network equipment, cloud accounts,
or unmanaged devices. It inventories repository assets only. The operator must
join repositories and source filesystem IDs to the enterprise asset inventory.

### Control 2 — Inventory and Control of Software Assets

#### Vaultic Contribution

- Build metadata records the vaultic/vaulticdb version, pinned SlateDB version
  or commit, target triple, and binding checksum.
- `go.mod` and the daemon's `Cargo.lock` provide dependency manifests.

#### Gap

No generated SBOM or centralized approved-software enforcement exists. The
operator should generate CycloneDX/SPDX artifacts and run SCA on every release
until an upstream SBOM pipeline is added. This aligns with the supply-chain gap
in the [NIS2 assessment](03-nis2.md#24-d-supply-chain-security--rating-36--50-gap).

---

## 3. Control 3: Data Protection — Rating: 4.8 / 5.0

### Direct Capabilities

- **Data at rest:** Client-side encryption covers file data, names, paths,
  metadata, indexes, and snapshots before provider upload.
- **Data in transit:** HTTPS/backend TLS and authenticated vaulticdb TCP/mTLS
  protect communications; local daemon IPC uses restricted Unix sockets.
- **Data retention:** Snapshot policy, delete protection, WORM/Object Lock,
  minimum-retention-aware queues, and placement policy enforce intended
  availability and deletion timing.
- **Data classification support:** UID/GID, SVM, volume, path group, file-size,
  time, and residency analytics can support repository-level data ownership and
  retention decisions.
- **Data disposal:** GDPR erasure analysis identifies exclusive versus surviving
  deduplicated chunks, while zero-reference packs enter a retention-aware
  deletion queue.
- **Data loss prevention by resilience:** Multiple failure domains, offsite
  deadlines, and hot/cold placement protect against destructive failure.

### Operator Responsibilities

- Define classifications, retention schedules, legal holds, and approved
  destinations.
- Enforce provider IAM, bucket policy, lifecycle policy, Object Lock, and key
  access independently of the backup host.
- Validate that deletion policy satisfies both privacy and legal-retention
  obligations.

---

## 4. Controls 4–7: Secure Configuration, Accounts, Access, Vulnerabilities

### Control 4 — Secure Configuration

- Unix socket directory mode `0700` and socket mode `0600` are secure defaults.
- TCP is disabled by default; enabling it without an allowlist and authenticated
  channel is a startup error.
- Schema and manifest versions are validated before SlateDB can become
  authoritative; malformed or ambiguous state fails closed.
- Repository configuration validation rejects inconsistent placement,
  analytics, and transport settings.

The operator remains responsible for OS hardening, filesystem permissions,
service sandboxing, patching, and configuration baselines.

### Control 5 — Account Management

Vaultic has no enterprise user directory or account-lifecycle subsystem. It
cannot create, disable, review, or expire workforce accounts. Repository
passphrases, key files, managed identities, backend credentials, and daemon
tokens must be tied to the organization's account-management process.

### Control 6 — Access Control Management

- Repository encryption keys gate access to backup content.
- Azure Arc Managed Identity can remove long-lived passphrases from host disks.
- Optional TCP uses mTLS/allowlists; local access uses Unix permissions.

Gaps: no built-in RBAC separating backup, restore, audit, and destructive
administration; no interactive MFA. Compensate with host identity, bastion MFA,
separate service accounts, cloud IAM, and change approval.

### Control 7 — Continuous Vulnerability Management

Vaultic does not scan hosts, containers, dependencies, or storage providers for
vulnerabilities. Operators must:

1. Run dependency and image scanning against `go.mod`, `Cargo.lock`, binaries,
   and deployment artifacts.
2. Monitor Go, Rust, SlateDB, crypto-library, backend-SDK, and OS advisories.
3. Patch/rebuild on risk-based timelines and test rollback/recovery.
4. Register vaultic in the enterprise vulnerability exception and remediation
   workflow.

---

## 5. Control 8: Audit Log Management — Rating: 4.5 / 5.0

### Direct Capabilities

- The append-only pack history (`ph:`) records creation, publication, placement,
  repack, promotion, delete-pending, and deletion transitions.
- Multi-target syslog routes structured events by category (`auth`, `integrity`,
  `gdpr`, `restore`, `lifecycle`) and supports syslog-over-TLS.
- Stable JSON output enables collection of check, analytics, placement, and
  lifecycle evidence.
- Logs and metrics deliberately omit plaintext paths, keys, and sensitive
  payloads where the event does not require them.

### Operator Responsibilities

- Synchronize clocks, collect host/cloud/vaultic logs centrally, set retention,
  restrict log access, monitor delivery failures, and alert on high-value events.
- Preserve SIEM evidence independently of the repository so compromise of one
  system cannot erase both backup and audit history.
- Periodically test event delivery and detection rules.

---

## 6. Controls 9–10: Browser/Email and Malware Defenses

### Control 9 — Email and Web Browser Protections

Not applicable to vaultic. These safeguards belong to mail, browser, DNS,
proxy, and endpoint controls.

### Control 10 — Malware Defenses

Vaultic can detect unexpected pack/index corruption and preserve pre-incident
snapshots, but it is not an anti-malware or content-scanning engine. It may back
up malicious files exactly as designed. Operators must deploy endpoint malware
defenses and use snapshot anomaly/restore exercises as complementary controls,
not as malware prevention.

---

## 7. Control 11: Data Recovery — Rating: 5.0 / 5.0

### Direct Capabilities

- Standard Restic/Rustic format permits recovery without proprietary licensing
  or a live vaulticdb service.
- Legacy JSON projection/rebuild permits loss of local SlateDB/NVMe state without
  loss of restore capability.
- Hot/cold and multi-backend placement protect against single-tier failure.
- Resumable, sparse, selective, and in-place restore modes support practical
  recovery at different scales.
- Cold warm-up integration automates retrieval prerequisites for Glacier packs.
- Differential checks and full-payload verification provide restore confidence.

### Required Operational Safeguards

- Define recovery priorities, RTO/RPO, alternate infrastructure, and credential
  escrow.
- Run scheduled restore tests, including full loss of the primary vaulticdb
  host and independent recovery with a compatible Restic/Rustic binary.
- Keep recovery keys and procedures available outside the protected production
  environment.

---

## 8. Controls 12–13: Network Management and Monitoring

### Control 12 — Network Infrastructure Management

Vaultic minimizes its own exposed network surface: local IPC is the default,
TCP is opt-in, and remote endpoints require explicit allowlists and
authentication. It does not configure routers, firewalls, segmentation, DNS,
VPN, or cloud network controls. Operators should isolate backup-management and
storage traffic, deny inbound access by default, and separately protect cloud
control-plane access.

### Control 13 — Network Monitoring and Defense

Vaultic emits authentication rejection, integrity, and lifecycle telemetry but
provides no packet capture, IDS/IPS, NDR, flow analysis, or DNS monitoring.
Forward vaultic events to the SOC and correlate them with host, network, cloud,
and identity telemetry.

---

## 9. Controls 14–15: Awareness and Service Providers

### Control 14 — Security Awareness and Skills Training

Documentation explains destructive operations, dry-run behavior, immutable
retention, recovery, and known constraints. The operator must turn those topics
into role-specific training and exercises for backup administrators, incident
responders, security analysts, and recovery personnel.

### Control 15 — Service Provider Management

- Backend and placement records identify which providers store each pack and
  which failure domains satisfy durability policy.
- Cost, retrieval class, minimum retention, bandwidth, and request-rate metadata
  support provider risk and exit planning.
- Standard repository format reduces provider and software lock-in.

The operator must perform due diligence, contract/SLA review, jurisdiction and
data-residency analysis, annual reassessment, and provider offboarding.

---

## 10. Control 16: Application Software Security — Rating: 3.4 / 5.0

### Vaultic Contribution

- Tests scale with blast radius; schema parsing rejects malformed data and
  preserves unknown state.
- Risky behavior uses feature progression and explicit phase exit criteria.
- Process separation keeps Rust/SlateDB code out of the Go CLI address space.
- Protocol/schema versions and capabilities are validated before use.

### Gaps

- No published secure-development lifecycle, threat model, SAST/SCA policy,
  signed SBOM, provenance attestation, penetration-test report, or coordinated
  vulnerability disclosure process is currently part of the documented
  release contract.
- Operators should treat vaultic as third-party application software subject to
  their own acquisition, scanning, testing, and exception processes.

---

## 11. Controls 17–18: Incident Response and Penetration Testing

### Control 17 — Incident Response Management

Vaultic supplies evidence and technical actions: categorized SIEM events,
append-only lifecycle history, index/pack checks, repair commands, placement
recovery, deterministic rollback guidance, and independent restore paths.
Incident declaration, roles, communications, regulator notification, tabletop
exercises, and lessons learned remain organizational responsibilities.

### Control 18 — Penetration Testing

Vaultic provides no built-in penetration-testing capability and this repository
does not document a recurring independent test program. Operators should include
vaulticdb RPC authentication, daemon lifecycle, backend credentials, repository
key handling, restore hosts, CI/release artifacts, and cloud storage policy in
their own scoped penetration tests.

---

## 12. Recommended CIS Implementation Plan

1. **IG1 baseline:** Encrypt all repositories, separate backup credentials,
   enable immutable retention where supported, maintain independent recovery
   keys, schedule integrity checks, and test representative restores.
2. **IG2 expansion:** Centralize syslog, establish placement/offsite policy,
   integrate repository assets into the CMDB, scan dependencies/artifacts, and
   enforce privileged-access MFA outside vaultic.
3. **IG3 assurance:** Use multi-provider failure domains, sampled/full cold-tier
   verification, independent recovery exercises, signed SBOM/provenance,
   adversarial testing, and SOC correlation across vaultic, endpoint, network,
   identity, and cloud telemetry.
4. **Evidence retention:** Preserve config baselines, check reports, restore-test
   reports, SIEM delivery evidence, placement status, key-access logs, and
   vulnerability scan results for audit review.
