# NIST Cybersecurity Framework 2.0 Assessment: Vaultic & VaulticDB

[← Back to compliance index](00-overview.md)

## 1. Executive Summary

This document assesses **vaultic** and its native metadata daemon
**`vaulticdb`** against the **NIST Cybersecurity Framework (CSF) 2.0**. The
assessment uses the six CSF Functions: **Govern, Identify, Protect, Detect,
Respond, and Recover**.

NIST CSF 2.0 is an outcome-based risk-management framework, not a product
certification. Vaultic is one technical component in an organization's
cybersecurity program. It directly contributes to data protection, integrity
monitoring, backup resilience, and recovery; it supplies evidence for
risk-management and incident-response processes; and it leaves enterprise
governance, workforce, legal, and communications outcomes to the operator.

The ratings below measure how well vaultic supplies capabilities and evidence
for each Function. They do not claim that deploying vaultic, by itself, makes an
organization conformant with the CSF.

### Function Assessment Matrix

| Function | Vaultic contribution | Score | Assessment |
|---|---|---:|---|
| **Govern (GV)** | Documented architecture, constraints, phased controls, provider and placement policy | **3.8 / 5.0** | Partial; enterprise policy, roles, risk appetite, and oversight remain operator-owned |
| **Identify (ID)** | Repository/backend inventory, pack placement catalog, ownership analytics, known-constraints register | **4.3 / 5.0** | Substantial |
| **Protect (PR)** | Client-side encryption, vaulted passphrases, mTLS/Unix-socket isolation, WORM-aware retention | **4.7 / 5.0** | Strong |
| **Detect (DE)** | Differential checks, sampled verification, append-only lifecycle events, SIEM export | **4.5 / 5.0** | Strong; detection requires operator scheduling and alert rules |
| **Respond (RS)** | Forensic event trail, categorized SIEM events, repair/rollback primitives | **3.9 / 5.0** | Partial; coordination and communications remain operator-owned |
| **Recover (RC)** | Open recovery format, independent restore tooling, hot/cold redundancy, legacy-index fallback | **5.0 / 5.0** | Strongest function |

**Overall NIST CSF 2.0 Enablement Rating:** **4.37 / 5.0 (Strong technical
enablement; Govern and Respond require organizational controls outside the
product).**

---

## 2. Scope and Responsibility Model

| Responsibility | Vaultic / VaulticDB | Operator / Organization |
|---|---|---|
| Repository encryption and integrity | Implements cryptographic protection and verification | Protects credentials, chooses retention, rotates keys, reviews findings |
| Asset and placement inventory | Maintains pack/backend catalogs and query surfaces | Integrates them into the enterprise asset inventory and CMDB |
| Detection evidence | Emits lifecycle/integrity events and check results | Schedules checks, establishes baselines, tunes SIEM alerts, triages events |
| Incident response | Preserves evidence and supplies repair/recovery commands | Owns incident command, legal decisions, notifications, and communications |
| Recovery | Preserves interoperable data and multiple restore paths | Defines RTO/RPO, runs exercises, supplies infrastructure and staffing |
| Third-party risk | Exposes backend identity, placement, and build assumptions | Assesses cloud/NFS suppliers and contract/service dependencies |

---

## 3. Govern (GV) — Rating: 3.8 / 5.0

### Relevant CSF 2.0 Outcomes

Vaultic contributes primarily to organizational context, risk-management
strategy, cybersecurity supply-chain risk management, and policy evidence.

### Implemented Capabilities

- **Documented constraints and risk assumptions:**
  [Vision, Scope, and Non-Negotiable Guarantees](../01-strategy/01-vision-and-principles.md),
  [Known Constraints](../04-roadmap/09-known-constraints.md), and
  [Rollout and Rollback](../04-roadmap/08-rollout-and-rollback.md) provide
  concrete inputs to a risk register.
- **Explicit trust boundaries:** The
  [VaulticDB Service Architecture](../02-architecture/01-vaulticdb-service.md)
  documents process, RPC, repository, and object-store boundaries.
- **Controlled change:** Roadmap phases have tests and exit criteria; risky
  rustic-parity behavior follows Alpha → Beta → Stable feature progression.
- **Supplier visibility:** Backend definitions and placement records identify
  where backup assets reside and which provider/storage class holds each pack.

### Gaps and Operator Responsibilities

- Vaultic does not define the organization's mission, risk appetite, roles,
  legal obligations, or cybersecurity policy.
- No published upstream SBOM, coordinated vulnerability disclosure policy, or
  formal supplier-assurance package currently exists. These are also identified
  in the [NIS2 assessment](03-nis2.md#24-d-supply-chain-security--rating-36--50-gap).
- Management review, segregation of duties, policy approval, and assurance
  reporting must be implemented in the operator's governance system.

---

## 4. Identify (ID) — Rating: 4.3 / 5.0

### Implemented Capabilities

- **Asset management:** The pack catalog (`p:`), backend registry, placement
  records (`pl:`), and backend-pack index (`bp:`) provide a queryable inventory
  of logical and physical backup assets.
- **Data understanding:** UID/GID, calendar time, SVM, volume, path group, size,
  and residency analytics support ownership and concentration analysis without
  scanning cold pack payloads.
- **Dependency mapping:** Hot/cold placement and promotion lineage show where
  data depends on NFS, S3 warm storage, Glacier, and replacement packs.
- **Risk discovery:** `vaultic index check`, `vaultic check --check-hot-cold`,
  crawl-debt records, and unresolved/unknown states expose incomplete knowledge
  rather than silently treating it as verified.
- **Improvement inputs:** Pack history and analytics provide objective evidence
  for capacity, churn, retention, and recovery planning.

### Gaps and Operator Responsibilities

- Vaultic inventories repository assets, not every host, identity, application,
  network, or business service in the organization.
- Business impact analysis and criticality classification must be joined from an
  external CMDB or service catalog.
- The operator must map repositories and source paths to business owners and
  recovery priorities.

---

## 5. Protect (PR) — Rating: 4.7 / 5.0

### Implemented Capabilities

- **Data security:** File content, paths, metadata, indexes, snapshots, and pack
  data are encrypted client-side using the Restic-compatible cryptographic
  format before reaching NFS or cloud providers.
- **Key protection:** Azure Key Vault Option A retrieves a repository
  passphrase once at process startup via Azure Arc Managed Identity or
  `DefaultAzureCredential`; no persistent plaintext credential is required on
  the backup host.
- **Platform and communications security:** `vaulticdb` defaults to a private
  Unix domain socket. Optional TCP requires an allowlist and authenticated
  channel; syslog supports TLS.
- **Data resilience:** S3 Object Lock/WORM constraints and retention-aware
  deletion queues prevent premature physical deletion. Placement policy can
  require multiple failure domains and offsite copies.
- **Protective technology:** Transactional metadata publication, immutable
  revision records, reference counts, and fencing reduce corruption and
  split-brain risk.
- **Awareness support:** Destructive commands expose dry-run/confirmation
  behavior, though organizational training remains external.

### Gaps and Operator Responsibilities

- Interactive repository access does not provide built-in MFA; enforce MFA and
  privileged-session control at the host, bastion, or identity-provider layer.
- Endpoint hardening, EDR, OS patching, network segmentation, and administrator
  role design are outside vaultic.
- The operator must configure provider-side Object Lock, IAM, lifecycle policy,
  and independent credential boundaries correctly.

---

## 6. Detect (DE) — Rating: 4.5 / 5.0

### Implemented Capabilities

- **Adverse-event analysis:** Differential index checks, hot/cold comparison,
  cryptographic pack validation, and sampled Level 1/2/3 storage verification
  detect absence, tampering, truncation, and payload corruption.
- **Continuous monitoring evidence:** The append-only `ph:` event stream records
  pack lifecycle transitions; categorized events (`auth`, `integrity`, `gdpr`,
  `restore`, `lifecycle`) can be routed to multiple SIEM targets.
- **Unknown-state discipline:** Missing physical-size, placement, freshness, or
  crawl evidence remains explicitly unknown and can generate findings instead
  of being normalized to a safe value.
- **Cold-tier inspection:** Warm-up integration permits checksum or full-payload
  verification of Glacier objects selected by risk attributes or statistical
  sampling.

### Gaps and Operator Responsibilities

- Checks are tools, not an autonomous SOC. The operator must schedule them,
  establish expected baselines, define alert thresholds, and investigate
  findings.
- Vaultic has no endpoint/network behavior analytics or threat-intelligence
  ingestion; integrate adjacent security controls for those outcomes.
- Detection latency depends on check frequency and cold-object retrieval time.

---

## 7. Respond (RS) — Rating: 3.9 / 5.0

### Implemented Capabilities

- **Incident evidence:** Lifecycle history, structured check results, immutable
  revision keys, and storage-placement records support scope and timeline
  reconstruction.
- **Incident management inputs:** SIEM categories distinguish authentication,
  integrity, privacy, restore, and lifecycle events for routing to the correct
  playbook.
- **Mitigation and repair:** Repair-index/packs/snapshots workflows, placement
  recovery, promotion/repack, and rollback guidance provide technical
  containment and remediation mechanisms.
- **Analysis:** GDPR survival analysis and reverse references can explain why
  content remains retained and which external references prevent deletion.

### Gaps and Operator Responsibilities

- Vaultic does not declare incidents, manage an incident commander, contact
  regulators/customers, or operate external communications.
- Evidence retention, legal hold, chain of custody, and forensic export policy
  require organizational procedures.
- Operators must test command authorization and change-control procedures before
  using destructive repair or erasure actions during an incident.

---

## 8. Recover (RC) — Rating: 5.0 / 5.0

### Implemented Capabilities

- **Recovery execution:** `restore`, mount/browse, sparse/in-place/resumable
  restore, and cold-storage warm-up support multiple recovery modes.
- **Independent recovery path:** Standard Restic/Rustic repository format avoids
  dependence on an active SaaS license or proprietary appliance. A compatible
  Restic/Rustic binary remains an emergency recovery tool.
- **Metadata disaster recovery:** Deterministic legacy JSON projection and
  rebuildable SlateDB-derived indexes permit recovery after loss of local
  vaulticdb/NVMe state.
- **Multi-tier resilience:** Placement policy, offsite deadlines, and
  multi-provider plans reduce common-mode failure.
- **Recovery validation:** Full-payload checks and representative restore tests
  can validate recoverability rather than merely backend object presence.

### Gaps and Operator Responsibilities

- RTO/RPO are deployment properties, not product constants. Operators must size
  bandwidth, warm-up lead time, compute, cache, and personnel against business
  requirements.
- Recovery communication and post-incident lessons-learned processes remain
  organizational controls.

---

## 9. Operational CSF 2.0 Adoption Guidance

1. **Create an Organizational Profile:** Select applicable CSF outcomes and map
   each repository to business service, owner, data classification, RTO, RPO,
   and required placement policy.
2. **Establish a Current Profile:** Record which Protect, Detect, and Recover
   capabilities are enabled versus merely available (Object Lock, offsite
   copies, scheduled checks, SIEM routing, sampled verification).
3. **Define a Target Profile:** Treat MFA enforcement, SBOM/CVD, detection
   cadence, restore exercises, and multi-provider metadata replication as
   explicit improvement outcomes.
4. **Measure Tier Progress:** Use command output and external evidence rather
   than this document's numeric ratings as audit evidence: check reports,
   restore exercise results, SIEM delivery receipts, placement status, and
   retention configuration.
5. **Exercise Respond and Recover:** At least annually, simulate loss of the
   vaulticdb host, loss of the hot tier, compromised credentials, and Glacier
   recovery. Confirm that emergency Restic/Rustic recovery works without the
   primary control plane.
