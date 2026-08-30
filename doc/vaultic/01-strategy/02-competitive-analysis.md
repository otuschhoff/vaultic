# Enterprise Backup Architecture Comparison: Vaultic vs. Rubrik

[← Back to index](../README.md)

## 1. Executive Summary

This document presents a comprehensive technical and financial comparison between **vaultic** (with its native Rust metadata daemon **`vaulticdb`**) and **Rubrik** (Rubrik Security Cloud / Cloud Data Management - CDM) for backing up, indexing, and archiving an enterprise dataset of **500 TB logical data** containing **1.5 billion inodes**.

The target topology leverages a **local Synology (NFS)** share as a high-speed staging and hot metadata location, paired with a hybrid cloud tier consisting of **S3 Warm (Standard/IA)** and **S3 Glacier Deep Archive** (very cold tier).

### High-Level Comparison Matrix

| Evaluation Dimension | Rubrik (CDM / Security Cloud) | Vaultic + VaulticDB |
|---|---|---|
| **Architecture Model** | Proprietary hyperconverged hardware/SaaS appliance cluster | Open, modular CLI + daemon architecture on commoditized hardware |
| **Data Format & Lock-in** | **High Vendor Lock-in:** Proprietary backup format; requires Rubrik software/SaaS subscription to restore | **Zero Lock-in:** Standard Restic/Rustic format; restore using open-source tools or standalone binaries |
| **5-Year Total Cost of Ownership (TCO)** | **$600k – $1.2M+** (Appliance nodes, per-TB SaaS licensing, cloud vaulting fees) | **$80k – $150k** (Commodity local NVMe/Synology storage + raw S3/Glacier cloud fees) |
| **Scale Sizing (1.5B Inodes)** | Requires high-node CDM cluster or enterprise SaaS tier to manage metadata footprint | SlateDB NVMe key-value database (~50–80 GB footprint for 1.5B inodes) managed by `vaulticdb` daemon |
| **Cold Storage Optimization** | Fixed chunk/pack sizing; standard S3 lifecycle transitions | Customizable cold pack target size (**512 MiB – 1 GiB**), cutting S3 object counts and API costs by 95%+ |
| **Disaster Recovery (DR)** | Dependent on active Rubrik SaaS/appliance cluster or Rubrik Cloud Cluster | Standalone binary execution; legacy JSON index fallback; zero cloud subscription dependency |
| **GDPR Compliance (Right to Erasure)** | Logical deletion in snapshots; CDC chunks remain in proprietary blocks until full retention expires | `vaultic index gdpr audit` with `--explain-surviving-chunks`, `execute-forget`, signed erasure certificates, and UID blocklist policies |
| **ISO 27001 Cryptography & SIEM** | Proprietary KMS & cloud portal log forwarding | Azure Key Vault Option A (SecretStore via Arc Managed Identity), Syslog over TLS (RFC 5424) to Sentinel, Sampled L1/L2/L3 verification |

---

## 2. Architecture & Topology Overview

### Rubrik Topology
Rubrik relies on hyperconverged physical appliances (Brik nodes) or virtual instances running Rubrik CDM (Cloud Data Management), paired with Rubrik Security Cloud (SaaS) for management.

```
[ Primary Data Sources ] ──► [ Rubrik Brik Cluster / CDM Nodes ] ──► [ Rubrik Vaulting ] ──► [ S3 / Glacier ]
                                (Proprietary Format & Index)         (SaaS Subscription)
```

- **Metadata Storage:** Distributed proprietary metadata filesystem (Atlas / Cassandra).
- **Archiving:** Proprietary "Archive Tiering" that encapsulates data into Rubrik's chunk store before pushing to cloud buckets.

### Vaultic + VaulticDB Topology
Vaultic uses a decoupled process boundary: a lightweight Go CLI for crawling and archiving, communicating via gRPC/Unix sockets with a Rust metadata daemon (`vaulticdb`) that owns a local SlateDB database on fast NVMe storage.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Backup Host (32 Core, 64 GB RAM, Enterprise NVMe)                           │
│ ┌─────────────────────────────────────────────────────────────────────────┐ │
│ │ vaultic CLI Archiver (128 parallel scanner workers)                     │ │
│ └────────────────────────────────────┬────────────────────────────────────┘ │
│                                      │ gRPC / Unix Socket (0600)            │
│ ┌────────────────────────────────────▼────────────────────────────────────┐ │
│ │ vaulticdb (Rust Singleton Daemon)                                       │ │
│ │ - Local SlateDB on NVMe (~50-80 GB footprint for 1.5B inodes)           │ │
│ │ - Inode/Directory Revisions (iv:, dv:), Path Index (pv:), GDPR Index    │ │
│ └────────────────────────────────────┬────────────────────────────────────┘ │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       │
            ┌──────────────────────────┴──────────────────────────┐
            ▼                                                     ▼
┌───────────────────────────────┐                     ┌───────────────────────────────┐
│ Hot Tier: Local Synology NFS  │                     │ Cold Tier: S3 Glacier         │
│ & S3 Warm                     │                     │ Deep Archive                  │
│ (Metadata, Tree Packs, Keys)  │                     │ (Data Packs: 512 MiB - 1 GiB) │
└───────────────────────────────┘                     └───────────────────────────────┘
```

---

## 3. Detailed Dimension Analysis

### 3.1 Total Cost of Ownership (TCO) & Financial Impact

For a **500 TB logical dataset** with **1.5 billion inodes**:

#### Rubrik Financial Model
- **Appliance / Node License:** Rubrik requires physical Brik appliances or CDM virtual node licensing sized for ingest throughput and metadata index memory.
- **SaaS Subscription (Rubrik Security Cloud):** Charged on a per-FETB (Front-End Terabyte) basis, typically $100–$250 per FETB annually.
  - **500 TB Subscription:** $50,000 – $125,000 per year in software licensing alone.
- **Cloud Egress & API Costs:** Standard lifecycle management writes millions of small objects unless carefully managed, triggering high AWS S3 PUT and lifecycle transition fees.
- **Estimated 5-Year TCO:** **$600,000 – $1,200,000+**.

#### Vaultic + VaulticDB Financial Model
- **Software License:** **$0** (Open-source, self-hosted, no per-TB or per-node subscription fees).
- **Hardware Cost:** Local backup host (32-core AMD EPYC, 64 GB RAM, 1.92 TB Enterprise NVMe) + Synology NFS enclosure: **~$15,000 – $25,000** one-time Capex.
- **Cloud Storage Cost (S3 Glacier Deep Archive):**
  - 500 TB at AWS Glacier Deep Archive pricing ($0.00099 / GB / month): **~$500 / month** ($6,000 / year).
- **S3 API Optimization:** Vaultic sets cold data target pack size to **512 MiB – 1 GiB**.
  - 500 TB is stored as only **500,000 to 1,000,000 S3 objects** (instead of tens of millions of small files).
  - S3 PUT request costs for 500,000 objects: **~$2.50 one-time**.
- **Estimated 5-Year TCO:** **$55,000 – $110,000** (Hardware + raw AWS S3/Glacier storage). **Savings: 80% – 90% compared to Rubrik.**

---

### 3.2 Performance & Scale (1.5 Billion Inodes)

Scaling to **1.5 billion inodes** poses severe challenges for metadata indexing, directory scanning, and memory management.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ 1.5 Billion Inodes Metadata Footprint in SlateDB (vaulticdb)                 │
├─────────────────────────────────────────────────────────────────────────────┤
│ Inode Revisions (iv:)           : ~25 - 35 GB                               │
│ Directory Revisions (dv:)       : ~10 - 15 GB                               │
│ Path History Index (pv:)        : ~15 - 20 GB (opt-in path-keyed encoding)  │
│ Blob Index & User Attributions  : ~8 - 10 GB                                │
├─────────────────────────────────────────────────────────────────────────────┤
│ Total Local NVMe Footprint      : ~58 - 80 GB                               │
└─────────────────────────────────────────────────────────────────────────────┘
```

- **Rubrik Approach:** Rubrik handles scale by distributing metadata across a multi-node cluster. However, scaling metadata for 1.5 billion small files requires adding more cluster nodes or increasing cloud SaaS tier capacity.
- **Vaultic Approach:** `vaulticdb` uses SlateDB's log-structured merge-tree (LSM-tree) architecture on local NVMe SSDs.
  - **Constant-Time Lookups:** Point lookups (`b:<blob-hash>`) and multi-gets utilize NVMe block caches and Bloom filters.
  - **128 Parallel Archiver Workers:** Vaultic’s Go archiver utilizes a bounded worker pool (128 workers by default) and a 50,000-item stat queue, enabling `lstat()` crawling across massive NFS file trees at line rate.
  - **Memory Efficiency:** Scanning and reconciling 1.5 billion inodes uses fixed memory (~32 GB RAM allocation for SlateDB block cache and write buffers).

---

### 3.3 Flexibility & Vendor Independence

| Feature | Rubrik | Vaultic + VaulticDB |
|---|---|---|
| **Data Format** | Proprietary Rubrik block format | Standard Restic / Rustic specification |
| **Restore Tooling** | Requires active Rubrik CDM software / cluster | Any restic/rustic binary, or `vaultic` CLI |
| **Cold Warm-up Customization** | Built-in S3 Glacier unfreezing via Rubrik cloud agent | Fully customizable `--warm-up-command` supporting external shell scripts, AWS CLI, or native S3 restore API |
| **API & Automation** | REST API & Rubrik SDK (Proprietary) | gRPC Protobuf API, standard CLI, structured JSON outputs |

---

### 3.4 Disaster Recovery (DR) & Business Continuity

#### Rubrik DR Workflow
- **Dependency:** To restore data from an archived cloud bucket, you must have an active Rubrik CDM instance or Rubrik Cloud Cluster deployed in AWS.
- **Lock-in Risk:** If the Rubrik subscription lapses, software license expires, or the Rubrik control plane is unreachable during a catastrophic event, raw data stored in S3 cannot be decrypted or restored independently.

#### Vaultic DR Workflow
- **Zero Software Dependency:** Vaultic backup data in S3/Glacier is self-contained.
- **Dual-Mode Recovery:**
  1. **Primary DR:** Launch `vaultic` with `vaulticdb` for high-speed SlateDB-backed recovery.
  2. **Emergency Fallback (No Daemon / Legacy Mode):** If `vaulticdb` or the local NVMe host is destroyed, `vaultic` automatically projects or reads standard **Restic/Rustic JSON indexes** from the S3 bucket.
  3. **Universal Recovery:** In an absolute worst-case scenario, the open-source `restic` or `rustic` standard binaries can read and restore the repository directly.

---

### 3.5 Operational Resilience & Consistency

- **Split-Brain & Fencing Protection:** `vaulticdb` maintains process-lifetime advisory locks and monotonic epoch sequence tracking in SlateDB. If a secondary host attempts to write to the repository simultaneously, SlateDB fencing immediately aborts the unauthorized transaction.
- **Transactional Writes:** Metadata updates (`iv:`, `dv:`, `b:`, `u:`) commit inside atomic SlateDB `WriteBatch` transactions. Partial index writes during host power loss are impossible.
- **Immutable WORM Compliance:** Combines AWS S3 Object Lock (Compliance Mode) with vaultic's retention-aware deletion queue (`dq:`):
  $$\text{delete\_after} = \max(\text{now} + \text{keep\_delete}, \text{min\_retention\_until})$$
  Unreachable cold packs linger safely in Glacier until their 180-day retention lock expires, preventing early-deletion fees while guaranteeing compliance.

---

### 3.6 GDPR & ISO/IEC 27001 Compliance Comparison

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Compliance & Security Capabilities Summary                                  │
├───────────────────────────────┬──────────────────────┬──────────────────────┤
│ Feature                       │ Rubrik               │ Vaultic + VaulticDB  │
├───────────────────────────────┼──────────────────────┼──────────────────────┤
│ ISO 27001 Cryptography        │ Proprietary KMS /    │ Azure Key Vault      │
│                               │ Cloud KMS            │ Option A (Managed ID)│
│ SIEM Audit Logging            │ Rubrik Envisions /   │ Multi-Target TLS     │
│                               │ Syslog / API         │ Syslog to Sentinel   │
│ Storage Verification          │ Automated SLA        │ Sampled L1/L2/L3     │
│                               │ Verification         │ Verification         │
│ GDPR Art. 15 (Access Audit)   │ Search Index         │ `gdpr audit` +       │
│                               │                      │ Surviving Chunks     │
│ GDPR Art. 17 (Right to Erasure│ Exclude from Index / │ `execute-forget` +   │
│                               │ Expire Snapshots     │ UID Blocklist Policy │
└───────────────────────────────┴──────────────────────┴──────────────────────┘
```

#### GDPR Article 15 (Right of Access) & Article 17 (Right to be Forgotten)
- **Rubrik:** Deleting a specific user's data from immutable backup snapshots is inherently challenging in Rubrik's chunked block store. Data remains in historical snapshots until the SLA retention period expires.
- **Vaultic:** Provides surgical, compliant GDPR controls:
  1. **Surviving Chunk Analysis (`vaultic index gdpr audit --uid <uid> --explain-surviving-chunks`):** Explains exactly which data chunks belong exclusively to `<uid>` vs. which chunks survive due to external deduplication references (with path samples and reference counts).
  2. **Erasure Execution (`vaultic index gdpr execute-forget --uid <uid> --signing-key <pem> --confirm`):** Redacts user metadata/content references while preserving retained graph structure, rebuilds chunk reference counts (`rc:`), enqueues wholly unreferenced pack placements into `dq:`, and generates an operator-signed erasure certificate.
  3. **Future Backup Exclusion (`vaultic index gdpr set-policy --exclude-uid <uid>`):** Enforces persistent blocklist rules (`u:policy:blocklist:<uid>`) so archiver crawlers automatically skip files owned by erased users during future backup runs.

#### ISO/IEC 27001:2022 Security Controls
- **Azure Key Vault Option A (Control A.8.24):** Fetches repository key passphrases directly from Azure Key Vault via Azure Arc Managed Identities (`SecretGet`). Fetched once at process startup; long-running backups are immune to mid-job Azure WAN outages.
- **Multi-Target Syslog Exporter (Control A.8.15):** Routes RFC 5424 TLS syslog events by category (`auth`, `integrity`, `gdpr`, `restore`, `lifecycle`) directly into **Microsoft Sentinel / Azure Arc**.
- **Sampled Storage Verification (Control A.8.12):** `vaultic index verify-storage` allows query filtering (`--tier`, `--backend`, `--created-after/before`, `--retention-status`) and statistical sampling (`--sample-percent P%`) across Level 1 (header), Level 2 (checksum), and Level 3 (full unpack) checks with automated Glacier warm-up.

---

## 4. Final Recommendation & Decision Matrix

### Choose Rubrik if:
- You require an out-of-the-box, turnkey hardware appliance with zero in-house operational engineering.
- You are already heavily invested in the Rubrik SaaS ecosystem for virtual machine (VMware/Hyper-V) snapshot management and database-native APM integrations.
- Budget is not a primary constraint, and vendor lock-in is acceptable to your organization.

### Choose Vaultic + VaulticDB if:
- **Cost Reduction is Critical:** You want to save 80%+ on TCO for 500 TB / 1.5B inodes by leveraging existing Synology NFS hardware and raw AWS S3 Glacier Deep Archive storage.
- **Zero Vendor Lock-in is Required:** You demand open, standard data formats (Restic/Rustic) and the ability to restore data even if software subscriptions or control planes are unavailable.
- **Strict GDPR Compliance is Mandatory:** You require surgical user data auditing (`--explain-surviving-chunks`), cryptographic erasure certificates (`execute-forget`), and UID backup blocklists (`set-policy`).
- **Enterprise SIEM & Key Vault Integration:** You want native Azure Key Vault passphrase integration via Azure Arc and multi-target Syslog-over-TLS streaming to Microsoft Sentinel.
