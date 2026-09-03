# Phase 17: ISO27001 & GDPR compliance, Azure Key Vault, Syslog, and Storage Verification

[← Back to roadmap index](00-overview.md)

[← Phase 16](phase-16-growth-churn-per-user-group-attribution-and-gdpr-audit-cli.md) · [Phase 18 →](phase-18-slatedb-metadata-encryption-and-unified-key-envelope.md)

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
