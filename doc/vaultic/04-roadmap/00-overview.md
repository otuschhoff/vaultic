# Roadmap Overview

[← Back to index](../README.md)

This is the tactical execution plan that implements the strategy in
[Vision, Scope, and Non-Negotiable Guarantees](../01-strategy/01-vision-and-principles.md)
and the design in [Architecture](../02-architecture/00-overview.md). Each phase
has a dedicated document containing its goals, implementation steps, tests, and
exit criteria.

Each phase is an independently executable change set. An implementation phase
must end with its listed tests passing, a documented artifact or API handoff,
and no unexplained changes to legacy behavior. Do not begin the next phase when
an exit criterion is failing. Keep each phase in a separate commit or small
commit series so it can be reviewed or reverted independently.

## Phases

| Phase | Milestone | Status |
|---|---|---|
| 0 | [Freeze assumptions and native build](phase-00-freeze-assumptions-and-native-build.md) | see phase detail |
| 1 | [Protocol contract and daemon lifecycle](phase-01-protocol-contract-and-daemon-lifecycle.md) | see phase detail |
| 2 | [Engine abstraction and legacy adapter](phase-02-engine-abstraction-and-legacy-adapter.md) | see phase detail |
| 3 | [Versioned schema and daemon storage adapter](phase-03-versioned-schema-and-daemon-storage-adapter.md) | see phase detail |
| 4 | [Best-effort legacy import](phase-04-best-effort-legacy-import.md) | see phase detail |
| 5 | [Backup crawl reconciliation](phase-05-backup-crawl-reconciliation.md) | see phase detail |
| 6 | [Authoritative dual-write and legacy projection](phase-06-authoritative-dual-write-and-legacy-projection.md) | see phase detail |
| 7 | [Import/export/check CLI workflows](phase-07-import-export-check-cli-workflows.md) | see phase detail |
| 8 | [Prune, GC, and operational hardening](phase-08-prune-gc-and-operational-hardening.md) | see phase detail |
| 9 | [Pack tier model and lifetime facts](phase-09-pack-tier-model-and-lifetime-facts.md) | see phase detail |
| 10 | [Pack history event log and rollups](phase-10-pack-history-event-log-and-rollups.md) | see phase detail |
| 11 | [Introspection CLI](phase-11-introspection-cli.md) | see phase detail |
| 12 | [Backend registry, placement records, and per-backend prune](phase-12-backend-registry-placement-records-and-per-backend-prune.md) | see phase detail |
| 13 | [Historical path resolution and file-history CLI](phase-13-historical-path-resolution-and-file-history-cli.md) | see phase detail |
| 14 | [Versioned path index](phase-14-versioned-path-index.md) | see phase detail |
| 15 | [Placement scheduler, offsite RPO, and promotion](phase-15-placement-scheduler-offsite-rpo-and-promotion.md) | see phase detail |
| 16 | [Growth, churn, per-user/group attribution, and GDPR audit CLI](phase-16-growth-churn-per-user-group-attribution-and-gdpr-audit-cli.md) | see phase detail |
| 17 | [ISO27001 and GDPR compliance](phase-17-iso27001-gdpr-compliance-azure-key-vault-syslog-and-storage-verification.md) | see phase detail |
| 18 | [SlateDB metadata encryption and unified key envelope](phase-18-slatedb-metadata-encryption-and-unified-key-envelope.md) | see phase detail |
| 19 | [Multi-provider cold storage and replicated metadata](phase-19-multi-provider-cold-storage-pool-and-replicated-metadata-store.md) | see phase detail |
| 20 | [Quorum-based encryption unlock](phase-20-quorum-based-encryption-unlock.md) | in progress |
| 21 | [Crawl optimization with cwalk and pathdiff](phase-21-crawl-optimization-with-cwalk-and-pathdiff.md) | see phase detail |
| 22 | [Operational resilience and deferred crawl commit](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md) | see phase detail |

## Supporting plans

- [Testing Strategy](01-testing-strategy.md)
- [Operational Observability](02-observability.md)
- [Rollout and Rollback](03-rollout-and-rollback.md)
- [Known Constraints](04-known-constraints.md)
