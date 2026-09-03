# Metadata resilience operations

Phase 22 separates durable staged data from a committed backup. A successful deferred crawl reports `data_durable_metadata_pending`; it does not create a normal snapshot, advance an incremental basis, emit normal backup-success telemetry, or satisfy a backup-completion objective. Normal restore and retention workflows must wait for reconciliation. The `index staging restore` command is an explicit emergency exception.

## Prerequisites

Keep a protected bootstrap profile and its trusted generation anchor outside the repository host. The profile contains seed repository locators and the expected repository identity, never backend credentials. Credentials must come from workload identity, provider configuration, protected files, or an approved secret store. Keep at least one topology copy reachable in every intended disaster scenario and exercise recovery from each seed independently.

The local Phase 20 broker must be unlocked. Topology discovery receives only its purpose-derived topology key. Repository-key access requires a separate authorized lease. Data-plane-only operation does not make remote key release possible.

Before relying on degraded ingest, verify that reachable staging backends satisfy the configured `min_copies`, `min_domains`, and `min_offsite` policy. Vaultic never lowers these values after a backend or metadata failure.

## Writer transfer

Inspect the daemon role before maintenance:

```console
vaultic index writer status
```

Request demotion and wait for `read-only` before treating ownership as released:

```console
vaultic index writer demote --reason "metadata maintenance"
```

Demotion rejects new writes, drains admitted writes and transactions, durably closes the writer, and then reports `read-only`. A timeout leaves the instance fenced. Promotion obtains a fresh fencing epoch and revalidates metadata before reporting `read-write`:

```console
vaultic index writer promote --reason "maintenance complete"
```

Clients must retry role-transition failures with their original durable idempotency key and query ambiguous results before replaying a mutation.

## Deferred crawl

Use automatic degradation only when pending durability is acceptable:

```console
vaultic --bootstrap-profile /protected/repository.toml backup \
  --allow-deferred-commit --deferred-mode=auto /source
```

`read-only-assisted` may use prior metadata only as stale hints. It uploads harmless duplicates rather than trusting a stale absence or location. `data-plane-only` never opens or trusts VaulticDB, performs a full crawl, and requires explicit acknowledgement:

```console
vaultic --bootstrap-profile /protected/repository.toml backup \
  --allow-deferred-commit --deferred-mode=data-plane-only \
  --acknowledge-metadata-bypass /source
```

A pending result includes the journal job ID, protected bytes, placement result, and expiry. Record the job ID in incident tracking. Hard staging quotas in repository configuration stop admission when staged bytes, jobs, or maximum age exceed policy. Pack bytes are reserved before the first upload, so an over-budget pack cannot become a claimed durable result.

## Journal operations

List and authenticate journals:

```console
vaultic index staging status
vaultic index staging inspect JOB_ID
```

Extend an expiry within repository policy:

```console
vaultic index staging extend JOB_ID --by 24h
```

Extensions are immutable, quorum-published, and chained to the seal and prior expiry. They do not commit the backup.

When a healthy writer returns, always attempt Plan A first:

```console
vaultic index staging reconcile JOB_ID
```

Retryable writer, broker, or object-store failures leave the journal pending. A deterministic journal or basis conflict may be recorded explicitly:

```console
vaultic index staging reject JOB_ID --reason "basis contract cannot be reconciled"
```

A `healing-required` result means mutation and destructive maintenance must remain disabled. It never starts a rebuild automatically.

## Emergency restore

When metadata remains unavailable, restore directly from a sealed journal and a bootstrap profile:

```console
vaultic --bootstrap-profile /protected/repository.toml \
  index staging restore JOB_ID --target /recovery
```

The command authenticates the quorum seal and segment chain, verifies every pack's size, SHA-256 identity, copies, failure domains, and offsite requirement, builds an in-memory index from validated blob ranges, and uses the normal restore engine. It does not publish a snapshot or write authoritative metadata. Use `--dry-run` before recovery and `--verify` after writing when time permits.

## Abandonment

Rejecting a journal does not remove GC protection. Abandonment is the explicit data-loss decision:

```console
vaultic index staging abandon JOB_ID \
  --reason "source superseded and independently verified" \
  --acknowledge-data-loss --safety-delay 48h
```

The authenticated abandonment record retains pack protection until `DeleteAfter`. Expiry alone never deletes data. Before deleting abandoned packs, recheck journal state, physical placement, and authoritative idempotency state.

## Metadata rebuild boundary

Plan B is reserved for authenticated metadata damage, identity mismatch, rollback, or an integrity preflight that proves the current generation cannot accept Plan A. A candidate must use an isolated persistent namespace, preserve the suspect namespace unchanged, rebuild from authenticated sources in documented authority order, represent unknown facts as crawl debt, pass full validation read-only, and activate through a conditional generation update plus a fresh writer fence. Activation, rollback, and retirement are separate operator approvals. Never use the legacy import activation flags as an in-place repair of a suspect generation, and never retire forensic data before the post-activation observation window and a clean `index check`.

## Exercises and monitoring

Alert on writer fencing, metadata corruption bypass, quota refusal, under-replicated or conflicting journals, expiry, rejection, abandonment, and emergency restore. Events intentionally exclude paths, credentials, keys, shares, journal plaintext, and broker secret material.

Exercise at least: each individual bootstrap seed; one unavailable staging mirror with policy still met; policy loss; daemon destruction followed by data-plane staging; crash before and after seal publication; crash after metadata commit but before completion publication; emergency restore; abandonment delay; and writer demotion/promotion with ambiguous idempotent writes.
