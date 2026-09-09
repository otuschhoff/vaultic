# Phase 27: Native Ceph librados object-store backend

[Back to roadmap index](00-overview.md)

[Previous: Phase 26](phase-26-ephemeral-cloud-storage-credentials.md) | [Next: Phase 28](phase-28-slatedb-wal-on-s3-and-librados.md)

**Status: implemented.**

The implementation includes sealed RADOS topology and CephX custody, optional
Go and Rust native builds, atomic repository and SlateDB object-store adapters,
large-object and multipart publication, and an isolated Ceph integration
harness. The harness exercises encrypted repository initialize/write/check/
restore/reopen, repository placement copy and fallback, durable SlateDB
close/reopen, namespace denial, bounded OSD outage, and data-plane recovery.
Native dependency, compatibility, permission, health, replication, and RGW
migration guidance is maintained in ``doc/046_native_rados.rst``.

**Goal:** support native Ceph RADOS object storage through `librados` for Vaultic repository backends and VaulticDB's SlateDB object-store interface. This is distinct from the existing Ceph RGW S3-compatible backend and does not require an RGW gateway. Provide the storage foundation for remote WAL and read-cache tiers in Phases 28-30.

## Scope and prerequisites

- Reuse the backend registry, sealed topology, placement identities, and credential custody from Phases 12, 19, 24, and 26.
- Implement both the Go repository backend contract and the Rust `object_store` contract used by the pinned SlateDB version.
- Address objects by cluster identity, pool, namespace, and repository prefix; reject ambiguous or cross-repository mappings.
- Keep S3/RGW configuration separate from native RADOS configuration. Existing repositories must not change behavior when librados is unavailable.

## Design

### Native storage contract

Use maintained librados bindings where practical and isolate native FFI behind small adapters. Map ranged reads, stat, listing, create, conditional replace, delete, copy, and multipart publication to explicitly tested RADOS operations. Validate the exact atomic-create and compare-and-swap semantics required by repository writes and SlateDB manifests/fencing; never emulate them with an unsafe read-then-write sequence.

Define handling for large pack objects and provider object-size limits. If chunking or multipart staging is needed, publish the logical object atomically only after all chunks are durable; readers must never observe partial objects. Clean up abandoned staging without deleting committed content. Preserve retry idempotency, error classification, bounded concurrency, cancellation, and resource cleanup across the FFI boundary. Offload blocking native calls from asynchronous executor threads.

### Configuration and credentials

Extend sealed topology with a distinct `rados` provider and validated monitor endpoints, cluster identity, pool, namespace, prefix, and CephX client reference. Keep keys in broker-managed custody, not CLI arguments or exported environment variables. Specify which connection settings are host-local overrides and audit their use.

Map data read, append, maintain, lock, WAL, and cache roles to the narrowest CephX capabilities supported by the deployed cluster. CephX is not AWS STS: do not claim provider-enforced ephemeral or create-only authority unless independently demonstrated. Report static authority and client-only restrictions as compliance findings. Cache eviction authority must never imply delete permission on authoritative repository data or WAL.

### Build and operations

Make native support an explicit build feature with supported Ceph versions and platform requirements. Builds without librados must still work and return a clear unsupported-backend error if selected. Document monitor connectivity, CephX setup, library installation, pool replication requirements, health checks, and migration from RGW as an explicit copy-and-verify operation rather than a URL substitution.

## Implementation steps

1. Verify maintained bindings, licensing, supported platforms, and the required native atomicity/durability primitives; record a compatibility matrix.
2. Add topology validation, credential resolution, backend registration, and capability reporting for `rados`.
3. Implement the Go backend and Rust object-store adapter, including conditional operations, ranged reads, listing, multipart publication, and cancellation.
4. Wire repository placements and metadata replicas without changing existing local/S3/Azure/GCS behavior.
5. Add secret-free latency, throughput, retries, capacity, and cluster-error diagnostics; document native build and deployment procedures.

## Tests

- Run shared Go backend and Rust object-store conformance suites, including identical create retries, conflicting writes, version preconditions, concurrent writers, and listing/range boundaries.
- Use an isolated Ceph cluster to test monitor/OSD interruption, connection recovery, partial uploads, cancellation, restart, and durable publication.
- Verify CephX denial for cross-pool, cross-namespace, and cross-repository access and each supported narrow role; report unsupported enforcement explicitly.
- Exercise encrypted backup, restore, check, placement copy, and a SlateDB reopen using native RADOS, plus builds with native support disabled.

## Exit criterion

Vaultic and VaulticDB can use native librados through their existing storage abstractions with verified atomicity, durability, isolation, and error behavior. RGW remains a separate supported route. Native dependencies and permission limitations are documented, and the adapter is ready for the WAL and cache phases without changing repository format or weakening writer fencing.