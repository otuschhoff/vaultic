# Phase 36: NFS server-side group authorization

[Back to roadmap index](00-overview.md)

[Previous: Phase 35](phase-35-nfsv3-locking-and-recovery.md) | [Next: Phase 37](phase-37-multi-server-nfs-ha-research.md)

**Status: design specification, not yet implemented.**

**Goal:** resolve an incoming NFS UID to server-authoritative primary and supplementary GIDs and check access against inode permissions on the server. Start with a validated JSON identity map; define an adapter contract for later `adbind` integration with cached Active Directory identity lookup, without depending on that unfinished service.

## Identity source and policy

Define a versioned, bounded JSON format, for example:

```json
{
  "version": 1,
  "users": [
    { "uid": 1001, "primary_gid": 100, "supplementary_gids": [200, 300] }
  ]
}
```

Reject duplicate UIDs, out-of-range numeric IDs, oversized membership lists, malformed data, unknown versions and ambiguous mappings. Load from an administrator-owned non-world-writable file. Reload atomically using a complete validated revision; never serve a partially parsed map. On invalid reload, retain the previous valid revision only within an explicit maximum-staleness policy and report degraded status; default fail closed when authoritative membership is unavailable or expired. Unknown UIDs are denied unless an explicit export policy maps them to a restricted anonymous identity.

Map/squash the incoming identity first, then resolve its effective UID through the server source. Ignore AUTH_SYS primary and supplementary GID claims for authorization in this mode. Root squash is default, with explicitly configured anonymous IDs and any exception clearly audited. Numeric identities are scoped to the export's configured identity domain; equal numbers from unrelated domains must not be implicitly merged.

## Inode authorization

Enforce POSIX owner/group/other permission selection, not a simple test that the caller belongs to the inode's group. Check search permission on traversed directories, read/readdir/readlink behavior as defined by the filesystem contract, file write/truncate, directory create/remove/rename, sticky-directory rules, setgid inheritance, chmod/chown, and privilege-bit clearing after writes or ownership changes. Define supported ACL semantics; do not silently treat stored but unsupported ACLs as successfully enforced.

`ACCESS` is advisory to the client. Every actual operation must independently authorize, including handle-based READ/WRITE/SETATTR after earlier LOOKUP, with inode version and permission checks tied to the mutation transaction to avoid time-of-check/time-of-use races. Revalidate after policy revision changes; do not let server caches preserve revoked access. Apply export/root restrictions even to forged or retained handles. NLM locks do not confer access permission, and denied identities must not gain lock authority through a separate unchecked endpoint.

Phase 31's permission-presentation overrides are not authorization. Disallow combinations that bypass this policy and explain how owner projection affects display versus enforcement. Previously delivered file data, client caches, and an already completed read cannot be revoked retroactively.

## AUTH_SYS limitation and future adbind

Server-authoritative GIDs prevent clients from inventing supplementary group membership, but AUTH_SYS still lets a malicious client claim a different UID. This phase is authorization for an asserted identity, not cryptographic user authentication. Continue to require explicitly trusted clients and network restrictions; VPN host authentication alone does not prove the end-user UID. Stronger user authentication such as RPCSEC_GSS/Kerberos needs a separate protocol/security design and is not claimed here.

Define an identity resolver returning UID, primary/supplementary groups, source/domain, revision, observation time, expiry and a typed not-found/unavailable result. Bound positive and negative caches, request concurrency, lookup timeout and maximum staleness. Coalesce lookups and invalidate cached authorization on source revision changes. JSON uses this interface first; provide a fake resolver for tests.

A later `adbind` adapter will implement that interface once its authenticated API is ready. Earmark stable AD-to-POSIX UID/GID mapping, nested-group expansion, trust/domain boundaries, deleted/reused accounts, invalidation and outage behavior, and protected transport/credentials. Do not invent an adbind endpoint or silently fall back to client groups during an outage. Shipping this phase does not require implementing or deploying adbind.

## Implementation steps

1. Define the identity resolver API and JSON schema, validation, atomic reload, domain and expiry policy.
2. Implement server-side effective-identity resolution and shared inode-operation authorization for read-only and writable NFS exports.
3. Integrate revision-aware caches, handle/mutation rechecks, NLM admission, root squash and permission-display constraints.
4. Document JSON administration, AUTH_SYS limitations and the future adbind contract; extend Phase 32 with bounded lookup/denial/cache metrics.

## Tests

- Cover owner/group/other precedence, forged client groups, unknown UID, root squash, directory search, sticky/setgid behavior, cross-directory rename and ownership changes.
- Reuse an existing handle after membership or inode permissions change; deny subsequent unauthorized operations even after a successful ACCESS response.
- Exercise malformed/duplicate/oversized JSON, atomic reload, stale-source expiry, resolver timeout, negative caching, domain collisions and bounded resources.
- Demonstrate explicitly that a trusted-client AUTH_SYS UID assertion is still spoofable; no test or documentation may claim that group lookup authenticates users. Test adbind adapter contracts with a fake resolver only.

## Exit criterion

Both read-only and writable NFS exports can enforce operation-specific inode permissions using a server-managed JSON UID-to-GID map rather than client-supplied groups. Reload, revocation, stale identities, root squash, and handle-based access are tested and observable. The AUTH_SYS trust limit is explicit and a bounded resolver interface is ready for later adbind integration.