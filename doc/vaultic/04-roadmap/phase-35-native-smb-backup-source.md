# Phase 35: Native SMB backup, restore, and Windows metadata indexing

[← Back to roadmap index](00-overview.md)

[← Phase 34](phase-34-operational-monitoring-and-metrics-export.md) · [Phase 36 →](phase-36-writable-fuse-and-durable-writeback.md)

[Backup documentation](../../040_backup.rst) · [Sealed topology and credentials](phase-24-sealed-topology-and-credentials-in-the-recovery-capsule.md) · [Analytics](../02-architecture/07-analytics-engine.md)

**Status: design specification, not yet implemented.**

**Goal:** back up and restore an SMB2/SMB3 share directly from Vaultic on Linux,
macOS, or Windows without a kernel CIFS mount. Use a pinned pure-Go SMB client,
preserve Windows metadata, and index SMB source identity, paths, SIDs, security
descriptors, extended attributes, and alternate data streams in VaulticDB.
Authentication is Kerberos-only: an externally supplied in-memory credential
cache, password-based AS exchange, or keytab-based AS exchange obtains a CIFS
service ticket. NTLM is never offered. Password/keytab custody may be external
or opt-in sealed through the existing broker and recovery capsule; tickets and
credential caches exist only in process memory.

This phase provides both a backup source and an SMB restore target. It reuses the
normal archiver, restorer, pack publication, snapshot publication, exclusion,
progress, retry, and durability contracts. Native SMB changes how bytes and
metadata are acquired or applied; it does not create a second archive format or
weaken snapshot publication.

## Scope and operator contract

Add a source URL and profile form:

```text
vaultic backup --smb-source smb://server.example/share/path [backup flags]

[backup.sources.office]
type = "smb"
url = "smb://server.example/share/path"
credential = "cred:smb-office"
security = "required"
streams = "all"
```

The URL contains no password, ticket, or keytab bytes. UNC input such as
`\\server\share\path` is accepted and normalized to the same structured source.
Multiple SMB roots may be combined only when each has a distinct source identity
and snapshot root. Existing positional local paths remain local filesystem
sources; Vaultic never mounts, invokes `mount.cifs`, shells out to `smbclient`,
or reads a host kernel mount table to implement this feature.

Initial controls:

| Control | Meaning |
|---|---|
| `--smb-source URL` | SMB share and source root; repeatable through named profile sources. |
| `--smb-target URL` | SMB share and restore root; mutually exclusive with local restore targets. |
| `--smb-credential REF` | Broker/capsule credential reference; never a secret literal. |
| `--smb-password-file FILE` | External Kerberos principal/password document read once from a protected file or stdin descriptor; used only for a KDC AS exchange. |
| `--smb-ccache-fd FD` | Import a ccache once from an inherited descriptor into memory; Vaultic never creates or updates a ccache file. |
| `--smb-keytab-file FILE` | External keytab plus explicit principal/realm; used for a KDC AS exchange and read into zeroizing memory. |
| `--smb-security required|best-effort` | `required` fails the item/run when requested owner, group, DACL, or SACL cannot be read; default `required`. |
| `--smb-ea required|best-effort|off` | Preserve NTFS extended attributes; default `required` when the server advertises support. |
| `--smb-streams all|unnamed-only` | Preserve alternate data streams or only the unnamed data stream; default `all`. |
| `--smb-snapshot PATH` | Explicit server snapshot namespace such as an operator-selected `@GMT-*` path; no snapshot is guessed. |
| `--smb-max-in-flight N` / `--smb-max-bytes BYTES` | Shared request and byte budgets across listing, metadata, ACL/EA, and reads. |
| `--smb-restore-metadata required|best-effort` | Apply preserved descriptors, EAs, streams, reparse data, attributes, and timestamps; default `required`. |

Secret-bearing file flags are mutually exclusive with a sealed reference. They
must follow existing protected-file ownership/mode checks and must not be copied
into profiles, status, events, process arguments beyond the non-secret filename,
or repository metadata. Interactive password prompting is allowed only on a TTY
and is unsuitable for automation.

## Native protocol client

Use SMB 3.1.1 when offered, then SMB 3.x/2.x according to an explicit minimum;
SMB1 is unsupported. Require signing by default and support required encryption.
Record negotiated dialect, signing/encryption state, server GUID, share
capabilities, maximum transfer sizes, DFS involvement, and time skew without
logging secret or path content.

Start with a dependency spike around a maintained pure-Go SMB2/3 transport. The
leading implementation shape is a pinned `go-smb2`-compatible client plus a
pure-Go Kerberos/SPNEGO implementation such as `gokrb5`, behind `internal/smb`.
Do not commit to a module until executable probes prove all required low-level
operations: compound/create/read/write/close, directory pagination, `QUERY_INFO` file
and filesystem classes, security descriptor queries, EA queries, named stream
enumeration/read/write, reparse-point handling, metadata setters, rename/link,
durable cancellation, signing, encryption, and custom SPNEGO/Kerberos initiation.
The transport must be able to disable every NTLM mechanism. Fork narrowly or
select another maintained pure-Go implementation if the public API cannot expose
those operations. Dependency types must not escape `internal/smb`.

Authentication modes:

- **Password:** use principal, realm, and password only for Kerberos pre-
  authentication to the KDC, obtaining a TGT and then the explicit
  `cifs/server-fqdn@REALM` service ticket. A password is never sent to the SMB
  server and never selects NTLM, including when Kerberos fails.
- **Kerberos credential cache:** import an externally supplied ccache from an
  inherited descriptor or memory-oriented credential service, validate expiry
  and realm, and retain the parsed cache only in locked/zeroizing process memory.
  Vaultic never writes a ccache to disk, seals it, or exports renewed tickets.
- **Kerberos keytab:** obtain/renew tickets in memory for an explicit principal;
  verify DNS canonicalization and SPN policy rather than guessing aliases.
- **Sealed credentials:** extend the Phase 24 credential enum with
  `smb-password` and `kerberos-keytab`. Password or keytab bytes may be sealed
  opt-in and leased with a new `source-read` binding. Non-secret server/share,
  principal, realm, SPN, signing/encryption policy, and credential reference live
  in versioned `backup_sources`/`restore_targets` topology sections. Generated
  TGTs, service tickets, session keys, and ccache state are ephemeral and are
  never persisted back into the capsule or any filesystem path.

SPNEGO advertises Kerberos only. KDC unavailability, pre-authentication failure,
unknown SPN, ticket expiry, realm mismatch, or server rejection is a hard
authentication error, never a trigger for NTLM negotiation. Do not use a library
default that enables NTLM or writes a conventional ccache as a side effect.

A sealed source credential expands the blast radius of repository custody to the
source share. Enrollment must state this explicitly, require an unlocked broker,
create a new capsule generation, and support rotation/removal. External files
remain the default when source and repository custody must stay independent.

## Source filesystem adapter

Implement `internal/fs/smb` as an `fs.FS`/`fs.File` adapter so scanner and
archiver behavior remains shared. Use Windows path semantics internally while
presenting canonical slash-separated archive paths. Reject NUL, separators in a
component, malformed UTF-16, traversal above the configured root, and server
responses outside the selected share. Preserve original UTF-16-derived names in
the archive representation and define deterministic handling for case-insensitive
collisions; never merge names only because Unicode case folding matches.

`OpenFile(..., metadataOnly=true)` opens with minimum metadata rights and no
implicit reparse traversal. `MakeReadable` reopens the same observed object for
the unnamed data stream and verifies identity/size/change time where supported.
Directory enumeration is paginated and byte-bounded. Reads use server-advertised
credits and transfer limits, support cancellation between requests, and never
allocate directly from an untrusted length. Retries reopen by stable identity
when safe or by path plus unchanged metadata; a reconnect never assumes an old
volatile SMB handle is valid.

SMB does not provide a general frozen view. A live-share backup has the same
moving-source limitations as a local live backup, plus network reconnects. If an
operator supplies a server snapshot namespace, bind all opens and enumeration to
that exact namespace and record its identity. Automatic VSS creation, NetApp
snapshot orchestration, DFS-wide consistency, and USN-journal selective crawling
are deferred. A server/share/DFS target change during a run is fatal or causes a
full-root restart before publication, never silent continuation.

## Restic-compatible archive metadata

Preserve the existing restic pack, index, snapshot, tree, and data-blob formats.
Do not add a new pack entry type, alter pack headers, or require a Vaultic-aware
reader to locate file content or restic-supported metadata. PR #4708, merged as
restic commit `92221c2`, established the format that Vaultic already carries in
`data.Node`: generic attribute `windows.security_descriptor` contains the exact
self-relative `SECURITY_DESCRIPTOR` byte sequence, represented by Go JSON as a
base64 string. Use those bytes directly; do not substitute SDDL, normalize ACEs,
or wrap the value in another envelope. Exact-byte hashes may deduplicate equal
descriptors in VaulticDB without changing the tree value.

Use the restic node schema directly wherever it has a representation:

| Windows value | Restic-compatible tree representation |
|---|---|
| Creation time | `generic_attributes["windows.creation_time"]` using the existing `syscall.Filetime` JSON shape. |
| File attributes | `generic_attributes["windows.file_attributes"]` as the existing `uint32`. |
| Security descriptor | `generic_attributes["windows.security_descriptor"]` as PR #4708's self-relative `[]byte` value. |
| NTFS extended attributes | Existing `extended_attributes` array of name and binary value; no SMB-specific EA manifest. |
| Unnamed `$DATA` stream | Existing file node `content` list and `size`. |
| Symlink-like reparse point | Existing `symlink` node and `linktarget` when semantics are lossless. |
| 64-bit persistent FileId | Existing `inode` plus volume-derived `device_id`; a wider FileId additionally uses a bounded inline `windows.file_id` value without truncation. |

Restic has no dedicated ADS field. The preferred compatibility experiment emits
each named stream as an ordinary sibling file node named with Windows stream
syntax, for example `report.docx:Zone.Identifier:$DATA`, whose standard `content`
list references its chunks. This lets unmodified restic traversal, `check`,
`copy`, and `prune` see every stream blob, and Windows restore should create the
stream by opening that name. Release this encoding only if stock supported restic
versions round-trip multiple/empty streams, streams on directories, selections,
overwrite/delete, Linux restore, mount, copy, and prune without collisions or
data loss. If that experiment fails, define a jointly reviewable restic format
extension before shipping ADS; do not hide stream blob IDs inside unknown JSON.

Only metadata with no lossless restic representation, such as an opaque non-
symlink reparse payload, may use a new namespaced `windows.*` generic attribute.
Keep it bounded and inline in the tree node so restic repository mutation cannot
orphan an indirectly referenced blob. Unknown attributes must remain optional to
ordinary restoration. The resulting node representation includes:

- creation, access, write, and change times with source precision;
- DOS/NTFS file attributes and allocation size;
- exact self-relative security descriptor containing owner SID, primary
  group SID, DACL, and SACL when authorized, plus control flags and completeness;
- NTFS extended attributes as ordered name/value records, preserving empty and
  binary values;
- alternate data stream names, sizes, and content references, including the
  unnamed `$DATA` stream without duplicating it;
- reparse tag and opaque reparse data for symlinks, junctions, and unknown tags;
- sparse/compressed/encrypted/offline flags and explicit acquisition outcome;
- stable source identity evidence and hard-link count where available.

Security descriptors are requested with OWNER, GROUP, DACL, and SACL security
information. Reading SACLs commonly requires `ACCESS_SYSTEM_SECURITY` and an
appropriately privileged account. Under `required`, access denial or a partial
descriptor prevents a successful complete snapshot. Under `best-effort`, store
which parts are absent, emit a finding, and mark the snapshot metadata coverage
incomplete. Never represent a missing SACL as an empty SACL. The same rule
applies to unsupported/inaccessible EAs, streams, and reparse data.

EAs and alternate data streams are different NTFS facilities and remain distinct.
Named streams are content, consume pack bytes, participate in change detection,
and need independent read/error accounting. Bound per-file metadata, descriptor,
EA, stream-count, and stream-name/value sizes; an oversized item fails precisely
or is incomplete under an explicit best-effort policy, never truncated silently.
Encrypted EFS content may be unreadable or transparently decrypted by the server;
record the attribute and actual acquisition result without claiming preservation
of EFS keys. Offline/cloud-tiered files are recalled only under an explicit
policy and otherwise produce an incomplete/fatal result.

Windows restore compatibility tests must prove the node decodes to the same
`WindowsAttributes` representation used by native Windows backup/restore where
semantics overlap. Include golden trees produced by unmodified restic 0.17.0 or
newer and require byte-equivalent JSON values for the three `windows.*` fields
introduced by PRs #4611 and #4708. Restic compatibility tests must prove that
`check`, `copy`, `prune`, `forget`, tree rewrites, mount, and restore preserve all
standard references. A supported restic Windows restore should apply creation
time, attributes, security descriptor, and EAs, not merely ignore them. It may
ignore only new Vaultic attributes for metadata restic does not yet understand.

## Identity, paths, and move detection

NTFS has file identity. Treat the persistent FileId exposed by SMB/file
information as the inode equivalent, scoped by source volume and identity epoch.
The volatile half of an SMB2 handle identifier remains connection-scoped and is
never archived as identity. Define the source identity tuple:

```text
source = hash(server GUID, canonical share identity, DFS target policy)
object = (source, volume serial, persistent FileId, identity epoch)
```

Use this volume-scoped persistent FileId for hard-link grouping and parent-to-
current move detection exactly as `(DeviceID, Inode)` is used for local sources.
Record FileId width and information class so it is never truncated into the
legacy 64-bit `inode` field. A capability probe and cross-reconnect test must
still reject servers that synthesize unstable IDs. DFS target change, volume
identity change, server rollback, detected identifier reuse, or unsupported
information classes starts a new identity epoch and disables cross-snapshot move
detection until a new baseline exists; current-snapshot hard links remain grouped
when the server returns the same FileId.

Store both original path and an SMB comparison key derived from negotiated
case-sensitivity and Unicode policy. Original path is authoritative for restore
and display. Comparison keys accelerate lookup only and collision buckets retain
all originals. Renames differing only in case are real namespace changes. Never
use a case-folded path as a unique database key.

## Native SMB restore

Implement an SMB restore target behind the existing restorer's target interface,
sharing transport, path validation, authentication, credit limits, and metadata
code with backup. Preflight the complete restore plan before mutation: validate
the target root and DFS binding, detect case-fold collisions and Windows-reserved
names, verify all content and metadata references, calculate required privileges
and space, and apply overwrite/delete policy. A server/volume/DFS change after
preflight aborts the run. No restored path may escape through `..`, a reparse
point, DFS referral, case alias, or symlink-like object.

Restore in dependency order:

1. Create directories with restrictive temporary permissions and create one
  primary object per archived persistent FileId/hard-link group.
2. Write unnamed data to temporary files using bounded parallel writes, verify
  size and content hash, then write named streams and EAs.
3. Create hard-link names to the verified primary object; use FileId to verify
  that links resolve to the same target. Restore reparse objects without
  following them.
4. Apply owner/group, DACL, and SACL using backup/restore privileges as required.
  Missing privilege is fatal in `required` mode and an exact finding in
  `best-effort` mode; never substitute an inherited or empty ACL silently.
5. Apply sparse/compression state where supported, final timestamps, and file
  attributes last. Finalize directories from leaves to root so child creation
  does not perturb archived directory times.
6. Atomically rename each completed temporary file into place when the server
  supports the required replace semantics. Record non-atomic fallbacks before
  mutation and require explicit operator consent.

The restore journal contains no secrets and records target identity, operation
ID, completed object/FileId groups, temporary names, content verification, and
metadata stages. Resume revalidates every completed target object before skipping
it. Cancellation removes owned temporary objects but never deletes a pre-existing
target. There is no claim of whole-tree atomicity; success means every selected
path and required metadata class was verified after application. A post-restore
scan compares content, FileId hard-link groups, descriptors, EAs, streams,
reparse data, timestamps, and attributes against the snapshot.

## VaulticDB schema and indexes

Add versioned records through the existing schema/migration rules:

| Record/index | Contents and purpose |
|---|---|
| SMB source record | Source ID, server GUID, share identity, DFS policy/target digest, dialect/capabilities, volume serial, case/Unicode policy, identity mode/epoch, credential-free endpoint digest, last complete crawl. |
| SMB object record | Stable identity when proven; node kind, original path key, parent identity/path, hard-link count, size/allocation, timestamps, attributes, metadata digests, latest revision and source generation. |
| SMB path index | `(source, snapshot/revision, comparison key, original UTF-8 bytes)` to object/revision; collision preserving and integrated with Phase 14 path history. |
| Security descriptor object | Content-addressed exact self-relative bytes from `windows.security_descriptor`, descriptor hash, owner/group SID, control/completeness flags, and bounded parsed ACE summaries. Raw bytes remain authoritative. |
| EA and stream summaries | Digests, completeness, counts, sizes, and links to the authoritative tree node/ordinary stream nodes. Do not duplicate EA values or stream chunk lists in hot SlateDB values. |
| SID principal record | Canonical binary SID bytes, string form, optional advisory account/domain display names with resolution source/time, type, first/last seen. SID bytes are authority; names are mutable cache data. |
| SID ownership/trustee indexes | Direct owner/group files, ACL trustee descriptor/object references, counts/bytes and live/archive residency. Separate direct ownership from ACL appearance and from computed effective access. |
| Snapshot source coverage | Source identity/epoch, root, server snapshot identity if any, metadata completeness flags, errors/findings summary and crawl mode. |

Canonical SID bytes are the authoritative key. Do not allocate one global
monotonic SID ID on the backup hot path: it creates contention and complicates
replay. Authoritative records may use length-prefixed SID bytes or a full digest
plus collision-checked bytes. Phase 16 analytics builds generation-local compact
integer dictionary codes and bitmap indexes for owner SID, primary-group SID,
and ACL trustee SID, just as it dictionary-codes other dimensions. Rebuilds may
assign different ordinals without changing query semantics.

Expose `--sid`, `--owner-sid`, `--group-sid`, and `--acl-trustee-sid` predicates
through analytics, history, and GDPR/audit commands. Group membership and deny
ACE ordering make “effective access for user” more than a trustee lookup. Do not
claim effective access unless a versioned directory-membership snapshot and
Windows access-check evaluator cover nested groups, deny/allow order, inheritance,
object-specific ACEs, claims, integrity labels, and unresolved SIDs. Initial
queries report direct ownership and descriptor trustees exactly.

Authoritative snapshot/revision publication atomically references descriptor/EA/
stream summaries and appends analytics deltas. Derived SID summaries/bitmaps may
lag under the existing watermark contract and are rebuildable. Index check
validates raw descriptor hashes, summary-to-tree references, path collision buckets,
identity epochs, SID dictionaries, ownership/trustee indexes, and completeness
markers. Paths, account names, share names, SIDs, ACLs, and EA values are
operationally sensitive and are excluded from metric labels and ordinary logs.

### Schema growth assessment

Measure incremental SMB schema cost separately from repository pack bytes and
from base VaulticDB node/history records that every source already creates. Let
`N` be objects, `P` total original-plus-comparison-key path bytes, `D` unique
security descriptors, `A` total canonical descriptor bytes, `T` unique
`(trustee SID, descriptor)` edges, `M` EA/stream summary records, and `B` named-
stream content bytes stored as ordinary repository blobs. Expected logical
VaulticDB growth and separate repository growth are:

```text
SMB VaulticDB bytes ~= N*object_overhead + P + D*descriptor_overhead + A
                    + T*trustee_edge + M*summary_overhead
repository bytes   += B
```

Initial planning ranges, to be replaced by codec benchmarks, are 180-320 bytes
per SMB object record, 40-80 bytes plus encoded path bytes per path entry,
80-160 bytes per object's direct owner/group index entries, 48-96 bytes per
unique trustee-to-descriptor edge, and 64-160 bytes per summary before names and
tree-node references. Index ACL trustees to deduplicated descriptors, then map a
descriptor digest to objects through the object's existing metadata digest; do
not materialize `(trustee, object)` for every ACE and file. Common inherited ACLs
therefore approach `O(D + T + N)`, not `O(total ACE appearances)`.

For a planning example with 100 million objects, 120 bytes of encoded path keys
per object, one million unique 512-byte descriptors, four trustee edges per
descriptor, and EAs/streams on 5% of objects, SMB-specific logical VaulticDB
growth is expected to be roughly 45-90 GB before large payload blobs. This is an
estimate, not a capacity promise. SlateDB block/index/filter overhead and
temporary compaction commonly require 1.5-3 times logical live bytes of backend
capacity; cumulative compaction traffic may write 5-15 times ingested metadata,
depending on level sizing and churn. Named-stream contents count in repository
capacity `B`, placement replication, and transfer cost through ordinary restic
`content` edges, not as hot SlateDB values.

S0 records corpus distributions for path length, descriptor uniqueness and size,
ACE count, SID cardinality, EA/stream incidence, and hard-link density. S6 must
publish measured bytes per object by record family, compression ratio, bloom/
index overhead, ingest and compaction write amplification, peak compaction space,
rebuild bytes/time, and query latency at 1M, 10M, and 100M synthetic objects plus
a representative enterprise corpus. Default enablement is blocked if median
SMB-specific logical growth exceeds 1 KiB per object excluding `B`, if a single
record can grow with unbounded ACL/path fanout, or if peak physical metadata
exceeds three times measured logical live bytes without an operator-facing
capacity estimator and documented mitigation.

## Key difficulties and safety rules

- **Library coverage:** many pure-Go clients cover file reads but not complete
  security, EA, stream, reparse, DFS, signing/encryption, and Kerberos behavior.
  The dependency proof is a release gate, not a paper selection.
- **Authorization versus completeness:** normal share read access may not grant
  SACL, backup-intent, reparse, stream, or offline-file access. Completeness is
  explicit and fail-closed by default.
- **Identity stability:** NTFS IDs exist, but SMB/server/DFS exposure and stability
  vary. Persistent FileId is inode-equivalent within a volume/epoch; capability
  and reconnect evidence prevent reuse across an invalid identity boundary.
- **Live consistency:** without an operator-selected server snapshot, directory,
  metadata, and stream reads can race. Re-stat/identity checks and normal
  archiver retries reduce races but cannot manufacture a point-in-time view.
- **Case and Unicode:** Windows comparison semantics vary by server and directory.
  Preserve originals and collision buckets; never normalize away distinct names.
- **ACL semantics:** raw descriptor preservation is tractable; effective-access
  computation requires directory identities and full Windows authorization rules.
- **Credential security:** keytabs and passwords are long-lived source authority.
  Never log/materialize them, and make capsule custody opt-in and auditable.
- **Latency and memory:** metadata-rich SMB backup can issue several requests per
  node. Use compounds only when semantically safe, bounded concurrency/credits,
  metadata batching/caches keyed by stable identity, and Phase 34 wait attribution.
- **Schema growth:** path keys and per-object indexes dominate when descriptors
  deduplicate well; streams dominate repository bytes when they do not. Enforce
  the measured budgets above before enabling SMB indexing by default.

## LLM-executable implementation plan

Execute one named substep per coding session. Before editing, inspect the owning
interface and neighboring tests, state one falsifiable hypothesis and the smallest
check, then run that check immediately after the edit. Do not combine protocol,
schema, credential, and performance changes. Every handoff records touched owners,
exact commands/results, fixtures/artifacts, preserved invariants, known server
limitations, and the next prerequisite. A failed gate blocks the next step.

### S0. Freeze protocol, fixture, and metadata contracts

**Owners:** `internal/fs`, `internal/data`, archiver fixtures, schema fixtures.
Capture packet-independent golden fixtures from Windows Server and Samba for
files, directories, hard links, case-only names, descriptors including SACL,
EAs, streams, reparse points, sparse/compressed/offline flags, and denied metadata.
Define exact restic node encodings, bounded extension limits, completeness
semantics, source/object identity, and expected archive nodes. **Gate:** fixtures
round-trip without SMB code; PR #4708 descriptors and existing creation-time,
file-attribute, and EA fields match restic golden trees byte-for-byte; unknown
fields survive or fail by version rule. No credential values enter fixtures.

### S1. Prove and pin the pure-Go SMB stack

**Prerequisite:** S0. Implement a disposable `internal/smb/probe` against candidate
modules. Test dialect negotiation, signing/encryption, Kerberos-only SPNEGO with
password AS exchange, imported in-memory ccache, and keytab AS exchange;
query/set-info classes, descriptor/SACL access, EAs, streams, reparse data,
stable FileIds across reconnect, DFS behavior, cancellation, and malformed/
oversized replies on both fixture servers. Attempt forced NTLM negotiation and
prove the client sends no NTLM token. **Gate:** publish module revision/license/
maintenance assessment and a capability matrix. Pin only after all required
operations work or document the smallest owned fork. Delete the probe or convert
it into conformance tests; do not start the adapter on an unproven high-level API.

### S2. Implement bounded SMB transport and authentication

**Prerequisite:** S1. Add `internal/smb` session/share/handle interfaces and one
Kerberos acquisition mode at a time: password AS exchange, in-memory ccache
import, keytab AS exchange, then sealed password/keytab lease. Add signing/
encryption/minimum-dialect policy, credit/request/byte budgets, reconnect
classification, DFS target validation, memory locking/zeroization, and secret-
safe errors. **Gate:** protocol fakes plus opt-in Samba/Windows tests cover
success, expiry, renewal, clock skew, SPN mismatch, signing/encryption refusal,
reconnect, cancellation, no NTLM token or fallback, no ccache file creation, and
zeroization at shutdown. Race and leak tests pass.

### S3. Implement the `fs.FS` adapter and data stream

**Prerequisite:** S2. Add path normalization, paginated listing, metadata-only
open, safe readable reopen, stat, identity verification, and unnamed-stream reads
in small patches. Wire an explicit `--smb-source` fixture through scanner and
archiver only after adapter contract tests pass. **Gate:** the same source tree
through local Windows and SMB adapters produces equivalent names, types, bytes,
size/times/attributes, ordering, errors, and cancellation where semantics match.
No kernel mount or external command is invoked.

### S4. Preserve complete Windows metadata

**Prerequisite:** S3. Add descriptor owner/group/DACL first, SACL and completeness
second, then EAs, named streams, and reparse data as separately tested changes.
Store stream bytes through normal blob/pack paths and enforce limits before
allocation. **Gate:** golden fixtures and live conformance prove byte-preserving
descriptor, EA, stream, and reparse capture; required mode fails on every missing
class, best-effort records exact omissions, and Windows decoding matches expected
restore metadata. Supported unmodified restic versions check and restore ordinary
content; mutation commands either preserve extension reachability or are marked
unsupported. Sparse/offline/EFS behavior is explicit.

### S5. Add identity and path behavior

**Prerequisite:** S3/S4. Implement source/volume identity and persistent FileId
capture, then hard-link coalescing and parent-to-current move detection using the
same model as local device/inode identity. Add case/Unicode comparison keys with
collision buckets. **Gate:** equal volume-scoped FileIds group hard links and
preserve identity across rename; server GUID/share/DFS/volume or identity-epoch
changes prevent stale reuse; identifier reuse is detected; case-only rename and
collisions preserve originals. Compare selective reuse with a full crawl oracle.

### S6. Extend VaulticDB schema and SID analytics

**Prerequisite:** S0/S5. Add schema keys/codecs and migrations in owner-sized
patches: source/coverage, objects/path index, descriptor/summaries, SID records,
then derived ownership/trustee analytics. Use golden Go/Rust/protobuf fixtures
where a cross-language contract exists. **Gate:** serializable publication,
idempotent replay, collision checks, bounded values, outbox/watermark behavior,
rebuild parity, index-check corruption fixtures, and old-reader compatibility
pass. Direct ownership/trustee queries match a brute-force descriptor scan; no
query claims effective access. Publish the schema-growth measurements and enforce
the per-object, fanout, capacity, and write-amplification gates above.

### S7. Add sealed source and target credentials

**Prerequisite:** S2 and Phase 24 owners. Extend Go/Rust topology schemas,
canonical fixtures, broker capability/policy, enrollment/rotation/removal, and
consumer leases for `backup_sources`/`restore_targets` and `source-read`/
`target-write`. Add password first and keytab second; generated Kerberos tickets
and caches remain memory-only. **Gate:** capsule cross-language digests, reference
validation, generation publication, least-privilege lease binding, expiry/lock/
revocation, clean-environment backup and restore, external-secret precedence,
and byte scans proving no secret or ticket reaches logs, status, profiles,
runtime files, crash dumps, or repository objects.

### S8. Integrate backup and restore workflows

**Prerequisite:** S3-S7. Add flags/profile schema, source selection, parent/source
identity matching, metadata coverage in snapshots, Phase 34 operation/wait states,
and normal pack/revision/snapshot durability ordering. Add the SMB restore target,
preflight, ordered data/metadata application, FileId hard-link creation, journal,
resume, verification, and cleanup. **Gate:** CLI golden tests, profile redaction,
timeout/retry/idempotency, no snapshot referencing unavailable pack/metadata,
no restore path escape, safe overwrite behavior, bounded shutdown, and stable
JSON. SMB latency injection separately covers listing, metadata, ACL/EA, stream
and content reads/writes.

### S9. Run interoperability, scale, and recovery acceptance

**Prerequisite:** S0-S8. Run at least Windows Server and Samba with password,
ccache, keytab, signing, and encryption combinations; live and explicit snapshot
roots; FileId identity epochs; cold/warm metadata caches; reconnect, credential
expiry/rotation, DFS failover, and server restart. Include millions of files,
descriptor deduplication, many SIDs/ACEs, streams, deep trees, case collisions,
high latency/jitter, and bounded fault injection. Restore each corpus to a fresh
share and verify content, metadata, and hard links by FileId. Run supported restic
list/check/mount/restore and permitted mutation commands. **Gate:** complete
required-mode round trip, byte-identical content, bounded memory/requests, correct
move/hard-link claims, exact SID queries, measured schema budgets, successful
index check, and reproducible artifacts. Missing privileged SACL, restore,
restic, NTFS, or Kerberos coverage is an explicit release blocker.

## Tests

Unit and fuzz tests cover URL/UNC parsing, component validation, UTF-16 decoding,
case collisions, response length arithmetic, directory pagination, security
descriptor/ACE parsing, SID canonicalization, EA/stream summaries and bounded
reparse extensions, file
identity reuse, DFS target changes, retry classification, and malformed SMB data.
Property tests compare path ordering and SID/descriptor indexes with independent
brute-force models.

Adapter contract tests run scanner and archiver fixtures through local and SMB
`fs.FS` implementations. Integration tests use isolated Samba plus an opt-in
Windows Server suite; Samba alone cannot certify NTFS, SACL, reparse, ADS, or
Windows Kerberos behavior. Network tests inject latency, jitter, disconnects,
short reads, duplicate replies, credential expiry, and cancellation while proving
bounded credits, requests, bytes, goroutines, and cleanup.

Metadata and restore tests cover owner/group/DACL/SACL completeness, inheritance/control bits,
unknown ACE preservation, EAs, named streams, sparse/compressed/offline/EFS
outcomes, hard links, case-only renames, stable FileIds across reconnect, and
server/volume epoch changes. Restore fault tests interrupt every journal stage and
prove safe resume/cleanup, target-boundary enforcement, metadata ordering, and
post-restore equivalence. Crash/retry tests prove idempotent VaulticDB publication
and no externally successful snapshot with missing packs or metadata. Analytics
tests compare direct owner/group/trustee SID predicates and residency against raw
records and verify derived-index rebuild/catch-up.

Credential tests cover protected external files, stdin descriptor handling,
password-to-KDC AS exchange, memory-only ccache import/expiry, keytab ticket
acquisition/renewal, SPN/realm/DNS policy, forced NTLM rejection, sealed password/
keytab generation and rotation, broker lock/lease expiry, and secret/ticket scans
of logs, status, events, profiles, crash errors, process arguments, runtime
directories, repository objects, and filesystem writes.

## Exit criterion

On Linux, macOS, and Windows, Vaultic can back up and restore a Windows Server SMB
share without a kernel mount using a pinned pure-Go client. Required mode round-
trips owner, group, DACL, SACL, attributes/timestamps, EAs, named streams, reparse
data, ordinary content, and hard links, with explicit failure rather than silent
omission. Persistent volume-scoped FileId provides inode-equivalent hard-link and
move detection across validated identity epochs.

Pack/index/snapshot/tree encoding remains readable by supported unmodified restic
versions, which can restore ordinary content while ignoring richer metadata;
repository-mutating compatibility is claimed only where metadata reachability is
proven. Normal pack/metadata/snapshot durability remains intact.
VaulticDB provides bounded, validated source/path/object/descriptor/EA/stream and
SID ownership/trustee lookups, while effective-access claims remain out of scope.
Password, imported ccache, and keytab modes all produce Kerberos CIFS tickets in
memory; NTLM is never offered, and no ccache, ticket, or session key is persisted.
Opt-in sealed password/keytab custody uses broker leases and capsule generations.
Measured schema growth meets the per-object and physical-capacity gates.
Interoperability and scale gates pass on both Samba and representative Windows
Server infrastructure, with missing SACL/NTFS/Kerberos coverage blocking release.
