# Known Constraints

[← Back to roadmap index](00-overview.md)

## 19. Known constraints

- The official Go binding is CGO-based and generated from UniFFI, but it is
  isolated inside the Rust `vaulticdb` build rather than loaded by vaultic.
- The native binding API and generated files must be pinned together.
- Static linking must be verified per target platform and toolchain.
- SlateDB's object-store consistency and fencing behavior must be tested with
  the actual S3/MinIO/NetApp deployment model.
- Legacy JSON indexes cannot by themselves reconstruct all inode and directory
  metadata.
- Backend `FileInfo` does not carry a reliable creation or modification time,
  so pack creation time must be recorded by vaultic at publish time. Packs
  inherited from a legacy repository stay retention-unknown permanently, and no
  timestamp may be invented for them.
- Record compatibility is forward-only. A newer build reads records written by
  an older one, but an older build rejects records carrying fields it does not
  know, because a record must end exactly where its decoder expects. Rolling
  back to an older binary means resuming from the legacy JSON indexes, not
  reopening a SlateDB catalog a newer build has written.
- A repository predating a derived namespace has none of its records. That is a
  pending rebuild, not corruption: it must not fail `index check`, and a
  missing accelerator must never block a destructive operation from completing
  correctly.
- Cold cost-model inputs are operator-supplied estimates, not billing data. The
  model exists to make the decision explicit and auditable, not to predict a
  cloud invoice.
- Placement is asynchronous, so a backup completes before every copy is live.
  The resulting window is the offsite recovery-point objective; it is bounded by
  a stated deadline and measured, never assumed to be zero.
- Deferring archival commitment means a pack that becomes unreachable before its
  promotion trigger never reaches the archival backend. That is the intended
  saving, and it is safe only because the offsite deadline is satisfied
  independently by a non-archival offsite placement. The two requirements must
  never be collapsed back into one.
- Independence of copies is a property of failure domains, not of backend count.
  Two buckets in one account, or two devices in one building, are one domain and
  count once toward the durability predicate.
- Metadata is never placed in an archival class. Indexes, trees, and snapshots
  must stay directly readable, because planning any restore requires them.
- Pack history is advisory and derived. It must never gate or influence a
  destructive decision, and a history gap must never fail a data path.
- Path and inode history are likewise derived and advisory. `sc:` and `pv:` are
  accelerators that must be rebuildable from snapshot and directory records, and
  snapshot membership is always confirmed through immutable directory revisions
  rather than from a binding record alone.
- `pv:` is the only variable-length key in the schema. Its `0x00` terminator is
  safe only because POSIX path components cannot contain a NUL byte, and key
  parsing must match it by prefix rather than by exact length.
- Path history cost without `pv:` has a floor proportional to the number of
  retained snapshots, because every snapshot root must be consulted before it can
  be excluded. Memoization reduces the constant, not the floor.
- Hardlinked inodes legitimately have several paths. Path history and inode
  history diverge for them by design, and neither view may present a shared
  inode's changes as exclusive to one path.
- Partial import is therefore intentional: JSON import recovers all available
  blob-index facts, while the next backup crawl fills metadata blanks.
- The existing vaultic index model and lock behavior must remain the fallback
  until SlateDB authority has passed interop and recovery acceptance tests.
- `vaulticdb` is an operational dependency only for SlateDB-authoritative
  repositories. Legacy repositories continue to work without Rust, CGO,
  protobuf services, or a running daemon.
- A daemon must not be shared across repositories or object-store endpoints
  unless its capability identity proves that endpoint and schema match exactly.
