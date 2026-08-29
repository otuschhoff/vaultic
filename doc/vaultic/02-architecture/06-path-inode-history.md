# Path and Inode History

[← Back to architecture index](00-overview.md)

## 13. Path and inode history queries

### 13.1 Objective

The schema already retains immutable inode and directory revisions, so a
repository physically contains the answer to "how did this file change over
time". None of that is queryable today. An operator asking the obvious support
question — *when did `/home/x/report.odt` change, and which backup captured each
version* — has no command to run, and no index that makes the question cheap.

Three capabilities are missing, and they are not the same problem:

1. **Path resolution.** Nothing maps a path to an inode. Paths are only
   discoverable by walking directory revisions from a snapshot root, one lookup
   per component.
2. **Snapshot attribution.** `s:` maps a snapshot to a commit sequence, but
   there is no reverse index, so a revision cannot be attributed to the snapshot
   that captured it without scanning every snapshot record.
3. **Path identity over time.** Inode history follows the file object through
   renames but loses the path; path history must additionally capture unlink and
   recreation, where the same path becomes a different inode.

The objective is to answer path and inode history questions from the CLI, in
time proportional to the number of changes reported rather than to the number of
snapshots retained or the size of the repository, without weakening the rule
that snapshot membership is resolved through immutable directory revisions.

Non-goals: changing restore, making history a required input to any destructive
decision, exposing history for legacy JSON repositories beyond what the JSON
index already contains, or replacing `diff` for whole-snapshot comparison.

### 13.2 Methodology

#### Three questions, three costs

The commands must not conflate these, because they give different answers
whenever a path is recreated or a file is renamed:

| question | mechanism | cost |
|---|---|---|
| revisions of an inode | prefix scan `iv:<fsid>:<inode>:` | proportional to revisions |
| revisions at a path | resolve path per snapshot, or scan `pv:` | see below |
| which snapshot captured a revision | commit-order lookup via `sc:` | proportional to results |

Only the first is answerable today, and it is already cheap: `revision-seq` is
fixed-width big-endian, so a single bounded prefix scan returns every revision of
an inode in chronological order.

#### Resolution by walking, without new keys

Path history is answerable from the existing schema. For each snapshot, read its
root directory revision from `s:`, then resolve the path one component at a time
through the child entries of each `dv:` record, following the versioned child
reference. Each snapshot yields either absent or a resolved
`(inode, metadata revision-seq)` pair. Coalescing consecutive identical pairs
across snapshots in commit order produces the change list:

| transition | meaning |
|---|---|
| revision-seq changes, inode unchanged | modified in place |
| inode changes | path rebound to a different file object |
| present to absent | deleted, **or** outside this snapshot's backup set |
| absent to present | created, **or** newly included in the backup set |

The last two rows are ambiguous by nature and must be disambiguated against the
snapshot's `paths` scope before being reported; a path that was never in scope
must never be presented as a deletion.

Because a directory revision is immutable, two snapshots that share a directory
revision along the path share the entire remainder of the walk. Memoizing on the
child revision key collapses most of the work: the root revision differs whenever
anything anywhere in the tree changed, but sharing appears within a level or two
below it, so the practical cost is a small number of lookups per snapshot rather
than full path depth. Lookups across snapshots are independent and must be issued
through `MultiGet` rather than serially.

This approach is exactly correct, requires no new namespace, and is the reference
implementation against which any index is validated. Its floor is
**O(retained snapshots)**, because every snapshot root must be consulted before
it can be excluded. That is acceptable for hundreds of snapshots and not for tens
of thousands.

#### Commit-ordered snapshot index

`sc:` (see [SlateDB Schema](02-schema.md)) removes the scan over `s:` and gives the walk its iteration
order. It is small, bounded by snapshot count, and is worth adding regardless of
whether the path index is adopted, since it also serves any other query that
needs snapshots in commit order.

#### Versioned path index

`pv:` (see [SlateDB Schema](02-schema.md)) makes path history a single prefix scan, bounded by the number
of times that path's binding changed. It is the only structure that answers the
recreation case directly, and the only one whose cost is proportional to the
answer rather than to the repository's snapshot count.

It must remain derived and rebuildable. A path query resolves the candidate
revision range from `pv:`, then confirms membership per snapshot through the
directory walk. Skipping that confirmation would report a binding as present in a
snapshot that never contained the path.

#### Path-keyed, not hash-keyed

A hashed path key (`pv:<fsid>:<path-hash>:<revision-seq>`) is the obvious way to
keep keys fixed width and match the rest of the schema. It is the wrong trade
here, for a reason that is easy to miss: hashing destroys sort locality.

With a hash key, adjacent entries are unrelated paths, so keys gain no prefix
compression, path strings in values do not compress against their neighbours, and
the path must be stored in the value anyway because a hash cannot be reversed for
listing. Subtree queries become impossible. Collisions additionally force a wide
hash: at 1.4 billion paths, a 64-bit hash has roughly a 5% birthday collision
probability, so 128 bits would be the minimum.

Storing the path as the key costs variable-length keys but wins on every other
axis, including size once block prefix compression and zstd are accounted for.
Order-of-magnitude estimates for the 1.4 billion path baseline, assuming a
100-byte average path:

| encoding | raw bytes/entry | baseline |
|---|---|---|
| hash key, full path in value | ~149 | ~210 GB |
| hash key, parent inode plus name | ~73 | ~105 GB |
| hash key, no path stored | ~49 | ~70 GB |
| **path-keyed, length-delimited** | ~134 raw, **~40 effective** | **~56 GB** |

Incremental cost is driven by binding churn, not repository size. At daily
backups and the path-keyed encoding: roughly 20 GB/year at 0.1% daily churn,
100 GB/year at 0.5%, 200 GB/year at 1%.

Against an estimated 350 to 400 GB baseline for the rest of the schema at this
scale, the path-keyed encoding adds roughly 15%, where the naive hashed encoding
would add over 50%. These figures are estimates whose dominant inputs are average
path length and real churn rate; both must be measured on a representative
filesystem before the namespace is enabled by default, and the measurement is an
explicit exit criterion below.

#### Hardlinks

A hardlinked inode has several paths, and the schema already records this: `hr:`
holds the full set of parent and name edges for an inode revision, and
reconciliation deliberately does not invent a single parent. Path history and
inode history therefore diverge legitimately for hardlinks, and both are correct.

Every path of a hardlinked inode receives its own `pv:` binding. Inode history
for such an inode must report all known paths at each revision rather than
choosing one, and path history must not present a shared inode's changes as
exclusive to the queried path.

#### Time semantics

Three different times exist and must never be merged into one column. The
revision sequence is monotonic but explicitly not derived from wall-clock time,
so it orders changes without dating them. Backup time comes from the snapshot
record. Filesystem `mtime` and `ctime` come from the source and, for imported
records, may be unknown or unverified.

Output must therefore report backup time as the time a change was *captured*,
report filesystem times separately, and preserve the existing distinction between
unknown and zero-valued fields. An imported record must never be rendered as
though its metadata were verified.

#### Commands

Add to the existing `index` group. `index history` is already the pack history
command from [storage placement policy](05-storage-placement.md), so file history takes a distinct verb.

`vaultic index file-history <path>` — binding and revision history for a path.

- `--fsid` select the filesystem when the repository spans several
- `--since`, `--until` bound the commit range
- `--follow` continue across renames by tracking the inode when a binding ends
- `--inode` query by `<fsid>:<inode>` instead of a path
- `--snapshots` annotate each change with the snapshots that captured it
- `--content` report content-manifest identity changes, distinguishing metadata
  only changes from content changes
- `--verify` resolve every reported revision through the directory walk and
  report any disagreement with `pv:`
- `--json`

`vaultic index path-at <path> --snapshot <id>` — resolve one path within one
snapshot, reporting the resolved inode, revision, and the directory revision
chain used. This is the primitive the walk exposes, and it is the diagnostic used
when `--verify` reports a disagreement.

Both commands are read-only, use the daemon `DbReader` path, and must fail
explicitly on legacy repositories rather than answering from current state.

Implementation phases for this section are Phases 13 and 14 of the plan below.

