# Phase 23: macOS FSEvents change detection and APFS snapshot backup source

[← Back to roadmap index](00-overview.md)

[← Phase 22](phase-22-operational-resilience-with-relinquishable-metadata-writers-and-deferred-crawl-commit.md) · [Phase 24 →](phase-24-sealed-topology-and-credentials-in-the-recovery-capsule.md)

[Phase 21 selective crawl contract](phase-21-crawl-optimization-with-cwalk-and-pathdiff.md) · [macOS takeover runbook](../../055_takeover_rustic_google_drive_macos.rst)

**Status: implemented.**

**Goal:** avoid repeated full inode crawls of large macOS home directories and volumes on modern macOS (APFS, macOS 12 or later on both Apple silicon and Intel). Reuse the Phase 21 selective-crawl machinery with a macOS-native change source: the per-volume FSEvents journal answers "which directories changed since the parent snapshot's recorded FSEvents position", and a temporary read-only APFS local snapshot of the source volume provides a frozen, self-consistent view that the backup reads instead of the live filesystem. The two mechanisms are independently optional and independently fall back. Any doubt about FSEvents completeness — including `kFSEventStreamEventFlagKernelDropped`, `kFSEventStreamEventFlagUserDropped`, `kFSEventStreamEventFlagEventIdsWrapped`, an unknown or unverifiable parent anchor, a changed volume UUID, or a purged journal — selects a full `cwalk` crawl. Selective mode is an optimization, never a correctness assumption.

## Why a macOS-specific phase

Phase 21 assumes an external change service (`pathdiff`) observing NetApp LIF/SVM/volume topology. A personal or workstation Mac has no such service, but macOS already maintains a persistent per-volume change journal:

- **FSEvents** (`/.fseventsd` on each APFS volume, `FSEventStreamCreate` with `kFSEventStreamCreateFlagFileEvents`) records directory- or file-level change notices with monotonically increasing 64-bit event IDs (`FSEventStreamEventId`) that survive process restarts and reboots. `FSEventsGetCurrentEventId()` yields the current position; `FSEventsCopyUUIDForDevice(dev_t)` yields the journal UUID that changes whenever the journal is reset. `FSEventStreamCreate` accepts a `sinceWhen` event ID for historical replay and delivers `kFSEventStreamEventFlagHistoryDone` when replay catches up to live.
- **APFS local snapshots** (`tmutil localsnapshot`, `tmutil listlocalsnapshots`, `tmutil deletelocalsnapshots`, and read-only mounting via `mount_apfs -s <snapshot> <volume> <mountpoint>`) provide a crash-consistent, immutable point-in-time view of the system Data volume with no copy cost. The installed macOS 12+ `tmutil localsnapshot` interface accepts no volume argument, so arbitrary external APFS volumes fall back to live reads or fail under `--apfs-snapshot-require`.

Together they let Vaultic do what Time Machine does for itself: freeze the source, ask the journal what moved since the last run, and read only that.

### What FSEvents does and does not guarantee

The design treats the following as hard facts, not tunables:

| Property | Consequence for Vaultic |
|---|---|
| Event IDs are per **device** (`dev_t`), not per path. | A backup source spanning multiple volumes (for example, `/Users/oli` plus a mounted external drive under it) needs one anchor per volume; a missing anchor for any volume forces a full crawl of the subtree on that volume. |
| Event IDs are 64-bit and monotonically increasing, but `kFSEventStreamEventFlagEventIdsWrapped` indicates a wrap. | Wrap invalidates ordering; treat as unknown anchor. |
| The journal UUID (`FSEventsCopyUUIDForDevice`) changes when `/.fseventsd` is deleted, the volume is reformatted, or the journal is rebuilt. | A UUID mismatch against the parent snapshot's recorded UUID means the anchor is meaningless; full crawl. |
| `kFSEventStreamEventFlagKernelDropped` and `kFSEventStreamEventFlagUserDropped` signal that events were lost in the kernel or user-space queue. | Loss is unbounded and unlocated; full crawl. The flag is reported once per stream; the design records it and never attempts to "resume after" it within the same window. |
| `kFSEventStreamEventFlagMustScanSubDirs` means changes under the reported path were coalesced. | Recursive crawl of that path is required; it is a partial-scan hint, not a loss. |
| `kFSEventStreamEventFlagRootChanged`, `kFSEventStreamEventFlagMount`, `kFSEventStreamEventFlagUnmount` change the path namespace under the watched root. | Full crawl. Volume identity or topology under the source changed. |
| Event delivery is coalesced by latency and directory; without `kFSEventStreamCreateFlagFileEvents` only directories are reported. | Vaultic requests file events, but still treats every event as "rescan this directory and its immediate children", the same granularity Phase 21 uses. Coalescing does not lose changes; it widens the rescan set. |
| Historical replay from `sinceWhen` is served from the on-disk journal, which macOS prunes by size and age. | Vaultic requires `HistoryDone` and fails closed on the stream's dropped/wrapped flags. It does not require contiguous event IDs: IDs are volume-wide and gaps are normal when a stream watches only a subtree. |
| Some filesystems do not journal: network mounts, FAT/exFAT, many FUSE volumes, and volumes with `noowners`-style options may deliver nothing or `kFSEventStreamEventFlagNone`. | `FSEventsCopyUUIDForDevice` returns null or the volume format is not APFS/HFS+; that subtree is full-crawled. |
| FSEvents does not report changes made while the volume was mounted elsewhere or by firmlink-crossed paths (`/System/Volumes/Data`). | Firmlinks are resolved to the real device before anchoring; a change in the resolved device set forces full crawl. |
| FSEvents does not attest content: it says a directory changed, not what. | Vaultic still `lstat`s every entry in every reported directory and compares against the parent tree, exactly as Phase 21 does. Metadata-only changes (mode, xattrs, flags) are reported by FSEvents as directory events on modern macOS but Vaultic does not rely on that: every rescanned directory entry is compared in full. |

Because FSEvents only narrows the rescan set, a false negative (a change FSEvents failed to report without raising a drop flag) would be silently missed, exactly as with Phase 21's `pathdiff`. The design therefore requires an independent guard: a periodic mandatory full crawl (`--fsevents-full-crawl-every DURATION`, default 7 days) and a per-snapshot "selective" marker so `vaultic check --read-data` and operators can distinguish observed from inferred trees. The exit criterion depends on the fallback discipline, not on trusting FSEvents.

## Design

### Anchoring FSEvents position in snapshots

A snapshot created on macOS records, per contributing device, an **FSEvents anchor**:

```json
{
  "fsevents_anchors": [
    {
      "device": 16777232,
      "volume_uuid": "3F2A…",
      "journal_uuid": "9C41…",
      "event_id": 8841339021,
      "captured_at": "2026-09-08T09:40:12Z",
      "source_roots": ["/Users/oli"],
      "apfs_snapshot": "com.vaultic.2026-09-08-094012"
    }
  ]
}
```

- `device` is the `st_dev` of the source root after firmlink resolution; `volume_uuid` is from `diskutil info -plist` / `IOKit` and identifies the APFS volume independent of `dev_t` numbering, which is not stable across boots.
- `journal_uuid` is `FSEventsCopyUUIDForDevice(device)` at capture time.
- `event_id` is `FSEventsGetCurrentEventId()` taken immediately before `tmutil localsnapshot`. It is the conservative lower bound stored for the next backup. The current backup separately captures its replay upper bound only after the APFS snapshot is mounted, so changes between anchor capture and snapshot creation are included in both the frozen source and the selective plan.
- `apfs_snapshot` is the local snapshot name if one was used; it is informational (the snapshot is deleted after the job) and helps forensics.

Anchors are stored in the snapshot's extended metadata (the same mechanism used for crawl claims and Phase 21 selective markers) and in VaulticDB alongside the snapshot record. They contain no content and no secrets.

### Selective plan construction

`internal/crawl` gains a second `ChangeService` implementation, `FSEventsService`, alongside `PathdiffService`, satisfying the same `Changes(ctx, ChangeQuery) (ChangeWindow, error)` contract and feeding the existing `BuildPathdiffPlan`-equivalent planner (renamed to a source-neutral `BuildSelectivePlan`). Per source root:

1. Resolve the root to its real path (firmlinks, symlinks in ancestors) and `st_dev`; look up the APFS volume UUID.
2. Locate the parent snapshot's anchor for that `volume_uuid`. **No anchor** → `ChangeWindow{Continuous: false, Reason: "no FSEvents anchor for volume … in parent snapshot"}` → full crawl of that root.
3. Read the current `journal_uuid`. **Mismatch** → reason `"FSEvents journal was reset since parent (uuid changed)"` → full crawl.
4. Freeze and mount the current APFS source, then read the current event ID as the replay upper bound. If it is **less than** the parent anchor → reason `"FSEvents event ID moved backwards"` → full crawl. A root that cannot be frozen is full-crawled from the live source and is never eligible for subtree reuse.
5. Create a one-shot `FSEventStream` on the root paths with `sinceWhen = anchor.event_id`, flags `kFSEventStreamCreateFlagFileEvents | kFSEventStreamCreateFlagNoDefer | kFSEventStreamCreateFlagWatchRoot`, and a private dispatch queue. Run until `kFSEventStreamEventFlagHistoryDone` or a 60-second cap (`--fsevents-replay-timeout`).
6. During replay, collect reported paths. On **any** of `KernelDropped`, `UserDropped`, `EventIdsWrapped`, `RootChanged`, `Mount`, `Unmount` → stop, reason names the flag, full crawl. On `MustScanSubDirs` → mark the path recursive. If `HistoryDone` never arrives → reason `"FSEvents replay did not complete"` → full crawl.
7. Build the selective root plan only after `HistoryDone`. Event IDs are range-checked against the anchor and captured current position, but are not required to be contiguous because unrelated paths consume IDs on the same volume.

The planner then does what Phase 21 does: collapse changed paths, mark unchanged sibling subtrees for parent-tree reuse, and always re-list every changed directory's ancestors so renames, creations, and deletions rebuild the containing directory deterministically. Paths reported by FSEvents that are outside the source roots or excluded by `--exclude` are dropped before planning.

If the source spans several volumes, each volume is planned independently; a full-crawl decision for one volume does not force a full crawl of the others.

### APFS snapshot as the read source

When `--apfs-snapshot` is enabled (default on macOS when the source is on an APFS volume and the invoking process is root or holds the Full Disk Access + `com.apple.developer.security.privileged-file-operations`-equivalent authorization needed by `tmutil`), the job:

1. Captures `event_id` (see above).
2. Runs the no-argument `tmutil localsnapshot` for a source on the system Data volume and parses the returned snapshot name (`com.apple.TimeMachine.<timestamp>.local`). Because `tmutil` names snapshots itself and Vaultic cannot choose the name, Vaultic records the name and the volume UUID, then verifies the snapshot exists with `tmutil listlocalsnapshots <mountpoint>` and that its timestamp is not earlier than the capture time. Other APFS volumes use the documented fallback.
3. Mounts it read-only at a private, mode-0700, job-scoped directory under `$TMPDIR/vaultic-apfs-<jobid>/` with `mount_apfs -o rdonly,nobrowse -s <snapshot-name> <volume-device> <mountpoint>`. `noowners` is deliberately **not** passed; ownership must be preserved so UID/GID metadata is backed up correctly. `nobrowse` keeps Finder and Spotlight from indexing the mount.
4. Rewrites the source roots to their snapshot-relative equivalents for the crawl and archiver **only**; snapshot node paths recorded in the repository remain the original live paths so restores and history are unaffected. The FS abstraction passes the snapshot root as a path-prefix remapping, not by changing the recorded tree.
5. Runs the (selective or full) crawl against the mounted snapshot.
6. On completion, success or failure, unmounts (`umount`, then `diskutil unmount force`) and deletes the snapshot (`tmutil deletelocalsnapshots <timestamp>`). A mode-0700 lease registry under `$TMPDIR/vaultic-apfs-leases` records the creating PID, exact Time Machine name, and private mount. Startup recovery ignores live PIDs and malformed or non-Vaultic mount paths; it only unmounts and deletes strictly validated dead-process leases.

Failure of any step (not APFS, `tmutil` unavailable or unauthorized, snapshot not listed, mount failed, snapshot timestamp older than the anchor capture) degrades to reading the **live** filesystem with a warning event and a `apfs_snapshot: null` anchor. That root is full-crawled; FSEvents subtree reuse is not permitted against a moving live source. With `--apfs-snapshot-require`, the same conditions are fatal before any repository write. With `--fsevents-require-coverage`, inability to freeze any contributing root is also fatal.

Using the snapshot removes the classic race between FSEvents and the crawl. The stored anchor precedes snapshot creation, while the current replay upper bound follows it. Changes in between are conservatively rescanned in this job; changes after the snapshot are absent from this job's immutable reads and remain after its stored anchor for the next replay.

### Interaction with Phase 21 and Phase 22

- `--use-fsevents` and `--use-pathdiff` are mutually exclusive per invocation; both require `cwalk` (`--no-cwalk` disables both with an explicit error if combined).
- A selective FSEvents run produces the same non-authoritative crawl claim as a selective `pathdiff` run: reused subtrees were not independently observed, and `vaultic check` reports them as such.
- Deferred ingest (Phase 22 `--allow-deferred-commit`) is compatible with APFS snapshots (the frozen source is exactly what a deferred journal wants) and with FSEvents selectivity, since anchors are part of the staged snapshot metadata and are committed with it.
- `--parent` overrides continue to work; the anchor is read from whichever parent is selected. `--force` disables selectivity.

### CLI surface

| Flag | Default | Effect |
|---|---|---|
| `--use-fsevents` | `false` initially; may become default-on for macOS after one release of opt-in telemetry | Enable FSEvents-driven selective crawl. |
| `--fsevents-require-coverage` | `false` | Fail before any repository write instead of falling back to a full crawl when any volume's window is not `Continuous`. |
| `--fsevents-replay-timeout` | `60s` | Cap on historical replay per volume. |
| `--fsevents-full-crawl-every` | `168h` | Force a full crawl if the last authoritative (non-selective) snapshot in the group is older than this. `0` disables the guard and is reported as a compliance finding. |
| `--apfs-snapshot` | `true` on macOS/APFS when authorized | Read from a temporary local snapshot. |
| `--apfs-snapshot-require` | `false` | Fail rather than read the live filesystem when a snapshot cannot be created or mounted. |
| `--apfs-snapshot-keep` | `false` | Do not delete the snapshot after the job (debugging; emits a warning and records the name in the event log). |

All flags are settable from `[backup]` in profiles so the macOS LaunchAgent wrapper in the takeover runbook does not need to change.

### Observability

Events (Phase 17 syslog/OTel categories) for: anchor captured, replay started/completed with event count, each fallback reason with the volume UUID, drop flags seen, replay timeout, snapshot created/mounted/unmounted/deleted, snapshot fallback to live read, stale snapshot swept. `vaultic backup --json` includes a `crawl_plan` object per source root with `mode: "selective"|"full"`, `reason`, `changed_paths`, `reused_subtrees`, and `apfs_snapshot`. `vaultic snapshots --json` exposes `fsevents_anchors`. A compliance finding is raised when the periodic full-crawl guard is disabled or when the last authoritative snapshot is older than twice the guard interval.

## Implementation steps

1. Add a small cgo/CoreServices binding (`internal/fsevents`, darwin-only build tag) exposing: current event ID, journal UUID for a device, and a one-shot historical replay that returns events plus a bitmask of the terminal flags seen (`KernelDropped`, `UserDropped`, `EventIdsWrapped`, `HistoryDone`, `RootChanged`, `Mount`, `Unmount`). Non-darwin builds compile a stub that returns `ErrUnsupported`. No long-lived daemon is introduced; the journal is the persistent state.
2. Add `internal/apfs` (darwin-only) wrapping `tmutil localsnapshot|listlocalsnapshots|deletelocalsnapshots`, `mount_apfs -s`, and unmount, with structured errors distinguishing "not APFS", "not authorized", "not available", and "snapshot name not found". Include the startup stale-snapshot sweep bound to Vaultic-recorded names only.
3. Extend the snapshot extended-metadata schema and VaulticDB snapshot record with `fsevents_anchors`; extend the legacy JSON projection so a snapshot with anchors remains readable by tools that ignore unknown fields.
4. Implement `crawl.FSEventsService` and generalize the Phase 21 planner into `BuildSelectivePlan` with a source-neutral `ChangeService`; keep `BuildPathdiffPlan` as a thin wrapper so existing tests and behavior are unchanged.
5. Wire anchor capture, optional APFS snapshot mount and path remapping, plan construction, and cleanup into the archiver's backup pipeline; record anchors on the produced snapshot; propagate the non-authoritative crawl claim for selective runs.
6. Add the CLI flags and profile keys, the mutual-exclusion and `--no-cwalk` validation, the periodic full-crawl guard, and `--json` plan reporting.
7. Update `doc/040_backup.rst` (macOS section), the macOS takeover runbook's recurring-backup profile (enable `use-fsevents` and `apfs-snapshot` with the LaunchAgent), and `doc/077_troubleshooting.rst` (how to read fallback reasons, how to grant `tmutil` authorization to the LaunchAgent context, how to clean up a leaked snapshot).

## Tests

Unit tests with a fake `FSEvents` binding: each terminal flag (`KernelDropped`, `UserDropped`, `EventIdsWrapped`, `RootChanged`, `Mount`, `Unmount`) yields a non-continuous window naming the flag; missing `HistoryDone` and replay timeout fall back; journal UUID mismatch, missing anchor, backwards event ID, and retention-truncated replay fall back with distinct reasons; `MustScanSubDirs` produces a recursive path, not a fallback; multi-volume sources plan independently; events outside roots or under excludes are dropped. Planner equivalence: a selective plan applied to a fixture tree produces a byte-identical snapshot tree to a full crawl for creations, deletions, renames across directories, metadata-only changes, and hardlink changes when FSEvents reports the containing directories. APFS tests (darwin CI runner, skipped elsewhere): snapshot create/mount/read/unmount/delete round trip; ownership preserved through the mount; live-read fallback on each simulated `tmutil` failure; `--apfs-snapshot-require` fatal before repository writes; stale-snapshot sweep deletes only Vaultic-named snapshots; crash between mount and cleanup is recovered by the next run. Integration: back up a fixture home directory, mutate a sparse subset, run selective, verify `crawl_plan.mode == "selective"`, `reused_subtrees > 0`, and identical `vaultic diff` output versus a forced full crawl; then purge `/.fseventsd`-equivalent in the fixture and verify the next run reports `journal was reset` and runs full. Guard test: with the last authoritative snapshot older than `--fsevents-full-crawl-every`, the run is full even when replay is clean. Race test: files changed between anchor capture and snapshot creation appear in the next run's window. CLI contract tests for flag validation, mutual exclusion with `--use-pathdiff`, and profile settability. Secret and privacy hygiene: no path contents in events beyond what `--json` already exposes; snapshot mount points are mode 0700.

## Exit criterion

On macOS with an APFS source volume and a clean FSEvents journal, a repeated backup of a large home directory with sparse changes completes with a selective plan that re-lists only reported directories and their ancestors, reuses all other subtrees from the parent, and produces a snapshot tree identical to a full crawl. The job reads from a temporary read-only APFS local snapshot that is created before the crawl and removed after it, with no snapshots or mounts leaked across crashes. Every FSEvents integrity signal — `kFSEventStreamEventFlagKernelDropped`, `kFSEventStreamEventFlagUserDropped`, `kFSEventStreamEventFlagEventIdsWrapped`, root/mount changes, journal UUID change, missing or unverifiable parent anchor, backwards or retention-truncated replay, or replay timeout — results in an explained full `cwalk` crawl of the affected volume, or a fatal pre-write error under `--fsevents-require-coverage`. A periodic authoritative full crawl bounds the exposure to undetected FSEvents false negatives, selective snapshots are marked non-authoritative for `check`, and no Phase 21 `pathdiff` behavior or test changes.
