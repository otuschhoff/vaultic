# Foundation Workstreams

[← Back to rustic-parity index](00-overview.md)

## 6. Foundation workstreams

Workstreams are ordered by dependency. Each lists: goal, rustic reference
behavior, vaultic design, code touchpoints, format impact, testing.

### WS-A — In-repo configuration (`config`, `show-config`)

**Goal.** Persist operational settings in the repository config file so they
are set once at `init --set-*` or via a new `config` command, instead of
repeating flags (`--compression`, `--pack-size`, …) on every invocation.

**Design.**

1. Extend `Config` in [internal/vaultic/config.go](../../../internal/vaultic/config.go)
   with an additive, optional extension block:

   ```go
   type Config struct {
       Version           uint          `json:"version"`
       ID                string        `json:"id"`
       ChunkerPolynomial chunker.Pol   `json:"chunker_polynomial"`
       // vaultic extensions (unknown fields are ignored by restic/rustic)
       Compression       *int          `json:"compression,omitempty"`        // -7..22, nil = auto
       AppendOnly        bool          `json:"append_only,omitempty"`
       ExtraVerify       *bool         `json:"extra_verify,omitempty"`
       Chunker           *ChunkerConfig `json:"chunker,omitempty"`           // F3
       TreePack          PackConfig    `json:"treepack,omitempty"`          // F4
       DataPack          PackConfig    `json:"datapack,omitempty"`          // F4
   }

   type PackConfig struct {
       Size        uint64 `json:"size,omitempty"`         // target bytes
       GrowFactor  uint32 `json:"growfactor,omitempty"`   // dynamic sizing
       SizeLimit   uint64 `json:"size_limit,omitempty"`   // hard cap, up to 4 GiB
   }
   ```

   > ⚠️ Compatibility check before writing: confirm rustic ignores unknown
   > config fields (serde `deny_unknown_fields` must not be set in
   > rustic_core's config struct — verify against rustic 0.11.x). If rustic
   > rejects unknown fields, nest all extensions under a single
   > `vaultic`/`x-vaultic` object which rustic already tolerates for
   > forward-compat (to be verified by interop test, see [Strategy & Appendices](05-strategy-and-appendices.md)).

2. Pack-size application: thread `PackConfig` into the packer manager in
   [internal/repository/packer_manager.go](../../../internal/repository/packer_manager.go);
   today a single target size is used — split into tree vs data packers.
   Raise the internal pack-size limit from 128 MiB to 4 GiB
   (verify uploader and backend `Save` paths for >128 MiB blobs).
3. New `config` command (new file `cmd_config.go` in [cmd/vaultic](../../../cmd/vaultic)):
   `vaultic config [--set-compression N] [--set-append-only] ...`
   reads-modifies-writes the config file (requires lock; refuse on v1 repos
   for chunker changes).
4. `show-config` command: prints effective config (repo config merged with
   profile/CLI overrides once WS-F lands).
5. `init --set-*` flags map onto the same struct ([cmd/vaultic/cmd_init.go](../../../cmd/vaultic/cmd_init.go)).
6. Compression: keep existing `--compression` CLI as override; resolution
   order CLI > repo config > default. Centralize in
   [internal/global/global.go](../../../internal/global/global.go).
7. F5 append-only: enforce in `backend.Backend` wrapper
   (new `internal/backend/appendonly`) that rejects `Remove`/`Save`-overwrite
   when enabled; checked early in forget/prune with a clear error.
8. F7 master-key open: add `--key`, `--key-file`, `--key-command` global
   options that load a master key directly and unwrap the repository without a
   password-based key file. Touchpoint: key search in
   [internal/repository/key.go](../../../internal/repository/key.go) — add
   `OpenWithMasterKey` path.

**Format impact:** additive JSON only; v2 repos remain v2.
**Flags:** `in-repo-config` (Alpha → Beta).
**Tests:** config round-trip unit tests; interop: repo written by vaultic must
`rustic show-config` / `restic cat config` cleanly.
**Effort:** L. **Depends on:** none. **Blocks:** F4–F6, WS-D (per-repo warm-up
settings), WS-F (config layering).

### WS-B — Snapshot metadata extensions

**Goal.** Support rustic's extra snapshot fields: `label`, `description`,
and delete protection.

**Design.**

1. Extend `Snapshot` in [internal/data/snapshot.go](../../../internal/data/snapshot.go):

   ```go
   Label       string     `json:"label,omitempty"`
   Description string     `json:"description,omitempty"`
   DeleteNever bool       `json:"delete_never,omitempty"`
   DeleteAfter *time.Time `json:"delete_after,omitempty"`
   ```

   (Names must match rustic's JSON exactly for cross-tool visibility — verify
   against rustic_core's `SnapshotFile` serde field names before merge.)

2. `backup`: `--label`, `--description`, `--description-from file`,
   `--delete-never`, `--delete-after <time>` in
   [cmd/vaultic/cmd_backup.go](../../../cmd/vaultic/cmd_backup.go).
3. `forget`: refuse to delete snapshots with `delete_never` or
   `delete_after > now` unless a new `--override-delete-protection` is given
   ([cmd/vaultic/cmd_forget.go](../../../cmd/vaultic/cmd_forget.go)).
4. `snapshots`/`ls`/`find`: display label; `--group-by` gains `label`.
5. `tag` command: add `--set-description`, `--set-label` editing.

**Format impact:** additive snapshot JSON; restic/rustic ignore unknown fields
on read. **Flags:** not needed (additive metadata). **Effort:** S–M.

### WS-C — Snapshot filtering & ID-resolution framework

**Goal.** One shared implementation of rustic's filter set, usable by every
command that selects snapshots (snapshots, forget, restore, ls, find, diff,
dump, copy, tag, stats, check).

**Design.**

1. New package `internal/data/snapshotfilter` (or extend
   [internal/data/snapshot_find.go](../../../internal/data/snapshot_find.go)):
   a `Filter` struct evaluated against `*data.Snapshot`:

   ```go
   type Filter struct {
       Hosts, Labels []string
       Paths, Tags   []string        // match-any (current semantics)
       PathsExact, TagsExact []string // full-list equality
       Before, After *time.Time
       SizeMin, SizeMax         uint64 // from Summary.TotalBytesProcessed
       SizeAddedMin, SizeAddedMax uint64
       Last    int                   // n latest per group
       JQ      string                // jq expression on snapshot JSON (P2; embed a Go jq impl, e.g. itchyny/gojq)
   }
   ```

2. Wire flag registration as a reusable pflag set (like today's
   `initMultiSnapshotFilterFlags` in [cmd/vaultic/find.go](../../../cmd/vaultic/find.go))
   so all commands expose identical `--filter-*` flags while keeping the legacy
   `--host/--paths/--tags` as aliases.
3. ID resolution: extend the snapshot ID parser
   ([internal/data/snapshot_find.go](../../../internal/data/snapshot_find.go)) with
   `latest~N` and `<id>:<sub/path>` **including file-level paths**; make the
   parser shared by restore/diff/dump/ls/cat.
4. Replace per-command ad-hoc filtering (cmd_forget, cmd_snapshots, …) with
   the shared filter; keep CLI behavior identical (regression tests exist in
   `cmd_*_integration_test.go`).

**Format impact:** none. **Effort:** M. **Depends on:** WS-B (label).

### WS-D — Hot/cold repositories & warm-up protocol

**Goal.** Full rustic-style cold storage: split hot/cold repositories and a
user-defined warm-up program. Builds on the existing `Warmup` backend API
([internal/backend/backend.go](../../../internal/backend/backend.go)) and
`feature.S3Restore` work.

**Design.**

1. **Warm-up command runner** (new `internal/warmup` package):
   - Global options `--warm-up-command`, `--warm-up-wait`,
     `--warm-up-batch N`, `--warm-up-wait-command`
     ([internal/global/global.go](../../../internal/global/global.go)).
   - Variable expansion `%id`, `%path`, `%ids`, `%paths`; batch semantics:
     batch size N, `%ids/%paths` expand inline, `%id/%path` spawn N parallel
     invocations.
   - JSON-lines progress protocol: parse `{"type":"pack-progress","warm":n}`
     from the command's stdout, drive a progress bar; non-JSON lines logged at
     info level with `[warmup]` prefix. Full spec in Appendix A.
   - Integrate into the restorer path
     ([internal/restorer/filerestorer.go](../../../internal/restorer/filerestorer.go))
     and checker ([internal/repository/checker.go](../../../internal/repository/checker.go))
     as an alternative to backend-native `Warmup` when a warm-up command is
     configured.
2. **Hot/cold split repository** (`--repo-hot`):
   - New composite backend `internal/backend/hotcold` implementing
     `backend.Backend` over two child backends. Routing by file type:
     config/key/snapshot/index + tree packs → hot (and mirrored to cold at
     write time so the cold repo stays complete); data packs → cold only.
     Reads: try hot first for metadata; data packs from cold.
   - Repository open resolves both locations
     ([internal/global/secondary_repo.go](../../../internal/global/secondary_repo.go)
     shows the existing pattern for a second repo).
   - `init` gains `--hot-only` (F16): creates the hot side from an existing
     repo by copying metadata only.
   - `check` gains hot/cold cross-verification mode (F17).
   - `prune`: default keeps cold packs until the last blob is unused
     (no repack of cold packs); `--repack-cacheable-only` extended to opt in;
     `--keep-pack <duration>` honors provider minimum-holding periods.
3. Promote `feature.S3Restore` semantics into this workstream; graduate flag
   when warm-up command + hot/cold are stable.

**Format impact:** none on the repo format itself (the cold repo *is* a
standard repo; hot repo is metadata-only and not standalone-valid — same as
rustic).
**Effort:** XL overall (warm-up runner L, hot/cold XL). **Depends on:** WS-A
(persist warm-up settings), existing S3Restore plumbing.

### WS-E — Lock-free operations & two-phase prune

**Goal.** Rustic-grade concurrency: normal operations (backup, forget
metadata writes, copy) run without lock files; prune becomes two-phase so it
can run concurrently with backups.

**Design.**

1. **Lock-free mode** (`--no-lock` today is an unsafe opt-out; make it safe
   and the default behind a flag):
   - Index writes become purely additive: index save in
     [internal/repository/index](../../../internal/repository/index) already writes
     new index files; ensure no operation *requires* removing indexes except
     prune (`--early-delete-index` stays prune-only).
   - Snapshot upload is already atomic single-file `Save`.
   - Audit every writer for read-modify-write sequences on shared files:
     key add/passwd/remove (keep requiring locks — rustic still serializes key
     management), prune (see two-phase), `repair index` (requires exclusive).
   - Locking layer: [internal/repository/lock.go](../../../internal/repository/lock.go)
     and [cmd/vaultic/lock.go](../../../cmd/vaultic/lock.go) — introduce
     `LockPolicy{None, Shared, Exclusive}` per command; restic/rustic clients
     holding classic lock files are tolerated (we ignore foreign locks in
     lock-free mode; document the mixed-client semantics).
2. **Two-phase prune** in [internal/repository/prune.go](../../../internal/repository/prune.go):
   - Phase 1 (no exclusive lock): compute plan, repack, upload new packs and
     new index; write a *prune plan marker* file.
   - Phase 2 (short shared window): delete superseded packs/indexes —
     `--instant-delete` (today's behavior) vs deferred delete honoring
     `--keep-delete`/`--keep-pack <duration>`.
   - Concurrency rule: prune must never delete files it didn't see in its
     plan; unknown/new files are left alone (this is what makes it
     backup-safe).
3. Prune extras (F20): `--fast-repack` (skip re-reading pack headers where
   index suffices), `--max-repack` accepting size/%/`unlimited` (superset of
   `--max-repack-size`), `--repack-all`, `--early-delete-index` (alias of
   `--unsafe-recover-no-free-space` semantics; keep old flag as hidden alias).

**Format impact:** new optional `prune-plan-*` files must be ignored by older
clients (use a new `backend.FileType` only if restic tolerates unknown
top-level dirs — verify; otherwise reuse `index/` namespace or a `lock`-like
type). **Flags:** `lock-free` (Alpha), `two-phase-prune` (Alpha).
**Effort:** XL + L. **Depends on:** WS-A. **Risk:** highest in this roadmap —
see [Strategy & Appendices](05-strategy-and-appendices.md).

#### Locking parity roadmap (remaining work)

**Current safe baseline (implemented).** The Alpha ``lock-free`` feature skips
lock-file creation for read-only commands only (restore, snapshots, ls, find,
dump, repoinfo, etc.). Append writers (backup, copy destination, merge, key
add) retain a non-exclusive lock; destructive/coherent-view commands (prune,
forget, repair, recover, config, tag, key mutation, rewrite, migrate, check)
retain an exclusive lock. ``--no-lock`` remains an explicit operator override
for non-exclusive operations, not a blanket safe concurrency guarantee.

The command wrapper also has a process-wide reader/writer guard: append
operations share it and exclusive operations serialize through it. This closes
same-process goroutine races; backend lock files remain the cross-process
guard. Deferred-delete prune (``--keep-delete``) is implemented, but its
planning, repack, and new-index phase still holds an exclusive lock.

**Why append writers are not lock-free yet.** Index files and snapshot files
are additive, but a prune planned before an unlocked backup can select and
delete an old pack/index that the backup still needs. Removing the append lock
without deletion revalidation caused a real backup ∥ prune corruption in the
race soak. Therefore the following stages are mandatory before widening the
feature flag.

1. **✅ Explicit command lock policies (implemented).** Replaced overloaded
   ``dryRun``/``noLock`` booleans in
   [cmd/vaultic/lock.go](../../../cmd/vaultic/lock.go) with
   ``LockPolicy{None, Shared, Exclusive}``:

   | Policy | Commands | Initial behavior |
   |---|---|---|
   | ``None`` | restore, snapshots, ls, find, dump, stats, repoinfo, cat | Alpha lock-free reads |
  | ``Shared`` | backup, copy destination, merge | classic non-exclusive backend lock; process RW read lock |
  | ``Exclusive`` | prune, forget, repair, recover, config, tag, key add/remove/passwd, rewrite, migrate, check | backend exclusive lock; process RW write lock |

   Preserve ``--no-lock`` only as an explicit override of ``None``/``Shared``;
   never let it silently install a dry-run backend. Keep command-specific
  validation for dangerous combinations (for example destructive prune).
  ``openWithReadLock``, ``openWithAppendLock``, and
  ``openWithExclusiveLock`` now map to these typed policies; policy unit tests
  and runtime lock-file tests enforce the mapping.

2. **✅ Prove append-only writer safety (implemented).** Audited backup, copy
  destination, merge, and key add. Backup/copy/merge now receive a restricted
  ``AppendRepository`` transaction capability that exposes blob loads,
  pack/index upload, and snapshot save but deliberately omits removal APIs.
  Their repository mutations are additive and ordered as:
   packs/tree blobs uploaded -> additive index written -> snapshot written.
  ``key add`` remains exclusive because validation can remove a newly-created
  broken key. Concurrent ``backup ∥ backup ∥ merge`` and
  ``backup ∥ copy-to-destination`` race tests pass with a final ``check``.
  Append writers still retain a shared lock while prune is classic; this stage
  does **not** authorize feature-driven lock-free append writes.

3. **✅ Persist and validate prune plans (implemented).** A durable encrypted
  ``prune_plan`` config extension now records the observed index IDs, exact
  candidate old pack/index IDs, replacement index IDs captured from successful
  index writes, immutable plan ID, and creation timestamp. Config extensions
  are verified to be ignored by restic/rustic; config replacement is required
  to be atomic, so unsupported backends refuse durable plans rather than risk
  a missing singleton config. Before every deferred deletion, vaultic loads
  one fresh current index view and proves:

   - the candidate was in the original plan;
   - the candidate is still obsolete in the current index view;
   - no index/snapshot written after plan creation references it;
   - unknown/new objects are never deleted.

  Immediate ``--instant-delete`` persists the marker before cleanup and
  revalidates using rewrite-captured replacement index IDs without a second
  backend index list. ``--keep-delete`` retains the marker; a later normal
  ``prune`` finalizes that marker as its own one-list invocation. Unit tests
  reject missing replacement indexes and referenced packs, and integration
  tests verify marker persistence after phase A and clearing after phase B.

4. **✅ Make prune genuinely minimal-lock (implemented).** A short exclusive
  claim writes a pending durable marker, then phase A runs under ``LockShared``:
  read the planning view, repack, upload replacement packs and additive
  indexes, and atomically promote the marker to ready. The shared lock is
  released before phase B takes a short exclusive lock to reload/revalidate
  candidates, delete only marker-listed obsolete objects, and retire the
  completed marker. A blocked-index integration test proves an append backup
  can finish while phase A is active. ``--instant-delete`` runs A then B;
  ``--keep-delete`` runs A and retains the ready plan for a later B.
  ``--keep-pack`` remains dependent on backend file mtimes.

5. **✅ Revalidated minimal-lock forget (implemented).** Forget now evaluates
  policy and delete protection under a shared/read phase, releases that lock,
  then re-lists/re-evaluates under a short exclusive delete phase. It deletes
  only snapshot IDs selected by both observations, so snapshots created,
  protected, or retained after phase A are never deleted by phase B. The
  existing ``forget --prune`` flow remains within that exclusive delete phase
  to retain its snapshot-to-prune handoff semantics. An integration test
  pauses phase A, completes a backup, and proves the new snapshot survives
  revalidation before a final prune/check.

6. **⚠️ Graduation test gates (local/cross-process subset implemented).**
   Completed gates:

   - lock-free read tests prove no new repository lock is written;
   - append tests cover backup ∥ backup ∥ merge and backup ∥ copy, followed
     by ``check --read-data``;
   - repeated ``-race`` tests cover backup during minimal-lock prune phase A
     and backup during minimal-lock forget phase A;
   - pending prune-plan recovery proves an interrupted phase-A claim clears
     without deleting a pack;
   - separate OS-process CLI tests cover concurrent append backups and backup
     versus retrying minimal-lock prune, followed by a clean ``check``.

   Remaining mandatory graduation gates before promoting Alpha behavior:

   - MinIO/S3 tests for backup ∥ ``prune --instant-delete`` and backup ∥
     ``prune --keep-delete`` under eventual-consistency/listing behavior;
   - destructive cross-process tests: prune ∥ forget, prune ∥ repair, then
     cancellation/crash at every prune phase boundary and
     restic/rustic/vaultic reopen + check;
   - mixed-client lock behavior tests against restic/rustic writers;
   - repeated long-running ``-race`` soaks with durable-snapshot assertions.

**Graduation policy.** Keep lock-free reads Alpha until cross-process and
eventual-consistency tests pass. Promote append concurrency separately, then
minimal-lock prune, then destructive forget. Do not make append writes
lock-free or enable lock-free by default until the prune revalidation protocol
and crash/MinIO test gates are complete.

### WS-F — Local config profiles (TOML) & hooks

**Goal.** `vaultic backup` / `vaultic forget` with zero flags, driven by TOML
profiles; hooks for automation.

**Design.**

1. **Profile loading** (new `internal/configfile` package):
   - Search order: `/etc/vaultic`, `$XDG_CONFIG_HOME/vaultic`
     (`~/.config/vaultic`), cwd; file `<profile>.toml`, default
     `vaultic.toml`; `-P/--use-profile` repeatable; `use-profiles` key for
     recursive includes (cycle detection).
   - Merge precedence: CLI flags > env vars > later `-P` profiles > earlier
     profiles > `use-profiles` includes > built-in defaults.
   - Schema (Appendix B): `[global]`, `[repository]`, `[forget]`, `[backup]`,
     `[[backup.snapshots]]` (named snapshot jobs with `sources`), per-command
     sections mapping 1:1 onto existing option structs
     ([internal/global/global.go](../../../internal/global/global.go),
     `ForgetOptions`, `BackupOptions`…). Use struct tags + a small reflection
     mapper; TOML lib: `github.com/BurntSushi/toml`.
   - Env var prefix: `VAULTIC_*` primary, `RESTIC_*` fallback (rename policy,
     Phase 0).
2. **Multiple snapshots per run** (F33): `vaultic backup` without args runs
   all `[[backup.snapshots]]`; `--name x` selects one. Each snapshot job gets
   its own hooks and label.
3. **Hooks** (`internal/hooks` package):
   - `run-before`, `run-after`, `run-failed`, `run-finally` at scopes:
     global, repository, backup, per-snapshot job.
   - Hook entry: `{ command = "...", args = [...], on-failure = "error|warn|ignore" }`;
     plain strings also accepted (split via existing
     [internal/backend/shell_split.go](../../../internal/backend/shell_split.go)).
   - Env context: `VAULTIC_HOOK_TYPE`, `VAULTIC_ACTION`,
     `VAULTIC_BACKUP_LABEL`, `VAULTIC_BACKUP_SOURCES`, `VAULTIC_BACKUP_TAGS`
     (plus `RUSTIC_*` aliases for script portability).
   - Execution: no shell by default (documented escape hatch: `sh -c`).
4. `backup --init` (init repo if missing), `backup --ls` (list created
   snapshot like `ls` output) — small additions in
   [cmd/vaultic/cmd_backup.go](../../../cmd/vaultic/cmd_backup.go).

**Format impact:** none (client-side only). **Effort:** L (profiles) + M (hooks).
**Depends on:** WS-A (profile must merge with in-repo config), WS-B (label).

### WS-G — Backend expansion

**Goal.** Close the backend gap: webdav, dropbox, ftp, gdrive, onedrive,
pcloud (rustic gets these from opendal).

**Design.**

1. Tier 1 (native Go, do first): **webdav** — small protocol, several
   maintained Go clients (or minimal custom client on `net/http` with
   `PROPFIND/MKCOL`). Skeleton: copy [internal/backend/rest](../../../internal/backend/rest)
   structure. Register scheme in
   [internal/backend/location](../../../internal/backend/location).
2. Tier 2 (native Go SDKs exist): dropbox, gdrive (Google Drive API client
   already in go.mod for `gs`? — check reuse), onedrive (Graph API), ftp
   (`jlaffaye/ftp`), pcloud.
3. Tier 3 (documented alternative): everything else → existing `rclone`
   backend; consider adding an **rclone-serve mode** (rclone serving REST
   locally, like rustic does over localhost HTTP) to get retries/caching for
   free.
4. All new backends must implement the full interface including
   `Warmup`/`WarmupWait` stubs and pass `internal/backend/test` suite.

**Format impact:** none. **Effort:** L total. **Priority:** P2 — rclone already
covers these services, so this is UX/perf, not capability.

### WS-H — Observability (logging, progress, telemetry)

1. `--log-file` + `--log-level(-*)`: route the `debug`/log plumbing
   ([internal/debug](../../../internal/debug), [internal/ui](../../../internal/ui)) into a
   leveled logger writing to file and/or stderr. S.
2. `--no-progress`, `--progress-interval` as first-class flags (today env
   only), profile-settable via WS-F. S.
3. `--prometheus`, `--prometheus-user`, `--prometheus-pass`: push backup
   summary metrics (`SnapshotSummary` in
   [internal/data/snapshot.go](../../../internal/data/snapshot.go)) to a
   pushgateway after successful backup; start in
   [cmd/vaultic/cmd_backup.go](../../../cmd/vaultic/cmd_backup.go). M.
4. `--opentelemetry`: trace spans around repository operations
   (backend Save/Load, index, packer). Use OTel Go SDK, no-op unless
   configured. M.

### WS-I — Remote backup sources (P3, XL)

Back up *from* cloud storage (rustic: any opendal service as source).
Design: implement an `fs.FS`-compatible read-only filesystem backed by the
REST/rclone backend listing ([internal/fs](../../../internal/fs) has the local FS;
the archiver consumes `fs.FS` already — verify abstraction seams in
[internal/archiver](../../../internal/archiver)). Start with S3 sources via the
existing S3 client. Defer until WS-G stabilizes.

### WS-J — Interactive TUI (P3, XL)

Rustic ships a ratatui TUI. Vaultic equivalent: bubbletea-based UI embedding
`snapshots`/`restore`/`stats` browsing with live progress from
[internal/ui](../../../internal/ui) progress interfaces. Purely additive
(`vaultic tui` command). Defer; revisit after Phase 5.

