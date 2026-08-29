# Command Work Items

[← Back to rustic-parity index](00-overview.md)

## 7. Command work items

Effort assumes the relevant workstreams are done. "Aliases" = accept rustic
flag spelling as hidden aliases (migration aid only).

### 7.1 `init` ([cmd_init.go](../../../cmd/vaultic/cmd_init.go))

| Item | Effort | Notes |
|---|---|---|
| `--set-*` for all in-repo config keys | M | via WS-A |
| `--hostname`, `--username`, `--with-created` | S | control config/snapshot identity metadata |
| `--hot-only` | M | WS-D |
| keep `--copy-chunker-params`, `--from-*` (rustic solves via `copy --init`) | — | no action |

### 7.2 `backup` ([cmd_backup.go](../../../cmd/vaultic/cmd_backup.go))

| Item | Effort | Notes |
|---|---|---|
| Multiple snapshots per run from config `[[backup.snapshots]]` | M | ✅ WS-F; `backup --name` selects named jobs |
| `--label`, `--description`, `--description-from` | S | WS-B |
| `--delete-never`, `--delete-after` | S | WS-B |
| `--as-path` (store relative/custom path) | S | Deferred — needs archiver root-path override |
| `--git-ignore`, `--no-require-git`, `--custom-ignorefile` | M | Deferred — needs gitignore matcher and per-root discovery |
| `--exclude-if-xattr` | S | Deferred — needs portable xattr predicate |
| `--set-atime/--set-ctime/--set-devid/--set-xattr/--set-blockdev` | M | Deferred — synthetic metadata/block-device semantics |
| `--stdin-from-command` | — | already present ([cmd_backup.go](../../../cmd/vaultic/cmd_backup.go#L117)) — no action |
| `--init` | S | ✅ auto-init missing repo |
| `--ls` | S | ✅ list contents of the created snapshot |
| Multiple `--parent` | M | Deferred — needs multi-tree change-detection semantics |
| Hooks + telemetry integration | S | ✅ WS-F/WS-H |

### 7.3 `restore` ([cmd_restore.go](../../../cmd/vaultic/cmd_restore.go))

| Item | Effort | Notes |
|---|---|---|
| `<snap>:<path>/file` syntax | S | WS-C resolver |
| `--no-ownership`, `--numeric-id` | S | Deferred — restorer metadata API lacks ownership-disable control; numeric IDs are current default |
| warm-up command integration | M | WS-D |
| keep `--overwrite`, `--sparse`, `--verify` (rustic lacks) | — | no action |

### 7.4 `forget` ([cmd_forget.go](../../../cmd/vaultic/cmd_forget.go))

| Item | Effort | Notes |
|---|---|---|
| `--keep-minutely`, `--keep-quarter-yearly`, `--keep-half-yearly` (+ `--keep-within-*` variants) | M | ✅ |
| `--keep-none` (≙ `--unsafe-allow-remove-all`; keep both) | S | ✅ alias |
| `--delete-unchanged` | M | Deferred — requires parent/tree identity retention pass |
| Respect delete protection | S | WS-B |
| `--group-by` gains `label` | S | WS-B |
| Retention from config profile `[forget]` | S | WS-F |

### 7.5 `prune` ([cmd_prune.go](../../../cmd/vaultic/cmd_prune.go))

Covered by WS-E: two-phase, `--fast-repack`, `--keep-delete`,
`--instant-delete`, `--max-repack` (size/%/unlimited), `--repack-all`,
`--early-delete-index`, tree/data pack sizing from repo config (WS-A),
cold-pack handling (WS-D). Deferred: ``--keep-pack`` needs generic backend
mtime support; keep `min_packsize_tolerate_percent` /
`max_packsize_tolerate_percent` configurable in-repo (today hardcoded /
`--repack-smaller-than` in [internal/repository/prune.go](../../../internal/repository/prune.go)).

### 7.6 `check` ([cmd_check.go](../../../cmd/vaultic/cmd_check.go))

| Item | Effort | Notes |
|---|---|---|
| `--trust-cache` (verify cached data integrity, then trust it) | M | Deferred — checker/cache trust state is not present |
| Hot/cold integrity mode | M | WS-D |
| `--read-data-subset` friendly names (`last-week`, `month-2026-01`, …) | S | Deferred — subset currently intentionally pack-based only |
| Use existing cache by default | S | verify current behavior matches; roadmap item from upstream already landed — confirm |

### 7.7 `snapshots` ([cmd_snapshots.go](../../../cmd/vaultic/cmd_snapshots.go))

`--all` (no grouping collapse), `--long`, and identical-snapshot summaries are
deferred. `--group-by label` and `--filter-*` are ✅ via WS-B/C.

### 7.8 `ls` ([cmd_ls.go](../../../cmd/vaultic/cmd_ls.go))

`--glob/--iglob(--file)` aliases, `--numeric-uid-gid`, `--summary`, and local
path listing are deferred — they need a shared glob/local-FS listing layer.

### 7.9 `find` ([cmd_find.go](../../../cmd/vaultic/cmd_find.go))

`--path <full-path>`, result summarization, `--show-misses`, `--group-by`, and
`--numeric-uid-gid` are deferred — they need a history/index walk redesign.
Keep vaultic-only `--blob/--tree/--pack`.

### 7.10 `diff` ([cmd_diff.go](../../../cmd/vaultic/cmd_diff.go))

✅ `latest`/`latest~N` resolve in diff. Snapshot-vs-local, glob filters,
`--no-content`, and file-level sub-paths are deferred — they need a local
metadata comparison engine.

### 7.11 `dump` ([cmd_dump.go](../../../cmd/vaultic/cmd_dump.go))

✅ `--archive tar.gz` and `--archive auto` based on `--target` extension.
File-level sub-paths are already supported by the existing dump tree resolver.

### 7.12 `copy` ([cmd_copy.go](../../../cmd/vaultic/cmd_copy.go))

`--init`, verify-chunker-params, and multiple targets are deferred — target
initialization needs a non-interactive destination-password/config flow.

### 7.13 New commands

| Command | Effort | Notes |
|---|---|---|
| `merge` (new command) | M | ✅ append-only merge: recursive tree union, newest source wins conflicts, source IDs saved in additive `merged_snapshots` metadata; uses an append lock only |
| `repoinfo` (new) | S | ✅ lock-free read aggregation: per-file-type object counts and stored sizes, JSON/text |
| `webdav` (new) | L | Explicitly out of scope for Phase 6; not implemented |
| `config` / `show-config` (new) | M/S | WS-A |
| `completions` alias of `generate` | S | ✅ rustic naming alias, keep `generate` |

### 7.14 `key`, `tag`, `unlock`, `version`, `self-update`

- `key`: no gaps (master-key open is WS-A/F7, not key mgmt).
- `tag`: `--set-label/--set-description` (WS-B); keep existing.
- `unlock`: keep; becomes mostly obsolete under lock-free (WS-E) — document.
- `self-update`/`version`: no gap.

