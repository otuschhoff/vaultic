# Current State & Gap Analysis

[← Back to rustic-parity index](00-overview.md)

## 4. Current state

### 4.1 Codebase map (relevant to this roadmap)

| Area | Location | Notes for this roadmap |
|---|---|---|
| Repo config (JSON) | [internal/vaultic/config.go](../../../internal/vaultic/config.go), [config_ext.go](../../../internal/vaultic/config_ext.go) | flat additive rustic-compatible config extensions; foreign config rewrites can strip vaultic-only keys |
| Snapshot model | [internal/data/snapshot.go](../../../internal/data/snapshot.go) | label, description, delete protection, merge provenance, and summaries implemented |
| Snapshot find/filter | [internal/data/snapshot_find.go](../../../internal/data/snapshot_find.go), [cmd/vaultic/filter_flags.go](../../../cmd/vaultic/filter_flags.go) | rustic-compatible structured filters and jq; file-level resolution remains missing |
| Warm-up plumbing | [internal/warmup](../../../internal/warmup), [internal/backend/warmupcmd](../../../internal/backend/warmupcmd) | warm-up command, batching, wait protocol, and hot/cold integration implemented |
| Locking | [internal/repository/lock.go](../../../internal/repository/lock.go), [cmd/vaultic/lock.go](../../../cmd/vaultic/lock.go) | typed `None`/`Shared`/`Exclusive` policy; lock-free reads Alpha; minimal-lock prune/forget with external graduation gates pending |
| Prune | [internal/repository/prune.go](../../../internal/repository/prune.go), [prune_plan.go](../../../internal/repository/prune_plan.go) | durable/revalidated minimal-lock plan phases; S3/MinIO and mixed-client graduation pending |
| Packer / pack size | [internal/repository/packer_manager.go](../../../internal/repository/packer_manager.go), [pack_sizer.go](../../../internal/repository/pack_sizer.go) | tree/data target sizing, square-root growth, explicit limits, and 4 GiB cap |
| Global options | [internal/global/global.go](../../../internal/global/global.go), [internal/configfile](../../../internal/configfile) | TOML profiles, environment fallback, log/progress/telemetry options implemented |
| Backends | [internal/backend](../../../internal/backend) | local, sftp, rest, s3, swift, b2, azure, gs, rclone |
| Feature flags | [internal/feature](../../../internal/feature) | alpha/beta/stable registry, env-driven |
| CLI | [cmd/vaultic](../../../cmd/vaultic) | cobra commands, one file per command |

### 4.2 Rustic features vaultic already has (no action)

- Backend-level warm-up for cold storage (S3 restore) in restore/check/repack
  (partial; CLI and hot/cold split missing — see WS-D).
- `rewrite`, `repair index/packs/snapshots`, `recover`, `copy`, `tag`,
  `self-update`, `generate` (completions/manpages), `--dry-run`,
  `--pack-size` (fixed), `--read-data-subset`, in-place restore, resumable
  restore, `latest` filtering for restore/ls/find/dump.

### 4.3 Vaultic features rustic lacks (retain, do not regress)

`mount` (all OSes; rustic: Linux only), `recover`, `repair packs`, `stats`,
`cache`, `--files-from*`, `--exclude-caches` header parsing, `--overwrite`,
`--sparse`, `--verify`, `--cacert`, `--insecure-tls`, `--tls-client-cert`,
`--key-hint`, `--repository-file`, `--retry-lock`, `--read-concurrency`,
`find --blob/--tree/--pack`, `--compact` output, `--human-readable`.

## 5. Gap analysis matrix

Legend — **Priority**: P0 = parity-critical / blocks other work, P1 = high user
value, P2 = nice to have, P3 = experimental/long-tail.
**Effort**: S < 1 wk, M = 1–2 wk, L = 2–6 wk, XL > 6 wk.

### 5.1 Repository & storage format

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F1 | In-repo config | `config` command edits compression, pack sizes, chunker, append-only inside repo config JSON | P0 | L | WS-A | ✅ |
| F2 | `show-config` command | prints repo config incl. extensions | P0 | S | WS-A | ✅ |
| F3 | Custom chunker config | min/max/avg chunk size, fixed-size chunker, stored in repo | P2 | M | WS-A | ✅ |
| F4 | Per-type pack sizing | `treepack_*`/`datapack_*` size, grow factor, limits; packs up to 4 GiB | P1 | M | WS-A | ✅ |
| F5 | Append-only repo mode | `append_only` in-repo flag blocks delete/overwrite | P1 | M | WS-A | ✅ |
| F6 | Extra-verify persist | `extra_verify` in-repo instead of per-call `--no-extra-verify` | P2 | S | WS-A | ✅ |
| F7 | Open repo via master key | `--key`, `--key-file`, `--key-command` bypass password keys | P2 | M | WS-A | ✅ |

### 5.2 Snapshot metadata & selection

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F8 | Snapshot label | `--label`, grouping/filtering by label | P0 | S | WS-B | ✅ |
| F9 | Snapshot description | `--description`, `--description-from` | P1 | S | WS-B | ✅ |
| F10 | Delete protection | `--delete-never`, `--delete-after`; forget/prune respect it | P1 | M | WS-B | ✅ |
| F11 | Rich snapshot filters | `--filter-host/label/paths/paths-exact/tags/tags-exact/before/after/size/size-added/jq/last` on all snapshot-taking commands | P1 | M | WS-C | ✅ |
| F12 | `latest~N` syntax | resolve N-th latest snapshot | P2 | S | WS-C | ✅ |
| F13 | `<snap>:<path>/file` | sub-path file selection in restore/diff/dump | P2 | S | WS-C | ⏳ deferred: dump tree paths work; restore/diff file-level resolver missing |

### 5.3 Cold storage

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F14 | Hot/cold split repo | `--repo-hot`; metadata in hot repo, data packs in cold repo; cold repo is a complete repo | P1 | XL | WS-D | ✅ |
| F15 | Warm-up command | user-supplied program, `%id/%path/%ids/%paths`, batch size, JSON-lines `pack-progress` protocol | P1 | L | WS-D | ✅ |
| F16 | `init --hot-only` | convert normal repo → hot/cold | P2 | M | WS-D | ✅ |
| F17 | check hot/cold integrity | cross-check hot vs cold metadata | P2 | M | WS-D | ✅ (`check --check-hot-cold`) |

### 5.4 Concurrency & prune

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F18 | Lock-free operations | no lock files; safe concurrent clients; additive index writes | P1 | XL | WS-E | ⚠️ partial: lock-free reads and tested local/cross-process gates; append writes retain shared locks; MinIO/S3/mixed-client gates pending |
| F19 | Two-phase prune | repack+upload first, delete later; prune parallel to backups | P1 | L | WS-E | ⚠️ partial: minimal-lock local implementation complete; eventual-consistency, destructive cross-process, and mixed-client gates pending |
| F20 | Prune extras | `--fast-repack`, `--keep-pack`, `--max-repack` (size/%/unlimited), `--repack-all`, `--early-delete-index` | P2 | M | WS-E | ⚠️ partial: all except `--keep-pack`; backend `FileInfo` has no modification time |

### 5.5 UX, configuration & observability

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F21 | TOML config profiles | `vaultic.toml`, search paths, `-P profile`, `use-profiles` inheritance, env overrides; `[repository] [forget] [backup] [[backup.snapshots]]` | P0 | L | WS-F | ⚠️ substantially complete: core rustic-compatible profiles, inheritance, precedence, and backup jobs implemented; full schema interop remains unexhaustive |
| F22 | Hooks | run-before/after/failed/finally at global/repo/backup/snapshot scope, env context, on-failure policy | P1 | M | WS-F | ⚠️ substantially complete: all listed scopes and failure policies implemented and tested; full rustic cross-client hook interop remains unexhaustive |
| F23 | Log to file + log levels | `--log-file`, `--log-level(-*)` | P1 | S | WS-H | ⚠️ partial: log file works; `--log-level` validates but does not route/filter logger output |
| F24 | Progress control | `--no-progress`, `--progress-interval` as real flags (env exists today) | P2 | S | WS-H | ✅ |
| F25 | Telemetry | `--prometheus(+user/pass)` push metrics, `--opentelemetry` tracing (backup first) | P2 | M | WS-H | ⚠️ partial: Pushgateway/Influx backup metrics work; OTel is command-span skeleton only |
| F26 | Interactive TUI | integrated terminal UI (stats live, selection) | P3 | XL | WS-J | Deferred — not implemented |

### 5.6 Backends & sources

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F27 | New backends | dropbox, ftp, gdrive, onedrive, pcloud (rustic: via opendal; WebDAV excluded) | P2 | L | WS-G | Deferred — not implemented; rclone remains an available external route where supported |
| F28 | Remote backup sources | back up *from* S3/cloud storage | P3 | XL | WS-I | Deferred — not implemented |
| F29 | Built-in sftp (no ssh binary) | rustic uses opendal sftp; vaultic shells out to ssh | P3 | M | WS-G | Deferred — vaultic uses the existing SSH/SFTP path |

### 5.7 Commands

| # | Feature | Rustic behavior | Priority | Effort | Workstream | Status |
|---|---|---|---|---|---|---|
| F30 | `merge` command | merge snapshots | P1 | M | §7 | ✅ implemented; full rustic cross-client interop is not exhaustively tested |
| F31 | `webdav` command | serve repo over WebDAV for browsing/restore | P2 | L | §7 | Deferred — explicitly out of scope |
| F32 | `repoinfo` command | repo statistics summary | P2 | S | §7 | ✅ implemented; full rustic cross-client output parity is not exhaustively tested |
| F33–F60 | Command enhancements | see per-command tables in [Command Work Items](03-command-work-items.md) | mixed | mixed | §7 | Mixed/deferred by command |

