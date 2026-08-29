# Strategy & Appendices

[← Back to rustic-parity index](00-overview.md)

## 9. Compatibility & interop strategy

1. **Read compat:** vaultic must read any repo written by restic ≥ 0.16 and
   rustic ≥ 0.7 (both directions of each feature: label snapshots, in-repo
   config, hot/cold layouts).
2. **Write compat:** everything vaultic writes must be accepted by restic
   0.19/rustic 0.11. Additive JSON only; no new mandatory fields; no repo
   version bump without a `migrate` entry
   ([internal/migrations](../../../internal/migrations)).
3. **Interop CI job** (`helpers/interop/`): matrix of {vaultic, restic,
   rustic} × {init/backup/restore/check/forget/prune} on shared local and
   minio-S3 repos. Allowed-to-fail until Phase 1 exit, then required.
4. **Mixed-client locking:** lock-free **reads** ignore classic lock files;
  append and destructive vaultic commands still honor backend locks. Do not
  mix future lock-free append/minimal-lock prune modes with classic restic or
  rustic writers until their cross-client revalidation behavior is explicitly
  tested and documented.
5. Env var compatibility: accept `RESTIC_*` for every new `VAULTIC_*` var.

## 10. Testing strategy

- **Unit:** per-package, mirroring existing `*_test.go` conventions; config
  parsing fuzz test (pattern: [internal/repository/fuzz_test.go](../../../internal/repository/fuzz_test.go)).
- **Integration:** every §7 item gets a `cmd_<x>_integration_test.go` case;
  reuse harness in [cmd/vaultic/integration_helpers_test.go](../../../cmd/vaultic/integration_helpers_test.go).
- **Backend suite:** new backends must pass
  [internal/backend/test](../../../internal/backend/test).
- **Cold-storage fake backend:** test double delaying `Load` until `Warmup`
  (extend [internal/backend/mock](../../../internal/backend/mock)).
- **Concurrency soak:** `go test -race` scenario tests for Phase 4
  (backup∥prune∥forget).
- **Interop:** interop harness (see [Strategy & Appendices](05-strategy-and-appendices.md)); golden snapshot JSON fixtures with
  label/description captured from real rustic output.

## 11. Risks & mitigations

| Risk | Impact | Mitigation |
|---|---|---|
| Unknown JSON fields rejected by rustic (config/snapshot) | breaks write-compat goal | Phase 0 spike: verify serde behavior; fall back to nested extension object |
| Two-phase prune data-loss bug | catastrophic | feature-flagged; chaos tests; keep classic prune path until Stable; require `--instant-delete` default = current semantics initially |
| Lock-free mixed-client confusion | index bloat, failed prune | docs + `check` detection of foreign locks; conservative defaults |
| Scope creep (60+ items) | roadmap stalls | phases are independently shippable; P2/P3 items may be deferred without blocking releases |
| Module-path rename churn | merge conflicts | do once, early (Phase 0), mechanical |
| jq filter dependency weight | binary size | gojq is a pure-Go runtime dependency; retain it for built-in `--filter-jq` parity |
| WebDAV server security surface | exposure | read-only by default, explicit auth flags, localhost default bind |

## 12. Implementation conventions (for every PR)

1. Reference the work item ID (F#/WS-#/§7.x) in the PR description and
   changelog entry ([changelog/unreleased](../../../changelog/unreleased)).
2. New behavior behind a feature flag unless purely additive metadata.
3. Update docs: relevant page in [doc](../..) (RST) + this roadmap's status.
4. Add/extend integration tests; run `go test ./...` and the backend suite
   when touching [internal/backend](../../../internal/backend).
5. Keep `restic` CLI spellings working; rustic spellings may be added as
   aliases but are never the only way.
6. JSON output stability: new fields are additive; do not rename/remove.
7. Mark completion in the [Current State & Gap Analysis](01-current-state-and-gaps.md) and [Command Work Items](03-command-work-items.md) tables (✅ + version) as items land.

## Appendix A — Warm-up command protocol (normative)

- Invocation: `<warm-up-command>` with variables substituted:
  `%id` / `%path` → one pack per invocation (N parallel invocations at batch
  size N); `%ids` / `%paths` → N packs inline per invocation.
- `%path` is the backend-native path (e.g. S3 key); `%id` the restic pack ID.
- Exit code 0 = packs (expected to become) ready; non-zero = abort with error.
- `--warm-up-wait <dur>`: max wait when the command returns before data is
  ready; retrieval is attempted after the command exits.
- Progress: stdout parsed as JSON Lines; `{"type":"pack-progress","warm":n}`
  where `warm` is monotonically non-decreasing within one invocation; other
  lines are logged at info level prefixed `[warmup]`.
- Config-file keys (WS-F): `warm-up-command`, `warm-up-time`,
  `warm-up-batch`, `warm-up-wait`, `warm-up-wait-command`.

## Appendix B — Config profile TOML schema (sketch)

```toml
[global]
use-profiles = ["retention"]       # recursive includes
log-level = "info"
log-file = "/var/log/vaultic.log"
no-progress = false

[global.hooks]
run-before = []
run-after = []
run-failed = []
run-finally = []

[repository]
repository = "s3:https://s3.example.com/bucket/repo"
repo-hot = "s3:https://s3.example.com/bucket-hot/repo"   # WS-D
password-file = "/etc/vaultic/pass"
no-cache = false
warm-up-command = "aws s3api restore-object ... %path"    # WS-D
warm-up-batch = 25

[repository.options]      # backend-specific options (replaces -o)
# region = "eu-central-1"
[repository.options-hot]  # hot-part overrides
[repository.options-cold] # cold-part overrides

[forget]
keep-daily = 14
keep-weekly = 5

[backup]
glob-file = ["/etc/vaultic/excludes.glob"]
git-ignore = true

[[backup.snapshots]]
name = "home"
label = "home"          # WS-B
sources = ["/home"]

[[backup.snapshots]]
name = "etc"
sources = ["/etc"]
```

Search order: `-P name` → `{cwd, ~/.config/vaultic, /etc/vaultic}/name.toml`;
default profile name `vaultic`. Precedence: CLI > env > profiles (later wins)
> includes > defaults.

## Appendix C — In-repo config JSON (sketch)

```jsonc
{
  "version": 2,
  "id": "…",
  "chunker_polynomial": "…",
  // --- vaultic extension fields (all optional; ignored by restic/rustic pending verification) ---
  "compression": 10,                 // -7..22
  "append_only": false,
  "extra_verify": true,
  "chunker": { "type": "cdc", "chunk-size": 1048576,
               "chunk-min-size": 524288, "chunk-max-size": 8388608 },
  "treepack":  { "size": 4194304,  "growfactor": 32, "size_limit": 134217728 },
  "datapack":  { "size": 16777216, "growfactor": 32, "size_limit": 536870912 },
  "min_packsize_tolerate_percent": 80,
  "max_packsize_tolerate_percent": 0
}
```

Changing chunker parameters after init affects dedup for new data only;
`copy` between repos requires matching chunker config (enforced by §7.12).
