# Interop test harness

All of vaultic, [restic](https://restic.net) and
[rustic](https://rustic.cli.rs) implement the same repository format. This
harness verifies that vaultic stays interoperable in both directions:
repositories written by vaultic must be readable by restic and rustic, and
vaultic must read repositories written by them.

`interop.sh` runs a writer × reader matrix over the three clients against
local repositories:

- **writer leg**: `init`, two `backup` runs (with a mutation in between)
- **reader leg**: `snapshots` (must list the backup), `ls latest`,
  `restore latest` (restored tree is diffed against the source), `check`
- **prune leg** (on a copy of each writer's repository): `forget
  --keep-last 1`, `prune`, then `vaultic check` and a restore verify as the
  reference implementation

## Usage

```sh
helpers/interop/interop.sh            # full 3x3 matrix
helpers/interop/interop.sh --keep     # keep the workdir for debugging
helpers/interop/interop.sh --clients "vaultic rustic"
```

The vaultic binary is built from the current checkout. The restic and rustic
binaries are downloaded once (pinned versions) into
`$XDG_CACHE_HOME/vaultic-interop` and reused across runs.

| Environment variable | Default            | Meaning                          |
|----------------------|--------------------|----------------------------------|
| `RESTIC_VERSION`     | `0.19.1`           | restic release to test against   |
| `RUSTIC_VERSION`     | `0.11.4`           | rustic release to test against   |
| `INTEROP_PASSWORD`   | `interop-test-secret` | repository password           |
| `INTEROP_WORKDIR`    | fresh tmp dir      | working directory                |
| `--skip-download`    | –                  | use `restic`/`rustic` from PATH  |

The script exits non-zero if any step fails and keeps the workdir (including
per-scenario logs) in that case.

## CI

The `interop` GitHub workflow runs this harness on demand
(`workflow_dispatch`). The job is marked `continue-on-error` — it is allowed
to fail while the parity work is in progress (see
`doc/rustic-parity-roadmap.md`, Phase 0).

## Extending

When parity features land (in-repo config, hot/cold repositories, extended
snapshot metadata), add scenarios here that exercise the new format
extensions in both directions. An S3/MinIO leg is planned (roadmap §9.3).
