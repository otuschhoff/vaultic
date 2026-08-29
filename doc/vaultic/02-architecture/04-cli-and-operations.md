# CLI and Operations

[← Back to architecture index](00-overview.md)

## 10. CLI command group

Add an `index` command group without changing existing `list index` behavior.
Each command resolves or starts the singleton `vaulticdb` only when the
repository has a valid SlateDB manifest. Import, export, and check logic stays
in vaultic; the daemon is used only for storage operations and performance
services.

### `vaultic index import`

Options should include:

- repository and backend options shared with existing commands
- `--from-legacy` or equivalent explicit source selection
- `--batch-size`
- `--max-errors`
- `--resume`
- `--snapshot-depth` for bounded optional tree traversal
- `--dry-run`
- `--json`

The command must be safe to rerun. It should detect already imported index IDs
and skip or verify them rather than duplicating records.

### `vaultic index export`

Options should include:

- `--full`
- `--since`
- `--output-repo` only if a separate destination is supported
- `--verify`
- `--json`

Export must use an exclusive metadata publication boundary when required by the
existing repository lock model.

### `vaultic index check`

Options should include:

- `--legacy-only`
- `--slatedb-only`
- `--include-crawl-debt`
- `--fail-on-warning`
- `--json`

Read-only check mode should use a daemon `DbReader` through the RPC client and
the existing read-lock policy. Vaultic remains the owner of the differential
comparison and exit-code decision.

### Introspection commands

`vaultic index stats`, `vaultic index packs`, `vaultic index history`,
`vaultic index history prune`, and `vaultic index backends` are specified in
[storage placement policy](05-storage-placement.md) and implemented in Phase 11. `vaultic index placement` is specified
in the same section and implemented in Phase 16, because it reports on machinery
that phase introduces.

`vaultic index file-history` and `vaultic index path-at` are specified in
[path and inode history queries](06-path-inode-history.md) and implemented in Phases 13 and 14. Note that `index history`
reports pack lifecycle history, not file history; the two are unrelated.

## 11. Locking and failure recovery

The daemon boundary does not replace vaultic's repository lock policy
automatically. `vaulticdb` is a singleton for one endpoint, not a substitute for
cross-tool repository locks.

- Legacy JSON writes continue to use existing vaultic lock behavior.
- SlateDB writers acquire the appropriate repository lock before pack,
  snapshot, and projection publication.
- Read-only commands use `DbReader` with `SkipWalReplay` only when the selected
  reader mode has been proven safe for the desired consistency level.
- Never run a JSON export concurrently with destructive prune until the export
  checkpoint and pack/index revalidation rules are defined.
- A crash between SlateDB commit and JSON export creates a visible pending
  export, not silent data loss.
- A crash during import resumes from the last committed batch checkpoint.
- A crash during crawl reconciliation leaves idempotent pending work.
- A detected manifest epoch change or fencing event aborts the current write
  transaction and requires reopening the engine.
- Daemon startup, attach, and shutdown use the endpoint singleton lock; normal
  RPC handling remains concurrent and does not serialize all vaultic clients
  behind one process-global mutex.
- A client disconnect does not roll back a committed batch. Uncommitted
  requests have bounded deadlines and are explicitly aborted.
- TCP remains disabled unless both configuration and daemon startup policy
  explicitly enable it.

