# Phase 32 P3a0 Import Durability Evidence

[Phase 32 status and document map](phase-32-scalable-legacy-metadata-bulk-import.md) |
[Repository-scale recovery evidence](phase-32-p3-p7-evidence.md#scale-and-external-limits)

P3a0 is complete for the supported fresh-import restart-from-zero contract as
of 2026-09-16. This record does not claim persistent resume for memory-WAL
imports and does not change normal backup or non-fresh import durability.

## Contract

Fresh Stage 3 transactions use deferred commits only on a reset candidate.
Their import checkpoints share the transaction's durability and are not
cross-process resume cursors. `MarkBulkImportComplete` waits for its SlateDB
write handle, which orders all preceding writes through the configured memory
WAL, but that WAL is process-local and therefore not crash durable.

The only activation-authorizing sequence is:

1. publish the completion marker;
2. successfully close SlateDB, flushing the current memtable to persistent
   main-store SSTs;
3. atomically replace the memory WAL-target binding with the local-WAL handoff;
4. reopen using the completed-import configuration; and
5. activate the imported metadata authority.

Failure before handoff leaves the memory binding intact. Completed-mode reopen
must reject that binding. Recovery destroys and restarts the private candidate
from input position zero; it never resumes from an intermediate checkpoint.

The later P7b checkpoint-only recovery occurred after successful durable close,
handoff, and persistent-WAL reopen. It completed activation after a final daemon
shutdown timeout; it is not recovery of an unfinished memory-WAL import.

`WriterStatus.last_durable_sequence` is a process-local count of successful
VaulticDB durability waits. It resets at process start and is neither the engine
sequence nor proof that a specific prefix survived a crash. The pinned SlateDB
fork's `WriteHandle::seqnum()` and `await_durable()` form an internal ordered-
prefix token, but VaulticDB does not expose that token and a wait against memory
WAL still does not provide process-crash persistence.

## Executable Crash Matrix

| Failure boundary | Required result | Evidence |
|---|---|---|
| Deferred ingest/reduction, completion marker, then SIGKILL before close | Completed-mode reopen rejected; reset candidate contains no pack, blob, checkpoint, or completion marker | `TestProcessFreshImportKillBeforeCloseRequiresReset` |
| Successful SlateDB close/flush, then injected failure before handoff CAS | No local-WAL handoff record; inherited-WAL reopen rejected | `flushed_memory_wal_rebuild_without_handoff_cannot_reopen` |
| Successful close and handoff | Completed-mode reopen succeeds and imported state is present | `clean_memory_wal_rebuild_reopens_with_persisted_local_wal` |
| Clean close without completion marker | No handoff; inherited-WAL reopen rejected | `clean_incomplete_memory_wal_rebuild_does_not_handoff` |

The focused gates pass with:

```console
go test ./internal/index/daemon \
  -run '^TestProcessFreshImportKillBeforeCloseRequiresReset$' -count=1

cd vaulticdb
rustup run stable cargo test --bin vaulticdb \
  flushed_memory_wal_rebuild_without_handoff_cannot_reopen
```

The process test uses an isolated local object store, memory WAL, repository ID,
socket, and child daemon. It deliberately calls `Process.Kill` and `Wait`
without the shutdown RPC, then tests both prohibited completed-mode reopen and
the supported destructive reset. The Rust test's one-shot failpoint fires only
after `db.close()` succeeds and immediately before `mark_local_wal_handoff`.

## Limits

There is no supported persistent-WAL Stage 3 resume mode to certify. Missing
SST recovery from durable WAL is therefore outside the fresh memory-WAL
contract. Adding persistent resume would require a stable input identity,
checkpoint-to-engine durable-prefix binding, exposed durable token, replay and
crash tests, and a separately reviewed activation contract. P3 experiments must
label deferred fresh-import commit responses as apply acknowledgements and must
include close, handoff, reopen, and validation in end-to-end completion.