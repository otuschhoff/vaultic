# Phase 35 M2: Pointer-light RAM backend

[Back to Phase 35](phase-35-ram-working-memory-and-kv-fallback.md)

Status: complete for the isolated adapter. M3-M6 remain unimplemented. The
production CLI still uses its existing working stores and policy; authoritative
VaulticDB/SlateDB metadata, checkpoints and publication are unchanged.

The request named "Phase 25-M2". Phase 25 has no M2, and the preceding completed
milestones were Phase 35 M0/M1. With the user unavailable for clarification, this
work explicitly assumes Phase 35 M2. Phase 25 was not modified.

## Representation And Admission

[Implementation](../../../internal/workingkv/ram.go) adds `OpenRAM(*Budget)` with
no path argument or filesystem operations. RAM implements the same private
engine contract as the M1 Pebble, bbolt and Badger adapters. Public operations
retain owned-copy results, binary lexicographic ordering, exclusive cursors,
last-write-wins duplicate keys, bounded atomic batches and stable close behavior.
Distinct location keys for the same overlay prefix remain distinct entries.

The byte arena uses 64-KiB chunks, or one larger chunk for a wide record. AVL
nodes live in 256-node chunks. Each node holds only integer spans, child indexes,
height and value-slot capacity: no maps, strings, interfaces or pointers per
entry. A node is 40 bytes on the captured platform. Chunk descriptors hold the
only cardinality-dependent pointers. AVL balancing bounds lookup and traversal
depth; scan stages at most 1,024 integer indexes before allocating output.
Existing 1-MiB batch, 8-MiB page and 32,768-byte key limits still apply.

One mutex-protected `Budget` may be shared by several stores. Reservations cover
base state, transaction planning, arena/index chunks, complete replacement
descriptor capacity while old capacity remains live, and transient read copies.
Each backing allocation is charged its capacity plus 25% and 64 bytes; base
state is charged 1,024 bytes. Planning also reserves 512 bytes beyond its two
integer arrays. `MemoryLimitError` records numeric limit/used/requested fields,
matches `ErrWorkingMemoryLimitExceeded`, and is not classified as backend IO.
Denied growth occurs before allocation or mutation of retained working state.

Planning collapses duplicate keys to their final value. Existing slots are
reused when replacement values fit. Larger replacements copy no redundant key,
release entirely dead byte chunks and reuse descriptor holes. Partially live
chunks retain dead capacity; shrinking a value retains its reusable slot.
There is no full compactor or delete API. This fragmentation remains charged and
can cause typed exhaustion despite smaller logical payload; it never authorizes
growth past the budget. `RAMSnapshot` exposes actual retained capacities and
the conservative charge. Close drains admitted calls and releases all retained
reservations; repeated close cannot double-release.

Cancellation is checked in admission, planning, ordered traversal and copy loops,
and before returning read results. Mutation after the final planning check is a
bounded noncancellable commit; cancellation racing that boundary can acknowledge
success. Engine read-lock waits are not immediately interruptible, but cancelled
reads do not return values after they unblock. Caller-held input and returned
copies are not retained backend state: copy reservations end at return. Error
objects, goroutine stacks, runtime/allocator metadata and memory awaiting GC are
not a process-wide limit. M3/M4 must define aggregate caller ownership and
pressure policies; this managed-capacity budget is explicitly not an RSS limit.

## Verification And Measurements

[Tests](../../../internal/workingkv/ram_test.go) reuse M1 encrypted overlay,
marker-churn and wide-directory fixtures and their exact ordered final digests.
Shared RAM/Pebble/bbolt/Badger tests cover concurrency, binary keys/values,
replacement, pagination, cancellation and ownership. RAM-specific tests verify
one-byte-below/exact admission boundaries, oversized batches, exhausted reads,
failed growth preserving existing state, simultaneous shared-budget creation
and writes, cancelled queued reads, AVL balance, pointer-free node types,
replacement/reclaimed-hole accounting and zero usage after close.

Reproduce with `helpers/phase35-m2/run.sh`; its
[analyzer](../../../helpers/phase35-m2/analyze.cjs) rejects missing/wrong benchmark
filters, missing required test names, missing profiles/metrics, accounting or
release violations, filesystem writes and incomplete build evidence. It also
contains negative filter self-tests. The runner captures full-package race tests,
nine separate profile/GC-trace/time runs, traced forced-RAM consumer replay,
12 `CGO_ENABLED=0` target builds, native 32-bit tests and a production CLI build.

Final implementation capture: `/tmp/vaultic-phase35-m2-KnXW3w`, Go 1.27.1 on
Linux/amd64, four runtime CPUs, default GC settings. Each cardinality is repeated
three times with reusable batch input, 8-byte keys and 64-byte values. Exact
capacities and all runtime/GC/allocated-byte endpoints are retained in
`analysis.json` and raw logs. CPU/heap profiles and decoded tops are retained.

| Entries | Charged Retained Bytes | Live Heap Delta Bytes | Retained Object Delta | Scannable Heap Delta Bytes |
| --- | ---: | ---: | ---: | ---: |
| 4,096 | 617,776 | 502,152-507,472 | 76-82 | -3,328 to 3,384 |
| 65,536 | 9,293,248 | 7,427,400-7,432,832 | 384-391 | 7,224-16,400 |
| 262,144 | 36,923,584 | 29,484,616-29,484,744 | 1,368-1,370 | 48,016-51,320 |

At the largest cardinality, 289 byte chunks and 1,024 node chunks retain
18,939,904 arena bytes, 10,485,760 index bytes and 45,056 descriptor bytes.
Reservation peak is 36,925,440 bytes. Object and pointer scanning growth track
chunk count, not individual entries. After close and an explicit benchmark-only
GC/scavenge, used budget is zero and heap is within 15,376 bytes of baseline.
Peak process RSS across the nine runs is 19,188-48,784 KiB. These are isolated
small-scale layout checks, not a throughput SLO or large-heap GC approval.

The separate unprofiled forced-RAM `strace -f -e trace=%file` process completes
all three encrypted fixtures. It performs no create/write/read-write open,
mkdir, rename, unlink or truncate, and accesses no working database paths.
Normal executable/runtime read-only filesystem access is not claimed absent.
Capture outputs and profiling files belong to the harness, not the RAM store.

The original evidence manifest SHA-256 is
`1a5ae8c62b5c59d27c2bbdcae9e98068f6594b748407e4ddeb81e40d1adb0a1c`.
Final-source and commit seals supplement the original manifest without replacing
it. Evidence is disposable local data, not an authoritative repository artifact.

## Exit Review

Every M2 requirement was reevaluated after failed-growth, shared-writer and
deterministic queued-read tests were added. Full RAM/KV conformance and race
checks pass; low-budget and oversize gates pass; capacity, allocation, object,
GC and release probes agree with the chunked pointer-light representation;
forced-RAM fixtures have zero working-state filesystem access. No M2 defect is
deferred. Production integration, aggregate admission/fallback, RSS/GC pressure
controls and realistic large-scale performance approval remain M3-M6 work.