# Phase 35 M1: KV candidate selection

[Phase 35](phase-35-ram-working-memory-and-kv-fallback.md) | [M0 contract](phase-35-m0-contract.md)

Status: complete. Select bbolt v1.5.0 for the bounded fallback adapter in the
following milestones. Production remains on Pebble; this is not default-rollout
or M5 performance approval. Badger v4.9.6 is rejected for close-cause loss.

## Preregistered M1 Gates

All three engines share the same adapter, caller-owned inputs, copied outputs,
one-MiB atomic write bound including 16 bytes/record framing allowance,
32-KiB maximum keys, and eight-MiB/1,024-entry bounded scan pages. Transactions
never escape the adapter. Context cancellation bounds writer admission and
record copying, not an already executing kernel I/O or the commit point.
Close serializes with active operations and preserves its first error.

Prototype roots must be new, empty, private directories, never reopened as
truth. Read values are bounded before copying, and error returns contain no
partial scan results. Unexpected engine errors have a stable `ErrBackendIO`
classification with the original **library-returned** error joined as cause;
known cancellation and batch bounds remain distinct. Native errno identity
cannot be manufactured when a dependency has already formatted it away.
Private mode checks are native-Linux evidence, not Windows ACL guarantees;
platform custody helpers and production integration remain M3 work.

Candidates: Pebble v1.1.5 control, bbolt v1.5.0, Badger v4.9.6. All use
relaxed sync for disposable state, without disabling growth/metadata safety
barriers. Engine-specific buffering/compression differs and must be recorded;
this is an adapter evaluation, not identical engine internals. Inputs/values
and transaction boundaries are identical. No new production caller uses these
adapters in M1. The authoritative daemon, checkpoints and publication remain
unchanged. Specialized encrypted checker sorts are not generic KV consumers.

Selection requires shared semantic and concurrency conformance, real ENOSPC and
permission failure evidence, cancellable admission, close-error propagation,
owned cursor results, confidentiality of fixture files, and supported pure-Go
builds. All repeated workload input/result digests must match across engines.

On this isolated slow VM-vdisk host, M1 safety gates are peak process RSS below
512 MiB, observed store mappings below 512 MiB, and logical scratch below
`8 * live encoded payload + 64 MiB`. These are spike rejection limits, not
aggregate working-memory enforcement or a production capacity claim.
Write p99 must stay below 100 ms, get p99 below 10 ms, and bounded scan p99 below
500 ms in each fixture. All repeats are retained, not only a best run. A failed
absolute safety/latency gate rejects selection; a candidate that passes but
regresses workload timings versus Pebble requires an explicit workload-specific
tradeoff record. Prefer lower retained/mapped memory, GC and I/O cost over a
small noisy timing advantage. Later M5 paired GC/assist and production latency
gates are not replaced or weakened by this small-scale M1 spike.

## Fixture and Codec Contract

The M0 source fixture shapes are retained: 8,192 overlay locations with 64-byte
HMAC keys and 84-byte AES-GCM values; 8,192 marker keys with four churn rounds;
513 directory records, mostly empty lists and one 512-name listing. Directory
stress adds sixteen records with 4-KiB, 64-KiB and 512-KiB values. This is a
deterministic replay of captured shape/size anchors plus explicit tail stress,
not a claim to possess a production key/value histogram.

Marker and directory exact keys are HMAC-tokenized and values authenticated
under fixture-specific ephemeral AES keys, including kind/version/key as AAD.
Nonce uniqueness covers every update. Overlay retains its two-token prefix and
distinct-location semantics; cursor ordering is on encoded keys, never claimed
to preserve plaintext path ordering. Tests decrypt final values and verify
complete prefix pagination and exact final state. Fixture keys are synthetic;
no repository, host secret, credentials or production daemon is accessed.
Production codec/key custody integration remains M3 work.

## Evidence And Decision

Final capture: `/tmp/vaultic-phase35-m1-x0f9gC`; original `SHA256SUMS` digest:
`57da22f61c2976e782c6a78c74c567867856f9eaabd7ccdaa92be311cc3ccd83`.
Run `bash helpers/phase35-m1/run.sh` as root on the Linux test host to reproduce
the isolated UID/syscall probes. Root is required only for the permission child
and tracing; this does not access production data or services. Three rotations
of candidate order produce 27 workload records, 27 CPU and 27 heap profiles,
decoded tops, time/RSS logs, sampled mappings/scratch, runtime/GC endpoints,
process I/O and all raw operation latencies. Final source/documentation and
commit identity are sealed separately alongside the original immutable capture.

### Pinned Implementation And Dependency Review

| Engine | Exact Pin / License | Effective Configuration |
|---|---|---|
| Pebble control | v1.1.5, BSD-3-Clause | 8-MiB cache, 4-MiB memtable, stop-writes threshold 2, `NoSync` batches; otherwise existing overlay defaults |
| Selected bbolt | v1.5.0, MIT | One private `state.db`, one bucket, one-second startup lock timeout, `NoSync=true`, `NoFreelistSync=true`; default safe growth synchronization retained |
| Rejected Badger | v4.9.6, Apache-2.0 | 16-MiB memtable, 2 memtables, L0 thresholds 2/4, 2-MiB base table, 8-MiB base level, 8-MiB block and index caches each, 16-MiB value-log file, 1-KiB value threshold, no compression, sync writes disabled |

Published release metadata resolved bbolt's release on 2026-06-03 and Badger's
on 2026-08-05; Go requirements are 1.25 and 1.24 respectively. The existing
project Go requirement remains 1.26.8; capture compiler is Go 1.27.1. Module
metadata/sums, complete compiled dependency graph, copied license texts and
successful `go mod verify` are retained. Added runtime dependencies are the two
pins plus Badger's Ristretto v2.2.0 and FlatBuffers v25.2.10+incompatible, both
Apache-2.0. No existing dependency version was upgraded. Additional checksums
cover candidate test dependencies; unrelated historical fork sums were retained.
No dependency types escape the adapter. Evaluation alternatives are not linked
into the CLI, since production does not import this package. M3 must keep
evaluation-only alternatives out of the production dependency path.

### Raw Tradeoffs

The table contains medians of three per-run nearest-rank p99 measurements.
Put samples are only 2/4/5 batches per run, so their p99 is a small-sample
observed maximum, not a production tail estimate. All repeats/raw arrays remain
available. Workload encoded final payloads are 1,212,416 bytes (overlay),
499,712 bytes (encrypted markers), and 3,013,181 bytes (directory tail stress).

| Engine / Workload | Put p99, ms | Get p99, us | Scan p99, us | Peak RSS, bytes | Peak Store Mapping, bytes | Peak Logical Scratch, bytes |
|---|---:|---:|---:|---:|---:|---:|
| Pebble / overlay | 16.048 | 7.848 | 42.746 | 36,691,968 | 0 | 1,238,716 |
| bbolt / overlay | 54.631 | 8.246 | 29.623 | 42,000,384 | 4,194,304 | 4,194,304 |
| Badger / overlay | 15.027 | 9.790 | 20.500 | 52,940,800 | 69,558,272 | 69,554,859 |
| Pebble / markers | 24.810 | 3.478 | 88.534 | 45,592,576 | 0 | 4,078,902 |
| bbolt / markers | 44.412 | 10.451 | 21.437 | 42,844,160 | 4,194,304 | 4,194,304 |
| Badger / markers | 17.200 | 7.897 | 53.260 | 68,890,624 | 70,782,976 | 70,782,194 |
| Pebble / directories | 13.097 | 107.433 | 609.058 | 47,480,832 | 0 | 5,507,094 |
| bbolt / directories | 10.401 | 34.366 | 306.662 | 43,479,040 | 4,194,304 | 4,194,304 |
| Badger / directories | 2.690 | 36.317 | 1,039.361 | 50,233,344 | 68,202,496 | 68,201,457 |

Median process write-byte deltas (overlay/markers/directories) are Pebble
1,261,568 / 4,169,728 / 5,545,984; bbolt 4,083,712 / 4,034,560 / 3,317,760;
Badger 2,871,296 / 5,275,648 / 3,129,344. Sparse mapped-file lengths are not
physical bytes written. Process counters include identical validation/sampling
instrumentation; warm-after-write reads do not represent cold production reads.
Store mappings are virtual extents, not independently measured mapped RSS.
Peaks sampled every 10 ms plus operation boundaries can miss short excursions;
OS maximum RSS is additionally retained in the time logs.

Median observed GC CPU deltas in seconds are Pebble
0.011671 / 0.022016 / 0.008363; bbolt 0.027372 / 0.030092 / 0.007007;
Badger 0.015253 / 0.021078 / 0.004879. Runtime before/after cover open through
close, including validation and resource observation, but exclude fixture
construction; CPU profiles cover the whole subprocess. Process CPU ticks use
the retained `CLK_TCK` rate and are quantized, so these tiny fixtures do not
establish M5's two-percentage-point GC/assist-share gate. Assist/allocation/
scannable-heap/pause data are retained rather than inferred from RSS alone.

**Decision:** bbolt passes every M1 safety, semantic, growth, confidentiality,
close, and build gate. Its bounded cursor/transaction lifetimes and much smaller
mapping/preallocation floor than Badger make it the selected fallback candidate.
Directory large-value reads/writes and marker scans favor bbolt on this vdisk.
However overlay writes are about 3.4 times Pebble's sampled put tail, overlay
write bytes about 3.2 times control, and overlay/churn GC CPU is higher.
These are explicit regressions, not evidence of a universal performance win.
M3/M5 must measure batching, churn and allocation mitigation on real-scale
workloads; a failure of the frozen M5 gates blocks rollout/default changes.
M1 does not authorize changing those gates or recommending a 100-GB RAM heap.

Badger passes the size/latency spike limits and returns initial ENOSPC, but its
default `y.Wrap` in v4.9.6 uses `%+v` instead of `%w` on close, destroying the
original errno cause. The real manifest-close EIO probe fails `errors.Is(EIO)`.
The adapter preserves the returned library error and classifies it as I/O, but
cannot restore the missing cause without string inference or a fork. This
rejects Badger for the shared close-failure contract, in addition to its larger
mapping/scratch floor. No dependency debug-mode/global setting or patch is used
to make it pass. Its failed probe is deliberately retained, not a passing test.

bbolt also formats its file-resize cause to text. A real isolated `RLIMIT_FSIZE`
growth probe verifies classified I/O failure, unchanged committed seed data and
invisibility of the rejected batch, with kernel EFBIG trace evidence. It does
not claim that native errno survives that growth wrapper. M4 must reserve growth
capacity before allocation and preserve the original library error when generic
I/O has no reliable native errno; reason labels must not be guessed from text.
The shared adapter's generic I/O classification repairs this gap without changing
the pinned library, and its close EIO cause remains independently preserved.

### Exit Checklist And Reevaluation

| M1 Requirement | Verified Evidence |
|---|---|
| Narrow adapters and consumer replay | Same batch/get/paged scan interface; exact operation-stream and final-state digests across all three engines/repetitions; overlay duplicate prefixes, encrypted marker churn and directory tails |
| Concurrent readers/writes and growth | Four-lane shared conformance writes/reads/scans, 16-MiB growth stress, cloned inputs/results, empty versus missing, overwrite ordering and strict size bounds |
| Mappings, memory and I/O | Raw runtime/CPU/GC snapshots, OS RSS, sampled store mappings/logical scratch, kernel process read/write bytes and syscall counts, CPU/heap profiles |
| Disk full and permissions | Path-scoped real ENOSPC on candidate creation, unprivileged UID/GID 65534 denial; Pebble's fatal MANIFEST ENOSPC retained as control behavior |
| Cancellation and close failures | Deterministic pre-commit rollback, cancellable queued writer, drain-versus-new-admission test, repeated-close error identity, real file-close EIO; Badger rejection retained |
| Growth failure | Selected bbolt's isolated child file-size cap produces traced EFBIG; generic I/O classification, failed-batch invisibility and acknowledged-state preservation pass; no host/daemon limit changed |
| Confidentiality and ownership | HMAC/AEAD codec tamper/AAD tests, private fresh roots, no plaintext directory names in closed files; oversized backend values rejected before copying |
| Pure-Go matrix | `CGO_ENABLED=0` test binaries for Linux amd64/arm64/386/arm, Darwin amd64/arm64, Windows amd64/arm64, FreeBSD amd64/arm64, OpenBSD amd64/arm64; native CLI build; native amd64 tests and actual Linux/386 conformance execution |
| Existing-package portability | Existing schema `MaxUint32` comparison and two large telemetry formatting arguments now use explicit unsigned widths; full schema/telemetry suites and native 32-bit conformance pass; serialized formats/messages unchanged |
| Reproducible acceptance | Full adapter race suite; complete capture analysis, all profile decodes, fault/build evidence and empty scratch; independent rerun and checksum verification |

Cross-build success is not native runtime/ACL validation on all twelve targets.
Platform custody and filesystem behavior require native integration tests in M3;
the new adapter remains a prototype rather than a production replacement.
All M0/M1 requirements were reevaluated after lifecycle/admission, foreign-root,
read-bound, input-stream completeness, portability, close and growth findings.
No remaining M1 implementation findings are deferred; documented dependency and
spike limitations constrain later milestones instead of masquerading as solved
production memory/performance issues. M2 is unblocked for the selected candidate;
M2-M6 remain unimplemented.

Prior captures are retained: `LlLNf3` stopped on the original 32-bit build
blockers; `H1onMt` and `JLCJhs` are superseded by the full final growth/error
contract capture. Early unscoped write injection hit Go's eventfd, not storage;
subsequent probes restrict faults to engine paths. These failures caused repairs
and stronger checks, not relaxed safety thresholds.
