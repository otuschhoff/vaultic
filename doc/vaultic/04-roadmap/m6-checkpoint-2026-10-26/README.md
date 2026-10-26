# M6 Next-Step Implementation And Negative Screening Checkpoint

Date: 2026-10-26. Full M6 acceptance remains unmet.

This checkpoint preserves the isolated implementation, controller repairs and
measured negative result. It does not enable the rejected optimization in the
active source tree, change defaults, or certify a completed import/check/backup.
Existing local edits to the main roadmap and schema-store implementation/tests
are deliberately outside this commit. In particular, the local roadmap edit
removes earlier evidence and is not silently incorporated or reverted.

## Preserved Implementation

`source.patch` is the exact 15-file patch against validated source base
`2c5061887600ac31c4a4e76c4cc065915b6644dc`, not the later WIP HEAD. It includes
the earlier accounted tree cache, mechanism telemetry and exporter fixes, plus:

- Linux-only, snapshot-scoped cached-pack descriptor reuse: at most 64 retained
  descriptors under a 512-KiB reservation, independent positional reads, pinned
  active readers, bounded LRU eviction, save/remove invalidation, and deferred
  release. Denied/unaccounted policies fall back. Tree-cache reservation priority,
  debt batch sizes, traversal, durability semantics and native engine stay intact.
- A deterministic full-catalog component diagnostic that establishes all 10,170
  indexes, 419,853 packs and 380,166,821 blobs before measurement. It inspects all
  143 snapshot roots, then uses a bounded equal-work corpus, not acceptance caps.
- Separate loader, pure JSON decode, diagnostic fact/debt encoding, native write,
  and combined pipeline measurements; five paired rounds alternate order.

The archived Python files are evidence, not installed commands. The original
`nextsteps.py` is the captured controller. `nextsteps-v2.py` is the separately
sealed post-capture repair: observer coverage/liveness must precede a positive
gate, and RAM additionally requires a completed receipt binding the gate,
observer stream, controller and launch hashes after observer cleanup. It does
not fabricate such a receipt for this rejected candidate. `pilot.py` preserves
fsynced failure summaries, closed-console handling, scope-error reporting and
fresh-cell enforcement. `observer-v2.py` retains allowlisted component rates
alongside process CPU/RSS/IO and pressure observations.

These scripts contain operator-specific paths and assume a private authorized
runtime root. Do not run them in this documentation directory. Reproduction
requires a fresh isolated tree from the base above, the archived patch, original
frozen inputs and native binary, explicit private paths and newly generated pins.
Do not reuse an earlier partial database or treat archival seals as a new launch
authorization. Original binaries and large raw logs remain in the private root.

## Verified Outcome

The underlying diagnostic passed in 7,676.996 seconds with combined CLI/native
RSS peaking at 59,748,937,728 bytes. Its outer exit 1 was intentional rejection
by the material-throughput gate, not a test timeout. Source preload took
2,602.440 seconds; native catalog preparation took 5,033.229 seconds.

Each combined pipeline round processed the same 512 trees and 35,038 nodes,
writing 70,076 debts in fresh namespaces. All canonical node digests matched;
45 selected debt key/value samples matched after native reopen. This is not a
complete integrity check. Each loader round read 53,685,387 plaintext bytes in
1,536 calls; these are not physical NFS bytes.

| Pair | Pipeline Speedup | Loader Speedup |
| --- | ---: | ---: |
| 1 | 1.0967x | 1.2056x |
| 2 | 1.0887x | 1.4337x |
| 3 | 1.1009x | 1.1978x |
| 4 | 0.9919x | 2.4393x |
| 5 | 1.0970x | 1.2147x |

Median paired pipeline speedup was 1.0967x, below the required 1.25x. One pair
regressed; none reached 1.20x. Median loader speedup was 1.2147x, below the
required 1.50x. No full RAM, KV, auto, check, init or backup workflow followed.

Median pure decode time was 0.22375 seconds; decode plus diagnostic fact hashing
and debt encoding was 0.55819 seconds; native write time was 0.55063 seconds per
70,076 puts. Decode is included in the second measurement: do not sum these as
exclusive wall partitions. The pipeline includes diagnostic hashing beyond
ordinary import. The short warm breadth-first corpus and shared storage/cache
limit generalization. Catalog population does not prove every native page was
resident in RAM, and a warm single-file microbenchmark is not import speedup.

All 30,324 RSS samples reconcile with the reported peak. The independent observer
retained 1,534 records, started before source preload, covered the measurements,
and had a largest interval of 5.030 seconds with no unavailable-process samples.
The workload unit is inactive, no owned processes survived, production PID 522
and its executable hash stayed unchanged, and all launch pins were reverified.

## Verification And Evidence

The five affected Go package suites and scoped vet passed. Snapshot race tests
included both real native reopen branches without skips; reader tests covered
range bytes, invalidation, pinning, eviction, concurrent offsets, cancellation,
fallback and charge release. Runner/observer/original-controller self-tests
passed 10/5/7 cases; revised-controller tests passed nine cases, including
missing/gapped/wrong-identity observations and incomplete/changed receipts.

The checkpoint manifest verifies the archived files against their private-root
source bytes. The following original seals are retained as evidence references,
not self-contained manifests that can be checked from this directory:

- Private root: `/volume2/NASDA2/vaultic-test-m6/pilot-nextsteps-kxnonzma`.
- CLI SHA-256: `28e5924403fea424ae0165e41c4f07fc650772dc1be930e9d9f6df9ee95a5403`.
- Native SHA-256: `2e69693454611fdc64c3b03ef633dc87f70151032c5c96f82cabf443533aa8bf`.
- 41-input launch seal: `004e93bf309b7a315402c966efe3da96fc37eaf976bea0cba01e5deefc498367`.
- Authorization seal: `9973151204eee475d370d45010d6f3009a721f2723c9a969d63deae8d5f02c03`.
- Controller revision: `ed63d00e8124c7523913c3dbbb3ba6e65f9a3bc19136cb2e300dc9bc50a16771`.
- 14-input outcome seal: `81a7313f3f4f5fa7a33d1c6add3c8cf74001b12cd35a5f4be537444b0f050e89`.

The historical missing summary/exit 120 cause remains unproven; reproducing and
repairing a broken-output failure path does not establish that historical cause.

## Next Steps

1. Do not promote this optimization, lower the gate, or launch another full-length
   workflow on the strength of its cache microbenchmark.
2. Make the next bounded benchmark more representative across snapshots and tree
   sizes. Keep equality validation outside the timed path where possible, while
   retaining exact debt facts, accounting, cancellation and native reopen checks.
3. Discriminate real debt encoding, native submission and native insertion costs.
   Prototype against the strongest measured bottleneck rather than another cache
   heuristic. Require reproducible paired pipeline gain, not overlapping timers.
4. Separately pin any new candidate and use the revised observer-completion gate.
   Require median pipeline speedup >=1.25, every pair >=1.10, four pairs >=1.20,
   loader median >=1.50 and every loader pair >=1.25 for a reader-path candidate.
5. Only after the gate passes, run fresh RAM import, index check, repository check,
   init and the complete backup. Stop at the first failed action. Evaluate RAM
   before KV/auto; no independent backup or automatic identical rerun matrix.

Preserve all 143 snapshots and all 52 backup sources with exact exclusions,
especially both `.snapshot` patterns, marker exclusions and one-file-system
selection. Keep originals kernel read-only and all writes private. Limits remain
200 GB combined RSS sampled every 250 ms, 160 GiB managed policy, default 8 GiB
scratch, 12 hours per import/check and one week per backup, with no swap, GC tuning
or production changes. Keep the shared NFS reserve at 57,481,929,687,040 bytes.