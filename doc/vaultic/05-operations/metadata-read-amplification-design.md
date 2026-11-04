# Metadata Read Amplification Reduction

Status: opt-in implementation and private correctness qualification; no production deployment.
Date: 2026-11-03.
Related contract: [backup readiness](indexdb-backup-readiness.md).

## Objective

Reduce repeated metadata fetching, decoding and decryption during complete
unchanged-source backups, while preserving repository availability checks,
snapshot correctness, cancellation, memory admission and durability.

The user-facing performance objective is at least 100 observed/scanned entries
per second on representative unchanged `fme_cae1` input. This is a full-backup
objective, not a blob-lookup benchmark objective. Reducing RPC count or hiding
latency alone does not prove reduced read amplification.

A candidate qualifies only if it meets all correctness gates, improves sustained
full-crawl throughput, and reduces the measured work targeted by its hypothesis.
Use the lowest useful concurrency that meets the objective within the approved
resource envelope. Do not select a concurrency peak from a subsecond screen.

## Evidence and Unknowns

The retained 100,000-entry unchanged pass lasted 2,266.464 seconds and achieved
44.122 entries/second. All 89,300 regular files were unchanged; restore and
encrypted reopen passed. Reconciliation waited for prepared work for 96.8% of
the main interval. Blob-size validation issued 78,718 RPCs for 85,078 handles,
averaging 1.08 handles/call and 26.121 ms/call. The CLI size cache had no evictions;
that does not establish that daemon metadata caches were effective.

The metadata object-store wrapper accumulated 2,033.370 worker-seconds of `get`
work and returned 952,145,533,531 logical plaintext bytes to SlateDB. It surrounds
cache access and encryption: these are neither exclusive wall-time shares nor
physical NFS traffic measurements. The current daemon uses 128 MiB of engine
metadata cache and 512 MiB of engine block cache.

An authenticated singleton lookup screen on a separate historical clone used
512 present and 128 absent keys in fixed seeded order. Fresh-engine runs took
10.846, 1.154 and 0.461 seconds at 1, 8 and 32 callers respectively. With one
caller, paired means were 16.907 ms end-to-end, 16.477 ms inside storage,
0.012 ms elsewhere in the handler and 0.417 ms outside the handler. At 32 callers,
median and tail latency increased while aggregate throughput still improved.
This short sequential screen establishes overlap, not a saturation point or
production throughput. Host/NFS caches could already be warm.

The verified 10k unforced retry used a four-RPC cache limiter shared by size
and location lookups. Size calls peaked at two in flight; size-slot waiting
totaled 10 ms. This counter does not cover location-slot waits. Record caller
parallelism, the effective cache limiter, exact size-call peak and sampled
overall RPC concurrency separately; the 32-caller direct screen bypasses this
cache limiter and cannot establish the backup path's concurrency knee.

Retained evidence roots:

- `/volume2/NASDA2/vaultic-test-m6/unchanged-crawl-fme-cae1-20261102-ten-minute`
- `/volume2/NASDA2/vaultic-test-m6/read-wait-fme-cae1-20261103`
- `/volume2/NASDA2/vaultic-test-m6/force-compare-fme-cae1-20261103-10k`

The first root's outer test failed after successful measurement/restore/reopen
because of a stale-client cleanup defect. Its failed receipt remains preserved;
the ownership fix passed a focused native test. Never rewrite it as a full pass.
The third root retains the requested comparison's first failed attempt: its
unforced content checks passed, but an overly strict harness guard rejected
1,449 bytes in four new ancestor-directory tree blobs. `data_added` includes
tree metadata, not just file content. The corrected fresh-leg retry is under
that root's `retry` child and uses the qualified parent/input read-only. Populate
its accepted results only after both exact-scope legs and outer cleanup succeed.

The retry is now accepted: unforced main/command time was 207.267/224.284 seconds
at 48.247 entries/second; forced was 47.484/66.944 seconds at 210.597 entries/second.
Both had zero new data blobs and exact restore/reopen/source verification.
Unforced/forced writer prepared-work waits were 199.777/38.118 seconds, and
logical metadata work was 112.860/98.296 GB. The 4.365-fold main-time contrast
primarily demonstrates overlap rather than elimination of amplification.
Neither leg was publication-dominated on this fixture. All 49 evidence hashes
verified; one short sequential pair does not meet sustained acceptance.

Captured paired header totals include four/three startup reads because the
critical reporter subtracted calls but not the newly added timing fields. The
report labels those means cumulative; unrecorded startup durations are not
invented. The reporter now subtracts all cumulative timing counters from the
same baseline and has a focused race-tested regression. Preserve raw captures.

Unresolved representative-workload questions:

- Which repeated ranges are SST indexes, filters, data blocks or other objects?
- How much time is raw fetching, crypto queueing, authentication/decryption,
  engine decoding, and cache admission or locking?
- Which ranges miss decoded metadata, block, encrypted-object or decrypted caches?
- How much backend traffic reaches NFS, and how much reaches physical storage?
- How do these answers change with concurrency, real batching and cache sizing?

## Implemented Controls

The implementation retains existing defaults. Content readers, unchanged-file
validators and actual metadata RPCs have separate concurrency controls.

| Control | Default | Behavior |
| --- | --- | --- |
| `--unchanged-read-concurrency` | 0 | Synchronous unchanged validation; 1-128 enables a separate bounded pool |
| `--metadata-read-concurrency` | 4 | Shared size/location RPC limiter; maximum 128 |
| `--metadata-batch-delay` | 0 | Optional cross-file coalescing; maximum 5ms and 256 unique handles/batch |
| `--metadata-exact-catalog` | false | Complete pinned-session ID/type/size scan into encrypted admitted working storage |
| `--metadata-daemon-meta-cache-bytes` | 0 | Startup override; zero preserves the daemon's metadata-cache default |
| `--metadata-daemon-block-cache-bytes` | 0 | Startup override; zero preserves the daemon's block-cache default |
| `--metadata-daemon-multi-get` | false | Enables genuine engine MultiGet at startup without enabling bulk-import WAL behavior |

Batching and catalog options require `--metadata-on-demand` and its existing
scratch/cache or shared-working-policy prerequisites. Startup overrides do not
reconfigure an already connected daemon. The CLI size-cache budget remains
distinct from both native engine caches.

Validation jobs and completed pending futures remain charged until completion
and ordered consumption respectively. Cancellation and consumption release
future charges exactly once. Workers preserve source ordering and ordinary
missing-content fallback, recheck source metadata through the configured
filesystem before reuse, and never allow the error callback to suppress a
metadata availability failure. Content-reader queues close only after validator
handoffs finish. The unbuffered validation queue bounds active jobs and the
single traversal waiter; admission also bounds retained results.

Coalescing deduplicates pending handles, preserves requested ordering and
duplicates, and flushes on the configured deadline or the batch bound. Joined
requests share the leader's RPC slot instead of consuming the whole limiter.
Individual caller cancellation does not cancel an owner-session fetch. Batch
buffers remain admitted until all consumers finish, and failed requests never
become cached negatives.

The exact catalog scans `b:` records in pinned 64-record pages and validates the
session before and after loading. No partial scan activates positive or negative
answers. ID and blob type remain distinct; zero sizes are valid. Keys are HMAC
tokens and values are AES-GCM authenticated sizes in the existing RAM/KV/auto
working store. Local writes override the old view, including an exact negative.
Corruption, budget exhaustion, cancellation and dead sessions fail closed.
Catalog entry count, load duration and working-store resources remain reported;
startup and total-command cost must still include the complete load.

Native optional encryption-read telemetry now exposes requested plaintext and
rounded ciphertext bytes, origin calls/bytes/time, encryption-header decoding,
header-cache hits/misses/evictions, crypto queue/service time and read classes.
Crypto queue timing includes semaphore and executor scheduling. Main critical
frames subtract the same valid daemon baseline; missing, partial, null or reset
telemetry is unavailable, not an invented zero. Location-slot wait time is also
reported separately from location RPC elapsed work.

Scope limits: ciphertext-origin reads can be served by an encrypted cache and
are not physical NFS/HDD traffic. Encryption counters can include wrapped WAL
origins and other clients. Header decoding is not full SST decoding; no claim
is made to isolate SST decode or cache-lock time. No trusted plaintext cache,
compaction change, metadata relocation or production configuration is introduced.

## Implementation Qualification

Focused race tests cover overlapping unchanged validation, exact restored
content, missing blobs, source mutation, fatal lookup errors, cancellation,
hardlinks, symlinks, exclusions and drained working reservations. Cache tests
cover enabled batching, shared caller cancellation, Close, duplicate ordering,
bounded batches, failed/partial catalog scans, authenticated malformed sizes,
RAM/KV/auto storage, budget exhaustion and write-overlay precedence. Native
read-session tests exercise MultiGet off/on and a catalog excluding post-session
writes. Injected origin and crypto delays pass two native attribution tests.

The affected index, global and backup-command suites pass race detection.
The full archiver race suite passes with root DAC-bypass capabilities dropped;
the unrestricted root run cannot exercise its unreadable-file permission cases.
Eight private encrypted native workflow cases pass exact restore and reopen:
metadata caches 128/256/512/1024 MiB, coalesced RPC limits 4/8/16, and exact catalog.
These 75-file fixtures check correctness and settings, not throughput gains or
the useful cache/concurrency knee.

`helpers/phase35-m6/metadata_read.py` supervises fresh named 10k `parallel`,
`coalesced` and `catalog` modes using the previously qualified parent/input
read-only. Each captures unforced and forced legs in independent checksum-verified
clones, retains exact restore/reopen/source checks, binary/source pins and process
observations, and bounds RSS, time, fixture bytes and shared free space. Run it in
a private mount namespace with core dumps disabled. It refuses existing evidence
and production activation; captures needing kill fallback are rejected.

The coalesced candidate's supervised 10k pair completed successfully. Its
settings were 16 unchanged validators, eight actual metadata RPC slots, 1ms
batch delay, engine MultiGet enabled, 128 MiB metadata cache and 512 MiB block
cache. Content readers remained 16 and publication groups remained 64.

| Mode | Main Seconds | Command Seconds | Entries/Second | Size RPCs / Handles | Logical Metadata GB |
| --- | ---: | ---: | ---: | ---: | ---: |
| Historical unforced control | 207.267 | 224.284 | 48.247 | 8180 / 9210 | 112.860 |
| Coalesced unforced candidate | 69.457 | 87.463 | 143.973 | 7695 / 9210 | 205.138 |
| Historical forced control | 47.484 | 66.944 | 210.597 | 9210 / 9210 | 98.296 |
| Coalesced forced candidate | 18.158 | 38.298 | 550.732 | 1613 / 9210 | 105.751 |

Both candidate legs observed/scanned all 10,000 entries, processed 9,209 regular
files with the expected force/parent semantics, stored zero new data blobs, and
passed exact restore, encrypted reopen, unchanged-source and idle-writer checks.
Their 1,449 added bytes were ancestor-tree metadata. The outer capture accepted
cleanup, and a subsequent process check found no candidate daemon survivors.
Peak combined owned RSS was 42,029,805,568 bytes; the 1,702.534-second outer time
includes clones, checksum verification and restores, not sustained main backup.

Unforced batching averaged only 1.197 handles/RPC; forced averaged 5.710. Actual
size RPCs peaked at eight in both legs. Writer prepared-work waits were
60.028 seconds (86.4% of main) and 9.404 seconds (51.8%). Writer batch time was
7.759/6.952 seconds; it matters more after the forced path's read waits shrink.
Unforced encryption counters recorded 141.865 origin worker-seconds, 15.489
crypto queue worker-seconds and 82.466 crypto service worker-seconds. Forced
recorded 126.748, 234.734 and 64.672 respectively. These overlap across requests
and are not additive wall-time shares. Header-cache evictions were zero.

**Verdict: correctness accepted; amplification candidate not qualified.**
The descriptive main-time ratios are 2.984 and 2.615 versus the historical
control, but logical metadata work rose by 81.8% and 7.6%. This is evidence of
latency overlap, not a measured amplification reduction. The combined setting
change and historical, non-interleaved control cannot isolate causes. Do not
promote this bundle, raise defaults, or call its short screen sustained acceptance.

Evidence root:
`/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-10k-coalesced`.
Its immutable `verified-comparison.json` distinguishes capture acceptance from
amplification, sustained and deployment qualification. All 11 launch pins and
38 root-relative `METADATA-READ-SHA256SUMS` entries verified. The earlier control
remains sealed and unchanged. Representative separate-variable cache/caller
screens, catalog loading cost and naturally ten-minute repeated full-crawl gates
remain open; correctness fixtures do not substitute for those measurements.

### Forced DB Resource Screen

The next forced-only screen holds content readers at 64 and metadata RPCs at 32,
except for one explicitly isolated 64-RPC probe. It uses the same frozen 50k
source, verified parent and measurement-pinned CLI/native binaries. No production
or NFS configuration changes are permitted.

| Profile | Crypto Workers | Metadata / Block Cache | Batch Delay | RPC Limit |
| --- | ---: | --- | ---: | ---: |
| control | 8 | 128 MiB / 512 MiB | 1ms | 32 |
| crypto32 | 32 | 128 MiB / 512 MiB | 1ms | 32 |
| meta2g | 8 | 2 GiB / 512 MiB | 1ms | 32 |
| block8g | 8 | 128 MiB / 8 GiB | 1ms | 32 |
| batch5ms | 8 | 128 MiB / 512 MiB | 5ms | 32 |
| rpc64 | 8 | 128 MiB / 512 MiB | 1ms | 64 |
| control-end | 8 | 128 MiB / 512 MiB | 1ms | 32 |

The beginning/end controls bracket run-order drift; the sequence is not a
replacement for repeated mirrored comparisons. Every non-control changes one
variable. The crypto setting is explicit rather than inferred from CPU count.
The authenticated launcher forwards it, and the supervisor requires observed
crypto-thread count plus cache/crypto environment from the owned native process.
Only those nonsecret settings are retained; credentials are not logged.

Each cell runs one forced full crawl from an independent checksum-verified
clone, then verifies exact restore, encrypted reopen, unchanged source, idle
writer and zero new data blobs. Existing private/read-only, 200 GB RSS,
350 GiB fixture, disk-floor, timeout and SIGINT-first cleanup guards remain.
Evidence roots append `-forced-db-{profile}` to the accepted 50k root.
Status: completed; all seven correctness captures and live-setting checks passed.
The analyzer sealed 27 files for the start control and 25 for each other cell;
all 177 checksums independently verified from their respective roots. There were
no candidate daemon survivors. Peak combined owned RSS stayed at 40.178--42.460
GB, well below the unchanged 200 GB ceiling.

| Profile | Main / Command Seconds | Entries/Second | Logical Metadata GB | Paired Storage Mean ms |
| --- | ---: | ---: | ---: | ---: |
| control | 74.573 / 93.896 | 670.484 | 451.079 | 39.491 |
| crypto32 | 78.982 / 100.099 | 633.057 | 458.435 | 36.490 |
| meta2g | 49.630 / 70.142 | 1007.446 | 1.132 | 1.772 |
| block8g | 74.450 / 92.569 | 671.592 | 452.873 | 38.889 |
| batch5ms | 85.599 / 105.013 | 584.117 | 439.509 | 40.177 |
| rpc64 | 76.941 / 96.727 | 649.847 | 451.629 | 39.343 |
| control-end | 76.589 / 97.261 | 652.838 | 463.024 | 40.666 |

The end control was 2.6% slower in throughput than the start control. The 2 GiB
metadata cache exceeded both controls by 50.3--54.3%, reduced main time by about
one third, and reduced logical metadata work by 99.75%. Native CPU fell from
1300--1344 seconds to 86.81 seconds; crypto queue/service work fell from about
1004--1061 / 261--262 worker-seconds to 1.621 / 6.688. This is strong evidence that
repeated hot metadata fetch/decrypt work controlled the read cost at 128 MiB.
`SlateDbTuning::cache` installs separate Foyer metadata/block caches through
`SplitCache`, so enlarging the 8 GiB block cache does not enlarge this metadata
cache. Direct metadata hit/eviction counters were not available in attribution;
the minimum cache capacity that fits this working set remains unmeasured.

The other screens distinguish symptoms from controlling causes. Crypto32 cut
queue time to 57.350 worker-seconds but increased service work to 435.449 and
native CPU to 1778.62 seconds; total throughput was 3.0--5.6% below controls.
Queue elimination alone was not a win. Batch5ms increased handles/RPC from
5.524--5.767 to 15.888 but reduced throughput 10.5--12.9%; fewer calls did not
justify the added wait. Block8g remained within the control range. Rpc64 peaked
at only 27 active size RPCs, with negligible admission wait and no throughput
gain. Do not combine these losing knobs into a presumed faster configuration.

For the next forced-only experiments, retain 64 readers / 32 RPCs, 8 crypto
workers, 2 GiB metadata / 512 MiB block cache and 1ms batching. This is a tested
diagnostic setting, not a changed default or production approval. At meta2g,
writer publication was 28.968 seconds (58.4% of main) and prepared-work waiting
14.780 seconds (29.8%). Publication batching is now the largest measured stage;
confirm the cache result and then vary publication group size independently.
None of these main intervals reached ten minutes or establishes repeated
sustained full-crawl qualification.

The immutable control `verified-db-screen.json` and each cell's
`DB-SCREEN-SHA256SUMS` retain complete measurements and live-setting evidence.
Do not rerun the create-only analyzer or overwrite sealed receipts.

Compare main/command throughput, metadata bytes/entry, batch density, RPC peak
and admission wait, paired storage latency, crypto queue/service, origin work,
daemon CPU and publication time. A faster result with unchanged work identifies
useful overlap; less work suggests batching/cache reuse, but does not establish
physical NFS savings. Cache-served origin work, overlapping worker-second sums
and unavailable full-SST decode attribution retain their existing limitations.
Neither resource headroom nor one short capture qualifies production defaults.
Unforced optimization remains deferred.

### Completed Forced Follow-Up Screen and 200k Scaling

The 23-cell forced-only follow-up completed with exact source, restore, encrypted
reopen, idle-writer, live-setting and cleanup acceptance in every cell. All 598
root-relative checksums and launch pins were independently verified before
changing the diagnostic driver. The create-only series evidence is under
`/volume2/NASDA2/vaultic-test-m6/forced-followup-fme-cae1-20261103`; each capture
has `verified-followup.json` and `FOLLOWUP-SHA256SUMS`.

Direct SlateDB cache counters are now available through the existing recorder:
`slatedb.db_cache.access_count`, with entry-kind and hit/miss labels. Concurrent
cache fetches retain SlateDB's own hit classification; errors are not misses.
These are cache-access counters, not physical NFS traffic or eviction counts.

| Metadata Cache | Entries/s | Metadata Hits | Metadata Misses |
| --- | ---: | ---: | ---: |
| 128 MiB | 640.4 | 1,661,516 | 188,758 |
| 256 MiB | 709.9 | 1,681,119 | 159,753 |
| 512 MiB | 818.4 | 1,732,727 | 83,413 |
| 1 GiB | 887.0 | 1,876,641 | 13,934 |
| 2 GiB, start | 975.3 | 1,926,836 | 327 |
| 4 GiB | 655.9 | 1,931,128 | 327 |
| 2 GiB, end | 754.1 | 1,941,228 | 327 |

The cache-fit boundary is between the tested 1 and 2 GiB settings for this
fixture. Two and four GiB had the same 327 metadata misses; 4 GiB showed no
additional work reduction. Throughput is not a causal cache-size ranking:
identical 2 GiB controls drifted from 51.268 to 66.305 main seconds.

Group-64 controls drifted from 75.093 to 51.349 seconds, while group-16 and
group-32 took 53.073 and 68.479 seconds. The automatic screen selected group 16
against the averaged controls, but it did not beat the ending group-64 control.
Do not call group 16 a qualified improvement. Hot-cache crypto and batch cases
took 50.629--53.304 seconds, while their identical controls took 57.717 and
49.732 seconds; these small apparent gains do not establish a winning setting.

The fixed-group-16 reader screen was more stable: 64-reader controls took
51.893 and 52.489 seconds; 96 and 128 readers took 50.562 and 49.003 seconds
(988.9 and 1,020.3 entries/s). The 128-reader result is a provisional scaling
candidate, not a repeated causal or production qualification.

Publication histogram export also has a population limitation: every forced
cell reported planning Count=0 and commit Count=5, totaling only
0.311--0.410 seconds. It does not account for the whole writer publication
stage; do not interpret it as proof that publication planning is free.

The user authorized a direct jump to 200,000 entries, explicitly skipping a
100k run. The guarded scale workflow freezes a new verified source subset,
creates a complete nonselective parent, then independently clones it for one
forced full traversal. Candidate settings are 128 readers, 32 RPCs, group 16,
2 GiB metadata cache, 512 MiB block cache, 8 crypto workers and 1ms batching.
The existing 200 GB RSS, 350 GiB fixture, 64 GiB source-byte and free-space
guards remain in force. The first attempt froze 200,000 entries and restored
23,520,039,140 source bytes, then failed a harness assertion that mistook its
source-freeze manifest for a bounded backup selection. No accepted parent
receipt or forced 200k measurement was produced. Its failed evidence is
preserved under
`/volume2/NASDA2/vaultic-test-m6/forced-scale-fme-cae1-20261103-200k`, with 23
artifacts verified by `FAILED-SHA256SUMS`.

The saved correction distinguishes a full frozen-source parent from a bounded
selection; all six crawl-mode regression cases pass under the Go race detector.
The end-to-end retry remains outstanding and requires fresh create-only paths
and a newly pinned driver. Source freeze, parent setup and restores do not count
toward the measured main interval. No short passes will be combined to claim
ten-minute sustained qualification. Production, defaults and NFS configuration
remain unchanged; unforced work stays deferred.

### Forced-Only Concurrency Screen

The follow-up optimization scope is forced backups only; unchanged-path work is
deferred. The initial mixed-mode concurrency sequence was interrupted via its
owned supervisor before any measured backup began. Its failed/aborted evidence
is preserved and is not a performance result.

The new screen reuses the exact frozen 50k input and verified parent from the
fivefold observation, rather than selecting a different source or rebuilding
the comparison binaries. Each cell gets an independent checksum-verified
repository/database clone, a fresh encrypted daemon and one forced full crawl.

| Profile | Content Readers | Metadata RPC Limit |
| --- | ---: | ---: |
| baseline | 16 | 8 |
| rpcs | 16 | 32 |
| readers | 64 | 32 |

Caches remain 128 MiB metadata / 512 MiB block, engine MultiGet stays enabled,
batch delay stays 1ms, and publication stays at group size 64. Unchanged
validator settings are not varied: forced files use the content-reader pool.
Fresh evidence roots append `-forced-concurrency-{profile}` to the 50k root.
The supervisor requires a forced-only schedule and rejects an unforced leg.
Existing ownership, read-only protected mounts, RSS, disk-floor, fixture,
timeout, SIGINT-first cleanup and exact restore/reopen/source checks remain.

Status: completed; all three forced-only correctness captures accepted. The
create-only analyzer verified the launch pins, fixed settings, source/parent and
force semantics, then sealed 27/25/25 files. All 77 root-relative checksums were
independently verified, and no candidate daemon survivors remained. Every cell
passed exact restore, encrypted reopen, source identity, idle-writer checks and
zero new data blobs. Peak combined owned RSS was 41.200/42.618/44.207 GB.

| Measurement | baseline 16/8 | rpcs 16/32 | readers 64/32 |
| --- | ---: | ---: | ---: |
| Main / command seconds | 105.077 / 125.392 | 96.424 / 116.856 | 79.823 / 98.967 |
| Entries/second | 475.841 | 518.542 | 626.388 |
| Throughput relative to baseline | 1.000 | 1.090 | 1.316 |
| Paired storage mean ms | 49.954 | 58.403 | 45.349 |
| Actual size RPC peak / limit | 8 / 8 | 32 / 32 | 26 / 32 |
| Handles/size RPC | 4.317 | 3.740 | 4.932 |
| Size slot wait worker-seconds | 371.534 | 0.633 | 0.032 |
| Crypto queue / service worker-seconds | 505.310 / 269.878 | 1094.242 / 314.205 | 1019.778 / 280.205 |
| Ciphertext-origin worker-seconds | 1223.814 | 1209.456 | 623.511 |
| Logical metadata GB | 547.026 | 532.757 | 488.931 |
| Prepared-work wait seconds / main share | 66.051 / 62.9% | 56.247 / 58.3% | 41.643 / 52.2% |
| Writer batch seconds | 33.195 | 34.454 | 32.175 |

Raising the RPC cap alone improved throughput 9.0% and removed admission waiting,
but increased mean storage latency and more than doubled crypto queue work.
More RPC overlap therefore helped while exposing an inner queue, not eliminating
latency. Raising content readers at the same 32-RPC cap improved throughput a
further 20.8%, improved batching and reduced measured logical metadata work.
The best cell was 31.6% faster in throughput than baseline, with 24.0% less main
time and 10.6% less logical metadata work. These are observations from one
sequential screen; host/NFS warming and timing-dependent grouping remain
confounders, so they do not qualify a causal amplification or physical-I/O gain.

At 64 readers the RPC peak was only 26 of 32 and admission waiting was negligible.
There is no evidence here to raise that cap further. The writer still waits for
prepared work for 52.2% of main time, while publication occupies 32.175 seconds,
about 40.3%. The next forced-path discriminating screens should hold readers/RPCs
fixed at 64/32 and separately vary effective engine caches and crypto workers.
`CryptoExecutor` uses a bounded dedicated pool with a matching semaphore;
`VAULTICDB_CRYPTO_THREADS` is its existing startup control. Its queue timer
includes permit waiting and pool scheduling, so this evidence suggests testing
worker capacity, not asserting that decrypt service alone is the bottleneck.
Batching/cache reuse can instead reduce the repeated work submitted to that pool.

The immutable baseline `verified-concurrency.json` and each cell's
`CONCURRENCY-SHA256SUMS` retain the measurements. Do not rerun the create-only
analyzer or overwrite sealed receipts. Worker-second sums overlap and cannot be
added as wall-time shares; logical/origin bytes are not physical NFS/HDD traffic,
and full SST decoding is not isolated. No main interval reached 600 seconds.
This screen is diagnostic, not repeated mirrored/sustained qualification or a
default/deployment change. Unforced optimization remains deferred.

### Fivefold Inode Observation

At the user's request, the next coalesced observation selects exactly 50,000
entries, five times the 10k diagnostic scope, from the accepted 100k restore.
It creates a fresh stable source and a complete verified parent before cloning
independent unchanged and forced legs. This is not permission to modify the
production source, repository, daemon, or earlier sealed captures.

The comparison CLI and native binaries remain byte-identical to the 10k
candidate. Validators 16, metadata RPCs 8, batch delay 1ms, engine MultiGet on,
metadata/block caches 128/512 MiB, content readers 16 and publication groups 64
remain fixed. The source mix and inode paths are a larger fresh selection, not
an interleaved same-input performance comparison. Report per-entry work as well
as totals, and do not assume total runtime must increase exactly fivefold.

The expanded supervisor uses `--mode coalesced --inode-limit 50000`. Its fresh
evidence root is
`/volume2/NASDA2/vaultic-test-m6/metadata-read-fme-cae1-20261103-50k-coalesced`;
runtime state is private under `/run`. Durable fixture storage is explicitly
bounded at 350 GiB, allowing the verified parent and cloned packs/databases plus
three verified restores. RSS, managed RAM, 2 GiB scratch, shared free-space and
time ceilings remain unchanged. Parent setup, clones and verification stay
outside measured main intervals. A main interval shorter than ten minutes must
still be labelled a short screen, without padding or extrapolation.

Status: completed and correctness accepted. Both measured legs covered exactly
50,000 entries, with 41,347 regular files and 19,493,496,103 payload bytes. The
source hash was
`8f32883b02c37ae550a0375fa6abfe79ec52d830ef0d9cff1053522687a41e87`.
Both stored zero new data blobs and passed exact restore, encrypted reopen,
source identity, expected force/parent semantics and idle-writer checks. Outer
cleanup passed; a subsequent check found no candidate daemon survivors. All
11 launch pins and 46 root-relative evidence checksums independently verified.

| Measurement | Unforced 50k | Forced 50k |
| --- | ---: | ---: |
| Main / command seconds | 316.380 / 335.310 | 105.016 / 122.747 |
| Entries/second | 158.038 | 476.119 |
| Prepared-work wait seconds / main share | 264.427 / 83.6% | 64.737 / 61.6% |
| Writer batch seconds | 46.203 | 34.560 |
| Size RPCs / handles | 32936 / 41372 | 9857 / 41372 |
| Handles/size RPC | 1.256 | 4.197 |
| Paired blob-read client / storage mean ms | 19.454 / 18.749 | 48.448 / 47.559 |
| Size slot / result wait worker-seconds | 0.220 / 749.318 | 390.108 / 3337.359 |
| Ciphertext-origin worker-seconds | 642.349 | 1115.296 |
| Crypto queue / service worker-seconds | 78.906 / 379.820 | 525.201 / 267.601 |
| Logical metadata GB | 932.811 | 545.185 |
| Logical metadata MB/entry | 18.656 | 10.904 |

The dependency remains writer waiting for prepared nodes, with the unchanged
availability lookup mostly waiting for the database response rather than RPC
admission. Its paired mean is 96.4% inside storage, not transport alone. Size
RPCs peaked at eight in both legs; a peak is not evidence of continuous
saturation or an engine concurrency knee. Unchanged batches remain close to
singletons despite 16 validators and the 1ms coalescing window.

The forced pipeline exposes a different pressure pattern: RPC-slot waiting is
substantial, paired reads are 98.2% inside storage, and origin fetching plus
crypto queueing dominate the measured encrypted-read work. Writer publication
also costs 34.560 seconds, but prepared-work waiting remains larger. These
worker-second totals overlap across concurrent requests; they cannot be added
as exclusive wall-time shares. Origin work can be encrypted-cache-served, and
neither its bytes nor logical wrapper bytes measure physical NFS/HDD traffic.
CLI size-cache and encryption-header-cache evictions were zero; this does not
establish that decoded engine metadata or block caches fit the working set.

Relative to the 10k coalesced screen, unforced main time grew 4.555-fold while
throughput rose 9.8%; forced main time grew 5.784-fold while throughput fell
13.5%. Logical metadata work per entry fell 9.1% unforced and rose 3.1% forced.
These are descriptive scope contrasts, not isolated optimization gains: payload
grew 7.9-fold and source mix/private paths differ. Weak unchanged batching and
continued amplified metadata work are still the controlling issues; no higher
defaults or production activation follow from this observation. Separate-variable
decoded-cache and batching screens remain the next amplification experiments.

Peak combined owned RSS was 42,161,598,464 bytes. Outer capture time was
5,682.491 seconds, including source preparation, parent setup, clones and
verification. **Neither measured main interval naturally lasted ten minutes**;
the larger observation still does not satisfy repeated sustained qualification.
The immutable `verified-observation.json` and `METADATA-READ-SHA256SUMS` in the
50k evidence root retain the measurements and explicit limits. Do not rerun the
create-only analyzer or rewrite the sealed receipt.

## Correctness Contract

1. Size/mtime and normally ctime/inode describe source changes, not repository
   blob availability. Keep checks for every required content reference.
2. Missing or malformed verified Vaultic metadata must take the ordinary safe
   read/rechunk path. A first post-import backup does not inherently require
   `--force`; that flag removes the parent and forces rereading.
3. Cache exact ID, type, found status and size. A probabilistic positive must
   never authorize content reuse. Generation/session identity and new-write
   overlays must preserve the existing freshness and deletion/retention contract.
4. Preserve authentication, encryption authentication, error identity, deadlines,
   caller cancellation, shared-request cancellation semantics and fail-closed
   daemon behavior. Do not turn lookup failure into a negative or positive hit.
5. Bound queues, handles, bytes, request lifetimes and retained directory state
   through the shared working-memory policy, not merely a goroutine count.
6. Preserve source ordering, hardlinks, directory edges, exclusions, ancestor
   coverage, snapshot scope, writer fencing and acknowledged durable completion.
7. A borrowed verifier connection must not close an owned daemon client. Test
   reader closure, owner `WriterStatus`, cleanup and encrypted reopen explicitly.
8. Do not modify production repositories, installed binaries/services, frozen
   seeds, sealed evidence or retention policy during diagnostic qualification.

## Options

| Option | Intended Reduction | Cost or Risk | Initial Decision |
| --- | --- | --- | --- |
| Fit decoded SST index/filter metadata in engine cache | Repeated fetch/decrypt/decode of hot metadata | Memory accounting; cache admission and invalidation | First configuration experiment after attribution |
| Cross-file batches plus SlateDB engine `multi_get` | Repeated table/index/filter visits and duplicate block reads | Bounded buffering, ordering, cancellation, engine parity | First code candidate if metrics show sharing opportunity |
| Bounded parallel unchanged-file validation/prefetch | Exposed synchronous storage latency | Does not necessarily reduce work; can increase contention and RSS | Combine only with measured work reduction or a separately reported latency-overlap hypothesis |
| Exact compact local blob catalog | Per-file DB requests after a sequential load | Startup work, memory/scratch use, generation/session freshness | Evaluate if lookup workload exceeds practical cache coverage |
| Local encrypted object/range cache | Repeated backend fetching | Decryption still required; quota and stale-version checks | Prefer over plaintext storage when backend fetching dominates |
| Trusted decrypted object/range cache | Fetching and repeated decryption | Explicit plaintext trust/permissions and approval | Separate security decision, not an implicit fallback |
| Reduce LSM/SST overlap or adjust compaction | Tables examined per point lookup | Writes, startup/compaction cost, maintenance authority | Only after overlap metrics demonstrate a cause |
| Move authoritative metadata to SSD | Access cost, not amplification itself | Deployment and durability change | Separate approved deployment experiment |

The existing `MultiGet` RPC can loop over independent engine gets. Reducing RPCs
does not necessarily share engine work: distinguish transport batching from the
`VAULTICDB_SLATEDB_MULTIGET` engine path. The flag is a candidate to test, not a
proven production recommendation.

Engine metadata caching is decoded in-process caching. Encrypted object caches
sit below decryption; trusted decrypted caches sit above it. Keep those layers
distinct in measurements and security decisions. An exact local catalog can
eliminate individual DB reads, but its initial scan remains part of startup and
total command cost; do not hide that cost outside the comparison.

## Execution Protocol for an LLM

Before each phase, read current applicable repository instructions and the
owning implementation and nearby tests. Preserve concurrent user edits. State
one falsifiable hypothesis, one focused disconfirming check, the smallest edit,
and the phase exit gate before editing. Validate immediately after the edit.
Do not implement multiple options before identifying which measurement supports
them. Do not launch a larger workload or change security/resource constraints
without approval.

For every experiment, retain a create-only manifest containing source revision
and diff, Go/Rust toolchains, actual executable paths and SHA-256, dependency
revision, input hash/count/types/bytes, parent/snapshot identity, cache and worker
settings, backend topology, source and seed provenance, owned process identities,
resource limits, commands, scheduling order, raw logs, observations and outcome.
Verify checksums from the manifest root. Preserve failed/preliminary captures.

Diagnostic envelope: private durable NFS evidence roots with exact 0700
directories and 0600 logs/configuration, short private `/run` sockets/scratch,
swap disabled, core dumps disabled, owned RSS ceiling no greater than
200,000,000,000 bytes, managed CLI RAM no greater than 160 GiB, CLI scratch budget
2 GiB, and an explicit shared free-space floor established before launch.
Fixture databases and packs are durable experiment storage, not hidden scratch;
declare their separate byte ceilings. Monitor the whole owned process group.
Use observed SIGINT cleanup before bounded kill fallback. Never force takeover
to conceal failed cleanup. Smaller phase-specific budgets are preferred.

### Phase 0: Seal the Baseline and Forced Control

Owner: `cmd/vaultic/working_benchmark_test.go` and existing qualification helpers.

Task: execute the requested complete 10,000-entry frozen `fme_cae1` comparison.
Establish one ordinary verified parent with all source data already cataloged;
close it, clone its DB/repository into separate legs, checksum-verify each clone,
and relocate only each private WAL binding. Hold the same source path/inodes,
content, binary, settings and publication group constant. Unforced uses the
explicit common parent; forced uses `--force` and must have no parent.

Check: exactly 10,000 observed/scanned entries; no failed/deferred entries;
matching regular-file count; zero newly stored content/data blobs in both legs;
report tree metadata and aggregate `data_added` separately rather than requiring
them to be zero; unforced regular files all unchanged; forced regular files reread rather
than counted unchanged; complete snapshot scope with no reused subtrees; exact
restore and source hash/type/size agreement; encrypted reopen; idle writer;
no owned survivors. Include setup, main, command and verification durations
separately. This is one diagnostic pair, not a statistically qualified gain.

Parent setup is explicit preconditioning: the 10k setup added two data blobs and
tree metadata from an older historical seed. These additions are outside the
measured legs, which both start with all source content present in the catalog.
Preserve setup summaries and costs rather than claiming zero setup additions.

Exit: two accepted leg results plus accepted outer cleanup, retained raw critical
frames/RPC/cache/native counters and independent checksum verification. On any
failure, retain the outcome and repair only the demonstrated local defect.

### Phase 1: Attribute the Amplified Reads

Owners: `internal/index/blob_lookup_cache.go`, daemon `client_wait.go`, native
`service/kv.rs`, `storage.rs`, `storage/cache.rs`, `encryption.rs`, and the pinned
SlateDB read/cache abstraction only where needed.

Hypothesis: repeated index/filter or rounded encrypted-range work dominates
metadata reads and survives warm passes because an effective cache is absent,
undersized or bypassed. This is a hypothesis, not an established root cause.

Task: extend fixed-cardinality metrics for requested plaintext bytes, rounded
ciphertext bytes, raw fetch duration, crypto queue/service duration, engine read
duration, cache hit/miss/admission/eviction and range class. Capture bounded
repeat-range samples using opaque identifiers; do not log secrets or unbounded
paths/blob IDs. Measure NFS traffic separately from logical wrapper bytes;
physical HDD attribution additionally needs permitted storage-side counters.
Explicitly mark unavailable counters rather than substituting zero.

Check: deterministic injected fetch/crypto/queue delays must be attributed to
the correct interval; boundaries must not overlap when reported as a partition.
Concurrent tests must preserve request outcomes and bounded state. Exercise
headers/capabilities through the actual authenticated transport, including old
daemon compatibility and cancellation/error cases.

Exit: reproducible attribution on the fixed 10k workload; choose one next option
from measured cache/range behavior. If raw fetching versus crypto remains
unresolved, refine this phase rather than guessing from NFS media type.

### Phase 2: Screen Cache and Engine Configuration

Hypothesis: measured hot decoded metadata can fit within an approved larger
cache, or genuine engine multi-get can share repeated range work.

Task: first compare metadata cache sizes 128, 256, 512 and 1024 MiB with block
cache, concurrency, source and binary fixed. Change one variable per leg.
Then compare engine multi-get off/on at fixed batches of 1, 8 and 64 handles,
including duplicates, mixed blob types, absent IDs and malformed records.
Count unique keys, table visits, required/coalesced reads and bytes; singleton
engine calls alone cannot demonstrate cross-file batching benefits.

Check: all results equal the control, memory accounting is bounded, total
startup/command cost is reported, and the targeted read work decreases. Reject
a purported amplification fix that only changes RPC latency or timing placement.

Exit: select a cache/configuration candidate only if measured work reduction
and memory cost support it. Otherwise advance to the specific code option
supported by Phase 1, not a combined speculative tuning bundle.

### Phase 3: Implement One Bounded Read-Work Candidate

Option A owners: archiver directory traversal and `allBlobsPresent`, cached blob
lookup, daemon read session, and nearby archiver/cache/session tests.

Task A: introduce bounded cross-file prefetch/coalescing where unchanged
validation currently blocks the directory-entry loop. Keep deterministic node
submission and per-file missing-content fallback. Account batch handles, result
buffers, pending deduplication and queue bytes before allocation. Maintain a
bounded flush/lifetime policy; cancellation must not strand callers or poison
unrelated shared requests. `ReadConcurrency` controls content readers and is
not automatically the unchanged-validation concurrency setting.

Option B owners: blob-lookup cache/catalog and read-session identity.

Task B: if an exact local catalog is selected, stream an ID/type/size view into
an admitted RAM or private scratch representation. Specify completeness,
generation/sequence pinning, negative-answer validity, duplicate handling and
new-write overlay semantics before coding. Count loading time and bytes. Stop
on budget exhaustion; never silently trust incomplete positive/negative state.

Checks for either option: unchanged, changed, missing blob, lookup failure,
malformed record, duplicate content, hardlink, symlink, excluded subtree,
renamed path, source mutation, cancellation while queued/in flight, daemon
loss, private writer closure and reopen. Use existing tests/helpers; add only
coverage that does not have a suitable existing home. Race-check shared paths.

Exit: focused correctness tests and the exact 10k control/candidate comparison
pass; the selected read-work metric decreases. No production activation.

### Phase 4: Find the Useful Concurrency Knee

Task: on the selected fixed implementation/cache setting, screen concurrency
1, 8, 16, 32, 64 and 128, stopping earlier on resource or tail-latency failure.
These values are an experiment grid, not proposed defaults. Use identical
input and retained, explicitly distinct odd/even execution schedules; do not
reverse a mirrored schedule and call it order alternation.

Specify whether each experiment changes the metadata caller pool or its cache
RPC limiter; change one at a time and log both effective values. Do not claim
that raising content `ReadConcurrency` changes unchanged-validation callers.
An apparent plateau behind a fixed four-RPC limiter is not an engine peak.
Attribute location-slot waiting explicitly before claiming the forced read
path is limited inside storage alone.

Then confirm the apparent knee with at least three paired sustained captures,
each main interval naturally lasting at least ten minutes on an approved
sufficiently large unchanged source. No sleeps, artificial throttling,
favorable-repeat replacement or short-screen extrapolation. If input is too
small, stop for approval of a larger fixture rather than quietly expanding scope.

Check: throughput, p50/p95/p99/max request latency, actual peak in-flight reads,
slot/queue waits, CPU, RSS, working budgets, range/cache work, backend traffic,
errors and source/snapshot identity. Define a practical plateau as less than
5% median throughput improvement for two successive concurrency increments
across repeated captures; this is a tuning criterion, not a universal peak.
Prefer the lower setting when throughput is comparable and tails/resources grow.

Exit: documented useful operating point for this implementation, source and
backend, including variance and rejected captures. Recheck if caching/batching
changes; the knee is not independent of amplification.

### Phase 5: Full-Crawl Qualification and Deployment Decision

Task: compare frozen control and candidate builds with the selected budgets on
approved representative full-source input. Retain ordinary unchanged and
forced controls separately; do not mix their bottleneck diagnoses. Establish
at least 100 entries/second over sustained completed unchanged main intervals,
without moving startup/catalog-load cost outside command reporting.

Check: complete coverage, exact restore/content/type/size, expected parent,
zero new data for unchanged input, changed/missing-content fallback, encrypted
reopen, cancellation/recovery and no resource/ownership/durability regression.
Report whether metadata logical work and actual backend traffic decreased,
and whether throughput improvement is batching, caching or overlap.

Exit: reviewed immutable acceptance receipt and an explicit production
deployment approval. Retain a pinned rollback binary/configuration; never
remove availability checks or weaken crypto/durability to meet throughput.

## Required Phase Handoff

At each phase end, record: hypothesis; exact edit/configuration; source and
binary pins; focused test names/counts; raw evidence root and checksum result;
workload/settings; accepted and failed outcomes; performance and work deltas;
resource peak; cleanup; unresolved questions; and the one authorized next task.
Do not claim a full test passed when only its measurement passed. Do not commit
or deploy unrelated changes. Future agents must read the last accepted handoff
and current files before continuing.