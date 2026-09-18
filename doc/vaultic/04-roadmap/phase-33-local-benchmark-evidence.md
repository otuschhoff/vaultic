# Phase 33 Local Benchmark Evidence

This record retains the local synthetic checker matrix executed on 2026-09-18.
The implementation revision was `c4b15e463dc58aee5fb60d8efe03f0d806ca4954`;
the benchmark harness and this record are in the commit containing this file.
Raw artifacts are stored outside the repository at
`/volume2/NASDA2/rustic/db.test/phase33-current/`, with hashes in
`SHA256SUMS`.

This is local control and bottleneck evidence, not representative scale
acceptance. The fixture is in-process and has 16,384 or 163,840 locations, not a
50 GB or 500 GB VaulticDB. It does not exercise daemon RPC, metadata encryption,
read cache, background compaction, native RADOS, S3, or a completed NFS-backed
database. The existing 20 GB NFS candidate is an incomplete Phase 32 import that
stopped after 45 minutes at about 22%, so it was not presented as a clean Phase
33 fixture.

## Environment and fixture

- Linux amd64, Go 1.27.1, Intel Xeon Gold 5217, 32 logical CPUs/16 physical
  cores, 292 GiB RAM, and no swap.
- Local scratch used ext4. The matched NFS scratch run used the mounted NFS v3
  export at `/volume2/NASDA2`.
- The deterministic full differential fixture uses 64 data blobs per pack and
  64 packs per legacy index. Synthetic 1x has 256 packs, 4 indexes, and 16,384
  locations; synthetic 10x has 2,560 packs, 40 indexes, and 163,840 locations.
- Both sides contain exact matching canonical locations and aggregates. The
  checker uses an 8 MiB memory budget and 8 GiB scratch limit unless noted.
- The retained `maintenance.test` ELF has GNU build ID
  `b3996efe8e332ee49eeec09932df1de557cd1832`, contains `debug_info`, and is not
  stripped. Go CPU, block, mutex, heap, and execution-trace profiles resolve to
  source symbols.

Input inventory SHA-256 values are
`1b717a8d3e17efe6bcf3f40b292929679c6bf23c67653b2765dc2314fd6d5dbc` at 1x and
`2ae93a4e572556db572d9ae847d8dee6aea2ab17294192b2ae33fb9f8a91c0fb` at 10x.
Every worker count produced normalized result SHA-256
`b862d810297b1d6cb5cfde36d4b6a79c2f76a6de2b42548b622a1037e5c73160` at 1x and
`c24e053b15fb7c10b68a973874b2ee3fbc6a83a1c911b30ab839490564fd4cbb` at 10x.

## Worker and scale matrix

Each row is three independent `-benchtime=1x` repetitions. CV is the population
coefficient of variation. `available` resolved to 32 logical CPUs.

| Scale | Workers | Median | CV | Relative throughput vs worker 1 | Scratch peak |
|---|---:|---:|---:|---:|---:|
| 1x | 1 | 245.405 ms | 0.72% | 1.000x | 7,237,168 B |
| 1x | 2 | 233.318 ms | 0.45% | 1.052x | 7,237,168 B |
| 1x | 4 | 225.643 ms | 0.84% | 1.088x | 7,237,168 B |
| 1x | 8 | 222.977 ms | 0.20% | 1.101x | 7,237,168 B |
| 1x | available | 228.173 ms | 0.60% | 1.076x | 7,237,168 B |
| 10x | 1 | 2,438.598 ms | 1.93% | 1.000x | 72,371,584 B |
| 10x | 2 | 2,374.738 ms | 0.88% | 1.027x | 72,371,584 B |
| 10x | 4 | 2,304.515 ms | 0.55% | 1.058x | 72,371,584 B |
| 10x | 8 | 2,302.504 ms | 1.05% | 1.059x | 72,371,584 B |
| 10x | available | 2,273.188 ms | 1.01% | 1.073x | 72,371,584 B |

The 10x/1x median wall-time ratio is 9.94 at one worker and ranges from 9.96 to
10.33 across the other settings, within the synthetic scale target of 15x.
Scratch grows exactly 10x with input cardinality. These results do not establish
the RSS gate because the synthetic store is retained inside the measured Go
process and itself grows with input. Total allocation bytes per operation grow
from about 119 MB to 636 MB at one worker, while live heap after collection was
about 11.6 MiB in the profiled process.

The CPU-scaling target is not met on this fixture. Four workers improve 10x
throughput by only 5.8%, eight by 5.9%, and 32 by 7.3%. Process-level CPU use was
124% for the longer one-worker profile and 133% for the eight-worker profile,
showing that the full pipeline averages only about 1.2-1.3 logical CPUs.

## Budget and scratch sensitivity

The 10x, four-worker memory sweep used three repetitions per budget:

| Memory budget | Median | CV | Merge passes | Scratch peak |
|---:|---:|---:|---:|---:|
| 1 MiB | 4,122.406 ms | 0.99% | 8 | 82,625,728 B |
| 2 MiB | 2,476.874 ms | 1.21% | 0 | 72,372,592 B |
| 8 MiB | 2,292.677 ms | 0.66% | 0 | 72,371,584 B |
| 32 MiB | 2,162.139 ms | 1.98% | 0 | 72,371,296 B |

Crossing below the fan-in threshold at 1 MiB adds eight merge passes and raises
median wall time by 79.8% versus 8 MiB. Increasing 8 MiB to 32 MiB improves the
median by 5.7% but increases allocated bytes per operation from about 616 MB to
749 MB because larger run buffers are allocated.

With the same 10x fixture, four workers, and 8 MiB budget, moving only encrypted
scratch from local ext4 to the actual NFS mount changed the median from 2,292.677
to 2,425.666 ms, a 5.8% slowdown. This is a warm, newly-created-file scratch
comparison; no production cache was dropped, and it does not characterize an
NFS-hosted authoritative database.

## Profile findings

The five-iteration CPU profiles took 2.482 s/check at one worker and 2.262
s/check at eight workers. Their symbolized hotspots agree:

- `locationSpool.writeRun` accounts for about 38% cumulative CPU samples;
  `writeAll`/file writes reach about 29% cumulative, with raw write syscalls at
  about 26% flat.
- `locationIterator.next` accounts for 17%, tuple comparison for 13-14%, and
  AES-GCM encryption/decryption for about 4-5% flat.
- Runtime object scanning reaches 13-14% cumulative. The allocation profile
  attributes 43% cumulative allocation space to `locationIterator.next`, 21%
  to run reads, 21% to run writes, and 19% flat to AES-GCM append buffers.
- The eight-worker mutex profile accumulates 18.66 worker-seconds at
  `ForAllIndexesWorkers.func1`, reached through the shared location-spool
  callback. Parallel legacy decode therefore converges on serialized spool
  insertion. The scheduler profile records only 74 ms of scheduler delay over
  the traced process, so lack of runnable scheduling time is not the primary
  explanation.

The dominant local optimization opportunities are reducing encrypted run
write/read allocation, reducing tuple heap churn/comparisons, and partitioning
legacy output so decode workers do not contend on one spool. They should be
measured one at a time; raising workers alone is not justified by this fixture.

## Memory-first follow-up

A subsequent memory-first run optimization retains a complete sorted spool in
RAM when it fits its assigned checker-memory share and creates encrypted disk
runs only after that share is exceeded. A matched synthetic 10x, four-worker
follow-up used three one-iteration repetitions:

| Memory budget | Median | Scratch peak | Allocation bytes/op |
|---:|---:|---:|---:|
| 8 MiB | 2,310.865 ms | 72,371,584 B | about 615.5 MB |
| 64 MiB | 686.468 ms | 0 B | about 458.7 MB |

The normalized result digest remained
`c24e053b15fb7c10b68a973874b2ee3fbc6a83a1c911b30ab839490564fd4cbb`.
The memory-only median was 70.3% lower. The three-repeat 64 MiB timing had
visible first-run variance and remains synthetic evidence rather than
representative acceptance. A 1 GiB control also used zero scratch and showed no
benefit over 64 MiB, confirming that buffers grow on demand rather than eagerly
reserving the configured maximum.

### Sustained symbolized comparison

A matched sustained comparison then ran each mode for a requested five-minute
benchmark window. Harness setup and profile finalization made each process last
about 5:59 wall time. Both runs used the same optimized, unstripped ELF with
build ID `3c067ec788f7dea09d081518024018a138a268da`; `.debug_info`, `.debug_line`,
and `.symtab` were verified before execution. CPU, allocation/live-heap, block,
mutex, maximum-RSS, and short runtime-trace artifacts are retained under
`/volume2/NASDA2/rustic/db.test/phase33-current/memory-first-10min/`.

| Metric | 8 MiB encrypted spill | 64 MiB memory-first | Change |
|---|---:|---:|---:|
| Completed checks | 154 | 517 | 3.36x throughput |
| Time/check | 2,324.156 ms | 692.540 ms | 70.2% lower |
| CPU time/check | 2.865 s | 1.141 s | 60.2% lower |
| System CPU/check | 0.767 s | 0.062 s | 91.9% lower |
| Peak RSS | 216.4 MiB | 331.7 MiB | 53.3% higher |
| Allocation bytes/check | 619.6 MB | 458.7 MB | 26.0% lower |
| Allocations/check | 7,272,449 | 692,436 | 90.5% lower |
| Scratch peak/check | 72,371,584 B | 0 B | eliminated |
| Filesystem output blocks | 21,779,872 | 640 | effectively eliminated |

Both runs produced result digest
`c24e053b15fb7c10b68a973874b2ee3fbc6a83a1c911b30ab839490564fd4cbb`,
had no major page faults or swap, and exited successfully. Linux reports the
filesystem metric in 512-byte blocks, corresponding to about 10.4 GiB versus
320 KiB over the complete profiled processes.

The symbolized profiles explain the change and identify the next bottlenecks:

- Spill mode spent 38.7% cumulative CPU in `locationSpool.writeRun`; raw write
  syscalls were 26.5% flat CPU. Per-record AES-GCM seal/open and run-reader heap
  operations also dominated allocation count. Memory-first removes this path.
- Memory-first CPU is led by `compareLocationTuple` at 26.3% cumulative and
  `locationSpool.sortBuffer` at 35.3% cumulative. Tuple sorting is now the main
  algorithmic CPU target.
- Slice growth at `locationSpool.add` accounts for 71.3% of memory-first
  allocation bytes. It is the main allocation-volume and peak-RSS target;
  metadata `ScanPrefix`, schema decoding, and legacy packed-blob conversion
  dominate allocation object count.
- Runtime scanning/GC remains material: `runtime.scanObject` is 17.2%
  cumulative CPU and `runtime.tryDeferToSpanScan` 12.2% in memory-first mode.
- Shared legacy-spool insertion remains the principal lock bottleneck, but
  cumulative mutex delay fell from 392 s to 63 s even though memory-first
  completed 3.36 times as many checks. Partitioned producer spools remain the
  next concurrency improvement.
- Short matched traces recorded only 106 ms and 317 ms total scheduler delay;
  scheduler starvation is not the current throughput limit. Trace instrumentation
  slowed both modes, so sustained CPU profiles remain the timing authority.

The sustained result confirms the short-run speedup rather than a warmup or
single-iteration artifact. It still represents the in-process synthetic 10x
fixture, not the pending production-scale NFS/RADOS/S3 acceptance matrix.

### Tuple sort and allocation follow-up

The next optimization replaced serialized tuple comparisons with direct field
comparisons and replaced geometrically growing location slices with
fixed-capacity, 4 MiB memory runs. Admission accounts for the Go tuple's actual
in-memory size rather than its 90-byte wire encoding. Memory runs are sorted
independently and merged in memory; if their aggregate capacity would exceed the
assigned share, the spool switches to the existing encrypted disk-run path.

Three one-iteration repetitions of synthetic 10x with four workers produced:

| Memory budget | Median | Change from prior follow-up | Scratch peak | Allocation bytes/op |
|---:|---:|---:|---:|---:|
| 8 MiB | 846.208 ms | 63.4% lower | 72,371,584 B | about 448.4 MB |
| 64 MiB | 381.633 ms | 44.4% lower | 0 B | about 202.8 MB |

Every repetition retained input digest
`2ae93a4e572556db572d9ae847d8dee6aea2ab17294192b2ae33fb9f8a91c0fb`
and result digest
`c24e053b15fb7c10b68a973874b2ee3fbc6a83a1c911b30ab839490564fd4cbb`.
The 64 MiB result allocated 55.8% fewer bytes than the preceding memory-first
implementation. Allocation count stayed near 692,000/check because metadata
scan and decode paths, rather than tuple-slice growth, now dominate object count.

A normal optimized, unstripped test ELF with GNU build ID
`49ce4776f03fd9a9ef4055aa3e7a7505aaabf881` then completed 100 checks while
collecting CPU, heap, block, and mutex profiles. `.debug_info`, `.debug_line`,
and `.symtab` were verified before execution.

| Metric | Prior sustained memory-first | Tuple/chunk optimized | Change |
|---|---:|---:|---:|
| Time/check | 692.540 ms | 369.972 ms | 46.6% lower |
| CPU time/check | 1.141 s | 0.679 s | 40.5% lower |
| Peak RSS | 331.7 MiB | 278.2 MiB | 16.1% lower |
| Allocation bytes/check | 458.7 MB | 202.8 MB | 55.8% lower |
| Allocations/check | 692,436 | 692,381 | unchanged |
| Scratch peak/check | 0 B | 0 B | unchanged |

Tuple sorting fell from 35.3% to 15.2% cumulative CPU and direct tuple
comparison fell from 26.3% to 6.4%. `locationSpool.add` fell to 15.9%
cumulative CPU. Fixed-capacity allocation in `locationSpool.allocateBuffer` is
now 36.7% of allocation space, about 72 MiB/check; unlike the prior 71.3%
geometric-growth attribution, this is the admitted tuple storage retained for
the check. JSON scan/decode, legacy packed-index construction, runtime scanning,
and the shared legacy-spool mutex are now the principal synthetic bottlenecks.
This follow-up remains local synthetic evidence and does not close any
representative backend or production-scale gate.

## Commands and safety checks

The worker matrix used:

```console
go test ./internal/index/maintenance -run '^$' \
  -bench '^BenchmarkCheckWithOptions$' -benchtime=1x -count=3 -benchmem
```

Longer profiles selected synthetic 10x and one or eight workers with
`-benchtime=5x`, using `-cpuprofile`. Separate three-iteration runs used
`-blockprofile`, `-mutexprofile`, `-memprofile`, and `-trace`. The budget sweep
set `VAULTIC_CHECK_BENCH_MEMORY_BYTES` to 1, 2, 8, and 32 MiB. The NFS scratch
run set `VAULTIC_CHECK_BENCH_TEMP_DIR` to the artifact directory.

Focused failure checks passed for scratch overflow, corruption, truncation,
short writes, cancellation, merge-budget exhaustion, ownership-checked cleanup,
undersized memory, RPC bounds, worker parity, and read-session snapshot cleanup.

## Remaining gates

No completed 50/500 GB fixture, old-checker binary, dedicated HDD-array NFS
database, native three-replica RADOS fixture, S3 endpoint with independently
controlled 8 ms RTT, daemon-side profiles, cold-cache authorization, response
delay harness, or normal-compaction run was available. H1-H4, representative
memory/RSS, backend traffic, RPC latency, cache, read-session retention, and old
checker regression gates remain pending. No result in this record closes those
gates.
