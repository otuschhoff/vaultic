# Phase 35 M0: inventory, baseline, and frozen contract

[Phase 35](phase-35-ram-working-memory-and-kv-fallback.md)

Status: complete. This milestone observes the
existing engines; it does not implement RAM/KV selection, enforce a new memory
limit, replace Pebble, or alter daemon durability. M1-M6 remain unimplemented.

## Inventory and ownership

Classification is about authority and function, not filename extensions. The
following covers Go-side working state reachable from import/check/backup.
Paths below are source references or patterns, never telemetry label values.

| Owner / creation boundary | Activation, retention, and cleanup | Classification / future action |
|---|---|---|
| `written_blob_store.go:newWrittenBlobStore` | On-demand backup creates `vaultic-written-*` under `--metadata-scratch`; exported new locations remain until lookup close; mutex serializes writes/lookups; close removes owned directory | Participating KV; HMAC keys and AES-GCM values, 148 encoded bytes/location; duplicates by blob with distinct pack/offset must survive |
| `marker_store.go:spill`, `exclude.go:rejectionCache.Store` | Store allocated for backup selection; Pebble opens lazily after a decision map reaches 4,096 entries; rereads can spill overwrites; OS temp; backup close removes directory | Participating KV plus in-RAM decision maps; plaintext marker/path keys today, boolean values; per-specification decisions remain independent |
| `cwalk.go:BuildDirectoryManifestWithProgress` | Local non-incremental cwalk creates `vaultic-cwalk-*`; batches 1,024 records, queue default 4,096; directory path to JSON child-name list; close/cancellation removes directory | Participating KV and queued listings; plaintext paths/names today; one very wide directory can exceed a nominal count budget |
| `cwalk_stream.go:NewDirectoryStreamWithFS` | Incremental/local and NFS streaming paths retain pending listings, bounded demand/lookahead and metadata LRU; close cancels/joins workers and clears metadata | Participating RAM/streaming, not a disk KV; metadata is additionally byte-bounded at 8 MiB; listing size and closure capture remain admission concerns |
| `blob_lookup_cache.go:NewCachedBlobLookup` | On-demand backup retains positive/negative size lookups and pending batches; entry accounting 192 bytes; existing default cache 64 MiB; shutdown joins workers | Participating recomputable RAM cache; eviction cannot discard sole-copy written locations |
| `repository/index/master_index.go`, `index.go`, `indexmap.go` | Legacy/full-index paths load compact indexes and pending blobs; saved indexes remain in RAM unless an explicit spilling callback is installed | Participating RAM; full-repository versus per-run overlay scale must not be confused; legacy JSON output is durable repository projection |
| `maintenance/check_spill.go:locationSpool` | Fixed-width 90-byte encoded locations, memory runs, retained backing capacities, partition children and bounded merge readers/writers; encrypted owned `vaultic-check-*/run-*` files on pressure | Participating RAM/sort; preserve exact set/multiset and ordered partition semantics; shared disk reservations include merge overlap, not all heap allocations |
| `maintenance/check_kv_spill.go:checkKVSpool` | Variable-length key/value/sequence records for inventory, joins, paths/analytics and other checks; sorted runs share checker scratch/key; spool close releases runs, session cleanup checks ownership marker | Participating RAM/sort; authenticated encryption; record copies, slice headers, and reader allocations are not comprehensively charged by logical record-size limits |
| `maintenance/maintenance.go`, `check_analytics.go` and neighboring check consumers | Multiple spools share allocated sub-budgets; retain catalogs, findings, and scan pages; defaults memory 64 MiB at library boundary and disk 8 GiB, with CLI auto-memory resolution | Participating RAM and queues; include all sibling spools, result/findings buffers and transient reads in the later aggregate coordinator |
| `legacyimport/import.go`, `import_stage3.go`, `import_stage3_group.go`, `tree.go` | Decode indexes, collect/group packs, prepare bounded worker queues, retain ingest/reducer batches until acknowledgement, and traverse snapshot trees; worker cancellation drains owned work | Participating streaming/RAM; prepared-byte/transaction budgets already exist but do not cover entire decoded indexes, grouping, traversal, findings, or all RPC copies |
| `repository/index/index_parallel.go` | Ordered legacy-index loading retains decoded read-ahead results and worker buffers; caller consumes/releases them | Participating RAM/read-ahead; no separate temp database; account both decoded capacity and raw source payload overlap |
| `daemon/schema_store_import*.go`, `legacy_import_filter.go` | Go planner/reducer maps, mutations, aggregate contributions, bounded catalog/receipt pages; fresh-import Bloom layers capped at 1,536 MiB, with safe database fallback | Participating RAM; existing `LegacyImportStats` exports filter layers/bytes/inserts and RPC/encoded-byte totals; false positives never justify omitting authoritative reads |
| `daemon/client*.go`, `schema_store*.go` | RPC request/reply encodings, sessions, split authorization/dependency state and pending publication bookkeeping | Participating transient RAM; durable receipts/checkpoints/catalogs live through VaulticDB, not a second Go database; no fallback may retry uncertain publication |
| `internal/backend/cache`, `cmd_check.go:prepareCheckCache` | Ordinary encrypted repository-file cache and atomic `tmp-*` saves; `check` may create/remove `vaultic-check-cache-*`; default cache optional and command-dependent | Explicitly excluded cache I/O, reported separately; strict working-state RAM mode is not `--no-cache` |
| `repository/read_cache*.go` | Optional Phase 30 read-cache configuration, capacity coordination, copies and reads; repository access may use durable cache objects | Excluded data-plane cache I/O; participating bounded transient copy buffers must still leave process headroom |
| `repository/packer_manager.go:newPacker`, `internal/fileio` | Encrypted pack staging `vaultic-temp-pack-*`, finalized then uploaded; Unix temp file is immediately unlinked while fd stays open; close releases fd | Explicitly excluded pack staging; file tracing must include unlinked-open files, not only directory snapshots |
| `repository/placement_actions.go` | `vaultic-placement-*` stages an encrypted pack for copy/upload; cleanup releases temporary file | Excluded pack transfer; ordinary import/check/backup do not gain placement actions merely because this milestone runs |
| `backend/local` and provider Save implementations | Atomic backend writes, CAS lock files, multipart upload/client buffers | Durable output/provider protocol, not working-state KV; preserve placement, publication and delete contracts |
| `repository/bootstrap/profile.go`, `daemon/client_rpc.go`, key/capsule activation paths | Bootstrap/profile atomic files and transient capsule-mutation handoff; may run during repository initialization/activation | Durable control output or custody handoff, explicitly outside disposable KV authority; secret-bearing handoff must retain existing protected-file rules |
| `internal/apfs` | Explicit APFS source creates temporary mountpoint plus lease/control state; teardown handles source lifetime | Excluded source mount/control, not a KV spill; unavailable in Linux fixture matrix |
| Healing, repository upgrade, selfupdate, NFS readiness and test-only temp helpers | Created by their own opt-in commands/tests, not normal M0 fixture import/check/backup paths | Nonparticipating command surfaces; must not be accidentally invoked by the benchmark harness |

### Dependency-owned files

Pebble v1.1.5 owns WAL/log, SST, manifest/options/current/lock and compaction
files under each of the three private store roots. Its VFS `Create` calls,
background workers, cache and memtables are included in store scratch/accounting
scope, not independent authoritative databases. Caller `NoSync` batches still
write files. M1 must include engine buffers and obsolete-file overlap.

The pinned cwalk dependency drives traversal; the Vaultic manifest owner is the
database boundary. Backend/SFTP/cloud/network dependencies can stage or write
repository objects or operational transport state, but do not introduce another
Go metadata KV on these paths. Standard-library process/temp/file APIs also
appear in static audits without necessarily executing. The harness retains the
compiled dependency-directory list and file-creation candidates, then uses
fixture `strace` to distinguish potential code from executed working files.
Static search is not a proof that every dynamic provider/platform branch ran.
Each provider/native-platform branch remains classified by its owning call path;
M3 must extend tracing when it integrates that branch.

## Observation schema and availability

`internal/telemetry/working_state.go` defines five fixed store kinds and three
fixed current backends. No paths, keys, values, repository IDs or per-entry
labels are accepted. State size is constant per store, counters saturate rather
than wrapping, and snapshots are synchronized for concurrent readers.

- `committed_entries` and `committed_encoded_bytes` count successful batch
  writes, including overwrites. Retained fields are **upper bounds**, not exact
  cardinality or live heap. Failed/uncommitted batches do not increment them;
  earlier successfully committed batches remain counted after later failure.
- `observed_buffer_bytes` is an observation of serialized Pebble batch length
  or importer prepared bytes, not backing-array capacity, a RAM reservation, or
  whole-process memory. Peak observation is retained after close.
- `scratch_bytes` uses Pebble's engine `DiskSpaceUsage` estimate; known means the
  engine supplied it, not `du`, physical blocks, or a precise directory audit.
  Failed cleanup changes availability to unknown. The observed peak is only a
  peak over snapshots, not a continuously sampled filesystem high-water mark.
- `reserved_scratch_bytes` and its peak expose existing checker disk reservations;
  scratch size remains unknown there because reservations differ from actual
  files and exclude the ownership marker. Existing check budget enforcement is
  distinct from the future aggregate RAM budget.
- `reservation_enforced=false` is intentional for M0. No unified RAM limit exists
  yet. For streaming import, zero committed entries does not describe daemon
  commits; use the existing import result/`LegacyImportStats` instead.
- `activated` distinguishes a lazy unused marker store from an opened database.
  `closed` identifies released store working state, not successful command
  publication or proof of recovery after a failed cleanup.

Existing output routes: overlay state in `metadata_lookup_stats`, marker state
in one final `working_state_stats` event, manifest state in cwalk progress,
checker state in result resources, and stage-3 import state in scheduler
snapshots. Quiet text output remains quiet; JSON output remains machine-readable.
Observation fields are additive and excluded from logical check digests when
resource fields are cleared. No background full-database counting scan is added.

`working_runtime.go` reads a fixed set of version-checked `runtime/metrics`:
allocated/live heap, object count, scannable heap, runtime memory, GC cycles,
total GC CPU, assist CPU and cumulative pause-histogram p99. Missing/wrong-kind
metrics are named unavailable, never silently interpreted as measured zero.
Histogram percentile is a bucket upper bound over process lifetime, not an
interval percentile or maximum. Allocation/GC counters must be differenced;
heap before/after is not a sampled peak. OS maximum RSS comes from retained
`/usr/bin/time -v`; profiles and GC traces are retained separately. No GC
setting is modified in the running host or production process.

## Frozen future policy and error contract

M4 will implement these exact long flags on import/check/backup and equivalent
profile fields under a shared `working_memory` section:

| Flag | Profile Field | Meaning |
|---|---|---|
| `--working-memory-mode` | `mode` | `ram`, `kv`, `auto`; omission preserves pre-phase behavior until reviewed default rollout |
| `--working-memory-ram-limit` | `ram_limit` | Positive aggregate managed capacity; explicit override wins over profile |
| `--working-memory-scratch` | `scratch` | Owned scratch parent required for KV/automatic fallback |
| `--working-memory-disk-budget` | `disk_budget` | Positive aggregate managed scratch cap |

Byte grammar: unsigned integer bytes, optionally suffixed by `B`, decimal
`KB/MB/GB/TB`, or binary `KiB/MiB/GiB/TiB`; case-sensitive, no fractions,
negative/zero, overflow, or ambiguous single-letter suffix. Existing flags retain
their old parsers when no shared policy is selected. Explicit CLI beats profile;
profile beats the conservative auto-derived allowance. A shared-mode setting
conflicting with explicitly set `--metadata-scratch`, `--metadata-cache-mib`,
`--check-memory`, `--check-temp-dir`, or `--check-temp-max-bytes` is rejected.
Compatible sublimits must share the aggregate pool, never add to it.
Import's existing prepared/transaction limits remain subordinate safe batching
constraints and may not increase a shared allowance.

Future typed classifications (stable `errors.Is` sentinels, not string matching):
`ErrWorkingMemoryLimitExceeded`, `ErrWorkingScratchLimitExceeded`,
`ErrWorkingPolicyConflict`, `ErrWorkingStateCorrupt`, and
`ErrWorkingMigrationFailed`. A limit detail holds fixed store identity,
configured/effective limits and required reservation, never raw key/path data.
Migration errors retain their original cause and cancellation identity. These
are milestone contracts, not newly implemented production exceptions in M0.

One run owns one coordinator. Every store/cache/queue and transient encoder or
iterator reserves conservative capacity before allocation. The reservation
includes both old/new capacities during growth, alignment/allocator slack,
index structures, copied messages, and migration buffers; ownership transfer
releases once. Retained serialized bytes are insufficient accounting.
Dependencies and unrelated heap get additional process headroom. No zero-budget
allocation or wait on a reservation larger than the effective limit is allowed.

Usable envelope `E` is the lower applicable host/cgroup allowance after explicit
co-resident/runtime/non-heap reservations. Startup safety headroom is at least
`max(128 MiB, E/5)`; smaller environments must reduce the working allowance or
fail startup, not silently omit headroom. Effective managed allowance `B` is
`min(operator limit, E - headroom)`. Report all components and reject an
unresolvable explicit request. Never allocate 100 GB merely because it was an
example in the phase discussion. No new global Go or OS limit is applied.

Freeze pressure actions before M2/M4: warn at 75% of `B`; first evict only
recomputable caches and bound admission. Migration begins at the **earlier** of
85% of `B` or `B - R - G`, where `R` is measured conservative bounded migration
overlap and `G` is the next growth/admission reservation. Percentages alone do
not guarantee migration headroom. Hard managed admission stops at `B`, and
external observed pressure can reduce the allowance earlier. Strict RAM returns
the typed limit error instead of migrating. KV/auto return the original failure
if disk or migration headroom is unavailable. One-way per-run fallback and a
5%-of-allowance low-water hysteresis prevent oscillating warning/backpressure.
These proposed thresholds are not enforced by M0 counters.

## Consumer and confidentiality contracts

| Consumer | Required interchange semantics |
|---|---|
| Blob overlay | Upsert exact `(type, blob, pack, offset)` location; return all distinct locations and first-location size; successful exported state survives local index release; failed batches never appear as acknowledged |
| Marker decisions | Exact specification + directory key; true/false/missing distinct; overwrites idempotent; spill errors remain observable and exclusion fails closed as today |
| Directory listing | Exact directory key and complete ordered name list; missing differs from empty; no truncation or hidden spill for an oversized listing |
| Fixed-location sort | Preserve set or multiset according to caller, tuple ordering and partition boundaries, bounded merge, cancellation and exact findings |
| Variable KV sort | Preserve key/sequence ordering, duplicate/history selection, owned copies and authenticated records; not forced into a transactional KV engine |
| Import pipeline/filter | Retain pending work until reducer acknowledgement; false-positive filter requires authoritative lookup; only proven-absent IDs may skip reads; cancellation joins workers |

Returned byte slices must be owned copies or have an explicit iterator lifetime;
no database page may survive its transaction. Contexts bound admission/copy/read;
close drains owned work before releasing buffers. No backend change creates a
second durable truth or modifies fences/checkpoints.

Disk codecs for M1-M3 must keep overlay HMAC-key/AES-GCM-value protection and
checker authenticated run encoding. Marker/crawl plaintext is a documented
current confidentiality gap, not an acceptable new KV default. Exact keys can
be HMAC-tokenized under a per-run key and values AEAD-encrypted; ordered scan
consumers must use the specialized encrypted-sort representation rather than
claiming HMAC tokens preserve plaintext ordering. Bind kind/version/key token
as associated data, use unique nonces, bound untrusted lengths before allocation,
and keep ephemeral keys out of disk/logs/profiles exported to shared locations.
Fixtures use synthetic names and private artifact directories. Private modes
are required but do not substitute for encryption. No plaintext key index may
be introduced to recover ordering without explicit security review.

## Baseline protocol and frozen evaluation gates

Run `bash helpers/phase35-m0/run.sh` from the repository. It compares the clean
pre-M0 revision `10ea6d6b5` with the current candidate using three balanced
repetitions, identical four-core Go settings, unchanged existing Pebble lookup,
import-transaction and full-check fixture benchmarks. It additionally profiles
deterministic overlay, marker, directory, stage-3 import and full-check probes.
All probes assert semantic counts/decisions and cleanup; checker logical/input
digests must match across control and candidate. Wrong/zero test filters, absent
profiles, invalid observations and leftover scratch reject the capture.

Retain binaries, source revision/patch and untracked-source archive, compiler
settings, raw logs, GC traces, CPU/heap profiles and decoded tops, OS time/RSS,
static dependency creation audit, fixture file traces, analysis and checksums.
The artifact parent may be configured explicitly; default `/tmp` measures this
host's VM vdisk and is not an SSD/NFS/RADOS production performance claim.
Trace runs are separate from timing runs. No production source, repository,
daemon, broker credential or socket is used. All test fixtures are disposable.

M0 gate: exact semantic/parity/privacy/cancellation/cleanup tests and the
reproducible baseline protocol pass. Timing distributions are observations, not
a guarantee of instrumentation neutrality. Median control/candidate ratios
above 1.15 require review and a focused repeat before accepting M0; a faster
sample is not proof of a performance win. Keep failed captures intact.

Future M5 policy gate, frozen before backend selection: complete workload
semantics first; no swap/OOM or managed-capacity violation; preserve at least
the resolved headroom. Under the same workload/concurrency, GC CPU and assist
shares may not increase by more than two percentage points over the paired
baseline, and cumulative-histogram pause-p99 upper bounds may not exceed
`max(10 ms, 1.25 * baseline)`. Operation p99 latency may not exceed 1.10 times
paired baseline. If production has a stricter operator SLO it must be recorded
before the matrix and wins over these initial conservative review gates.
GC CPU share uses process CPU (not summed concurrent wall durations); endpoint
runtime data alone is not sufficient to establish that production SLO.
An unmet/unknown gate means no default/recommendation approval, not permission
to change the threshold after seeing candidate results.

The following workload-specific M0 endpoint ranges freeze the initial synthetic
fixture comparison reference. Each future matching-fixture comparison uses its
own paired CPU/assist/latency baseline, the two-percentage-point GC/assist rule,
and the 1.10 latency ratio above; it must not substitute another workload's
measurements. The pause gate remains `max(10 ms, 1.25 * paired baseline)`.
There is no supplied production operator SLO, so none is claimed validated.

| Synthetic probe | After-run scannable heap, bytes (three runs) | Cumulative pause-p99 upper bound, ms (three runs) |
|---|---:|---:|
| 8,192 written locations | 1,393,832-1,424,704 | 0.098304-0.131072 |
| 8,193 marker decisions / 8,192 committed | 1,653,624-2,419,808 | 0.163840-0.393216 |
| 513 directories | 1,436,168-1,857,728 | 0.131072-0.163840 |
| 64 stage-3 import packs | 1,470,520-1,660,872 | 0.196608 |
| 16,384 checked locations | 2,771,376-11,101,384 | 0.393216 |

Runtime deltas in each probe cover that benchmark calibration's **entire
iteration loop**, while the store snapshot describes its last iteration. Heap
endpoints are neither retained-payload ratios nor maximum heap measurements.
The retained benchmark lines supply iteration counts; the profile contains
allocation and in-use sample types. M2/M5 must add sampled peaks and aligned
intervals before deriving admission multipliers from these measurements.

GC mitigations remain prerequisites for those milestones: fixed-width scalar
indexes, chunked byte arenas with offset/length records, bounded growth overlap,
streaming release, small encoder/RPC queues, early cache eviction, and reserved
one-way migration. Measure scannable bytes and assist CPU alongside allocation
rate and RSS; do not disable GC or silently tune `GOGC`/`GOMEMLIMIT` to make a
RAM candidate appear viable. An explicit limit is a ceiling, not an instruction
to fill all available memory.

## Completion record

Accepted final isolated capture: `/tmp/vaultic-phase35-m0-mL1kwz`. Its original
`SHA256SUMS` digest is
`62fcd6b4a103be00d580831c5aeac7ac104c3cf9fe87d1b842cf73d5e2191915`.
Baseline revision: `10ea6d6b5`; source patch and untracked archive identify the
candidate. Final note/status/evidence-pointer amendments are separately
sealed alongside the original capture; production instrumentation is unchanged
after the accepted run. Artifact directories are private and marked `.nobackup`.

| Exit gate | Evidence / disposition |
|---|---|
| Complete classified inventory | Owning creation/retention/cleanup paths above, pinned command dependency audit, separate runtime traces of all three Pebble store owners; unavailable platform/provider branches explicitly classified |
| Bounded and honest observations | Fixed identities, mutex/atomic snapshots, saturation and availability tests; overwrite upper bounds, unsupported runtime metric and failed-cleanup checks; no unified RAM enforcement claim |
| Original/candidate baseline | Three balanced runs; median timing ratios lookup 0.993637, import 0.975314, check 1.000691; all below the 1.15 review trigger; no production throughput claim |
| Exact checker parity | All six original/candidate input digests `1b717a8d3e17efe6bcf3f40b292929679c6bf23c67653b2765dc2314fd6d5dbc`, result digests `8952d2f88277ef3c39335a64cd3b51967b35e12439a961620bc9a05d97a18b08` |
| Probe counts and cleanup | Three runs of all five probes; 8,192 overlay entries / 1,212,416 encoded bytes; 8,192 marker commits / 344,064 bytes; 513 directories; import peak prepared 37,600 bytes, drained; checker peak reserved scratch 1,749,452 bytes; empty final scratch |
| Profiles and resource evidence | 33 CPU and 33 heap profiles, nonempty decoded profiles, process time/RSS and GC logs; first paired-run RSS KiB: lookup control/candidate 31,424/31,968; import 33,928/34,164; check 98,164/98,488; full repetitions retained |
| Tests | Full telemetry/index/crawl/maintenance/legacyimport/backupcmd suites pass; complete archiver suite passes as UID/GID 65534; focused seven-package race suite passes, including handoff, spill failures, manifest cancellation and scheduler concurrency |
| Privacy/defaults/contracts | Cancelled backup final JSON/quiet/privacy tests, checker digest-boundary regression, existing expected-value/parity tests and analyzer rejection self-tests; future policy/errors/codecs/admission frozen above |
| Reproduction | Analyzer independently rerun against accepted logs; every original checksum independently verified; shell syntax, document links and whitespace checked |

The first capture `/tmp/vaultic-phase35-m0-bqdbDl` is retained **rejected**.
It caught value-typed check telemetry changing zeroed-resource JSON and therefore
the canonical result digest. Making the field optional/omitted restored exact
baseline parity; the regression test and fresh capture verify the repair without
weakening the gate. A second review strengthened missing-profile/trace/metadata
checks and ensured staged source edits are captured.

The intermediate accepted capture `/tmp/vaultic-phase35-m0-jxziOZ` is retained
but superseded by the final capture after a third review found that cwalk could
notify completion before resetting its observed batch. The writer now closes
its batch and clears the observation before notification. A deterministic
blocked-receiver test passes 20 race-detector repetitions; touched caller suites
and the complete baseline protocol were rerun after the repair.

Root's ability to read mode-000 files makes the preexisting archiver unreadable
fixture panic; the untouched baseline reproduces it. The full candidate suite
was run with privileges dropped, rather than changing or omitting those tests.

M0 was reevaluated against every milestone bullet after repairs and independent
evidence verification, with no remaining M0 findings. This is not RAM-only
coverage, a backend selection, enforced aggregate memory accounting, or a
production recommendation. Large-scale RAM/KV comparisons, actual RAM-limit
actions, mmap/RSS pressure integration and rollout remain M1-M6 work.
