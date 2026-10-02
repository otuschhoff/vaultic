# Phase 35 M3: Command-Path Integration

[Back to Phase 35](phase-35-ram-working-memory-and-kv-fallback.md)

Status: complete for programmatic forced-RAM/forced-KV integration. M4-M6
remain unimplemented. There are no new CLI/profile flags, automatic migration,
process-pressure controls, default rollout, or production performance approval.
The historical M0-M2 documents describe their own source/captures, not this
milestone's current implementation.

## Selection And Ownership

[Policy and owned stores](../../../internal/workingkv/policy.go) expose
`NewPolicy(mode, limit, scratch)` and `WithPolicy(ctx, policy)`. A run shares one
policy through its context. Only `ram` and `kv` are accepted; limits must be
positive. RAM uses the M2 pointer-light backend and never examines or creates
its scratch parent. KV selects the pinned bbolt adapter for participating KV
stores, honoring the policy scratch override. With no policy, existing backend
selection remains Pebble and legacy traversal fallback remains available.
Disposable marker/manifest encoding is now protected even without a policy.

Backup constructors, marker selection, directory walking, fresh-import setup,
import preparation/reduction, and check spools preserve that context. Public
compatibility constructors retain their old signatures. Owned close operations
join workers, release reservations once, close the engine, and remove only the
owned scratch root; original causes and cleanup failures remain observable.
The shared mode/context carrier is a dependency-free leaf package, preventing
storage-test/telemetry/daemon import cycles.

Explicit-policy manifest errors propagate instead of silently falling back to
sequential traversal. Strict RAM sort pressure returns
`ErrWorkingMemoryLimitExceeded` before creating scratch, including when the
scratch parent does not exist. Capacity exhaustion is not permission to change
backend, retry uncertain publication, or acknowledge incomplete work.

## Every M0 Surface

This table closes the [M0 inventory](phase-35-m0-contract.md). KV selection is
not a demand to turn an efficient streaming consumer into a database. Evidence
names identify executed fixtures/suites, not unexecuted provider branches.

| M0 Owner | RAM / KV Contract | Parity Or Preservation Evidence |
| --- | --- | --- |
| Written-blob overlay | Shared-budget RAM / bbolt; original HMAC/AES-GCM codec and distinct pack/offset keys retained | `TestM3OverlayParity`; real-daemon published-overlay test in both modes |
| Marker decisions/rejection maps | Existing bounded entry maps; lazy encrypted exact-key store is RAM / bbolt; sticky selection failure | `TestM3BackupParity`, marker suite, required KV marker file trace |
| Local directory manifest/listing queue | Encrypted RAM / bbolt; wide listings use bounded chunks; no explicit-policy traversal fallback | `TestM3ManifestParity`, backup parity and exhaustion, full crawl suite |
| Incremental/local/NFS directory stream | Existing streaming/lookahead/metadata-cache path in both modes; no new KV or spill | Full crawl and unprivileged archiver suites; streaming paths not replaced |
| Lookup LRU/pending batches | Recomputable RAM cache in both modes; policy clamps capacity and reserves fixed/cache overhead; existing bounded channels | `TestM3LookupCapacity`, full lookup suite, daemon authority fixture |
| Legacy master/index/indexmap and pending blobs | Existing in-memory compact indexes in both modes; exported legacy JSON remains durable projection, not scratch | Backup snapshot/restore parity, import record/checkpoint parity, existing index suite |
| Fixed-location check spools/partition children | Reserve retained capacity in shared pool; RAM cannot spill; KV retains encrypted external sort, not bbolt | `TestM3SorterParity`, RAM spill rejection, reservation ownership-transfer test |
| Variable key/value/sequence check spools | Validate before copying and reserve backing overhead; RAM cannot spill; KV retains authenticated ordered runs | Exact key/sequence sorter parity, RAM spill rejection, maintenance suite |
| Check catalogs/analytics/findings/scan pages | Policy clamps existing check memory envelope; streaming findings/joins remain unchanged; optional unreserved compaction map disabled | `TestM3CheckParity` against independent legacy baseline; exact semantic result excluding resource/options fields |
| Import decoded indexes/grouped packs/tree traversal/preparation/reducers | Retain streaming/source transformations; reserve prepared work before worker construction, clamp transaction/prepared limits; join dispatch and release on acknowledgement/cancellation | `TestM3ImportParity`, `TestM3SplitImportParity`, checkpoint/resume/failure and cancellation suite |
| Ordered legacy-index read-ahead | Existing ordered decoded-source consumption in both modes, no new working database | Full import suite and matched imported records/reduction sequence |
| Go fresh-import filter/planner/catalog/receipts | Explicit policies use existing authoritative-read fallback rather than an independently growing Bloom filter; preserve fresh/deferred durability semantics | `TestM3FreshImportPolicyFallback`, original filter tests, import suite |
| Daemon RPC/session/split dependency/publication state | Existing transient RAM and durable daemon authority in both modes; no second Go truth or uncertain-publication fallback | Real-daemon pinned-session/nonadvancement/readmission fixture, split-import exact reduction and checkpoint tests |
| Ordinary encrypted repository cache/check cache | Excluded cache I/O in both modes; strict RAM is not `--no-cache` | Existing command/check suites; excluded from working-file assertions |
| Phase 30 repository read cache | Excluded data-plane cache; existing configuration unchanged in both modes | Existing repository authority fixture; no cache policy change |
| Pack staging/fileio | Excluded encrypted pack staging in both modes, including unlinked-open files | Backup/restored-byte fixtures and `%file` traces; not counted as metadata spills |
| Placement-action pack transfer | Excluded transfer, not enabled by M3 | Owner unchanged; not claimed dynamically exercised |
| Local/provider Save, multipart and locks | Durable backend output/provider protocol in both modes, not disposable KV | Local backup/restore fixtures; remote provider branches not claimed exercised |
| Bootstrap/profile, capsule activation and custody handoff | Durable control/custody state; existing protected-file rules unchanged | Owners unchanged; no production/bootstrap trial performed |
| APFS mount/control | Excluded source lifetime/control, not a KV spill | Unchanged; unavailable in this Linux matrix |
| Healing/upgrade/selfupdate/NFS readiness/test helpers | Nonparticipating opt-in surfaces; not activated by M3 | Owners unchanged; harness uses disposable fixtures only |

This is not a complete process/RSS bound. Existing decision maps, full-source
decoding/indexes, read-ahead, traversal closures, RPC encodings, findings, and
caller-held inputs/results retain their current ownership/bounds. The shared
reservations cover the RAM backend, lookup cache, admitted prepared import work,
and retained check-spool capacities. M4 must coordinate remaining caller copies,
all siblings, bbolt mapped/engine state, aggregate disk limits, and process
headroom. KV may still use RAM for streaming and caches; RAM may still perform
excluded cache, source, pack, control, and durable-output I/O. None of these
exceptions authorizes a participating metadata spill in explicit RAM mode.

## Confidentiality And Exactness

[SecureMap](../../../internal/workingkv/secure_map.go) uses independent per-store
random AES-256/HMAC-SHA256 key material, HMAC-tokenized exact keys, unique GCM
nonces, and authenticated kind/version/key-token associated data. No plaintext
path index is stored. Marker specification/directory decisions remain exact;
manifest names retain their ordered, complete JSON representation.

Values above 128 KiB use separately authenticated bounded chunks, identified by
random generation and ordinal. The authenticated root is published last, so a
failed wide write cannot expose a partial replacement. Root/chunk shape and
lengths are validated; authentication failure returns no plaintext. Overwritten
or failed generations can remain unreachable until store close; they remain
charged in RAM and can cause typed exhaustion, never hidden disk fallback.
The adapter has no delete/compactor API. Caller reconstruction buffers are not
a claim of whole-process accounting. Existing overlay and checker codecs remain
unchanged. Tests include binary/nil/wide values, absent sensitive disk strings,
tampered ciphertext, and wrong-kind/version authentication failure.

Import dispatchers are joined before returning, so acknowledged/cancelled work
releases capacity deterministically. Exact reduction order is compared between
modes; deterministic batch IDs are not assumed to be monotonic ordinals. Sorter
adoption transfers capacity ownership without early/double release. The final
review repaired a tight-budget M2 race-test assumption: concurrent planning can
reject both writers. The test now requires at most one winner, no overspend,
zero final usage, and successful retry after released planning reservations;
the RAM implementation itself was not changed.

## Retained Acceptance Evidence

Reproduce with [run.sh](../../../helpers/phase35-m3/run.sh); its
[analyzer](../../../helpers/phase35-m3/analyze.cjs) rejects missing/skipped named
gates, absent mode/authority cases, incomplete traces/matrices, and leftover
scratch. Final capture: `/tmp/vaultic-phase35-m3-49apDE`, Go 1.27.1,
Linux/amd64, four runtime CPUs, unchanged production settings. Original evidence
manifest SHA-256:
`107c9d94540f1f7877aaf20bfebcfa970ee95379fb105c94445575a428c1a8a0`.

- 966 passed race test/subtest events across workingkv, telemetry, crawl, index,
  maintenance, import, backupcmd and indexcmd; separately passed daemon/filter
  gates and both real-daemon authority cases, without skipping.
- Entire archiver race suite passes as UID/GID 65534 in isolated scratch;
  unreadable-directory semantics are tested without weakening permissions.
- Forced-mode snapshot contents and actual restored text/NUL/binary bytes
  match; import records/checkpoints/resume and exact check findings match.
- Separate `%file` traces cover six package binaries in each mode, with zero
  participating RAM paths and required KV overlay/marker/manifest/check paths.
  Source fixtures, restored files, durable packs/output and ordinary caches are
  excluded explicitly, not described as a completely filesystem-free CLI.
- All 12 `CGO_ENABLED=0` core target builds pass; native Linux/386 secure-map
  execution passes. Ten CLI targets build. Linux/386 and Linux/arm CLI builds
  retain the unchanged pre-existing `MaxPackSize`-overflows-`uint` failure in
  repository pack-sizing code. The harness requires that exact error and
  unchanged owning files; this is not claimed as 12 successful CLI builds.
- All retained checksums verify; every fixture releases its owned scratch and
  tested reservations. Analyzer negative missing/skipped-gate self-tests pass.

Failed captures remain retained, including the final-review reservation-test
failure. Source/commit seals supplement, rather than replace, the original
manifest. Evidence is disposable local data, not authoritative repository state.

## Exit Review

Each M3 conversion slice, every M0 inventory row, confidentiality and wide-record
failure behavior, admission/release ownership, exact comparator/reduction
semantics, checkpoint/publication authority, restored bytes, and working-file
traces were reevaluated after repairs. The final-source acceptance capture and
independent analyzer pass. No unresolved M3 finding is deferred. M4 policy/CLI,
fallback and aggregate/headroom controls, M5 realistic performance/GC approval,
and M6 rollout remain separate gates. No production restart, cache change,
credential use, data deletion, or production backup trial was performed.