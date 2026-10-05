# Phase 35 M6 Preparation

[Phase 35 roadmap](phase-35-ram-working-memory-and-kv-fallback.md) |
[Sealed M5 evidence](phase-35-m5-benchmarks.md)

**Status: M6 preparation complete. Performance diagnosis/repair, fresh sealed
repeated evidence, reproducible decision package, operator contract and cutover
review are complete. Production pilot, defaults and Pebble removal remain
blocked; this is not full M6 production acceptance.**

## Scope and Decision Boundary

Preparation diagnoses the remaining KV costs, publishes reproducible conditional
observations, defines operator acceptance inputs and prepares an isolated pilot.
It is not the full M6 removal/default milestone. In the operator's absence,
only private disposable generated fixtures are used. Production services,
credentials, repositories and source data are untouched. Existing frozen gates,
payload shapes, memory/disk limits and default GC settings remain unchanged.

The prior four-repetition 10 GB KV rejection was GC CPU share, not semantic
failure or missing observations. A cumulative 100 GB CPU profile found repeated
whole-database freelist reconstruction in the write path. Bbolt's physical
rollback after `ErrMaxSizeReached` reconstructs free pages when freelist
persistence is disabled. Incremental growth was safe but triggered this scan
at successive ceilings.

The prepared candidate checks allocated pages before large writes and attempts
admitted growth when the existing ceiling is near. The estimate uses twice the
encoded batch size plus per-entry page allowance; it is a performance hint,
not a claim that every B-tree split can be predicted. Denied speculative growth
does not reject a write that still fits. The exact library ceiling, original
typed failure/retry path, mapping/disk charging and cleanup remain authoritative.
Small mappings keep the existing behavior. No consistency checking, freelist
recovery, encryption verification or durability contract is disabled.

`TestKVPreemptiveGrowthAdmission` verifies admitted and denied speculative
growth, exact committed contents and final release. It is mandatory in fresh
sealed captures alongside the existing conformance, pressure, migration,
failure, cancellation, race and pure-Go build gates.

## Reproducible Decision Package

Run the generator only after captures finish:

```sh
node helpers/phase35-m6/prepare.cjs --selftest
node helpers/phase35-m6/prepare.cjs /private/completion/m6-report.json - \
  /private/reduced-capture REDUCED_MANIFEST_SHA256 \
  /private/extended-capture EXTENDED_MANIFEST_SHA256
```

The manifest hashes are explicit trusted identities of completed captures,
not values the generator silently adopts from mutable inputs. Every listed
artifact is hashed with bounded streaming buffers. Required plan, analysis,
case and verification artifacts must be sealed; paths escaping the owned root,
changed identities, modified bytes, duplicate entries and changed frozen gates
are rejected. Reports cannot overwrite retained outputs or reside inside a
sealed capture. Original captures remain unchanged.

The JSON includes every mode/scenario/envelope/repetition, storage qualification,
wall dispersion, observed managed retained/peak charge, mapping capacity,
RSS, GC/assist ranges, throughput and operation tails. Required capacity and
physical residency are distinct; managed peak charge is not a measurement of
physical migration coexistence. Unknown observations stay unknown. All
rejected/incomplete classifications and reasons are retained.

Disposition is conditional and scoped to the observed generated workload:

- `eligible-for-disposable-fixture-comparison`: all repetitions complete and
  meet frozen gates. This is not production recommendation or total-throughput
  acceptance.
- `not-recommended-on-this-evidence`: any known rejection or typed exhaustion.
- `insufficient-performance-evidence`: semantics may pass but gates are unknown.
- `historical-baseline-only`: retained Pebble measurements, not a new default.

Prefer comparing eligible candidates by actual wall/throughput, not GC cycles
alone. RAM fit does not establish acceptable GC for another distribution; KV
tail acceptance does not justify slower throughput without an operator tradeoff.
Auto must demonstrate exact migration, bounded overlap and cleanup rather than
merely selecting the mode. CLI single-invocation duration is not operation p99.
Generated, warm-cache NFS evidence is not a representative production legacy
distribution, cold-cache result or proof of physical HDD identity.

## Operator Contract

Using `-` emits a blocked template in `operator_plan`. Passing a reviewed JSON
file in its place records the proposed pilot inputs and their blockers. The
generator does not execute a pilot or grant production/default/removal approval,
even with a complete operator plan. Operator input must be frozen before its
own measurements, not retrofitted to candidate results.

Required contract:

```json
{
  "schema": "phase35-m6-operator-plan-v1",
  "authorization": {
    "scope": "disposable-fixtures-only",
    "reviewer": null,
    "approval_reference": null,
    "source_roots": [],
    "destination_root": null,
    "scratch_root": null
  },
  "workload_slos": [],
  "rollback": {"stop_conditions": [], "recovery_procedure": null},
  "runtime_tuning_allowed": false,
  "default_change_allowed": false,
  "pebble_removal_allowed": false
}
```

Each workload SLO must provide `workload`, `storage`,
`distribution_manifest_sha256`, `repetitions` (at least three),
`minimum_tail_observations` (at least 100), `maximum_wall_seconds`,
`minimum_throughput_bytes_per_second`, `maximum_rss_bytes`,
`minimum_headroom_bytes`, `maximum_gc_cpu_share`, `maximum_assist_cpu_share`,
`maximum_pause_p99_seconds` and `maximum_operation_p99_seconds`. Numeric targets
must be finite and positive; CPU shares must not exceed one. Specify whether
throughput means logical inspected bytes, physical transfer or completed
operations. These units must match the pilot's measured counters. No production
SLO is invented here.

The operator must identify representative distributions for initial, unchanged,
high-churn and high-live backup; full/index import with duplicate/damaged-input
semantics and resume; exact/smaller-scope checks with ordered/multiset joins and
bounded findings. Storage/source types, worker counts, batching, encryption,
daemon binary/configuration and durability must be pinned. Read-only real-input
authorization does not permit writing to the source or a production repository.

## Pilot Checklist

1. Obtain exact source/destination/scratch authorization, reviewed operator
   identity and approval reference. Confirm private durable destinations are
   disjoint from sources and production state; do not use production credentials.
2. Hash distribution/configuration inputs and freeze SLOs, existing semantic/GC
   gates, repetitions and balanced mode order before timings. Keep immutable
   historical baselines and pinned binaries/settings.
3. Check measured headroom and aggregate cgroup/host limits, process/runtime
   residency, migration reserve and disk capacity. Managed byte limits are not
   RSS guarantees. Prohibit swap; never relax margins to make a case fit.
4. Exercise below/near/above envelopes, actual retained live state, interleaved
   allocation/reads and GC, true unchanged metadata, complete imports/checks,
   exact contents/restores/reopen/resume and owned cleanup. Preserve failures.
5. Stop on semantic divergence, unknown publication outcome, resource/cgroup
   violations, corruption, unbounded growth, scratch leak or unacceptable SLO.
   Do not retry uncertain daemon publication. Keep the last verified production
   configuration unchanged; discard only demonstrably owned disposable state.
6. Independently reproduce analysis and seal raw profiles/logs/tool/source
   identities. Retain all repetitions and unknown gates. Review workload-specific
   mode/envelope recommendations before authorizing another stage.

The disposable repeated matrices are the currently authorized pilot evidence.
A real-input or production pilot remains blocked on representative inputs,
operator SLOs, exact authorization and reviewed stop/rollback procedures.

## Cutover Inventory

Pebble v1.1.5 remains a live dependency in `go.mod` and
`internal/workingkv/store.go`. The unconfigured `OpenWorking` disk path still
selects Pebble; `internal/archiver/marker_store.go` retains its unconfigured
Pebble-backed marker behavior. `internal/index/written_blob_store.go` uses
`OpenWorking` for written-blob bookkeeping. The generic store tests exercise
existing backends, while historical fixtures require a pinned Pebble baseline.

Before full M6 removal, migrate and validate every live unconfigured consumer,
default/profile/environment precedence, cleanup and return-value ownership.
Run focused/required repository suites, exact named compatibility tests,
supported pure-Go builds and a complete dependency/import audit. Historical
artifacts must stay readable without shipping a hidden fallback implementation.
Removing the dependency now would change existing behavior and exceed preparation
authorization; it is deliberately not performed.

The relevant compatibility anchors include `TestM4WorkingMemoryCommandRoundTrip`,
`TestM4WorkingMemoryEnvironmentPrecedence`, `TestM4WorkingMemoryProfileConflictAndStop`,
`TestM4WorkingMemoryProfilePrecedence`, `TestWorkingPolicyStore`, store conformance,
marker/exclusion tests and written-blob lifecycle tests. Production defaults,
runtime tuning and dependency removal each require their own reviewed evidence.

## Completed Evidence and Recommendations

Fresh reduced `/tmp/vaultic-phase35-m5-rRTnLC` passed all 400 semantic cases
and four balanced repetitions: accepted 12 / rejected 115 / incomplete 273.
Its original manifest identity is
`3ea39635c6ce1d525dce37c16281ef421aaf5b17db64ff3ed50f99b6cbe342d5`.
Actual cgroup peak was 3,942,993,920 bytes, with 17,597 owned-monitor checks.

Fresh extended
`/volume2/NASDA2/vaultic-phase35-remediation-probes-20261004/vaultic-phase35-m5-ofuu0H`
passed all 416 semantic cases and four balanced repetitions: accepted 24 /
rejected 64 / incomplete 328. Its original manifest identity is
`cb7fb7002fd38b88b9ab4545729fea81cc3916c6b30bab716b5444aca85cb4c2`.
Required preflight recorded 281 measurement and 429 import/check race
test/subtest passes with no skips/failures, plus all 12 supported pure-Go core
builds. All modes completed four actual 10,000,007,168-byte and
100,000,006,144-byte targets with exact contents/sequence/digest, 1,536
interleaved reads, 3 GiB churn, actual GC and owned cleanup. All 48 full-import
cells completed. Actual cgroup peak was 163,753,431,040 bytes; 278,460
owned-monitor checks passed. Both captures had no max/OOM/OOM-kill events or
observed swap. Their original manifests, source/seed/tool pins, decoded profiles
and independently reproduced frozen analysis remain intact. Empty sampled
external-process peaks are not proof that the whole host was idle.

The following recommendations apply only to these generated warm-cache NFS
retained-state fixtures, not production backups or an arbitrary distribution.
No minimum safe production envelope is inferred from a successful configured
limit. Throughput includes encryption, instrumentation and allocation churn.

| Target / Mode | Configured Limit | Retained Managed Bytes | Median Wall (s) | GC CPU Share Range | Frozen Gate / Conditional Recommendation |
|---|---|---:|---:|---|---|
| 10 GB / RAM | 32 GiB | 12,920,897,024 | 9.1 | 0.41-0.58% | accepted; first fixture candidate when the measured envelope fits |
| 10 GB / KV | 32 GiB | 14,763,991,104 | 37.5 | 19.27-19.68% | rejected; not recommended on GC-sensitive evidence |
| 10 GB / auto | 32 GiB | 14,763,991,104 | 100.2 | 4.04-4.63% | accepted; bounded migration candidate, not a throughput win |
| 100 GB / RAM | 140 GiB | 129,199,218,112 | 79.0 | 0.11-0.13% | accepted; first fixture candidate when the measured envelope fits |
| 100 GB / KV | 140 GiB | 139,586,478,144 | 337.6 | 7.79-8.20% | rejected; not recommended on GC-sensitive evidence |
| 100 GB / auto | 140 GiB | 139,586,478,144 | 845.9 | 2.88-3.35% | accepted; bounded migration candidate, not a throughput win |

Historical Pebble medians were 50.8 and 517.3 seconds at 10/100 GB. The
pre-growth repair reduced the prior candidate KV medians from 47.8/1,601.3 to
37.5/337.6 seconds, and auto from 110.4/2,085.3 to 100.2/845.9 seconds.
Those before/after captures are sequential diagnostics, not matched production
speedup proof. Within the new capture, KV throughput exceeds its paired Pebble
baseline but fails the unchanged GC CPU-share gate in every large repetition.
Removing work changes the CPU-share denominator; faster wall time cannot
override that rejection. At 100 GB sampled KV RSS peaked at 738,435,072 bytes,
but owned file cache remains charged to the cgroup and full mapping capacity
remains admitted. Small RSS does not authorize a smaller whole-process budget.

Profile re-evaluation no longer finds repeated freelist reconstruction among
dominant large-write paths. The remaining 10 GB allocation profile attributes
71.57% of allocated bytes to bbolt's multi-page transaction buffers. Bbolt
v1.5.0 pools single-page buffers, but this allocation path has no supported
external reuse hook. This is a measured library/workload cost, not a reason to
change payloads, inflate retained heap, disable recovery checks, fork the
allocator or silently tune GC. A separately reviewed library/tuning experiment
would need new semantic and resource/performance evidence. Strict RAM may still
fail on other distributions; automatic mode does not guarantee fit either.

The final decision package is retained outside both captures at
`/volume2/NASDA2/vaultic-phase35-remediation-probes-20261004/m6-preparation-completion/preparation-report-final.json`.
It includes 204 scenario/mode/envelope groups and all 816 per-repeat records.
Its independently reproduced SHA-256 is
`694accee946b051cd3786d99f488966ff4b0fb6221448ccb1dbf8d1aa6f0ca31`.
Every listed capture artifact was stream-hashed twice against the explicit
identities above. Prior draft reports remain diagnostics, not replacements.

## Final Re-evaluation

Preparation fulfills the performance diagnosis, bounded local repair, repeated
verification, conditional workload/storage/envelope observations, operator
contract, disposable pilot evidence and live-consumer cutover inventory.
No unresolved implementation finding remains in this exercised preparation
scope. Re-evaluation repaired the report CLI's capture-pair count, separated
managed peak from physical migration residency, rejected overlapping/malformed
pilot roots and retained all repeat metrics rather than only group ranges.
Selftests reject changed manifests/artifacts, missing/invalid SLOs, unreviewed
default/removal changes, capture modification and report overwrite. Even a
structurally complete reviewed plan cannot implicitly approve or execute
production work. A complete read-only real-input plan still records production
pilot authorization as missing. Actual full-capture generation and byte-identical independent
report reproduction passed.

Named compatibility regressions passed under the race detector: all four
`TestM4WorkingMemory*` owner tests listed above, with RAM/KV/auto command round
trips and backup/import/index-check/check profile subcases. Existing omission
behavior and Pebble dependency remain unchanged. The measured Go sources match
both sealed captures; final patch/tree/commit and generator/report identities
receive a separate completion seal outside the original manifests.

Known gate rejections and unknown CLI p99 are retained limitations, not waived
acceptance findings. Five production prerequisites remain explicit: reviewed
authorization, real workload inputs, exact source/destination/scratch roots,
operator SLOs/distribution and reviewed stop/rollback procedure. The full M6
recommendation/default/removal milestone remains pending those external inputs
and its own evidence. No production changes, runtime tuning, default change,
dependency removal or production pilot occurred.