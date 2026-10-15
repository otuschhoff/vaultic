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

## Authorized Real-Input Screening: 2026-10-06

The operator subsequently authorized read-only input from
`/volume2/NASDA2/rustic/repo`, using the password-file reference
`/volume2/NASDA2/rustic/etc/repo-password.txt`, and every backup source/option
in `/volume2/NASDA2/rustic/etc/rustic.toml`. Only isolated test destinations,
state and scratch under `/volume2/NASDA2/vaultic-test-m6/` are writable.
No password contents are published. This authorization supersedes the missing
input/path/SLO prerequisites for this screening scope only, not the historical
preparation report or production/default/removal decisions.

The ceiling is 200,000,000,000 bytes combined CLI/daemon RSS, excluding unmapped
filesystem page cache. Linux file-backed mapped RSS still counts as RSS.
RSS is sampled every 250 ms; this is a fail-stop sampled guard, not an
instantaneous kernel RSS limiter. Cgroup current/file-cache bytes are separate
diagnostics, never added to RSS or used as a substitute 200 GB cgroup limit.
Shared NFS free space must remain above the larger of 5% of filesystem capacity
and 20% of the initial available space; reaching that reserve stops the pilot.
Import and each check have hard 43,200-second limits; each backup has a
604,800-second limit. No two-day extension is approved without a new request.
Completed-work throughput and total wall time are the performance criteria;
latency/GC observations remain diagnostic for this operator screening, while
historical synthetic frozen gates remain unchanged.

`helpers/phase35-m6/pilot.py` freezes binaries, the original profile, metadata
file-name/size inventory and a copy of itself. Each mode uses a separate
repository view with private configuration/locks and read-only source metadata
and pack references, an independent native database and a fresh backup target.
Kernel read-only mounts protect the original repository, configuration,
password-file and backup sources. Import may activate only its private copied
configuration. Hot/cold mode is deliberately not used: it would write locks on
the cold input. Original metadata/pack deletion by another operator remains an
external hazard; source changes or failures are not silently repaired/retried.

All 52 sources must be present. The original `.snapshot` directory/descendant,
trash/lost+found, marker-file and one-filesystem exclusions/options are preserved.
The profile matcher and real-walk regressions prove `.snapshot` trees are
pruned before descent. NFS create modes alone were insufficient on this export;
owned private directories are explicitly chmodded and verified as 0700.
The bounded dry-run preflight is not a completed import/performance result.

The first matrix is one screening repetition per Pebble/RAM/KV/auto mode, with
a 160 GiB managed working-state cap for configured modes and unchanged runtime
defaults. It runs complete import, native/legacy index comparison, all-snapshot
repository structural check and an initial backup. Structural check does not
claim an all-pack payload scrub. Partial/failed/timed-out/resource-stopped
commands are retained failures, not completed-work throughput. Frozen import
counts must match every index and snapshot. Single fixed-order screening,
warm-cache effects, shared host activity and changing backup files preclude
repeated-production acceptance or a truly unchanged-backup claim. Real results
remain pending; the original preparation seals are not modified.

### Throughput and Bottleneck Observation

At the operator's request, `helpers/phase35-m6/observe.py` runs as a separate
lightweight service, outside the measured cgroup, sampling every five seconds.
It does not change the pinned runner/binaries or original launch manifest.
Import/index-check native telemetry retains work counters, queues and operation
timings; rates match counter labels, component process identities and actual
sample windows, rejecting decreases/restarts/stale observations. Import encoded
batch bytes, packs and mutations are not labeled logical backup throughput.
Backup JSON retains allowlisted logical bytes/files and final processed counts,
never current filenames. Structural repository-check has process/cgroup and
completion evidence; missing phase-work/GC counters remain unknown.

Per-process CPU-core rates, RSS, threads, I/O and scheduler counters are correlated
with cgroup CPU/I/O/memory pressure, file cache and queue/operation time series.
NFS RPC and host pressure are shared-host diagnostics, not pilot-attributed
throughput; `/proc/PID/io` does not capture all NFS traffic. Observation attached
after the first import started, so earlier OS data cannot be reconstructed.
Fixed-order screening and observation overhead remain limitations. Separate
observer versions/pins/logs are retained and will receive a supplemental/final
seal rather than extending the original immutable launch manifest.

Each scenario's final findings must report completed scope, wall time and work
units, sustained/window throughput, resource/queue evidence and availability
gaps. CPU saturation, I/O waits, memory/GC pressure or coordination stalls require
supporting measurements; low CPU or a single empty queue does not prove a cause.
The active pilot and all full-workload acceptance results remain pending.

### First Real-Input Finding: Pebble Import Timeout

The first full Pebble cell stopped at its 12-hour action deadline, followed by
30 seconds of termination grace. It failed runtime/full-scope acceptance; checks
and backup did not run. Peak combined CLI/daemon RSS was 45,328,183,296 bytes,
below the 200 GB RSS ceiling. The termination summary reports all 10,170 indexes,
419,853 packs and 380,166,821 blob records imported, but only 22 of 143 snapshots
completed. It visited 36,288,545 trees and 194,058,757 nodes; the single finding
was `load-tree: context canceled`. These partial counts are not successful
full-import throughput or approval to activate the candidate.

Batch publication stopped increasing about 7.2 hours before termination while
snapshot work remained unfinished. Observed combined CLI/daemon CPU averaged
about 2.06 cores, with no sampled nonzero memory-pressure avg10. This identifies
snapshot traversal as the incomplete phase, not a memory stop, but does not
prove network latency, serialization or cache behavior is the root cause.
Aggregate planning/blob-read timers overlap other activity and cannot be
interpreted as percentages of wall time. Cgroup file cache peaked around
175.3 GB in observed samples; it is not RSS and does not violate the RSS-only
operator ceiling.

The failed cell, counters and observer records remain in
`/volume2/NASDA2/vaultic-test-m6/pilot-wbacd8_5`. Its separate
`PEBBLE-TIMEOUT-SHA256SUMS` identity is
`66f028d11b5d557d2924d75227ef22ab75f3de0e3eca80f2c4f368645a4441c8`.
The original launch manifest is unchanged. Remaining modes must keep complete
snapshot scope and the original limits; no automatic timeout extension,
snapshot omission, uncertain-publication resume or accepted-prefix speedup is
authorized. All other full workload outcomes remain pending.

### RAM Setup Failure and Capture Repair

The first RAM cell failed while loading source indexes, before any indexes or
snapshots were imported. Its configured 160 GiB managed-state admission limit
was exhausted (`used=171788394608`, `requested=12582976`); its reported managed
peak was 171,790,646,576 bytes. Managed charges are not measured RSS: peak
sampled combined RSS was only 6,732,713,984 bytes. This is a rejection under the
frozen pilot configuration, not evidence that RAM exceeded the operator's
200 GB RSS ceiling or could never fit under another configuration.

An RSS sampling exception during termination prevented the supervisor from
retaining its normal action result. The child's exit code is therefore unknown,
although its structured log contains the explicit admission error. The retained
logs and limitations are separately sealed by `RAM-FAILED-CAPTURE-SHA256SUMS`,
identity `1d873288c48fb043c0bd2e2dc74bdb2166fed4cd01c8e766b7f2fb0ec63c60c3`,
in the original pilot root. No checks or backup ran against this partial state.

The workspace runner now ignores missing RSS only for positively identified
exiting/dead processes, and preserves a failed action result if live-process
RSS monitoring is unavailable. Seven runner regressions pass, including both
exit-race handling and fail-closed live-RSS handling. The original frozen runner
and captures are unchanged. Remaining cells require a separately pinned runner
publication with identical input inventory, binaries and workload limits;
neither this repair nor the admission failure authorizes a budget increase,
partial-state resume or production change.

### KV Shared Setup Rejection

The fresh KV cell in `/volume2/NASDA2/vaultic-test-m6/pilot-tit30h5a` exited
with code 1 after 505.052 seconds. Its peak combined RSS was 6,680,723,456 bytes,
with no timeout or RSS stop. Like RAM, it failed in the shared `repo.LoadIndex`
preload before `legacyimport.Import` began: managed admission reported
`limit=171798691840 used=171797808368 requested=3145792`. The managed peak was
171,798,392,496 bytes; reported managed disk usage and peak were both zero.
Thus this is a shared setup rejection under the frozen configuration, not a
demonstrated KV scratch-capacity or completed-import throughput limit.

The repaired supervisor retained the action and incomplete-cell results.
Observed CLI/daemon CPU averaged about 6.12 cores over 496.159 seconds; no
sampled nonzero memory-pressure avg10 was recorded. Source totals remained
uninitialized at zero, which does not mean the input repository was empty.
Checks, initialization and backup did not run. The separate verified failure
seal `KV-FAILURE-SHA256SUMS` has identity
`30b2c28709f40f4c0a97b9253cb8b9c12c424b453766ae2f0f4b4ad65ec76543`.
The continuation launch seal is
`3ad7476efec2eeed11089f182525e024166e78bd4000cda9408e584b544a2302`;
only root identity and runner pin differ from the original plan. The final
auto cell remains pending with the same scope, binaries and limits.

### Auto Outcome and Configured Screening Disposition

Auto exited with code 1 after 504.228 seconds, with peak combined RSS of
6,652,547,072 bytes and no RSS or timeout stop. It failed in the same source
preload at `limit=171798691840 used=171798691760 requested=128`.
`fallback_pending` was reported, but effective mode remained RAM, migration
reservation was zero, and reported disk usage/peak were zero. Its separate
verified `AUTO-FAILURE-SHA256SUMS` identity is
`58459a19e1ab5f2cb4c90cbf178ea12942550456789a2d1d548d9f5e5f120261`.
Observed CLI/daemon CPU averaged about 6.28 cores over 496.344 seconds, with no
sampled nonzero memory-pressure avg10. Checks, initialization and backup did
not run.

All four configured import attempts have now failed. No complete real-input
import/check/backup acceptance or successful mode throughput comparison exists.
RAM's child exit code was not retained; its failure capture must not be treated
as a normal measured result. The original and continuation roots preserve each
failure independently, including the supervisor-version change. No candidate
was accepted and no production/default change is approved.

The shared setup path reserves generic JSON decode capacity in
`workingstate.ReserveJSON`, attaches that reservation to the compact result in
`decodeWorkingIndex`, marks it retained in `MasterIndex.Insert`, and transfers
the releases during index merging. `DecodeIndex` returns compact lookup tables,
not the temporary JSON representation. Retaining that conservative decode
reservation accounts for the admission/RSS distinction; neither KV selection
nor auto's pending fallback changed this legacy-source preload in these runs.
The nearby `TestM4IndexDecodeOwnership` regression passes and verifies retention
and eventual release, but does not validate that retained charges match compact
storage capacity. The recommended next experiment is a separately tested,
pinned retained-index accounting repair, preserving decode-peak admission and
charging actual retained capacity. Raising limits, disabling admission or
skipping snapshots is not an accepted substitute. Pebble's snapshot traversal
timeout remains a separate unresolved throughput finding.

### Authorized Compact-Index Accounting Repair

On 2026-10-07 the operator authorized the targeted accounting fix and isolated
rerun. Successful source-index decoding now admits a replacement charge based
on allocated compact index capacities: pack/index-ID arrays, hash buckets,
hashed-array-tree block headers and entry blocks. Fourfold merge/allocator
growth headroom and 8192 bytes of per-index ownership overhead remain charged.
This is conservative capacity accounting, not a claim of measured RSS.

The generic JSON decode reservation is unchanged and remains owned until the
replacement reservation is admitted. Failed replacement admission and
cancellation release owned charges; insertion, merge and close preserve
existing retained-charge ownership. The new regression reproduced a compact
512-entry index retaining its entire 1,120,512-byte transient charge before the
fix. Four focused repair tests cover retained size, transient/retained admission
failure, cancellation after retained admission and cumulative loading/merging
of 64 indexes within 16 MiB. Existing decode-ownership and incremental-load
regressions also pass under race instrumentation.

The separate publication at
`/volume2/NASDA2/vaultic-test-m6/pilot-kd0p0kjv` pins CLI identity
`46dfc59ef8faf21370fc1a861c9df08400f57bc978737dc6ca52313a91be5ae5`.
Only root and CLI identity differ from the prior continuation plan; the native
daemon, source inventory, profile/exclusions, all workload limits and absolute
disk reserve are identical. Eight retained verification gates pass, including
both CDOT snapshot-exclusion race regressions and the evidence analyzer and
decision-generator selftests. An extra Linux/386 check is blocked by unchanged
repository `MaxPackSize` constants overflowing `uint`; that failed log is
retained and no 32-bit pass is claimed.

Fresh RAM/KV/auto candidates must retain the complete input/snapshot scope and
telemetry-first observation. No original failed state is resumed. The original
Pebble timeout remains independently sealed; this accounting repair does not
establish that snapshot traversal will meet the 12-hour target. Real-input
import/check/backup acceptance and production approval remain unachieved.

### Repaired RAM Runtime Finding

The first repaired RAM cell reached the 12-hour import deadline and stopped
after 30 seconds of termination grace: wall time 43,230.201 seconds, exit -9,
peak combined RSS 40,111,030,272 bytes. The compact-accounting repair cleared
source-index setup, but only 5,632 of 10,170 indexes and 231,602 packs completed
publication. It imported 211,758,114 blob records and reached no snapshots.
This differs from the original Pebble timeout, which completed all index
publication and stopped during snapshot traversal. Neither run is accepted.

Managed-state peak was 96,423,672,094 bytes, below the unchanged 160 GiB budget;
the RSS ceiling did not stop the run. CLI/daemon CPU averaged about 3.23 cores
over observed samples. Only 12 of 8,626 resource samples had nonzero memory
pressure avg10, peaking at 0.36 percent. Observed cgroup file cache peaked at
61,063,135,232 bytes, separately from RSS. These observations do not establish
memory pressure as the primary cause of the timeout.

Aggregate lane timers report 63,201.482 seconds in `blob_read`, 65,237.677
seconds in planning and 71,854.505 seconds in transactions. They overlap and
must not be summed or treated as percentages of wall time. The `blob_read`
timer covers existing blob-record lookup through `loadLegacyBlobRecords`,
including batched metadata reads and record decoding, not source pack-payload
reads. This identifies metadata lookup as a bottleneck lead; the underlying
cache, RPC or storage mechanism remains unproven. Final telemetry reports
25,591,681,086 encoded committed metadata bytes, not logical backup throughput.

The separate verified `RAM-TIMEOUT-SHA256SUMS` identity is
`bd19990271cadca87834a6ddd7220bcff1bb263edfe5d8ec4b4a5d722028ac99`
under `/volume2/NASDA2/vaultic-test-m6/pilot-kd0p0kjv`. The original variant
launch seal is unchanged. Checks, initialization and backup did not run;
fresh KV and auto candidates remain pending under the same complete scope and
limits, without timeout extension or partial-state resume.

### Repaired KV Runtime Finding

The repaired KV cell also reached the 12-hour deadline: wall time 43,230.271
seconds including termination grace, exit -9, peak combined RSS
39,931,330,560 bytes. It imported 5,570 of 10,170 indexes, 228,680 packs and
209,560,270 blob records, reaching no snapshots. Managed peak was
96,373,652,506 bytes, below the 160 GiB limit; reported managed scratch
usage/peak were zero. Neither admission nor the RSS ceiling stopped this run.

Observed CLI/daemon CPU averaged about 3.21 cores. Only 5 of 8,623 resource
samples had nonzero memory-pressure avg10, peaking at 0.14 percent. Existing
blob-record lookup again dominated aggregate planning: `blob_read` reported
63,663.495 seconds and planning 65,703.613 seconds across overlapping lanes.
Final encoded committed metadata was 25,326,075,534 bytes. These partial
counters and overlapping timers are not completed full-import throughput or
a causal RAM/KV speedup comparison. KV selection did not demonstrate scratch
work in this incomplete run; the shared metadata-lookup mechanism remains
the bottleneck lead, not a proven storage or cache root cause.

The verified `KV-TIMEOUT-SHA256SUMS` identity is
`bb61a82d9ea001bc920027205dca6bb19af7cec419c2edb46db64dbe637bf495`
in the same accounting-variant root. Checks, initialization and backup did
not run. The final fresh auto candidate subsequently ran with the unchanged
binary, complete scope, observer and limits, as recorded below; neither
timeout is accepted.

### Repaired Auto Runtime Finding

The final fresh auto cell also timed out: wall time 43,230.299 seconds
including termination grace, exit -9, peak combined RSS 40,367,001,600 bytes.
It imported 5,596 of 10,170 indexes, 229,636 packs and 210,466,332 blob
records, reaching no snapshots. Auto remained effective RAM, with no
migration or reported managed scratch use. Managed peak was 96,474,850,928
bytes, below the unchanged 160 GiB limit.

Observed CLI/daemon CPU averaged about 3.18 cores. Memory-pressure avg10
was nonzero in 22 of 8,620 resource samples, peaking at 7.08 percent;
this observation does not establish memory pressure as the timeout cause.
Aggregate `blob_read` was 64,009.843 seconds against planning 65,960.474
seconds across overlapping lanes. Existing blob-record metadata lookup is
again the bottleneck lead; native cache, RPC and storage attribution remains
unproven. Final encoded committed metadata was 25,435,573,126 bytes, not
logical backup payload or a completed full-import throughput measurement.

The verified `AUTO-TIMEOUT-SHA256SUMS` identity is
`c0a45deac34215ce24bce907dba473d6084db4aaa412a158c0a3afe73954ab26`
in the accounting-variant root. Checks, initialization and backup did not run.

### Repaired Screening Disposition

All three repaired full-scope import attempts are deadline failures. The
accounting repair removed the shared preload admission blocker, but did not
meet operator acceptance. None reached snapshot traversal or completed import.

| Mode | Indexes Imported / Total | Snapshots Imported / Total | Peak Combined RSS (GB) | Partial Encoded Metadata / Failed Wall (MB/s) |
| --- | --- | --- | --- | --- |
| RAM | 5,632 / 10,170 | 0 / 143 | 40.11 | 0.592 |
| KV | 5,570 / 10,170 | 0 / 143 | 39.93 | 0.586 |
| Auto | 5,596 / 10,170 | 0 / 143 | 40.37 | 0.588 |

GB and MB are decimal. The final column amortizes only committed encoded
metadata over each failed action's wall time, including preload and termination
grace. It is not logical backup throughput, a successful full-import rate or
a causal mode ranking. All runs stayed under the RSS and managed RAM limits;
KV selection showed no reported scratch work, and auto did not migrate.
Single fixed-order runs, shared NFS/cache effects and the different baseline
CLI variant preclude production recommendations or intrinsic speedup claims.
The original Pebble baseline instead imported all indexes and then timed out
in incomplete snapshot traversal; that separate bottleneck remains unresolved.

`repaired-screening-disposition.json` links the three outcome seals, immutable
variant launch, input pins and original configured-screening disposition.
The verified `REPAIRED-SCREENING-SHA256SUMS` identity is
`d8733a37ea035011caaf2056be15957ad5708a0287b1c6501395751f87079ff2`.
There are zero completed index checks, repository checks or backups. No
failed database was resumed, timeout extended, exclusion relaxed or production
default changed. The complete operator workflow remains blocked, not accepted.

The next proposed scope is a bounded diagnostic of existing blob-record lookup
across the client/RPC/native cache/store path, followed by a separately pinned
optimization only if justified and authorized. Neither another runtime variant
nor decoupling backup from failed import is authorized by this screening result.

### Authorized Fresh-Filter Diagnostic And Rerun

On 2026-10-08 the operator authorized bounded metadata-lookup diagnostics,
an evidence-backed separately pinned fix and isolated reruns under the existing
limits and exclusions. This supersedes the pending diagnostic authorization
above; it does not authorize limit changes or resuming failed databases.

The controlling path is `SchemaStore.EnableFreshLegacyImportContext`: every
working policy previously selected an empty database-fallback filter, even
with enough budget. A new 64 MiB regression first failed for RAM, KV and auto.
Bounded final telemetry confirms fallback in all three repaired runs and
212,175,552 / 209,981,710 / 210,892,442 catalog read keys respectively. The
original unconfigured run's active filter instead recorded 376,663,016
definitely-absent lookups and 4,247,499 catalog read keys. Those unequal-scope,
fixed-order observations confirm read amplification, not a causal speedup ratio
or native cache/RPC/storage service-time attribution.

The candidate admits and charges each filter layer before allocation, including
conservative per-layer ownership overhead, while preserving the existing size
bound. Initial/growth denial, cancelled admission or policies without accounting
fall back to authoritative reads. CLI import-session teardown releases all
charges; repeated release is safe. Tests cover admitted hints in all modes,
no false negatives, growth charges, denied growth, cancellation and cleanup.
Existing native idempotence and fallback-equivalence fixtures also pass.

The fresh publication is
`/volume2/NASDA2/vaultic-test-m6/pilot-5goxo86c`, with CLI SHA-256
`cee7072b8e97988cd5156acc71dc95ffbbc370eba2b50190dc7b3a2b56fa93e0`.
Its plan differs from the prior accounting variant only in root and CLI hash;
daemon, source inventory/profile/config, runner, observer, absolute free-space
reserve and all runtime limits remain pinned identically. Seven retained
verification gates passed. Fresh RAM, KV and auto cells use full import scope,
then only proceed to checks and backup if prior actions complete successfully.
No full-workload throughput or operator acceptance is claimed before outcomes;
the separate original snapshot-traversal bottleneck remains unresolved.

### Charged-Filter RAM Runtime Finding

The fresh RAM rerun reached the unchanged deadline: wall time 43,230.245
seconds including termination grace, exit -9, peak combined RSS
47,180,910,592 bytes. All 10,170 indexes, 419,853 packs and 380,166,821 blob
records were imported. Snapshot traversal remained incomplete: 19 of 143
snapshots imported, 21 seen, 32,611,085 trees and 175,360,140 nodes visited.
All 21 recorded findings were `load-tree` / `context canceled` at termination;
retain them as deadline-related findings, not a clean source audit.

The charged filter reached six layers / 1,368,893,127 bytes, with no observed
fallback while filter bytes were live. It avoided 376,663,017 lookups; catalog
read keys were 4,247,498. Final telemetry shows zero filter storage and
fallback after the explicit teardown release, not the old admission bypass.
Managed peak was 96,402,032,293 bytes, below 160 GiB; reported managed scratch
usage/peak remained zero. Neither admission nor the 200 GB RSS ceiling stopped
the run. The filter repair cleared read amplification but did not establish
complete full-import acceptance or a causal variant speedup.

Observed CLI/daemon CPU averaged about 2.26 cores over 8,626 resource samples.
Memory-pressure avg10 was nonzero in 86 samples, peaking at 0.74 percent.
Observed cgroup file cache peaked at 202,172,809,216 bytes, separately from RSS.
Index-publication counters were unchanged for the final 6.30 hours; this does
not imply an idle process or measure snapshot work. Aggregate `blob_read` was
15,048.928 seconds and planning 16,536.231 seconds across overlapping lanes.
Final encoded committed metadata was 46,002,455,652 bytes, not logical backup
payload throughput. The remaining snapshot-traversal mechanism is unproven.

The verified `RAM-TIMEOUT-SHA256SUMS` identity in the charged-filter root is
`02cdd13335deb06e3286f3c27de1459be995edc684c9a3008cb8b60bb3940600`.
The immutable launch seal is
`0318f9267bf6378aff9a05109e177958794847934cfa52e96ec59ef8a4851033`.
Index checks, repository checks, initialization and backup did not run.
Fresh KV and auto reruns remain authorized under the same frozen complete
scope, binaries, observer and limits; no partial RAM database is resumed.

### Charged-Filter KV Runtime Finding

The fresh KV cell also reached the unchanged deadline: wall time 43,230.152
seconds including termination grace, exit -9, peak combined RSS
45,512,536,064 bytes. All 10,170 indexes, 419,853 packs and 380,166,821 blob
records were imported. Snapshot traversal remained incomplete: 19 of 143
snapshots imported, 21 seen, 31,864,185 trees and 172,713,460 nodes visited.
All 70 recorded findings were `load-tree` / `context canceled` at termination;
this remains a failed full import, not a clean source audit.

Live filter peak was six layers / 1,368,893,127 bytes, with no observed fallback
while filter bytes were live. It avoided 376,663,019 lookups; catalog read keys
were 4,247,496. Final released-filter fallback is expected teardown, not renewed
policy bypass. Managed peak was 96,445,030,131 bytes, below 160 GiB; reported
managed scratch usage/peak were zero. KV selection alone did not establish
scratch work or intrinsic mode superiority, and no resource ceiling stopped
this run.

Observed CLI/daemon CPU averaged about 2.28 cores over 8,623 samples.
Memory-pressure avg10 was nonzero in 59 samples, peaking at 1.92 percent;
cgroup file cache peaked at 209,261,506,560 bytes, separately from RSS.
Publication counters were unchanged for the final 6.25 hours, which does not
measure snapshot work or imply process inactivity. Aggregate `blob_read` was
15,098.739 seconds and planning 16,582.357 seconds across overlapping lanes.
Final encoded committed metadata was 46,002,455,652 bytes. Neither these
partial workflow counters nor the single fixed-order RAM/KV results establish
completed full-import throughput or a causal mode ranking. The shared
remaining snapshot-traversal mechanism is still unproven.

The verified `KV-TIMEOUT-SHA256SUMS` identity in the charged-filter root is
`b045a501907ad1eb5ec6039da2ace4bb127d78ca95dd2e3f8f34e5da3b1e06d3`.
Checks, initialization and backup did not run. The final fresh auto cell
subsequently ran with the identical frozen scope, binaries, observer and
limits, as recorded below; neither failed database was resumed and no deadline
was extended.

### Charged-Filter Auto Runtime Finding

The final fresh auto cell also reached the unchanged deadline: wall time
43,230.357 seconds including termination grace, exit -9, peak combined RSS
47,111,458,816 bytes. All 10,170 indexes, 419,853 packs and 380,166,821 blob
records were imported. Only 19 of 143 snapshots were imported, with 21 seen,
32,540,576 trees and 175,122,198 nodes visited. All three recorded findings
were `load-tree` / `context canceled` at termination, not a clean source audit.

Auto stayed effective RAM without migration or reported managed scratch work.
Managed peak was 96,282,984,897 bytes, below 160 GiB. The live filter peaked
at six layers / 1,368,893,127 bytes, with no observed fallback while allocated;
it avoided 376,663,038 lookups and catalog read keys were 4,247,477.
Terminal released-filter fallback is expected cleanup, not policy bypass.

Observed CLI/daemon CPU averaged about 2.26 cores over 8,621 samples.
Memory-pressure avg10 was nonzero in 54 samples, peaking at 2.37 percent;
cgroup file cache peaked at 208,404,713,472 bytes, separately from RSS.
Publication counters were unchanged for the final 6.29 hours, which does not
measure snapshot progress. Aggregate `blob_read` was 14,997.973 seconds and
planning 16,478.497 seconds across overlapping lanes. Final encoded committed
metadata was 46,002,455,652 bytes, not logical backup payload throughput.

The verified `AUTO-TIMEOUT-SHA256SUMS` identity in the charged-filter root is
`bb76e934b6936a8ad5cc4a257f6cd86f20267f89a91c0c182ecfa6a4b01002ea`.
Checks, initialization and backup did not run. Full import remains incomplete.

### Charged-Filter Screening Disposition

All three charged-filter runs reported complete index publication and cleared
the policy-induced catalog-read amplification, but failed the unchanged
12-hour full-import deadline during snapshot traversal. No candidate met
operator acceptance, and no completed checks or backup results exist.

| Mode | Indexes Imported / Total | Snapshots Imported / Total | Peak Combined RSS (GB) | Index-Publication Counter Span (h) |
| --- | --- | --- | --- | --- |
| RAM | 10,170 / 10,170 | 19 / 143 | 47.18 | 4.99 |
| KV | 10,170 / 10,170 | 19 / 143 | 45.51 | 5.00 |
| Auto | 10,170 / 10,170 | 19 / 143 | 47.11 | 4.97 |

GB is decimal. Counter spans run from the first positive mutation sample to
the last index-publication counter change, with matched process identity;
they exclude earlier preload and are not full-import wall times. Mutation
operation rates over those spans were about 21.5-21.6 thousand operations
per second, not logical backup bytes or distinct blob-record throughput.
Single fixed-order shared NFS/cache runs do not establish causal mode or
variant speedups. Complete counters alone do not prove durable reopen
integrity or clean source audits; the required checks never ran.

`charged-filter-screening-disposition.json` links the diagnostic, frozen
variant launch, input pins, three outcome seals and prior accounting-screening
disposition. The verified `CHARGED-FILTER-SCREENING-SHA256SUMS` identity is
`f797cbc70eb992ff46ff23e82adc7a7488582e665c98427b603bef1447b65c37`.
The accounting and filter-admission defects were repaired without raising
limits or changing defaults, exclusions, daemon, runner or observer. No
partial database was resumed, timeout extended or independent backup started.

The complete operator workflow remains blocked by snapshot traversal. Its
load/traversal/audit cost and underlying runtime/storage mechanism require
a separately bounded diagnostic; no new snapshot-runtime variant or decoupled
backup is implied by these failed outcomes.

### Authorized Snapshot Diagnostics And Debt Candidate (2026-10-11)

The operator subsequently authorized bounded snapshot-traversal diagnostics
and a separately pinned repair/rerun under the same resource, input and
exclusion contract. This does not authorize timeout extensions, failed-state
resume, tree/source omission or independent backup after failed import.

Diagnostics use a read-only source view, an empty fresh native catalog and
at most 100,000 visited nodes per sorted snapshot, across all 143 snapshots.
Full source preload has a separate 90-minute bound; traversal has 30 minutes
inside a two-hour context. Tracking retains at most 100,000 unique tree IDs
under an explicit 12.8 MB charge; repeat counts are therefore lower bounds.
The native binary, observer, combined 200 GB RSS ceiling, input pins and
absolute shared NFS free-space reserve remain unchanged. These prefixes do
not reproduce the complete post-index catalog or constitute snapshot audits.

The initial diagnostic exhausted a combined 30-minute bound during preload,
without traversal. The staged first-prefix run completed preload in 3,026.44
seconds and visited 100,000 nodes with 200,000 debts and no findings. The
all-prefix baseline and debt candidate visited exactly the same 12,113,704
nodes, 1,907,677 trees and 24,227,408 debts with no findings; neither imported
identity-known nodes. Source loads, decoded bytes and bounded repeat counts
also matched exactly.

| Matched Prefix Observation | Baseline | Debt Candidate |
| --- | ---: | ---: |
| Native debt batch submissions | 94,733 | 6,642 |
| Aggregate debt submission seconds | 504.21 | 205.64 |
| Traversal wall seconds | 1,403.67 | 793.33 |
| Full source preload seconds | 2,617.24 | 3,061.01 |
| Peak combined process RSS, decimal GB | 47.086 | 46.797 |

These are single fixed-order shared NFS/cache observations, not a causal wall
speedup or a full-workflow throughput claim. Tree-load and lazy-decode service
times changed too; aggregate timers are not exclusive wall partitions.
The exact-scope reduction in submissions justifies a separately pinned full
screening, not acceptance.

The candidate reserves 4 MiB before allocating a per-snapshot buffer, targets
4,096 records and at most 1 MiB encoded key/value bytes, and retains the
256-record fallback if accounting is absent, denied or cancelled. Duplicate
keys flush first; oversized records receive separate admission. Successful
flushes clear retained references, failed flushes retain pending work, and
snapshot cleanup releases the charge. Enlarged debt batches flush separately
before revision publication; final durable checkpoint ordering is unchanged.
Load, lazy-decode and standalone debt-write timings distinguish their costs.

Focused tests cover RAM/KV/auto admission and byte-for-byte equivalence,
cancellation, byte limits, duplicate keys, failed writes, native reopen and
deferred publication ordering. The native fixture seeds its root catalog,
uses sorted names and allows native RPC headroom; the optimized-daemon native
fixture, focused race gate and owning-package gates all passed before pinning.

All diagnostic roots are beneath `/volume2/NASDA2/vaultic-test-m6/`:

| Diagnostic Root | Verified Outcome Seal SHA-256 |
| --- | --- |
| `snapshot-diag-20261011-Dtj4Wa` | `876f30e725f70496c7c0a986dee1a01e8eb2c68a6213b05f2ae9013b933c3833` |
| `snapshot-diag-staged-20261011-HfFF6P` | `40a52cac23b4c63e8d8d173adc5a0a1207e1eebbfba0d00cb0e55444e6332d77` |
| `snapshot-diag-matrix-20261011-JmeI2L` | `6556bfc7a58325134eca4ffbc075db4d48fb3128c90b29db8f6dd14a234e6b25` |
| `snapshot-debt-candidate-20261011-g7Zrll` | `0f41bb80f5896233811849b8b02d4a0474889781ef6d92b2e2ddc02fe795ea78` |

The separate full-workflow candidate is `pilot-ru5_eh_8`, with CLI SHA-256
`8554e47f2204c469b247a8117eb37c2ec9592f46b58694d0c946b5ae69d0e8f9`
and launch seal
`525769d6bda466a0f15070bd7cd89c8bc06731f273090f0661cc42d18fd647f1`.
Its plan differs from the preceding charged-filter pilot only by private root
and CLI hash. Source inventory, profile, config, optimized daemon, runner,
observer, 52-source selection and exact exclusions match. The absolute NFS
reserve remains 57,481,929,687,040 bytes, managed policy 160 GiB and scratch
8 GiB; import/check bounds remain 12 hours each and backup seven days.
Fresh RAM, KV and auto cells must each complete import, both checks, init and
the full backup in order. No complete workflow or M6 acceptance is claimed.

### Debt Candidate RAM Full-Scope Outcome

The fresh RAM cell in `pilot-ru5_eh_8` reached the unchanged 12-hour import
deadline: action exit `-9`, wall 43,230.176969 seconds including termination
grace, peak combined CLI/native RSS 53,559,631,872 bytes. All 10,170 indexes,
419,853 packs and 380,166,821 blob records completed, but only 28 of 143
snapshots were imported, with 30 seen. Final counters recorded 48,014,077
trees, 317,792,343 visited nodes, zero imported nodes and 635,584,742 crawl
debts. The 56 findings all report `load-tree` / `context canceled`; they are
retained deadline findings, not a clean source audit or evidence that every
counted debt was durably published. Neither check, init nor backup ran.

Managed peak was 96,576,378,561 bytes under the 160 GiB policy, with closing
usage 90,316,228,480 bytes and reported managed scratch usage/peak zero.
The independent observer retained 8,627 samples over 43,235.994419 seconds,
averaging 2.422717 owned CLI/native CPU cores. Peak cgroup file cache was
220,191,236,096 bytes, separate from the RSS ceiling. Memory PSI avg10 was
nonzero in 51 samples, peaking at 0.84%; pressure was not absent, and these
observations do not identify its causal contribution.

The identity-matched positive index-publication counter span was 18,330
seconds, with 386,695,476 additional mutations, or 21,096.316 operations/s.
It excludes preload before the first positive counter and does not measure
logical backup payload. Its subsequent 21,916.744-second unchanged interval
does not imply process inactivity: snapshot work is outside that counter.
Aggregate index-stage microseconds included existing-blob metadata reads
15,239,498,239, planning 16,766,262,369, transactions 26,676,530,962 and
cleanup 3,047,007,954; these timers overlap, not partition wall time.

Live absence-filter retention peaked at 1,368,893,127 bytes across six layers.
Final released-filter fields may be omitted and the final fallback flag may
reflect teardown, not a live policy bypass. The full CLI exporter does not
expose the new snapshot-specific timers recorded by the bounded diagnostic;
its index-stage counters cannot establish full-scale debt submission costs.
More partial traversal than the preceding screening does not demonstrate
causal speedup, durable reopen integrity, or complete import acceptance.

The verified `RAM-TIMEOUT-SHA256SUMS` identity is
`325889fe3e55885ab15af0d7541535b5fdf0343e173b952f26b886b2eef55781`.
All launch pins were reverified unchanged; RAM workload and observer were
inactive/dead and production remained active/running. The next authorized
KV and auto cells remain fresh, with the same scope, exclusions and limits;
the failed RAM database is not resumed and no independent backup is started.

### Operator Upgrade Pause (2026-10-12)

The operator requested stopping the current KV run for a VS Code/Copilot
upgrade. Its workload and observer were stopped and verified inactive/dead;
no pilot CLI/native processes remained, production stayed active/running,
and auto was not started. KV ended on operator SIGTERM after a reported
9h 25m 19.487s, not on the 12-hour deadline or a completed import. Its logs,
RSS samples and telemetry remain retained, but supervisor result/summary
files were not produced. The launcher's second stop of the already-collected
observer reported an unloaded unit; that cleanup error does not indicate a
remaining workload. Cgroup memory peak includes file cache and is not a
combined-RSS acceptance result. The workflow is paused pending explicit
continuation; this interrupted database must not be silently resumed.
M6 full-workflow acceptance remains unmet.

## Fresh Upgrade Continuation (2026-10-13)

The operator requested continuation after the upgrade pause. A separate private
root, `/volume2/NASDA2/vaultic-test-m6/pilot-c13-YGnAIt`, preserves the interrupted
KV database and all prior RAM/KV artifacts in `pilot-ru5_eh_8`. No partial database
is copied or resumed. The continuation sequence is fresh KV, followed by fresh
auto after evaluation of KV's outcome; failed RAM is not automatically rerun.

The new plan differs from the frozen prior plan only in `root`. CLI
`8554e47f2204c469b247a8117eb37c2ec9592f46b58694d0c946b5ae69d0e8f9`, native daemon,
runner, observer v2, profiles, source inventory, source view and exclusions remain
unchanged. Existing uncommitted edits to the schema store and transaction tests
are preserved and are not built into this frozen candidate.

Before launch, all 10,170 indexes and 143 snapshots matched inventory counts and
file sizes; all 52 source directories were available. The absolute free-space
reserve was met. Original launch pins, copied artifact identities and
`load_plan` checks passed, as did seven runner and four observer self-tests. The
paused workload/observer were inactive, no old pilot processes remained, and
production was active/running without intervention.

Combined CLI/native RSS remains limited to 200 GB, sampled every 250 ms; managed
policy remains 160 GiB with the unchanged 8 GiB scratch default. Import and each
check retain their 12-hour bounds, backup retains its seven-day bound, and the
absolute NFS reserve remains 57,481,929,687,040 bytes. The unchanged launcher
starts the five-second observer first, makes originals read-only, permits writes
only in the new private root, disables swap/core dumps and retains default GC.
Every mode stops downstream actions on a failed import or check.

`continuation-contract.json` records authorization, fresh-state requirements and
the prior launch-seal identity. The new `LAUNCH-SHA256SUMS` verifies 18 inputs and
has SHA-256
`63c27293a812cd974e149f12d43bfd24fb4401be65d52498d624133730b49532`.
This is continuation preparation, not completed workflow evidence or approval
for production defaults or Pebble removal. Full M6 acceptance remains unmet.

## Fresh Continuation KV Outcome

The fresh KV import in `pilot-c13-YGnAIt` reached the unchanged 12-hour deadline:
exit `-9`, wall 43,230.203138 seconds including termination grace, peak combined
CLI/native RSS 55,006,658,560 bytes. This was a deadline failure, not an RSS-limit
failure. All 10,170 indexes, 419,853 packs and 380,166,821 blob records completed,
but only 28 of 143 snapshots were imported and 30 were seen. Snapshot work
visited 48,720,120 trees and 325,191,087 nodes, imported zero identity-known
nodes, and created 650,382,798 crawl debts.

All 624 retained findings were `load-tree` / `context canceled`. They are
deadline findings, not a clean source audit or proof that every debt is durable.
The saved summary contains only the failed import: no index check, repository
check, initialization or backup ran. The partial KV database remains preserved
and must not be resumed.

Effective policy remained KV. Managed peak charge was 96,486,821,935 bytes,
closing usage 90,316,228,480 bytes, and reported managed scratch usage/peak zero.
These accounting values are not physical residency. The live absence-filter
peak remained 1,368,893,127 bytes across six layers; the final released-filter
fallback metric does not establish a live policy bypass.

Observer evidence contains 8,627 samples over 43,235.127766 seconds, with mean
sampled CLI/native CPU 2.397847 cores; two startup process CPU rates were
unavailable. Peak cgroup file cache was 234,949,394,432 bytes, separate from RSS.
Memory PSI avg10 was nonzero in 91 samples, peaking at 8.41 percent; this does not
establish the cause of the deadline miss. The frozen observer's static
after-import-attachment caveat is not adopted: the unchanged launcher started
the observer first, and its earliest saved sample precedes first CLI telemetry.

Identity-matched positive index-publication counter samples span 18,075.001
seconds and 386,823,530 mutations, approximately 21,401.024 operations/second.
This excludes preload and snapshot traversal. The subsequent unchanged counter
tail of 22,412.212 seconds does not mean process inactivity or measure snapshot
debt throughput. Full CLI telemetry still lacks the bounded diagnostic's
snapshot-specific timers. Aggregate stage timers overlap; this single,
fixed-order shared-NFS run is not completed-work throughput or causal mode
ranking.

`kv-timeout-findings.json` and nine retained outcome inputs are verified by
`KV-TIMEOUT-SHA256SUMS`, SHA-256
`62c873c9ef89bd4f300aa1d555595e2e18ef00d09af3c6cc7332e2c9bb6c265e`.
The 18-input launch seal remains unchanged. Workload and observer were
inactive/dead after completion; production remained active/running without
intervention. Fresh auto is the next authorized cell, with the same inputs,
limits and exclusions. No full-workflow acceptance, production-default change
or Pebble removal is justified by this outcome.

## Fresh Continuation Auto Outcome

Fresh auto in `pilot-c13-YGnAIt` also reached the unchanged 12-hour import
deadline: exit `-9`, wall 43,230.370264 seconds including termination grace,
peak combined CLI/native RSS 54,039,212,032 bytes. All 10,170 indexes, 419,853
packs and 380,166,821 blob records completed, but only 28 of 143 snapshots were
imported and 30 were seen. Snapshot work visited 48,856,049 trees and 328,119,737
nodes, imported zero identity-known nodes, and created 656,239,472 crawl debts.
The saved partial result reports zero errors/findings; this is not a clean
source audit or successful import because the deadline still failed and full
snapshot scope did not complete. No checks, initialization or backup ran.

Auto selected RAM; its two saved policy events show RAM, no migration reserve
and no demonstrated migration. Managed peak charge was 96,342,711,325 bytes,
closing usage 90,316,228,480 bytes, and reported managed scratch usage/peak zero.
This does not certify migration coexistence or physical residency. The live
filter peak remained 1,368,893,127 bytes across six layers; final released-filter
fallback does not establish a live policy bypass.

Observer evidence contains 8,623 samples over 43,232.621151 seconds, with mean
sampled CLI/native CPU 2.318345 cores and two unavailable startup process CPU
rates. Peak cgroup file cache was 239,199,408,128 bytes, separate from combined
RSS. Memory PSI avg10 was nonzero in 66 samples, peaking at 2.26 percent; its
causal role remains unproven. Observer-first launch and the earliest saved
sample preceding first CLI telemetry again disconfirm the frozen observer's
static after-import-attachment caveat.

Identity-matched positive index-publication samples span 17,895.001 seconds and
386,665,542 mutations, approximately 21,607.461 operations/second. They exclude
preload and snapshot traversal; the subsequent unchanged counter tail of
22,802.094 seconds is not inactivity or snapshot debt throughput. Full CLI
still lacks the bounded diagnostic's snapshot-specific timers, and aggregate
stage timers overlap. No completed-work throughput or causal mode ranking is
claimed.

`auto-timeout-findings.json` and nine retained outcome inputs are verified by
`AUTO-TIMEOUT-SHA256SUMS`, SHA-256
`6ad7740f768d645dfc892c16647af3566ee32707ac0c8f44df715162b6e58530`.
The launch seal remains unchanged. Auto workload/observer were inactive/dead,
no owned pilot processes remained, and production stayed active/running without
intervention. Preserve the failed auto database; do not resume it.

## Final Full Debt-Candidate Screening Disposition

All three frozen candidate imports missed the 12-hour deadline after completing
all indexes but only 28 of 143 snapshots. The original RAM result predates the
upgrade pause; fresh KV and auto use a root-only continuation of the same CLI
and unchanged contract, not a new code variant or repeated evidence.

| Mode | Effective Mode | Imported Snapshots | Peak Combined RSS (Decimal GB) | Findings | Outcome |
| --- | --- | --- | --- | --- | --- |
| RAM | RAM | 28/143 | 53.56 | 56 cancellation findings | Import deadline failure |
| KV | KV | 28/143 | 55.01 | 624 cancellation findings | Import deadline failure |
| Auto | RAM | 28/143 | 54.04 | 0; not a clean audit | Import deadline failure |

There are zero accepted full workflows, completed index checks, completed
repository checks or completed backups. The operator-interrupted original KV
attempt remains separate historical evidence, not a completed screening case;
its database was not resumed. No scope, exclusion, memory/scratch limit,
deadline or default GC setting changed.

`full-debt-screening-disposition.json` links the independently verified original
RAM, fresh KV and fresh auto outcome seals, both launch identities and the
continuation contract. The seven-input `FULL-DEBT-SCREENING-SHA256SUMS` has
SHA-256 `569a2ff71165a5168fabe9763f74620dfc68853511213795c2f9eb08d97819c5`.
All continuation workload/observer units were inactive/dead and no old/new
owned pilot processes remained; production was active/running without
intervention. Existing uncommitted source edits remain untouched and unbuilt.

The bounded debt-batching improvement did not establish full-workflow
acceptance. Equal completed snapshot counts do not imply equal partial
traversal scope or causal mode ranking on shared NFS/cache. Full-scale snapshot
load, traversal and debt-publication costs remain insufficiently attributed by
the full CLI. The next technical step requires operator-authorized bounded
diagnosis of that full-scale phase, then a justified separately pinned candidate
and fresh unchanged-contract workflow validation. Do not repeat identical
screenings, extend deadlines, omit snapshots/sources, decouple backup, change
production defaults or remove Pebble on this negative evidence. Full M6
acceptance remains unmet.

## Authorized Full-Scale Snapshot Diagnostic (2026-10-14)

The operator authorized bounded full-scale snapshot profiling, followed by a
justified separately pinned fix under unchanged limits. The diagnostic uses
fresh private root `/volume2/NASDA2/vaultic-test-m6/pilot-f14-JZhHNa` and an
isolated source copy of committed `2c5061887600ac31c4a4e76c4cc065915b6644dc`.
Existing uncommitted schema-store and transaction-test edits are preserved and
not built into this variant. No failed catalog/database is copied or resumed.

Unlike the earlier empty-catalog prefix diagnostic, this action runs the full
fresh index import before snapshot traversal, preserving all 10,170 indexes,
143 snapshots, original exclusions and the 52-source read-only protection
contract. There is no node/depth cap, snapshot omission, deadline extension or
GC tuning. It is an import-only diagnostic, not an independently started backup
or workflow-acceptance run. Import retains its 12-hour deadline, 200 GB combined
CLI/native RSS ceiling sampled every 250 ms, 160 GiB managed policy, unchanged
8 GiB working scratch default and absolute 57,481,929,687,040-byte NFS reserve.

The isolated diagnostic exports the three existing scheduler snapshot
count/time pairs: tree load, tree decode and debt write. Only their three fixed
labels are admitted to the bounded telemetry schema. It adds opt-in CLI CPU
profiles at snapshot ordinals 1 and 29, at most ten minutes each, reserving
32 MiB of managed charge before each profile starts and releasing it
idempotently. Window markers and profiles are private; ordinals select profiling
windows, not a reduced traversal scope. Unreached windows remain unavailable.

The owned-native sampler attaches only to the pinned daemon executable in the
CLI's process group, recording PID/start identity. Sampling is 49 Hz with
frame-pointer callchains, eight ring pages and no DWARF/raw stack dump. Each
native window is bounded by its matching CLI marker and ten-minute deadline;
128 MiB is a sampled file stop threshold, with up to five seconds shutdown
grace, not an instantaneous byte ceiling. Frame-pointer/symbol coverage is not
assumed complete. Auxiliary collector use remains observable in the cgroup,
distinct from the unchanged CLI/native RSS gate. Profiling is intrusive and
does not establish a causal speedup or completed-work throughput.

Seven focused native/race cases passed, including native debt reopen and the
new profile admission/release test. Affected importer, CLI and telemetry
packages passed; controller four, runner seven and observer four self-tests
passed. The exact address-only native sampler permission probe and reproducible
source-patch check passed. The source patch matches final on-disk source bytes;
compiler validation is authoritative for this external module, which is not
registered in the editor workspace. Original input pins, inventory sizes,
counts, all 52 sources, reserve and absence of surviving pilot processes passed
preflight; production was active/running without intervention.

Diagnostic CLI SHA-256 is
`34d56ad483de6848be1826d5b7b2481df340cc0f13624c0dbb36ba1f5fdff692`;
the native daemon remains
`2e69693454611fdc64c3b03ef633dc87f70151032c5c96f82cabf443533aa8bf`.
The plan differs from its frozen parent only in root and CLI hash.
`diagnostic-contract.json` and the 22-input `LAUNCH-SHA256SUMS` pin the source
patch, build/collector identities, controller, observer, runner, validation logs
and parent negative evidence. Launch-seal SHA-256 is
`49367b75c1d8e392b8a8900f0f9d6dc9f0fc73e43ea8f079e9704f3b15ac87f3`.
No performance fix or full M6 acceptance is claimed before this diagnostic is
evaluated. Production defaults and Pebble removal remain blocked.

## Full-Scale Snapshot Diagnostic Outcome (2026-10-15)

The authorized diagnostic completed negatively: import exceeded its unchanged
12-hour bound (43,230.366866 seconds including shutdown grace), exit -9,
peak combined CLI/native RSS 50,986,250,240 bytes. All 10,170 indexes,
419,853 packs, 380,166,821 blobs and 82,649 index batches completed, but only
28 of 143 snapshots were imported (30 seen). Traversal visited 51,622,146
trees and 362,703,851 nodes, creating 725,407,700 debts; identity-known imported
nodes were zero. Zero reported errors is not a clean audit of this partial
import. No independent checks, init or backup ran; no workflow was accepted.
Closing managed charge was 90,316,228,480 bytes, peak 96,494,817,181 bytes;
working scratch remained zero. Managed charge is not process RSS.

Both CLI profile windows completed and synced/closed successfully: snapshot
ordinals 1 and 29, 600.071508 and 600.081630 seconds, with 251.49 and 274.65
seconds of CPU samples respectively. Both matched native windows stopped
normally with SIGINT (collector exit -2), retained readable reports, and stayed
within the five-second shutdown grace and sampled 128 MiB file threshold.
Native file sizes were 11,075,592 and 5,241,824 bytes. Frame-pointer callchains
and symbol coverage are not assumed complete; no raw stack dump was captured.

The late CLI profile attributes overlapping cumulative CPU shares of 32.79%
to tree iteration/decoding, 28.37% to tree loading and 24.34% to debt writing.
These are not additive wall-time attribution or causal speedup evidence.
Native hot symbols include comparisons, skiplist searches, copying and
retention iteration. The observer retained 8,633 samples, mean CLI/native
rate 2.507291 cores across 8,630 available samples, and peak cgroup file cache
245,921,615,872 bytes. File cache is not the combined process RSS gate.

CLI monitoring retained only its startup snapshot (19,586 bytes), so the new
snapshot-stage counters provide no full-scale timing evidence. Saved stderr
contains no validation error, and the exclusive cause of the gap is not
established. A fresh isolated regression did reproduce an exporter defect:
active scheduler metrics, native import statistics and the six exporter-health
fields exceed the fixed 256-metric ceiling after the diagnostic additions.
The isolated candidate reserves health slots, keeps all stage count/time
counters, omits unobserved percentile gauges, and explicitly records any
optional percentile omissions at that unchanged ceiling. Full assembled
snapshot and all-active-stage regressions pass; this does not retrospectively
recover unavailable timing evidence.

The 22-input launch seal was independently reverified unchanged. The actual
outcome, profiles, markers, native status/reports, observation stream and saved
action/output/accounting are retained under `pilot-f14-JZhHNa` and sealed in
30-input `DIAGNOSTIC-OUTCOME-SHA256SUMS`, SHA-256
`810004e7b1149cf9d38667d1cfb0583650aeff3eb2907ef7c9378b56d94b1294`.
Owned workload/observer units are inactive, with no surviving owned CLI,
daemon or sampler; production remains active/running without intervention.
The sealed build contract records Go 1.27.1 after the operator upgrade.

The profile-backed local candidate adds bounded reuse of immutable tree bytes,
not decoded node sharing or traversal skipping. Its 256 MiB reservation precedes
allocation; denied or unaccounted admission falls back to existing loading.
Decoder leases pin retained bytes, early exits release idempotently, and each
visit still freshly decodes nodes and creates its required debts. RAM/KV/auto
admission tests pass. A synthetic repeated-tree/alias fixture preserves every
snapshot, debt, checkpoint and resume byte while reducing source loads from
nine to two. The reused native reopen fixture passes for admitted and denied
caching, retaining both snapshots' debts/checkpoints after reopen. These are
correctness and load-reduction checks, not a full-scale speedup or acceptance.
Main-worktree schema-store and transaction-test edits remain untouched; further
candidate validation and a separately pinned fresh unchanged-contract workflow
are still required. Defaults and Pebble removal remain blocked.

## Pinned Tree-Byte Cache Workflow Candidate

The cache/export candidate is separately frozen under fresh private root
`/volume2/NASDA2/vaultic-test-m6/pilot-tree-cache-s01Xpm`, from committed
`2c5061887600ac31c4a4e76c4cc065915b6644dc` plus the retained eight-file patch.
Patch replay reproduces the exact candidate source bytes; the CLI was built
from the isolated tree, excluding pending main-worktree source edits.
Fifteen importer/native race cases passed, including admitted and denied cache
reopen; two assembled-monitor race regressions and all five affected importer,
CLI, telemetry and working-state packages passed. Frozen runner seven and
observer four self-tests passed. No required native test was skipped.

CLI SHA-256 is
`22637ccb472ae43bfa529787aec17d6699e4e2a1bd632c0ac5773f0334797a61`;
native remains
`2e69693454611fdc64c3b03ef633dc87f70151032c5c96f82cabf443533aa8bf`.
The 21-input `LAUNCH-SHA256SUMS` pins binaries, reproducible source patch,
variant contract, profiles/configuration, full inventory, unchanged runner,
observer-first protected launcher, validation logs and parent diagnostic
launch/outcome seals. Launch SHA-256 is
`67174836a91899a1fc918b50e0374a8f6af0682b6e0d0ea69080e52449b6972a`.

The plan differs from the diagnostic parent only in root and CLI hash. All
10,170 indexes, 143 snapshots, 52 sources, exclusions, 200 GB combined RSS
gate, 160 GiB managed policy, 8 GiB scratch default, 12-hour import/check
bounds, seven-day backup bound, absolute 57,481,929,687,040-byte free-space
reserve and default GC are unchanged. Fresh workflow actions are import,
index check, repository check, init and full backup, stopping on any failed
action. No failed database is copied or resumed; production/default/removal
changes remain unauthorized. The candidate is ready for fresh workflow
screening, not accepted or demonstrated faster at full scale.

## Operator Reboot Hold (2026-10-15)

The operator requested a pause after the currently running background command
finishes, to allow a host reboot. This supersedes earlier automatic KV/auto
continuation. Do not automatically launch KV or auto, another RAM run, a build,
diagnostic or independent backup. Do not reboot or stop production on the
operator's behalf. The current RAM command is allowed to finish naturally.

When the hold was recorded, workload unit
`vaultic-m6-ram-tree-cache-s01Xpm.service` and observer unit
`vaultic-m6-observe-ram-tree-cache-s01Xpm.service` were still active/running.
Background terminal was `b79bfa4e-f309-4783-a7f4-49b418399bbc`. It runs the
whole RAM workflow, including downstream checks/init/backup if preceding
actions pass, and stops on failure. KV and auto had not started. This note is
not a completion result or a declaration that reboot is safe yet.

Persistent root `/volume2/NASDA2/vaultic-test-m6/pilot-tree-cache-s01Xpm`
contains `REBOOT-PAUSE.md` with the exact hold, binary/launch identities and
ordered resume checklist. Frozen source/patch, binaries, configuration,
validation and accumulating RAM artifacts remain there. Because `/run` is
volatile, the current eight changed development files are checkpointed under
`reboot-source-checkpoint/`, with hashes and any post-pin differences recorded
in `reboot-source-checkpoint.json`. These development copies do not replace
the frozen measured source or binary. Pending main-worktree edits are not
reverted, committed or built.

After completion, stop workload execution; do not advance on a completion
notification. Before reboot, verify the owned workload/observer have exited.
After explicit authorization to resume, first inspect and seal the actual RAM
outcome, reverify frozen hashes and production/owned-process state, recreate
volatile build directories if needed, and revalidate the unchanged inventory,
all sources and resource contract. Missing results after reboot mean an
interrupted or unknown outcome; never silently resume the partial database.
The planned next step was fresh KV, then fresh auto if still appropriate, but
both remain paused until the operator explicitly lifts this hold. M6 acceptance,
production-default changes and Pebble removal remain blocked.

### Reboot Hold Reached

The current RAM background command has now exited with launcher status 1.
Saved `ram/import/result.json` records import exit -9, wall timeout exceeded,
43,230.291134 seconds including shutdown grace, peak combined CLI/native RSS
54,793,609,216 bytes, and `completed: false`. Saved import output reports all
10,170 indexes imported, but only 28 of 143 snapshots imported (30 seen),
49,218,935 trees and 335,157,127 nodes visited, and 670,314,252 debts created.
Zero reported errors in the partial import is not a clean audit or acceptance.
The workflow stopped at import; no follow-on workload was launched.

Both owned RAM workload and observer units were verified inactive/dead with
MainPID zero, with no surviving owned CLI/native/sampler/observer processes.
Production remained active/running, MainPID 1160366. KV and auto remain
unstarted. The launch manifest and all eight persistent source-checkpoint
hashes were reverified unchanged. This resolves the pilot-process prerequisite
for the operator's reboot; it is not a host-wide assessment of other work.

Execution is now paused as requested. No KV, auto, build, diagnostic or backup
will start until explicit operator authorization. The actual RAM artifacts are
preserved; detailed outcome analysis/sealing remains the first resume step,
not an accepted M6 result. The persistent root's `REBOOT-PAUSE.md` also records
this completed-command handoff. No reboot was initiated by the assistant.