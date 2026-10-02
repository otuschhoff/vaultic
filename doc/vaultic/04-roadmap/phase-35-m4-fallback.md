# Phase 35 M4: bounded automatic fallback and operator policy

[Phase 35 roadmap](phase-35-ram-working-memory-and-kv-fallback.md) |
[Frozen contract](phase-35-m0-contract.md) |
[M3 integration](phase-35-m3-integration.md)

M4 is complete, including the repeated completion review and accepted retained
verification below. This record does not authorize production experiments,
deployment, default changes, or M5/M6 claims.

## Selection and precedence

Backup, repository check, index import, and index check accept:

| Flag | Profile field | Default when a mode is selected |
|---|---|---|
| `--working-memory-mode` | `mode` | Required: `ram`, `kv`, or `auto` |
| `--working-memory-ram-limit` | `ram_limit` | Conservative 256 MiB ceiling, clamped to resources |
| `--working-memory-scratch` | `scratch` | Existing directory required for KV/auto |
| `--working-memory-disk-budget` | `disk_budget` | 8 GiB aggregate scratch ceiling |
| `--working-memory-reserve` | `reserve` | 128 MiB co-resident allowance, additional to safety/runtime allowances |

```toml
[working_memory]
mode = "auto"
ram_limit = "256MiB"
scratch = "/private/existing/scratch"
disk_budget = "8GiB"
reserve = "512MiB"
```

The same settings use `VAULTIC_WORKING_MEMORY_*`, with the existing
`RESTIC_WORKING_MEMORY_*` fallback. CLI overrides environment; environment
overrides profile defaults. Shared profile settings also reach named backup
jobs, whose explicit CLI overrides retain normal precedence. Omitting all
shared controls preserves the existing behavior, without resource probes,
samplers, new index leases, or changing the Pebble default. Supplying dependent
controls without a mode is a policy conflict.

Shared byte values are positive unsigned integers, optionally suffixed with
`B`, decimal `KB/MB/GB/TB`, or binary `KiB/MiB/GiB/TiB`. Suffixes are
case-sensitive. Fractions, negatives, zero, whitespace, single-letter suffixes,
and overflow are rejected. Older checker controls keep their existing byte
grammar. Explicit older scratch/cache/check limits, including profile and
environment values, must agree with the shared policy; cache capacity remains
subordinate to one eighth of the aggregate allowance. No second budget is
granted. `--metadata-on-demand` remains independent of the backend selection.

## Capacity and pressure

Startup observes host total/available memory, process RSS, runtime committed
memory, and Linux cgroup v1/v2 ancestor limits/headroom. Portable sampling uses
gopsutil v4.26.9 with a bounded probe; unavailable observations fail explicitly.
The resource envelope is the smaller host/cgroup allowance, including the
process's current RSS, minus the co-resident allowance and observed unmanaged
memory. Safety headroom is at least 128 MiB or one fifth of that remaining
envelope. Effective capacity is the smaller configured limit and remaining
safe envelope, and must be at least 8 MiB for CLI startup. Unsafe requests are
reported as smaller effective limits, never enlarged silently.

The sampler runs once per second. Limits can only decrease. Missing resources
or a reduced limit below retained usage stop admissions and cancel participating
command work with a preserved typed cause. Warning starts at 75%; recomputable
caches are evicted before pressure is reevaluated. Pressure begins at 85% and
clears only below 70%. Queues, worker selection, serialized store admissions,
and RPC reply admission provide backpressure; there is no unbounded waiter for
an impossible reservation. Events are fixed-cardinality and capped at 32,
with one event per action. JSON/text report requested/effective mode, configured
and effective limits, measured resources, managed capacity, disk capacity,
pressure cause/action, and remaining migration reserve. Lifecycle `complete`
means the sampler closed, not that the command succeeded.

The cap is managed capacity, not a whole-process RSS guarantee. Arena/index
capacity, growth overlap, cipher batches, owned-copy preparation, decoder
workspace, retained decoded indexes, crawl queues/lookahead, disposable caches,
sort buffers/run metadata/findings, RPC framing, bbolt mapping ceilings, and
migration overlap share the coordinator. Caller-owned returned buffers transfer
ownership at API return; participating retaining consumers admit their own
copies/decoding before retention. Runtime, backend/library internals and ordinary
backup data buffers still need the reported unmanaged and safety allowances.
Reactive sampling cannot prevent arbitrary external OOM. No global GC tuning,
cgroup mutation, or off-heap allocation was added.

## Handoff and exactness

Auto working stores start in RAM. The first RAM store reserves half the
effective run limit as shared migration overlap before allocating retained
state. Siblings cannot spend this reserve independently. Migration starts
before the remaining space is exhausted, considering growth/copy needs,
pressure, and cumulative encoded work; the encoded trigger is one eighth of
the remaining reserve. Repeated updates can therefore migrate early.

A context-aware owner gate drains readers/mutations. A shared, context-aware
migration gate serializes sibling migrations. The RAM traversal holds a stable
view; a private bounded bbolt destination receives exact entries. Authenticated
owners verify ciphertext and every copied value is read back and compared.
Only a fully verified destination is installed, under the owner gate. Old RAM
is then released, mapped/file ceilings transfer to the run budget, and unused
reserve is released or replenished for remaining siblings. Later stores inherit
the one-way KV selection. There is no partial destination visibility or return
to RAM. Copy/open/verification/handoff/cancellation errors retain valid RAM and
preserve their original causes; cleanup failures also remain observable.

External sorts retain their specialized encrypted sorted runs, not a new KV
schema. KV/auto may release exact memory runs on aggregate pressure and continue
with bounded encrypted merges. Required tuples, duplicates, sequence ordering,
new blob locations and findings are never evicted. Strict RAM may reduce a
buffer allocation or fail with typed exhaustion, but never examines/creates
working scratch. Unlimited requested findings fail rather than silently
truncating; limited findings retain the exact canonical prefix.

Neither handoff nor RPC admission retries daemon publication. Only a
definitely-uncommitted private RAM/bbolt working transaction can be retried
locally. Transport uncertainty, server resource exhaustion, corruption, and
authentication failures retain their meanings. Rollback/CancelBegin, including
read-session cleanup, use independent bounded 64 KiB request/4 KiB reply limits
so exhausted state admissions cannot block transaction cleanup.

## Scratch and failures

KV uses pinned bbolt v1.5.0, with a 64 KiB initial ceiling and admitted doubling;
mapped ceilings and file ceilings are charged separately. Scratch budgets cover
all sibling stores, sort runs and transfer/merge overlap. Protected owner codecs
continue to encrypt values and authenticate their domain/key bindings. Roots
are private, uniquely owned and disposable; arbitrary supplied parents and
leftovers are not adopted or removed. Cleanup releases reservations once.
Failed removal retains disk charges for files that may still exist.

Memory, scratch, policy conflict, corruption and migration errors preserve typed
identities. Command errors report run identity and configured/effective limits
without keys/source paths. Memory remediation is reduced concurrency/state,
an explicit safe larger allowance, or KV with suitable scratch. Scratch
remediation is a larger disk ceiling or suitable parent. Corruption/migration
requires abort and inspection, never a publication retry. A cleanup or resource
stop cannot produce a successful incomplete check/snapshot verdict.

## Verification

`bash helpers/phase35-m4/run.sh` retains fresh private artifacts and requires
all named gates through an independently runnable strict analyzer:

- Full touched core/crawl/index/import/check/config/global/command-package race
  suites, plus direct leaf/JSON ownership tests.
- Real root-command RAM/KV/auto backup and binary/NUL restore bytes, startup
  exhaustion, profile/environment/CLI precedence and profile-only conflicts.
- All-mode exact overlay/manifest/import/split-reducer/check/backup parity;
  native authoritative lookup, cancellation ownership, and real-daemon rollback
  with a completely spent budget and no Commit attempts.
- Genuine concurrent 640-entry migration, stable readers, no duplicate/missing
  entries; copy/open/verify/handoff, ENOSPC, authentication, cancellation and
  cleanup injection; shared reserve/disk pools and one-way sibling inheritance.
- Entire unprivileged archiver race suite; all-mode file traces and genuine
  migration trace, with strict RAM participating-scratch access rejected.
- A unique transient service at 512 MiB, swap disabled, admitting managed state
  alongside unrelated allocations, lowering unsafe configured/effective limits,
  rejecting excess admission and exiting normally. This is a reduced pressure
  fixture, not M5 GC/performance evidence.
- Twelve pure-Go core cross-builds and the supported CLI matrix. Existing
  Linux/386 and Linux/arm `MaxPackSize` overflow is classified only after an
  actual build of the archived parent reproduces the same blocker. No baseline
  failure is labeled a passing CLI build. Portable builds are not claims of
  measured RSS behavior on those operating systems.

Accepted final capture: `/tmp/vaultic-phase35-m4-cMxA8K`, based on parent
`c18c37b2cbd6f33267e97a253cbe2508ed78654a` plus retained candidate patch and
untracked source archive. Compiler: Go 1.27.1; module minimum remains 1.26.8.
The original 113-entry `SHA256SUMS` digest is
`78fcb4fd7fec01b82b18800c78eb608a425101246105f5c5fb9c2f486ae3b1ed`.
All entries and the runtime-source pins were independently rechecked; the
strict analyzer was rerun and reproduced the accepted result.

| Gate | Accepted evidence |
|---|---|
| Full touched-package race suites | 1,111 passed test/subtest events |
| CLI policy/workflow gate | 27 passed events, including all three actual byte restores |
| Daemon admission/ownership gate | 64 passed events, no skipped cases |
| Native authority/raw loading | 7 passed events, all three authoritative-overlay modes |
| Unprivileged full archiver race suite | 230 passed test/subtest events |
| Pure-Go core builds | 12/12 OS/architecture targets |
| CLI builds | 10 passed; archived parent reproduces Linux/386 and Linux/arm overflow |
| Isolated 512 MiB cgroup | Normal exit 0; no OOM termination |

The isolated pressure fixture admitted 32 MiB of managed storage alongside
64 MiB of unrelated allocation. Its unsafe 2 GiB request resolved to
312,745,984 bytes, then lowered to 244,379,648 bytes after pressure. Observed
RSS was 124,719,104 bytes and runtime committed memory was 115,587,368 bytes;
excess admission returned typed exhaustion. The transient service's own
reported 3.8 MiB peak is inconsistent with process RSS and is retained as raw
output, not accepted as a residency measurement or performance result.

Optional skips were subprocess-helper entry points, the cgroup test outside its
dedicated capped invocation, macOS Secure Enclave enrollment on Linux, and
external S3-compatible workflow coverage without that fixture. They are not
substitutes for required M4 gates and no required named gate was skipped.
Sampler graph additions are pinned/checksummed; gopsutil uses BSD licensing
(`LICENSE` SHA-256
`ad1e64b82c04fb2ee6bfe521bff01266971ffaa70500024d4ac767c6033aafb9`).

The first capture, `/tmp/vaultic-phase35-m4-U3ELRa`, remains rejected for the
missing leaf-package gate. The subsequent review/test cycle fixed truncated
JSON preflight, cancellable aggregate migration waits, pressure hysteresis,
copy admission before migration, profile-origin conflicts, sampler stop cause
propagation, exhausted-budget rollback, retained run metadata, constructor/
abort cleanup charges, metadata length overflow, and early checker ownership
release. Required M4 exit invariants were rechecked against the final source:
exact tuples/duplicates/restores, verified-only atomic handoff, strict no-spill,
aggregate caps, one-way fallback, safe cleanup, and no publication retry/false
success. No remaining M4 findings were identified by this completion review.

The final patch/tree/commit seal is retained alongside the capture; documentation
is the only post-capture source addition. M5/M6, realistic large-heap GC
benchmarks, recommendations, and removal of the legacy Pebble path remain
pending. This reduced fixture/build evidence is not production performance or
non-Linux runtime sampling evidence; production behavior is not rolled out.