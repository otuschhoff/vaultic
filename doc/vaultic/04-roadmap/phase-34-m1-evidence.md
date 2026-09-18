# Phase 34 M1 bounded-accounting evidence

This artifact records the Phase 34 M1 implementation and measurements taken on
2026-09-18 at base revision `7a272ab30` plus the M1 worktree. It is primitive
and adapter evidence, not representative workload or exporter evidence.

## Scope and ownership

M1 adds shared Go wait, action, dependency, and sampled-correlation accounting;
uses fixed schema-v2 identities, saturating counters, fixed histograms, bounded
active detail, and cancellation-safe guards; and replaces the Rust timing
attribution active map with bounded atomic slots. The legacy import and index
checker own production adapters for their existing queue, admission, database,
and scratch boundaries.

The Go active-wait and active-operation capacities are each capped at 128.
Completed sampled correlations use a channel capped at 128 and count samples
that cannot be retained. Rust active timing uses 128 atomic slots and reports
active-detail overflow separately from aggregate completion. Aggregate counters
continue when active detail overflows.

M1 does not add delay/failure injection, broad backup/restore/maintenance
instrumentation, cross-process collection, terminal rendering, or export. Those
remain M2 through M6 work.

## Correctness evidence

The following checks cover concurrent snapshots and settlement, overflow,
stalled age, cancellation and timeout classification, disabled no-op behavior,
component schema validation, import queue ownership, checker database and
scratch bytes, and enabled/disabled checker result equivalence:

```text
go test -race ./internal/telemetry -count=1
ok github.com/otuschhoff/vaultic/internal/telemetry

go test -race ./internal/index/daemon ./internal/index/legacyimport \
  ./internal/index/maintenance ./cmd/vaultic/indexcmd -count=1
ok github.com/otuschhoff/vaultic/internal/index/daemon
ok github.com/otuschhoff/vaultic/internal/index/legacyimport
ok github.com/otuschhoff/vaultic/internal/index/maintenance
ok github.com/otuschhoff/vaultic/cmd/vaultic/indexcmd

cargo test --manifest-path vaulticdb/Cargo.toml
test result: ok. 172 passed; 0 failed
test result: ok. 1 passed; 0 failed

./vaulticdb/generate-proto.sh
cargo fmt --manifest-path vaulticdb/Cargo.toml -- --check
```

Protobuf Go output was hash-identical after a second generation. The Go
binaries cross-compiled with `CGO_ENABLED=0` for Linux, Windows, FreeBSD, and
macOS on `amd64`. The repository-wide Go race run also completed every touched
package successfully; unrelated baseline failures are documented in the M1
acceptance commit.

## CPU and allocation evidence

Environment: Linux 7.0.14-11-pve x86_64, Intel Xeon Gold 5217 at 3.00 GHz,
Go 1.27.1, Rust 1.98.1. Five benchmark samples were collected with:

```text
go test ./internal/telemetry -run '^$' \
  -bench 'Benchmark(Action|Dependency|Wait|Correlation)Accounting' \
  -benchmem -count=5
```

| Primitive path | Time range | Heap bytes/op | Allocations/op |
|---|---:|---:|---:|
| wait disabled | 5.897-5.952 ns | 0 | 0 |
| wait enabled | 212.3-214.7 ns | 32 | 1 |
| dependency disabled | 4.559-4.631 ns | 0 | 0 |
| dependency enabled | 187.7-188.5 ns | 32 | 1 |
| correlation disabled | 6.812-6.883 ns | 0 | 0 |
| correlation enabled, every event sampled and drained | 304.4-341.9 ns | 176 | 3 |
| action disabled | 8.061-8.174 ns | 0 | 0 |
| action enabled, including progress update | 2.678-3.120 us | 3,016 | 15 |

Wait and dependency accounting are the request-path primitives. Correlation is
sampled at a configured every-N interval. Action accounting is command-level;
it is not installed per object or per database request. All disabled operation
paths are allocation-free.

## Memory bounds

Three constructor samples were collected with:

```text
go test ./internal/telemetry -run '^$' \
  -bench '^BenchmarkAccountingConstruction$' -benchmem -count=3
```

| Owner at maximum configured capacity | Disabled bytes/owner | Enabled bytes/owner |
|---|---:|---:|
| dependency | 3,128 | 3,128 |
| wait, 128 active slots | 4,704 | 4,704 |
| sampled correlation, 128 retained completions | 14,632 | 14,632 |
| action, 128 active operations | 20,128 | 53,080 |

These are constructor allocation measurements, not live-heap object-size
claims. Tests additionally assert the 128-entry Go capacities; Rust tests fill
all 128 atomic slots, exercise seven overflows, and verify that all 135 guards
settle into aggregate cancellation counts. No primitive retains an unbounded
history or derives a label from repository, object, path, or request data.
