# Phase 34 M5 aggregate-monitoring evidence

This artifact records Phase 34 M5 at base revision `84630d2dc` plus the M5
worktree on 2026-09-19. M5 completes aggregate rate/window derivation and the
interactive `vaultic monitor watch` terminal. It does not certify or change M6
export delivery.

## Live monitor behavior

`vaultic monitor watch` defaults to the overview and requires terminal input.
Collection, input polling and rendering have independent schedules: collection
uses the requested interval, keys are polled every 100 ms, and rendering is
limited to four updates per second. At most one collection is in flight. Quit,
Ctrl-C and context cancellation cancel that collector before raw terminal state
is restored.

The keyboard controls are:

| Key | Action |
|---|---|
| `1` or `o` | Overview |
| `2` | Storage |
| `3` or `p` | Active operations |
| `4` or `w` | WAL/writeback |
| `5` or `c` | Caches |
| `6` or `l` | Latency |
| `s` / `S` | Cycle sort key / reverse direction |
| Space | Pause or resume collection |
| `r` | Reset derived windows |
| `q` or Ctrl-C | Clean shutdown |

View switching operates on a render-only deep copy, so filtering and sorting do
not mutate the retained sample while paused. Every rendered line is clamped to
the current terminal width. Widths below 96 columns use a stable narrow summary;
the width is read on every render, so resize needs no collection restart.

## Fixed windows and honest availability

Only compact component identity plus counter/histogram data enters watch history.
Samples are anchored no more often than every 15 seconds, while process,
component, metric-availability and counter/histogram regression transitions are
retained immediately. The hard limit is 62 anchors: 60 fifteen-second intervals,
the current anchor and one predecessor. Attachment duration and refresh rates do
not increase retention.

Rates and bucket-derived p50/p95/p99 values use bounded 1, 5 and 15 minute
windows. Local aggregate capture time decides whether a missing, stale or reset
transition belongs to a window; each component's own capture time supplies its
rate denominator. This prevents clock skew from hiding outages and prevents old
resets from contaminating shorter windows. Warming history and counter/process
resets are separate visible states.

Overview values inherit source availability. Missing or estimated collectors
make cross-component totals partial or unavailable rather than authoritative
zeroes. Cache occupancy and byte-hit ratio reject unavailable and zero-denominator
inputs. Failure aggregation is limited to exact
`operation_completed{outcome=failure}` operation counters, so byte counters and
wait/dependency counters cannot be mixed into the total.

The bounded M4 schema has latency role labels but no per-backend latency
dimension. M5 therefore reports `slowest_backend=unavailable(no bounded
per-backend latency)` instead of inventing attribution or expanding schema
cardinality. Writeback rate is derived from the bounded
`engine_memtable_write_bytes` counter.

## Operator output

The deterministic overview fixture renders this representative 140-column
output after two samples one minute apart:

```text
monitor watch view=overview sort=component desc=false paused=false sample_age=1s retention=15m windows=1m,5m,15m reset=none width=140
keys: 1..6 view, s sort, S reverse, space pause, r reset-window, q quit
monitor schema=2 captured=1970-01-01T00:01:01Z
cache vaultic.repository occupancy=70.0% byte_hit_ratio=75.0% origin_bytes=25 availability=exact
writeback queue=vaultic.batch_write depth=8 capacity=10 pressure=0.80 throttle=capacity availability=partial
writeback rate_1m=1.00(partial) bytes/s
wal retained_bytes=unavailable outstanding_flushes=unavailable
active retries=1(partial) cumulative_operation_failures=0(partial)
slowest_backend=unavailable(no bounded per-backend latency)
collectors stale=vaulticdb=unavailable
window summaries:
  rate vaultic.engine_memtable_write_bytes 1m:1.00 5m:1.00 15m:1.00 bytes/s
```

The deterministic 80-column resize fixture renders:

```text
monitor narrow width=80 view=status pause=false age=1s
components exact=1 stale_or_estimated=0 unavailable=0
keys: 1..6 s S space r q
```

Latency rows expose window deltas rather than lifetime distributions, for
example:

```text
window summaries:
  latency vaultic.request_latency 1m:p50<=100,p95<=1000,p99<=1000 5m:p50<=100,p95<=1000,p99<=1000 15m:p50<=100,p95<=1000,p99<=1000 microseconds
```

A manually reset window is explicit in the header and begins warming again:

```text
monitor watch view=overview sort=component desc=false paused=false sample_age=0s retention=15m windows=1m,5m,15m reset=manual@1970-01-01T00:00:20Z width=140
```

## Reconciliation boundary

Watch collection always requests `reconcile=false`; refreshes never scan
inventory. Only explicit `vaultic monitor storage --reconcile` starts the
asynchronous reconciliation collection. Interactive terminals receive bounded
progress such as:

```text
monitor storage reconcile running elapsed=1.5s cancel=ctrl-c
```

Progress is suppressed when output is noninteractive or status updates are not
supported, preserving clean JSON and pipeline output. Cancellation returns
promptly through the command context.

## Validation

Focused acceptance commands are:

```text
go test -race ./cmd/vaultic -run 'Test(Monitor|Watch|CollectMonitor)' -count=10
go test ./internal/ui/...
GOOS=windows GOARCH=amd64 go test -c ./cmd/vaultic
```

Deterministic tests cover rates, bucket percentiles, process/counter resets,
reset expiry per window, missing and stale transitions, component clock skew,
62-anchor retention, default view, all controls, displayed-value sorting,
nonmutating view switches, pause/resume, blocked-collector cancellation,
noninteractive reconciliation, narrow and resized terminals, line bounds,
partial/unavailable overview values, and a real Linux pseudo-terminal raw-mode
read and restoration. The focused race suite passed ten consecutive runs. A
strict findings-only implementation audit reported exactly `No actionable
findings.`
