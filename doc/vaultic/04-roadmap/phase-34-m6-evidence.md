# Phase 34 M6 optional-export evidence

This artifact records Phase 34 M6 at base revision `b2a3cc94e` plus the M6
worktree on 2026-09-20. M6 completes optional monitoring export delivery. It
does not certify the representative cross-workload experiments required by M7.

## Ownership and delivery

`vaultic` owns a vendor-neutral `SnapshotExporter` interface and one
asynchronous bounded worker. The worker validates and clones each accepted
schema-v2 snapshot, retains at most the configured queue plus one in-flight
snapshot, replaces the oldest queued snapshot on overflow, and bounds each
attempt, retry count, exponential backoff, and shutdown wait. Export failure
cannot enter VaulticDB, repository, WAL, cache, broker, or filesystem data paths.

`vaultic monitor export influxdb` is opt-in. It writes bounded HTTP batches to
the InfluxDB v2 write endpoint and exports gauges, histogram buckets, and
counter values/deltas with reset markers. Component process identity resets
counter baselines. A partially delivered snapshot invalidates all baselines so
the next successful snapshot is marked as a reset rather than reporting a false
delta. Cardinality loss and exporter failures, drops, pending work, capacity,
in-flight state, and oldest age are exported as bounded health series.

The continuous export collector reserves six metric identities before
collection. Health metrics therefore never displace an arbitrary production
metric at the 256-identity component limit. Ordinary snapshot and watch
collection retain the full 256-identity budget.

VaulticDB, the key broker, and cache coordinators contain no exporter client and
never contact monitoring destinations. Monitoring history is not written to a
repository, VaulticDB, a WAL, or a cache tier.

## Maximum-cardinality measurement

The deterministic maximum fixture is schema-valid and contains all four
component identities. Each component contains 256 unique valid metric
identities, 128 active operations, overflow records for all 18 operation
classes, all 3 schema-valid queues, 128 storage records, 64 cache records, and a
WAL record. Its Influx representation contains 7,020 points and 1,606,401 bytes.
The test rejects responses above 32 MiB and verifies the cardinality-loss
counter is present.

On `linux/amd64`, Go 1.27.1, Intel Xeon Gold 5217 at 3.00 GHz, three benchmark
samples produced:

```text
BenchmarkInfluxSnapshotMaximumCardinality-32  40.044868 ms/op  40.12 MB/s  66703621 B/op  119052 allocs/op
BenchmarkInfluxSnapshotMaximumCardinality-32  40.458891 ms/op  39.70 MB/s  66701053 B/op  119042 allocs/op
BenchmarkInfluxSnapshotMaximumCardinality-32  41.423259 ms/op  38.78 MB/s  66702044 B/op  119046 allocs/op
```

This is a deliberately adversarial four-component maximum, not a typical
snapshot. Serialization has a fixed schema/cardinality ceiling but currently
allocates about 66.7 MB transiently because line-protocol construction and HTTP
batch splitting copy data. M6 accepts that measured bounded cost; reducing it
would be an optimization, not an unbounded-retention correction.

## Blocked endpoint and loss behavior

The blocked-endpoint race test holds a maximum-cardinality request inside a real
HTTP handler. Three additional maximum snapshots fill the two-entry queue and
replace its oldest item, proving maximum-cardinality overflow. One hundred
normal replacements then complete in 9.176953 ms while the request remains
blocked. Observed worker health was:

```text
pending=3 capacity=3 dropped=101 oldest_age=554.110254ms in_flight=true
```

Capacity includes the configured two queued snapshots plus one in-flight
snapshot. Drops and failures are cumulative process-lifetime counters; oldest
age uses the worker's monotonic clock domain and is clamped at zero. Stats only
read queue/atomic state and never dequeue or reorder work.

While that same maximum request was blocked, a production-accounting operation
opened and read a wrapped source filesystem payload in 188.156 microseconds.
The test imposes a one-second bound. Request release precedes bounded exporter
shutdown, and every test synchronization point has a timeout, so endpoint
failure cannot strand the test or a worker.

## Authentication, redaction, and reset coverage

Tokens are accepted only from a named environment variable or an owner-only,
size-bounded regular file. Symlinks and permissive token files are rejected.
Non-loopback plaintext HTTP is rejected, redirects are not followed with the
authorization header, response bodies are bounded while draining, and tokens
are absent from command flags and errors. Snapshot validation restricts tags to
the versioned allowlists; active-operation IDs, paths, object IDs, error text,
and arbitrary labels are not emitted as Influx tags.

Tests cover retry classification and bounds, unavailable fields, counter and
process resets, partial-batch failure, maximum unsigned values, bounded request
batches, token sources, TLS/redirect policy, snapshot ownership, concurrent
close, draining, cancellation, overflow, staleness, and the blocked data path.

## Reproduction

```text
go test -race -timeout=5m ./internal/telemetry -count=1
go test -race -timeout=5m ./cmd/vaultic -run '^TestMonitor' -count=1
go test -race -timeout=30s ./internal/telemetry -run '^TestAsyncInfluxExporterBlockedEndpointRemainsBounded$' -count=1 -v
go test ./internal/telemetry -run '^TestInfluxExporterMaximumCardinalityBatchesRemainBounded$' -count=1 -v
go test ./internal/telemetry -run '^$' -bench '^BenchmarkInfluxSnapshotMaximumCardinality$' -benchmem -count=3
GOOS=windows GOARCH=amd64 go test -c -o /tmp/vaultic-telemetry.test.exe ./internal/telemetry
GOOS=windows GOARCH=amd64 go test -c -o /tmp/vaultic-command.test.exe ./cmd/vaultic
git diff --check
```

The telemetry package and all monitor-prefixed command tests pass under the race
detector. Both touched packages compile for Windows. A strict findings-only
audit identified the missing published evidence and maximum-cardinality
overflow proof; both are included above.