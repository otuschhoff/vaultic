# Phase 32 P1 Instrumentation Evidence

[Phase 32 status and document map](phase-32-scalable-legacy-metadata-bulk-import.md)

This is frozen P1 evidence, not the current binary profile or experiment policy.

This record retains the paired benchmark summaries used for the P1 overhead
check. The baseline is commit `236330f4403c0b89fc136f5a65e2fa17a56d1b7c`.
The candidate is the commit containing this record. Both runs used Linux amd64
on an Intel Xeon Gold 5217, Go's `-benchtime=3x`, `GOMAXPROCS=4`, and
`GOMEMLIMIT=8GiB`.

The frozen input SHA-256 was
`f2b658363adcb6760a1fa9fd411fa60a4a9de0a0d2ff15d6ed961efeb9b9e943`.
The validated symbol-enabled VaulticDB SHA-256 was
`531d14a54ca983c10366ebd8759d20390deef437eac93eb93a7b07dc16b2e4c4`.
Every iteration used a fresh temporary candidate and completed mark, close,
WAL handoff, reopen, and all-checkpoint verification.

```console
GOMAXPROCS=4 GOMEMLIMIT=8GiB \
  VAULTICDB_TEST_BINARY="$PWD/bin/profile/linux-amd64/vaulticdb" \
  go test -v ./internal/index/legacyimport -run '^$' \
  -bench '^BenchmarkImportStage3Daemon$' -benchtime=3x -count=1
```

## Raw summary rows

Baseline output SHA-256:
`3794776315958966ef83f1bf1b844a62b6ffa14310ffab268faec31067355205`.

```text
lanes=1 deferred=false  1166517761 ns/op  56181 end-to-end-blobs/s  101825 import-blobs/s  209371469 B/op  1092870 allocs/op
lanes=2 deferred=false  2678669483 ns/op  24466 end-to-end-blobs/s   32929 import-blobs/s  259919480 B/op  1900458 allocs/op
lanes=2 deferred=true   1162590810 ns/op  56371 end-to-end-blobs/s  132696 import-blobs/s  258110112 B/op  1900418 allocs/op
lanes=4 deferred=true    694519476 ns/op  94362 end-to-end-blobs/s  180073 import-blobs/s  261422013 B/op  1900590 allocs/op
lanes=8 deferred=true    694978499 ns/op  94299 end-to-end-blobs/s  178712 import-blobs/s  263139760 B/op  1900605 allocs/op
```

P1 output SHA-256:
`a3682091b3d9ad291192fa379e4bc30da561392916b31844f7cbd5f7d6480f5b`.

```text
lanes=1 deferred=false  1155887816 ns/op  56698 end-to-end-blobs/s  118645 import-blobs/s  207243402 B/op  1092857 allocs/op
lanes=2 deferred=false  2666778851 ns/op  24575 end-to-end-blobs/s   32914 import-blobs/s  259002992 B/op  1900740 allocs/op
lanes=2 deferred=true    695626245 ns/op  94212 end-to-end-blobs/s  141933 import-blobs/s  257344616 B/op  1900745 allocs/op
lanes=4 deferred=true    691113164 ns/op  94827 end-to-end-blobs/s  190678 import-blobs/s  262088994 B/op  1900907 allocs/op
lanes=8 deferred=true    681176736 ns/op  96210 end-to-end-blobs/s  200581 import-blobs/s  262826536 B/op  1900936 allocs/op
```

## Comparison

Rate change is `(P1 / baseline - 1) * 100`. Allocation change uses the same
formula. Ordinary end-to-end rates changed by +0.92% for one lane and +0.45%
for two lanes. Their allocation counts changed by -0.0012% and +0.0148%.
Deferred import-only rates changed by +6.96%, +5.89%, and +12.24% for two,
four, and eight lanes. The large two-lane deferred difference is finalization
variance, not an instrumentation claim. These three-iteration aggregates reject
obvious overhead only; they are not P3 performance evidence.