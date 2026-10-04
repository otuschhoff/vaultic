#!/usr/bin/env bash
set -euo pipefail
repo=$(git rev-parse --show-toplevel)
cd "$repo"
protocol=$(cd "$(dirname "$0")" && pwd)
if [[ ${1:-} == --matrix ]]; then
    root=$2
    node_bin=${VAULTIC_PHASE35_M5_NODE:?pinned Node executable required}
    [[ -x "$node_bin" ]]
    [[ -f "$root/plan.sha256" && -f "$root/.nobackup" ]]
    memory_bytes=$("$node_bin" -p 'require(process.argv[1]).isolated_memory_max_bytes' "$root/plan.json")
    test_timeout=$("$node_bin" -p 'require(process.argv[1]).test_timeout' "$root/plan.json")
    group=$(awk -F: '$1 == "0" {print $3}' /proc/self/cgroup)
    cgroup="/sys/fs/cgroup$group"
    [[ $(< "$cgroup/memory.max") == "$memory_bytes" && $(< "$cgroup/memory.swap.max") == 0 ]]
    command cp "$cgroup/memory.events" "$root/cgroup-before.events"
    printf '%s\n' "$group" > "$root/cgroup-path.txt"
    cat "$cgroup/memory.max" "$cgroup/memory.swap.max" > "$root/cgroup-limits.txt"
    "$node_bin" "$protocol/analyze.cjs" --overlap-check "$root"
    "$node_bin" "$protocol/analyze.cjs" --watch-overlap "$root" > "$root/logs/overlap-monitor.log" 2>&1 &
    guard=$!
    trap 'kill "$guard" 2>/dev/null || true; wait "$guard" 2>/dev/null || true' EXIT
    while IFS=$'\t' read -r id kind mode scenario limit payload; do
        kill -0 "$guard"
        [[ ! -f "$root/overlap-violation.json" ]]
        variant=candidate
        if [[ "$mode" == pebble ]]; then variant=baseline; fi
        mkdir "$root/results/$id"
        printf 'RUN %s\n' "$id"
        if [[ "$kind" == workflow ]]; then test=TestM5Workflow; else test=TestM5WorkingSet; fi
        /usr/bin/env VAULTIC_PHASE35_M5_MODE="$mode" VAULTIC_PHASE35_M5_SCENARIO="$scenario" \
            VAULTIC_PHASE35_M5_LIMIT="$limit" VAULTIC_PHASE35_M5_PAYLOAD="$payload" \
            VAULTIC_PHASE35_M5_OUTPUT="$root/results/$id" /usr/bin/time -v -o "$root/logs/$id.time" \
            "$root/bin/$variant-$kind.test" -test.run="^$test$" -test.count=1 -test.timeout="$test_timeout" -test.v \
            > "$root/logs/$id.log" 2>&1
        rg -q -- "--- PASS: $test " "$root/logs/$id.log"
        [[ -s "$root/results/$id/semantic.json" && -s "$root/results/$id/measurement.json" ]]
        kill -0 "$guard"
        [[ ! -f "$root/overlap-violation.json" ]]
    done < "$root/schedule.tsv"
    kill "$guard"
    wait "$guard"
    trap - EXIT
    [[ ! -f "$root/overlap-violation.json" && -s "$root/overlap-monitor.json" ]]
    command cp "$cgroup/memory.events" "$root/cgroup-after.events"
    command cp "$cgroup/memory.peak" "$root/cgroup-peak.txt"
    exit 0
fi
export GOCACHE=${GOCACHE:-/run/vaultic-go-cache}
export TMPDIR=/tmp
export GOMAXPROCS=4
unset GOGC GOMEMLIMIT GODEBUG
unset VAULTIC_PHASE35_M5_ROOT
export VAULTICDB_TEST_BINARY=${VAULTICDB_TEST_BINARY:-/run/vaultic-diagnostics/cargo-target/debug/vaulticdb}
root=$(mktemp -d "${PHASE35_M5_PARENT:-/tmp}/vaultic-phase35-m5-XXXXXX")
node_bin=$(command -v node)
chmod 700 "$root"
mkdir "$root/bin" "$root/logs" "$root/results" "$root/scratch" "$root/protocol"
command cp "$protocol/run.sh" "$protocol/analyze.cjs" "$root/protocol/"
protocol="$root/protocol"
sha256sum "$protocol/run.sh" "$protocol/analyze.cjs" > "$root/protocol.sha256"
printf 'Disposable Phase 35 M5 benchmark fixture.\n' > "$root/.nobackup"
command cp "$VAULTICDB_TEST_BINARY" "$root/bin/fixture-vaulticdb"
chmod 700 "$root/bin/fixture-vaulticdb"
export VAULTICDB_TEST_BINARY="$root/bin/fixture-vaulticdb"
printf '%s\n' "$root"
git rev-parse HEAD > "$root/revision.txt"
git diff --binary HEAD > "$root/candidate.patch"
git ls-files --others --exclude-standard -z | tar --null -T - -cf "$root/untracked.tar"
git ls-files -z -- '*.go' go.mod go.sum | xargs -0 -r sha256sum > "$root/runtime-source.sha256"
git ls-files --others --exclude-standard -z -- '*.go' | xargs -0 -r sha256sum >> "$root/runtime-source.sha256"
sha256sum "$VAULTICDB_TEST_BINARY" > "$root/fixture-daemon.sha256"
sha256sum "$node_bin" > "$root/node.sha256"
"$node_bin" --version > "$root/node-version.txt"
go version > "$root/go-version.txt"
go list -m all > "$root/modules.txt"
printf 'GOMAXPROCS=4\nGOGC=default\nGOMEMLIMIT=default\nGODEBUG=default\n' > "$root/runtime-settings.txt"
findmnt -T "$root" -o TARGET,FSTYPE,SOURCE > "$root/filesystem.txt"
cat /proc/self/cgroup > "$root/parent-cgroup.txt"
node "$protocol/analyze.cjs" --selftest > "$root/logs/analyzer-selftest.log"
node "$protocol/analyze.cjs" --plan "$root" > "$root/schedule.tsv"
memory_max=$(node -p 'require(process.argv[1]).isolated_memory_max' "$root/plan.json")
seed_timeout=2m
if [[ $(node -p 'require(process.argv[1]).profile' "$root/plan.json") == extended ]]; then
    [[ $(findmnt -T "$root" -n -o FSTYPE) == nfs* ]]
    seed_timeout=30m
fi
baseline=$(mktemp -d /run/vaultic-phase35-m5-baseline-XXXXXX)
baseline_revision=c567dd4561d512162d823ccbfcb3d8578c9690a1
git rev-parse "$baseline_revision^{commit}" > "$root/baseline-revision.txt"
git archive "$baseline_revision" | tar -x -C "$baseline"
command cp go.mod go.sum "$baseline/"
mkdir "$baseline/internal/workingbench"
command cp internal/workingbench/*.go "$baseline/internal/workingbench/"
command cp internal/telemetry/working_runtime.go "$baseline/internal/telemetry/"
command cp cmd/vaultic/working_benchmark_test.go "$baseline/cmd/vaultic/"
command cp internal/workingkv/working_benchmark_test.go "$baseline/internal/workingkv/"
printf '%s\n' 'Identical measurement-only overlay: runtime interval reader, independent workingbench helper and opt-in fixture tests; no production algorithms or GC settings changed.' > "$root/baseline-instrumentation.txt"
for variant in baseline candidate; do
    source="$repo"
    if [[ "$variant" == baseline ]]; then source="$baseline"; fi
    pushd "$source" > /dev/null
    go test -c -o "$root/bin/$variant-workflow.test" ./cmd/vaultic > "$root/logs/build-$variant-workflow.log" 2>&1
    go test -c -o "$root/bin/$variant-working.test" ./internal/workingkv > "$root/logs/build-$variant-working.log" 2>&1
    popd > /dev/null
done
go test -race -json ./internal/telemetry ./internal/workingbench ./internal/workingkv -skip '^(TestM5WorkingSet|TestGrowthFailureProcess|TestFilesystemFailureProcess|TestM4IsolatedCgroupPressure)$' -count=1 > "$root/logs/measurement-race.jsonl" 2>&1
go test -race -json ./internal/index/legacyimport ./internal/index/maintenance -count=1 -timeout=5m > "$root/logs/semantic-regressions.jsonl" 2>&1
for target in linux/amd64 linux/arm64 linux/386 linux/arm darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm64 openbsd/amd64 openbsd/arm64; do
    label=${target//\//-}
    CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go test -c -o "$root/bin/core-$label.test" ./internal/workingkv > "$root/logs/core-$label.log" 2>&1
    printf 'PASS %s CGO_ENABLED=0\n' "$target" >> "$root/core-matrix.txt"
done
export VAULTIC_PHASE35_M5_ROOT="$root"
"$root/bin/candidate-workflow.test" -test.run='^TestM5BuildSeed$' -test.count=1 -test.timeout="$seed_timeout" -test.v > "$root/logs/seed.log" 2>&1
find "$root/seed" -type f -print0 | sort -z | xargs -0 sha256sum > "$root/seed.sha256"
sha256sum "$root"/bin/*.test > "$root/binaries.sha256"
node "$protocol/analyze.cjs" --overlap-check "$root" > "$root/overlap-preflight.log"
export VAULTIC_PHASE35_M5_ROOT="$root"
unit="vaultic-phase35-m5-$(basename "$root")"
systemd-run --unit="$unit" --collect --wait --pipe -p MemoryMax="$memory_max" -p MemorySwapMax=0 -p WorkingDirectory="$repo" \
    /usr/bin/env GOMAXPROCS=4 TMPDIR="$root/scratch" VAULTIC_PHASE35_M5_ROOT="$root" VAULTICDB_TEST_BINARY="$VAULTICDB_TEST_BINARY" \
    VAULTIC_PHASE35_M5_NODE="$node_bin" \
    /usr/bin/bash "$protocol/run.sh" --matrix "$root" > "$root/logs/cgroup.log" 2>&1
for profile in "$root"/results/*/*.pprof; do
    go tool pprof -top -nodecount=5 "$profile" > "$profile.top" 2>&1
done
sha256sum -c "$root/seed.sha256" > "$root/seed-verified.log"
sha256sum -c "$root/runtime-source.sha256" > "$root/source-verified.log"
sha256sum -c "$root/node.sha256" > "$root/node-verified.log"
sha256sum -c "$root/fixture-daemon.sha256" > "$root/fixture-daemon-verified.log"
sha256sum -c "$root/protocol.sha256" > "$root/protocol-verified.log"
node "$protocol/analyze.cjs" "$root"
sha256sum "$root/analysis.json" > "$root/analysis.sha256"
node "$protocol/analyze.cjs" "$root" > "$root/logs/analysis-reproduced.log"
sha256sum -c "$root/analysis.sha256" > "$root/analysis-reproduced-verified.log"
find "$root" -type f ! -name SHA256SUMS ! -name verified.log -print0 | sort -z | xargs -0 sha256sum > "$root/SHA256SUMS"
sha256sum -c "$root/SHA256SUMS" > "$root/verified.log"
printf 'PASS retained M5 evidence: %s\n' "$root"