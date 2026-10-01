#!/usr/bin/env bash
set -euo pipefail

repo=$(git rev-parse --show-toplevel)
baseline=${PHASE35_BASELINE_REVISION:-10ea6d6b5}
repetitions=${PHASE35_REPETITIONS:-3}
threads=${PHASE35_GOMAXPROCS:-4}
if [[ ! "$repetitions" =~ ^[0-9]+$ || "$repetitions" -lt 3 || ! "$threads" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s\n' 'Require at least three repetitions and a positive GOMAXPROCS.' >&2
    exit 1
fi
for tool in go git node strace tar sha256sum rg findmnt; do
    command -v "$tool" >/dev/null
done
[[ -x /usr/bin/time ]]
root=$(mktemp -d "${PHASE35_ARTIFACT_PARENT:-/tmp}/vaultic-phase35-m0-XXXXXX")
chmod 700 "$root"
mkdir "$root/control" "$root/bin" "$root/logs" "$root/profiles" "$root/scratch"
printf '%s\n' 'Disposable Phase 35 fixture/profiling artifacts.' > "$root/.nobackup"
printf '%s\n' "$root" > "$root/artifact-root.txt"
printf '%s\n' "$root"
git -C "$repo" rev-parse "$baseline" > "$root/baseline-revision.txt"
git -C "$repo" rev-parse HEAD > "$root/candidate-revision.txt"
git -C "$repo" diff --binary HEAD > "$root/candidate.patch"
git -C "$repo" ls-files --others --exclude-standard > "$root/untracked-files.txt"
git -C "$repo" ls-files --others --exclude-standard -z | tar -C "$repo" --null -T - -cf "$root/candidate-untracked.tar"
go version > "$root/go-version.txt"
printf 'GOMAXPROCS=%s\nGOGC=%s\nGOMEMLIMIT=%s\nGODEBUG=%s\n' "$threads" "${GOGC:-default}" "${GOMEMLIMIT:-default}" "${GODEBUG:-default}" > "$root/runtime-settings.txt"
findmnt -T "$root" -o TARGET,FSTYPE,SOURCE > "$root/artifact-filesystem.txt"
git -C "$repo" archive "$baseline" | tar -x -C "$root/control"
for variant in control candidate; do
    source="$repo"
    if [[ "$variant" == control ]]; then source="$root/control"; fi
    for package in index archiver crawl index/legacyimport index/maintenance; do
        label=${package//\//-}
        (cd "$source" && GOCACHE="${GOCACHE:-/run/vaultic-go-cache}" TMPDIR="$root/scratch" go test -c -o "$root/bin/$variant-$label" "./internal/$package") > "$root/logs/build-$variant-$label.log" 2>&1
    done
done

run_case() {
    local variant=$1 label=$2 package=$3 benchmark=$4 repeat=$5
    local stem="$variant-$label-$repeat"
    GOMAXPROCS="$threads" TMPDIR="$root/scratch" GODEBUG="${GODEBUG:+$GODEBUG,}gctrace=1" \
        /usr/bin/time -v -o "$root/logs/$stem.time" "$root/bin/$variant-$package" \
        -test.run='^$' -test.bench="$benchmark" -test.benchtime=1s -test.benchmem -test.count=1 -test.v \
        -test.cpuprofile="$root/profiles/$stem.cpu" -test.memprofile="$root/profiles/$stem.heap" \
        -test.outputdir="$root/profiles" > "$root/logs/$stem.log" 2>&1
}

for ((repeat=1; repeat<=repetitions; repeat++)); do
    variants=(control candidate)
    if ((repeat % 2 == 0)); then variants=(candidate control); fi
    for variant in "${variants[@]}"; do
        run_case "$variant" overlay index '^BenchmarkWrittenBlobLookup$/^true$' "$repeat"
        run_case "$variant" import index-legacyimport '^BenchmarkImportPackTransactionSizes$/^packs-8$' "$repeat"
        run_case "$variant" check index-maintenance '^BenchmarkCheckWithOptions$/^synthetic-1x$/^workers=4$' "$repeat"
    done
    run_case candidate probe-overlay index '^BenchmarkPhase35M0WrittenBlobs$' "$repeat"
    run_case candidate probe-markers archiver '^BenchmarkPhase35M0Markers$' "$repeat"
    run_case candidate probe-directories crawl '^BenchmarkPhase35M0Directories$' "$repeat"
    run_case candidate probe-import index-legacyimport '^BenchmarkPhase35M0Import$' "$repeat"
    run_case candidate probe-check index-maintenance '^BenchmarkPhase35M0Check$' "$repeat"
done

for package in index archiver crawl; do
    GOMAXPROCS="$threads" TMPDIR="$root/scratch" strace -f -e trace=%file -o "$root/logs/files-$package.trace" \
        "$root/bin/candidate-$package" -test.run='^$' -test.bench='^BenchmarkPhase35M0.*$' -test.benchtime=1x -test.count=1 \
        > "$root/logs/files-$package.log" 2>&1
done
if [[ -n $(find "$root/scratch" -mindepth 1 -print -quit) ]]; then
    printf '%s\n' 'Fixture scratch was not cleaned.' >&2
    exit 1
fi
for profile in "$root"/profiles/*.cpu "$root"/profiles/*.heap; do
    go tool pprof -top -nodecount=5 "$profile" > "$profile.top" 2>&1
done
GOCACHE="${GOCACHE:-/run/vaultic-go-cache}" TMPDIR="$root/scratch" go list -deps -f '{{.Dir}}' ./cmd/vaultic > "$root/dependency-directories.txt"
while IFS= read -r directory; do
    rg -n --glob '*.go' --glob '!**/*test.go' 'MkdirTemp|CreateTemp|TempFile\(|TempDir\(|pebble.Open|\.Create\(' "$directory" || [[ $? -eq 1 ]]
done < "$root/dependency-directories.txt" > "$root/dependency-working-files.txt"
node "$repo/helpers/phase35-m0/analyze.cjs" "$root" "$repetitions"
find "$root" -type f ! -path "$root/control/*" ! -name SHA256SUMS ! -name checksums-verified.log -print0 | sort -z | xargs -0 sha256sum > "$root/SHA256SUMS"
sha256sum -c "$root/SHA256SUMS" > "$root/checksums-verified.log"
printf 'PASS: retained isolated M0 artifacts at %s\n' "$root"
