#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(git rev-parse --show-toplevel)
cd "$repo"
root=$(mktemp -d "${PHASE35_M2_PARENT:-/tmp}/vaultic-phase35-m2-XXXXXX")
mkdir "$root/bin" "$root/logs" "$root/profiles" "$root/scratch"
printf '%s\n' "$root"
printf '%s\n' 'Disposable Phase 35 M2 evidence.' > "$root/.nobackup"
git rev-parse HEAD > "$root/revision.txt"
git diff --binary HEAD > "$root/candidate.patch"
git ls-files --others --exclude-standard -z | tar --null -T - -cf "$root/untracked.tar"
go version > "$root/go-version.txt"
printf 'GOMAXPROCS=4\nGOGC=%s\nGOMEMLIMIT=%s\n' "${GOGC:-default}" "${GOMEMLIMIT:-default}" > "$root/runtime-settings.txt"
export GOCACHE=${GOCACHE:-/run/vaultic-go-cache}
export TMPDIR="$root/scratch"
go test -race ./internal/workingkv -count=1 -timeout=3m -v > "$root/logs/race.log" 2>&1
CGO_ENABLED=0 go test -c -o "$root/bin/workingkv.test" ./internal/workingkv
for repeat in 1 2 3; do
    for entries in 4096 65536 262144; do
        stem="entries-$entries-$repeat"
        GOMAXPROCS=4 GODEBUG="${GODEBUG:+$GODEBUG,}gctrace=1" /usr/bin/time -v -o "$root/logs/$stem.time" \
            "$root/bin/workingkv.test" -test.run='^$' -test.bench="^BenchmarkRAMScaling$/^entries-$entries$" \
            -test.benchtime=1x -test.count=1 -test.v -test.cpuprofile="$root/profiles/$stem.cpu" \
            -test.memprofile="$root/profiles/$stem.heap" -test.outputdir="$root/profiles" > "$root/logs/$stem.log" 2>&1
    done
done
strace -f -e trace=%file -o "$root/logs/forced-ram.trace" "$root/bin/workingkv.test" \
    -test.run='^TestRAMEncodedConsumerReplay$' -test.count=1 -test.v > "$root/logs/forced-ram.log" 2>&1
for target in linux/amd64 linux/arm64 linux/386 linux/arm darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm64 openbsd/amd64 openbsd/arm64; do
    label=${target//\//-}
    CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go test -c -o "$root/bin/$label.test" ./internal/workingkv > "$root/logs/build-$label.log" 2>&1
    printf 'PASS %s CGO_ENABLED=0\n' "$target" >> "$root/build-matrix.txt"
done
"$root/bin/linux-386.test" -test.run='^(TestRAM.*|TestConformance)$' -test.count=1 -test.timeout=2m -test.v > "$root/logs/native-386.log" 2>&1
CGO_ENABLED=0 go build -o "$root/bin/vaultic" ./cmd/vaultic > "$root/logs/build-cli.log" 2>&1
for profile in "$root"/profiles/*.cpu "$root"/profiles/*.heap; do go tool pprof -top -nodecount=8 "$profile" > "$profile.top" 2>&1; done
node "$repo/helpers/phase35-m2/analyze.cjs" "$root"
find "$root" -type f ! -name SHA256SUMS ! -name verified.log -print0 | sort -z | xargs -0 sha256sum > "$root/SHA256SUMS"
sha256sum -c "$root/SHA256SUMS" > "$root/verified.log"
printf 'PASS retained M2 evidence: %s\n' "$root"