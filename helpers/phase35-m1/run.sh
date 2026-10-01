#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(git rev-parse --show-toplevel)
cd "$repo"
repetitions=${PHASE35_M1_REPETITIONS:-3}
[[ "$repetitions" =~ ^[0-9]+$ && "$repetitions" -ge 3 ]]
for tool in go git node strace sha256sum findmnt setpriv; do command -v "$tool" >/dev/null; done
[[ -x /usr/bin/time && $(id -u) -eq 0 ]]
root=$(mktemp -d "${PHASE35_M1_PARENT:-/tmp}/vaultic-phase35-m1-XXXXXX")
mkdir "$root/bin" "$root/logs" "$root/profiles" "$root/scratch" "$root/licenses"
printf '%s\n' "$root"
printf '%s\n' 'Disposable Phase 35 M1 fixture evidence.' > "$root/.nobackup"
git rev-parse HEAD > "$root/revision.txt"
git diff --binary HEAD > "$root/candidate.patch"
git ls-files --others --exclude-standard -z | tar --null -T - -cf "$root/untracked.tar"
go version > "$root/go-version.txt"
go env GOOS GOARCH > "$root/native-platform.txt"
printf 'GOMAXPROCS=4\nGOGC=%s\nGOMEMLIMIT=%s\nGODEBUG=%s\n' "${GOGC:-default}" "${GOMEMLIMIT:-default}" "${GODEBUG:-default}" > "$root/runtime-settings.txt"
getconf CLK_TCK > "$root/cpu-ticks-per-second.txt"
findmnt -T "$root" -o TARGET,FSTYPE,SOURCE > "$root/filesystem.txt"
export GOCACHE=${GOCACHE:-/run/vaultic-go-cache}
export TMPDIR="$root/scratch"
go list -m -json github.com/cockroachdb/pebble go.etcd.io/bbolt github.com/dgraph-io/badger/v4 github.com/dgraph-io/ristretto/v2 github.com/google/flatbuffers > "$root/module-pins.json"
for module in github.com/cockroachdb/pebble go.etcd.io/bbolt github.com/dgraph-io/badger/v4 github.com/dgraph-io/ristretto/v2 github.com/google/flatbuffers; do
    directory=$(go list -m -f '{{.Dir}}' "$module")
    name=${module//\//-}
    find "$directory" -maxdepth 1 -iname '*license*' -type f -exec cp {} "$root/licenses/$name.txt" \;
    [[ -s "$root/licenses/$name.txt" ]]
done
CGO_ENABLED=0 go list -deps -f '{{if .Module}}{{.ImportPath}} {{.Module.Path}} {{.Module.Version}}{{end}}' ./internal/workingkv > "$root/dependencies.txt"
go mod verify > "$root/module-verification.log"
go test -race ./internal/workingkv -count=1 -timeout=3m -v > "$root/logs/race.log" 2>&1
CGO_ENABLED=0 go test -c -o "$root/bin/replay.test" ./internal/workingkv
for ((repeat=1; repeat<=repetitions; repeat++)); do
    variants=(pebble bbolt badger)
    if ((repeat % 3 == 2)); then variants=(bbolt badger pebble); fi
    if ((repeat % 3 == 0)); then variants=(badger pebble bbolt); fi
    for backend in "${variants[@]}"; do
        for workload in overlay markers directories; do
            stem="$backend-$workload-$repeat"
            GOMAXPROCS=4 GODEBUG="${GODEBUG:+$GODEBUG,}gctrace=1" /usr/bin/time -v -o "$root/logs/$stem.time" \
                "$root/bin/replay.test" -test.run='^$' -test.bench="^BenchmarkReplay$/^$backend$/^$workload$" \
                -test.benchtime=1x -test.count=1 -test.v -test.cpuprofile="$root/profiles/$stem.cpu" \
                -test.memprofile="$root/profiles/$stem.heap" -test.outputdir="$root/profiles" > "$root/logs/$stem.log" 2>&1
        done
    done
done
for backend in pebble bbolt badger; do
    call=write; filename=MANIFEST-000001
    if [[ "$backend" == bbolt ]]; then call=pwrite64; filename=state.db; fi
    if [[ "$backend" == badger ]]; then filename=MANIFEST-REWRITE; fi
    fault="$root/fault-$backend"
    set +e
    PHASE35_FAULT_BACKEND="$backend" PHASE35_FAULT_ROOT="$fault" PHASE35_FAULT_MODE=enospc \
        strace -f -e trace="$call" -e inject="$call:error=ENOSPC:when=1" -P "$fault/$filename" \
        -o "$root/logs/enospc-$backend.trace" "$root/bin/replay.test" \
        -test.run='^TestFilesystemFailureProcess$' -test.count=1 > "$root/logs/enospc-$backend.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" > "$root/logs/enospc-$backend.exit"
    if [[ "$backend" != pebble ]]; then [[ "$status" -eq 0 ]]; fi
    rg -q 'ENOSPC.*INJECTED' "$root/logs/enospc-$backend.trace"
done
for backend in bbolt badger; do
    filename=state.db
    if [[ "$backend" == badger ]]; then filename=MANIFEST; fi
    fault="$root/close-$backend"
    set +e
    PHASE35_FAULT_BACKEND="$backend" PHASE35_FAULT_ROOT="$fault" PHASE35_FAULT_MODE=close \
        strace -f -e trace=close -e inject=close:error=EIO:when=1 -P "$fault/$filename" \
        -o "$root/logs/close-$backend.trace" "$root/bin/replay.test" \
        -test.run='^TestFilesystemFailureProcess$' -test.count=1 > "$root/logs/close-$backend.log" 2>&1
    status=$?
    set -e
    printf '%s\n' "$status" > "$root/logs/close-$backend.exit"
    if [[ "$backend" == bbolt ]]; then [[ "$status" -eq 0 ]]; fi
    rg -q 'EIO.*INJECTED' "$root/logs/close-$backend.trace"
done
PHASE35_GROWTH_ROOT="$root/growth-bbolt" strace -f -e trace=truncate,ftruncate,pwrite64 \
    -o "$root/logs/growth-bbolt.trace" "$root/bin/replay.test" \
    -test.run='^TestGrowthFailureProcess$' -test.count=1 -test.v > "$root/logs/growth-bbolt.log" 2>&1
public=$(mktemp -d /tmp/vaultic-phase35-m1-permission-XXXXXX)
chmod 755 "$public"
cp "$root/bin/replay.test" "$public/replay.test"
chmod 755 "$public/replay.test"
for backend in pebble bbolt badger; do
    PHASE35_FAULT_BACKEND="$backend" PHASE35_FAULT_ROOT="$root/permission-$backend" PHASE35_FAULT_MODE=permission \
        TMPDIR=/tmp setpriv --reuid=65534 --regid=65534 --clear-groups "$public/replay.test" \
        -test.run='^TestFilesystemFailureProcess$' -test.count=1 > "$root/logs/permission-$backend.log" 2>&1
done
for target in linux/amd64 linux/arm64 linux/386 linux/arm darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm64 openbsd/amd64 openbsd/arm64; do
    label=${target//\//-}
    CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go test -c -o "$root/bin/$label.test" ./internal/workingkv > "$root/logs/build-$label.log" 2>&1
    printf 'PASS %s CGO_ENABLED=0\n' "$target" >> "$root/build-matrix.txt"
done
CGO_ENABLED=0 go build -o "$root/bin/vaultic" ./cmd/vaultic > "$root/logs/build-cli.log" 2>&1
for profile in "$root"/profiles/*.cpu "$root"/profiles/*.heap; do go tool pprof -top -nodecount=8 "$profile" > "$profile.top" 2>&1; done
[[ -z $(find "$root/scratch" -mindepth 1 -print -quit) ]]
node "$repo/helpers/phase35-m1/analyze.cjs" "$root" "$repetitions"
find "$root" -type f ! -name SHA256SUMS ! -name verified.log -print0 | sort -z | xargs -0 sha256sum > "$root/SHA256SUMS"
sha256sum -c "$root/SHA256SUMS" > "$root/verified.log"
printf 'PASS retained M1 evidence: %s\n' "$root"