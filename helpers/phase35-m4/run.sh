#!/usr/bin/env bash
set -euo pipefail
umask 077
repo=$(git rev-parse --show-toplevel)
cd "$repo"
root=$(mktemp -d "${PHASE35_M4_PARENT:-/tmp}/vaultic-phase35-m4-XXXXXX")
mkdir "$root/bin" "$root/logs" "$root/scratch"
printf '%s\n' "$root"
printf '%s\n' 'Disposable Phase 35 M4 evidence; no production trial.' > "$root/.nobackup"
git rev-parse HEAD > "$root/revision.txt"
git diff --binary HEAD > "$root/candidate.patch"
git ls-files --others --exclude-standard -z | tar --null -T - -cf "$root/untracked.tar"
git diff --name-only -z HEAD -- '*.go' go.mod go.sum | xargs -0 sha256sum > "$root/runtime-source.sha256"
git ls-files --others --exclude-standard -z -- '*.go' | xargs -0 sha256sum >> "$root/runtime-source.sha256"
go version > "$root/go-version.txt"
go list -m all > "$root/modules.txt"
export GOCACHE=${GOCACHE:-/run/vaultic-go-cache}
export TMPDIR="$root/scratch"
export GOMAXPROCS=4
export VAULTICDB_TEST_BINARY=${VAULTICDB_TEST_BINARY:-/run/vaultic-diagnostics/cargo-target/debug/vaulticdb}
sha256sum "$VAULTICDB_TEST_BINARY" > "$root/fixture-daemon.sha256"
umask 022
go test -race -json ./internal/workingkv ./internal/workingstate ./internal/telemetry ./internal/crawl ./internal/index \
    ./internal/index/maintenance ./internal/index/legacyimport ./internal/repository/index ./internal/configfile ./internal/global \
    ./cmd/vaultic/backupcmd ./cmd/vaultic/indexcmd -count=1 -timeout=8m > "$root/logs/race.jsonl" 2> "$root/logs/race.stderr"
go test -race -json ./cmd/vaultic -run '^(TestM4.*|Test.*Profile.*|TestIndex.*|TestCheck.*)$' \
    -count=1 -timeout=8m > "$root/logs/cli.jsonl" 2>&1
go test -race -json ./internal/index/daemon -run '^(TestFreshImport.*|TestIDSeenFilter.*|TestM[34].*|TestClientCanceledBegin.*|TestSchemaStorePreparedCommit.*|TestSchemaStorePackPublication.*|TestClientOwnedBegin.*)$' \
    -count=1 -timeout=5m > "$root/logs/daemon.jsonl" 2>&1
go test -race -json ./internal/repository -run '^(TestCachedAuthoritativeLookupWithPublishedOverlay|Test.*LoadRaw.*)$' \
    -count=1 -timeout=3m > "$root/logs/native.jsonl" 2>&1
go test -race -c -o "$root/bin/archiver-race.test" ./internal/archiver
chown 65534:65534 "$root" "$root/bin" "$root/scratch"
pushd "$root/scratch" > /dev/null
setpriv --reuid=65534 --regid=65534 --clear-groups "$root/bin/archiver-race.test" \
    -test.count=1 -test.timeout=8m -test.v > "$root/logs/archiver.log" 2>&1
popd > /dev/null
for package in workingkv index crawl archiver index/maintenance index/legacyimport; do
    label=${package//\//-}
    CGO_ENABLED=0 go test -c -o "$root/bin/$label.test" "./internal/$package"
    for mode in ram kv auto; do
        strace -f -e trace=%file -o "$root/logs/$label-$mode.trace" "$root/bin/$label.test" \
            -test.run="^TestM3.*$/^$mode$" -test.count=1 -test.timeout=3m -test.v > "$root/logs/$label-$mode.log" 2>&1
    done
done
strace -f -e trace=%file -o "$root/logs/migration.trace" "$root/bin/workingkv.test" \
    -test.run='^TestM4ConcurrentMigration$' -test.count=1 -test.timeout=3m -test.v > "$root/logs/migration.log" 2>&1
unit="vaultic-phase35-m4-$(basename "$root")"
systemd-run --unit="$unit" --collect --wait --pipe -p MemoryMax=512M -p MemorySwapMax=0 \
    /usr/bin/env VAULTIC_PHASE35_M4_CGROUP=1 "$root/bin/workingkv.test" \
    -test.run='^TestM4IsolatedCgroupPressure$' -test.count=1 -test.timeout=1m -test.v > "$root/logs/cgroup.log" 2>&1
baseline=$(mktemp -d /run/vaultic-phase35-m4-baseline-XXXXXX)
git archive HEAD | tar -x -C "$baseline"
for target in linux/amd64 linux/arm64 linux/386 linux/arm darwin/amd64 darwin/arm64 windows/amd64 windows/arm64 freebsd/amd64 freebsd/arm64 openbsd/amd64 openbsd/arm64; do
    label=${target//\//-}
    CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go test -c -o "$root/bin/workingkv-$label.test" ./internal/workingkv > "$root/logs/core-$label.log" 2>&1
    printf 'PASS %s CGO_ENABLED=0\n' "$target" >> "$root/core-matrix.txt"
    if CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go build -ldflags='-s -w' -o "$root/bin/vaultic-$label" ./cmd/vaultic > "$root/logs/build-$label.log" 2>&1; then
        printf 'PASS %s CGO_ENABLED=0\n' "$target" >> "$root/build-matrix.txt"
    elif [[ "$target" == linux/386 || "$target" == linux/arm ]]; then
        rg -q 'MaxPackSize.*overflows|overflows.*MaxPackSize' "$root/logs/build-$label.log"
        pushd "$baseline" > /dev/null
        if CGO_ENABLED=0 GOOS=${target%/*} GOARCH=${target#*/} go build -o "$root/bin/baseline-$label" ./cmd/vaultic > "$root/logs/baseline-$label.log" 2>&1; then exit 1; fi
        popd > /dev/null
        rg -q 'MaxPackSize.*overflows|overflows.*MaxPackSize' "$root/logs/baseline-$label.log"
        printf 'BASELINE_BLOCKED %s MaxPackSize_overflow\n' "$target" >> "$root/build-matrix.txt"
    else
        exit 1
    fi
done
"$root/bin/workingkv-linux-386.test" -test.run='^TestM[34].*$' -test.count=1 -test.timeout=2m -test.v > "$root/logs/native-386.log" 2>&1
sha256sum -c "$root/runtime-source.sha256" > "$root/source-verified.log"
node "$repo/helpers/phase35-m4/analyze.cjs" "$root"
find "$root" -type f ! -name SHA256SUMS ! -name verified.log -print0 | sort -z | xargs -0 sha256sum > "$root/SHA256SUMS"
sha256sum -c "$root/SHA256SUMS" > "$root/verified.log"
printf 'PASS retained M4 evidence: %s\n' "$root"