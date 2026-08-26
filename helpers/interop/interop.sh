#!/usr/bin/env bash
#
# interop.sh - interoperability test harness for vaultic, restic and rustic.
#
# All three tools implement the same repository format. This script verifies
# that a repository written by one client can be read and modified by the
# others. It runs a matrix of writer x reader scenarios on local
# repositories:
#
#   writer: init, backup (two snapshots)
#   reader: snapshots, ls, restore latest + diff against source, check
#   pruner: forget --keep-last 1, prune, then vaultic check + restore verify
#
# Usage: interop.sh [--keep] [--clients "vaultic restic rustic"] [--skip-download]
#
# Environment:
#   RESTIC_VERSION    restic release to download (default: 0.19.1)
#   RUSTIC_VERSION    rustic release to download (default: 0.11.4)
#   INTEROP_PASSWORD  repository password        (default: interop-test-secret)
#   INTEROP_WORKDIR   working directory          (default: fresh mktemp dir)
#   BINDIR            override directory containing/preceiving binaries
#
# The vaultic binary is always built from the current checkout.

set -uo pipefail

RESTIC_VERSION="${RESTIC_VERSION:-0.19.1}"
RUSTIC_VERSION="${RUSTIC_VERSION:-0.11.4}"
INTEROP_PASSWORD="${INTEROP_PASSWORD:-interop-test-secret}"

KEEP=0
SKIP_DOWNLOAD=0
CLIENTS="vaultic restic rustic"

while [ $# -gt 0 ]; do
	case "$1" in
	--keep) KEEP=1 ;;
	--skip-download) SKIP_DOWNLOAD=1 ;;
	--clients)
		CLIENTS="$2"
		shift
		;;
	*)
		echo "unknown argument: $1" >&2
		exit 2
		;;
	esac
	shift
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

WORKDIR="${INTEROP_WORKDIR:-$(mktemp -d /tmp/vaultic-interop.XXXXXX)}"
BINDIR="${BINDIR:-$WORKDIR/bin}"
LOGDIR="$WORKDIR/logs"
mkdir -p "$BINDIR" "$LOGDIR"

VAULTIC_BIN="$BINDIR/vaultic"
RESTIC_BIN="$BINDIR/restic"
RUSTIC_BIN="$BINDIR/rustic"

CACHE_DIR="${XDG_CACHE_HOME:-$HOME/.cache}/vaultic-interop"

log() { printf '%s %s\n' "$(date +%H:%M:%S)" "$*"; }

STEPS=0
FAILED=0

# step <logfile> <description> <command...>
step() {
	local logfile="$1" desc="$2"
	shift 2
	STEPS=$((STEPS + 1))
	if "$@" >>"$logfile" 2>&1; then
		log "ok   - $desc"
	else
		log "FAIL - $desc (see $logfile)"
		FAILED=$((FAILED + 1))
	fi
}

# --- client adapters -------------------------------------------------------
# All invocations pass the repository and password via the environment so
# that the flags stay identical across clients wherever possible.

# run <client> <args...>
run() {
	local client="$1"
	shift
	case "$client" in
	vaultic)
		VAULTIC_REPOSITORY="$REPO" VAULTIC_PASSWORD="$INTEROP_PASSWORD" "$VAULTIC_BIN" "$@"
		;;
	restic)
		RESTIC_REPOSITORY="$REPO" RESTIC_PASSWORD="$INTEROP_PASSWORD" "$RESTIC_BIN" "$@"
		;;
	rustic)
		RUSTIC_REPOSITORY="$REPO" RUSTIC_PASSWORD="$INTEROP_PASSWORD" "$RUSTIC_BIN" "$@"
		;;
	*)
		echo "unknown client: $client" >&2
		return 2
		;;
	esac
}

# restore_latest <client> <target-dir>
restore_latest() {
	local client="$1" target="$2"
	case "$client" in
	rustic)
		# rustic takes the target as a positional argument
		run "$client" restore latest "$target"
		;;
	*)
		run "$client" restore latest --target "$target"
		;;
	esac
}

# --- setup -----------------------------------------------------------------

detect_platform() {
	OS="$(uname -s | tr '[:upper:]' '[:lower:]')"   # linux, darwin
	ARCH="$(uname -m)"                               # x86_64, arm64, aarch64
	[ "$ARCH" = "arm64" ] && ARCH="aarch64"
}

# download_restic <dest>
download_restic() {
	local dest="$1" goarch
	case "$ARCH" in aarch64) goarch="arm64" ;; *) goarch="$ARCH" ;; esac
	local name="restic_${RESTIC_VERSION}_${OS}_${goarch}"
	local url="https://github.com/restic/restic/releases/download/v${RESTIC_VERSION}/${name}.bz2"
	log "downloading $url"
	mkdir -p "$CACHE_DIR"
	if [ ! -f "$CACHE_DIR/$name" ]; then
		curl -fL -sS "$url" | bunzip2 >"$CACHE_DIR/$name" || return 1
	fi
	cp "$CACHE_DIR/$name" "$dest"
	chmod +x "$dest"
}

# download_rustic <dest>
download_rustic() {
	local dest="$1" triple
	case "${OS}_${ARCH}" in
	linux_x86_64) triple="x86_64-unknown-linux-gnu" ;;
	linux_aarch64) triple="aarch64-unknown-linux-gnu" ;;
	darwin_x86_64) triple="x86_64-apple-darwin" ;;
	darwin_aarch64) triple="aarch64-apple-darwin" ;;
	*)
		echo "unsupported platform for rustic download: ${OS}/${ARCH}" >&2
		return 1
		;;
	esac
	local name="rustic-v${RUSTIC_VERSION}-${triple}"
	local url="https://github.com/rustic-rs/rustic/releases/download/v${RUSTIC_VERSION}/${name}.tar.gz"
	log "downloading $url"
	mkdir -p "$CACHE_DIR"
	if [ ! -f "$CACHE_DIR/$name/rustic" ]; then
		rm -rf "${CACHE_DIR:?}/$name"
		mkdir -p "$CACHE_DIR/$name"
		curl -fL -sS "$url" | tar -xz -C "$CACHE_DIR/$name" || return 1
	fi
	# the tarball layout may be flat or contain a top-level directory
	local bin
	bin="$(find "$CACHE_DIR/$name" -type f -name rustic | head -1)"
	[ -n "$bin" ] || return 1
	cp "$bin" "$dest"
	chmod +x "$dest"
}

setup_binaries() {
	log "building vaultic from $REPO_ROOT"
	(cd "$REPO_ROOT" && CGO_ENABLED=0 go build -o "$VAULTIC_BIN" ./cmd/vaultic) || exit 1

	if [ "$SKIP_DOWNLOAD" = "1" ]; then
		RESTIC_BIN="$(command -v restic || true)"
		RUSTIC_BIN="$(command -v rustic || true)"
	fi
	for client in $CLIENTS; do
		case "$client" in
		restic)
			[ -x "$RESTIC_BIN" ] || download_restic "$RESTIC_BIN" || exit 1
			;;
		rustic)
			[ -x "$RUSTIC_BIN" ] || download_rustic "$RUSTIC_BIN" || exit 1
			;;
		esac
	done

	log "client versions:"
	for client in $CLIENTS; do
		case "$client" in
		vaultic) "$VAULTIC_BIN" version ;;
		restic) "$RESTIC_BIN" version ;;
		rustic) "$RUSTIC_BIN" --version ;;
		esac
	done
}

# --- fixtures ---------------------------------------------------------------

SRC=""
create_fixtures() {
	# canonicalize: rustic resolves symlinks in backup paths while
	# restic/vaultic store them as given, so $SRC must be the physical path
	# (e.g. /tmp -> /private/tmp on macOS) for restore comparisons to line up.
	rm -rf "$WORKDIR/src"
	mkdir -p "$WORKDIR/src"
	SRC="$(cd "$WORKDIR/src" && pwd -P)"
	mkdir -p "$SRC/sub/deep"
	echo "interop fixture" >"$SRC/file1.txt"
	dd if=/dev/urandom of="$SRC/sub/blob.bin" bs=1024 count=64 2>/dev/null
	echo "deep data" >"$SRC/sub/deep/file2.txt"
	ln -sf "file1.txt" "$SRC/link1"
}

mutate_fixtures() {
	echo "changed content" >"$SRC/file1.txt"
	echo "new file" >"$SRC/newfile.txt"
}

# --- scenarios ---------------------------------------------------------------

# scenario_writer <client> <repo>
scenario_writer() {
	local client="$1"
	REPO="$2"
	local logf="$LOGDIR/writer-$client.log"
	log "=== writer: $client (repo: $REPO) ==="
	create_fixtures
	step "$logf" "$client init" run "$client" init
	step "$logf" "$client backup (snapshot 1)" run "$client" backup "$SRC"
	mutate_fixtures
	step "$logf" "$client backup (snapshot 2)" run "$client" backup "$SRC"
	# keep the final fixture state for later restore comparisons; the next
	# writer leg regenerates $SRC (with fresh random content)
	rm -rf "$WORKDIR/src-$client"
	cp -a "$SRC" "$WORKDIR/src-$client"
}

# scenario_reader <client> <writer>
scenario_reader() {
	local client="$1" writer="$2"
	REPO="$WORKDIR/repos/$writer"
	local logf="$LOGDIR/reader-$client-on-$writer.log"
	log "=== reader: $client on repo written by $writer ==="
	STEPS=$((STEPS + 1))
	if run "$client" snapshots >>"$logf" 2>&1 && run "$client" snapshots 2>/dev/null | grep -F "$SRC" >>"$logf" 2>&1; then
		log "ok   - $client snapshots lists backup path"
	else
		log "FAIL - $client snapshots lists backup path (see $logf)"
		FAILED=$((FAILED + 1))
	fi
	step "$logf" "$client ls latest" run "$client" ls latest
	local target="$WORKDIR/restore-$client-on-$writer"
	rm -rf "$target"
	step "$logf" "$client restore latest" restore_latest "$client" "$target"
	step "$logf" "restored data matches source ($client on $writer)" \
		diff -r "$WORKDIR/src-$writer" "$target$SRC"
	step "$logf" "$client check" run "$client" check
}

# scenario_prune <client> <writer>
scenario_prune() {
	local client="$1" writer="$2"
	REPO="$WORKDIR/prune-$client-on-$writer"
	rm -rf "$REPO"
	cp -a "$WORKDIR/repos/$writer" "$REPO"
	local logf="$LOGDIR/prune-$client-on-$writer.log"
	log "=== prune: $client on copy of repo written by $writer ==="
	step "$logf" "$client forget --keep-last 1" run "$client" forget --keep-last 1
	step "$logf" "$client prune" run "$client" prune
	# use vaultic as the reference implementation to verify the result
	step "$logf" "vaultic check after $client prune" run vaultic check
	local target="$WORKDIR/restore-prune-$client-on-$writer"
	rm -rf "$target"
	step "$logf" "vaultic restore latest after $client prune" \
		restore_latest vaultic "$target"
	step "$logf" "restored data matches source after $client prune" \
		diff -r "$WORKDIR/src-$writer" "$target$SRC"
}

# --- main -------------------------------------------------------------------

main() {
	detect_platform
	log "workdir: $WORKDIR"
	log "clients: $CLIENTS"
	setup_binaries

	for writer in $CLIENTS; do
		scenario_writer "$writer" "$WORKDIR/repos/$writer"
	done
	for writer in $CLIENTS; do
		for client in $CLIENTS; do
			scenario_reader "$client" "$writer"
		done
	done
	for writer in $CLIENTS; do
		for client in $CLIENTS; do
			scenario_prune "$client" "$writer"
		done
	done

	log "----------------------------------------"
	log "steps: $STEPS, failed: $FAILED"
	log "logs:  $LOGDIR"
	if [ "$KEEP" = "1" ]; then
		log "keeping workdir: $WORKDIR"
	elif [ "$FAILED" = "0" ]; then
		rm -rf "$WORKDIR"
	else
		log "keeping workdir for debugging: $WORKDIR"
	fi
	[ "$FAILED" = "0" ]
}

main
