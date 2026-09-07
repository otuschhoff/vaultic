#!/bin/sh
set -eu

bin_dir=${1:-.}
work_dir=${TMPDIR:-/tmp}/vaulticdb-static-smoke-$$
mkdir -p "$work_dir"
trap 'rm -rf "$work_dir"' EXIT HUP INT TERM

VAULTICDB_NATIVE_SMOKE=1 "$bin_dir/vaulticdb"

if "$bin_dir/vaultic-key-broker" >"$work_dir/broker.log" 2>&1; then
    echo "vaultic-key-broker unexpectedly accepted an empty command line" >&2
    exit 1
fi
grep -q 'usage: vaultic-key-broker' "$work_dir/broker.log"

if "$bin_dir/vaultic-key-custodian" >"$work_dir/custodian.log" 2>&1; then
    echo "vaultic-key-custodian unexpectedly accepted an empty command line" >&2
    exit 1
fi
grep -q 'invalid vaultic-key-custodian operation or argument count' "$work_dir/custodian.log"

echo "vaulticdb static Linux runtime smoke test passed"