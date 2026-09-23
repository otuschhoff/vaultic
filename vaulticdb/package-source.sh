#!/bin/sh

set -eu

if [ "$#" -ne 1 ]; then
    echo "usage: $0 <output.tar.gz>" >&2
    exit 2
fi

output_dir=$(cd "$(dirname "$1")" && pwd)
output="$output_dir/$(basename "$1")"
root=$(git rev-parse --show-toplevel)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT HUP INT TERM
bundle="$work/vaultic-rust-source"
mkdir -p "$bundle/.cargo"
cd "$root"
git ls-files --cached --others --exclude-standard -z -- vaulticdb LICENSE VERSION Makefile |
    tar --null -T - -cf - | tar -C "$bundle" -xf -
cd "$bundle"
cargo vendor --manifest-path vaulticdb/Cargo.toml --locked rust-dependencies > .cargo/config.toml
cargo metadata --manifest-path vaulticdb/Cargo.toml --locked --offline --features rados --format-version 1 > /dev/null
tar -C "$work" -czf "$output" vaultic-rust-source