#!/bin/sh
# Fails with an actionable message if the given Rust target triple is not
# installed for the given toolchain, instead of a cryptic cargo error.
set -eu

target=$1
toolchain=$2

command -v rustup >/dev/null 2>&1 || {
    echo "vaulticdb: rustup is required to cross-compile for $target" >&2
    exit 1
}

if ! rustup target list --toolchain "$toolchain" --installed 2>/dev/null | grep -qx "$target"; then
    echo "vaulticdb: Rust target $target is not installed for toolchain $toolchain" >&2
    echo "install it with: rustup target add --toolchain $toolchain $target" >&2
    exit 1
fi
