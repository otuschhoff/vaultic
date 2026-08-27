#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
target=${VAULTICDB_MUSL_TARGET:-x86_64-unknown-linux-musl}
out_dir=${VAULTICDB_OUTPUT_DIR:-"$repo_root/dist/vaulticdb/linux-amd64"}
toolchain=${VAULTICDB_RUST_TOOLCHAIN:-stable}

command -v rustup >/dev/null 2>&1 || {
    echo "vaulticdb: rustup is required for the native build" >&2
    exit 1
}

rustup run "$toolchain" cargo --version >/dev/null 2>&1 || {
    echo "vaulticdb: Rust toolchain $toolchain with cargo is required" >&2
    exit 1
}

command -v cargo-zigbuild >/dev/null 2>&1 || {
    echo "vaulticdb: cargo-zigbuild is required for the musl build" >&2
    echo "install it with: cargo install cargo-zigbuild" >&2
    exit 1
}

if ! rustup target list --installed 2>/dev/null | grep -qx "$target"; then
    echo "vaulticdb: Rust target $target is not installed" >&2
    echo "install it with: rustup target add $target" >&2
    exit 1
fi

mkdir -p "$out_dir"
CARGO_NET_OFFLINE=${CARGO_NET_OFFLINE:-false} \
    RUSTUP_TOOLCHAIN="$toolchain" cargo-zigbuild zigbuild --manifest-path "$repo_root/vaulticdb/Cargo.toml" \
    --target "$target" --release

cp "$repo_root/vaulticdb/target/$target/release/vaulticdb" "$out_dir/vaulticdb"
{
    printf 'target=%s\n' "$target"
    printf 'slatedb_revision=%s\n' 'ae07acd4498068d1b9ba799cc9f6c9824e6f6251'
    printf 'rustc=%s\n' "$(rustup run "$toolchain" rustc --version)"
    printf 'cargo=%s\n' "$(rustup run "$toolchain" cargo --version)"
} > "$out_dir/build-metadata.txt"
