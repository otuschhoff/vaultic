# Rebuilding Static Releases

Vaultic v0.3.0 distributes static Linux executables for amd64 and arm64,
with generic and RADOS-enabled variants. It publishes no container images.
macOS development builds remain available locally, but cannot be fully static
and are not release artifacts.

The matching `vaultic-v0.3.0-rust-source.tar.gz` release asset contains the
VaulticDB, key broker, and key custodian sources, build scripts, protocol
definitions, lockfile, and vendored Rust dependencies with their license files.
It includes the LGPL-2.1-only rados-rs client at revision
`8a724626e614dbedff836d98f27c81f0ed3b65ea`. The binary archives also carry
its license and third-party notices. Vaultic's own sources are BSD-2-Clause.
The source bundle permits rebuilding and relinking these executables with
modified library sources. Reverse engineering for debugging modifications
to the LGPL-covered library is permitted.

Install Rust 1.98 or newer, the desired musl target, Zig 0.16.0,
cargo-zigbuild 0.23.2, make, a C/C++ build toolchain, CMake, and LLVM binutils.
Toolchain installation requires network access; dependency resolution from
the extracted source bundle does not. From its `vaultic-rust-source` root:

```sh
rustup target add x86_64-unknown-linux-musl
CARGO_NET_OFFLINE=true make vaulticdb-linux-amd64-rados \
    VAULTICDB_FEATURES=rados VAULTICDB_PREPARE_DEBUG=1 \
    OBJCOPY=llvm-objcopy READELF=llvm-readelf
```

For arm64, install `aarch64-unknown-linux-musl` and use
`vaulticdb-linux-arm64-rados`. For generic binaries, omit `VAULTICDB_FEATURES`
and use `vaulticdb-linux-amd64` or `vaulticdb-linux-arm64`.
The Make targets check that every Rust executable has no dynamic interpreter
or linked shared-library dependencies. Debug symbols are emitted separately.

To modify rados-rs, copy `rust-dependencies/rados-rs` to `modified-rados-rs`
and append this section to `vaulticdb/Cargo.toml`:

```toml
[patch."https://github.com/otuschhoff/rados-rs.git"]
rados-rs = { path = "../modified-rados-rs" }
```

Edit that copy, run `cargo update --manifest-path vaulticdb/Cargo.toml --offline`,
then rebuild with the same command. This avoids editing Cargo's checksummed
vendor files directly. Modified builds need not be byte-identical to the
published release.

The CLI is independently built with CGO disabled from the matching Vaultic
Git tag. Its Go sources are available in the repository's source archive.
The Rust client's release qualification is incomplete; static-link and offline
smoke checks do not establish live Ceph correctness or production readiness.