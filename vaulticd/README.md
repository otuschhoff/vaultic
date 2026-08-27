# vaulticd

`vaulticd` is the Rust process boundary for Vaultic's optional SlateDB metadata engine.
Phase 0 provides the versioned protobuf health/capability service and deterministic
build contract. SlateDB data operations are intentionally not enabled until the
schema and engine adapter phases are complete.

## Local development

Prerequisites:

- Rust toolchain compatible with the pinned SlateDB revision
- `protoc`
- C toolchain for any generated binding checks

Run the daemon on the default Unix socket:

```sh
cargo run --manifest-path vaulticd/Cargo.toml
```

Run the native SlateDB binding smoke test:

```sh
VAULTICD_NATIVE_SMOKE=1 cargo run --manifest-path vaulticd/Cargo.toml
```

The Phase 0 service reports protocol `vaulticd.v1`, schema `0`, and does not
expose TCP unless `VAULTICD_TRANSPORT=tcp` and a non-empty
`VAULTICD_TCP_ALLOWLIST` are provided. TCP authentication is intentionally a
follow-up before enabling remote use.

## Static Linux build

```sh
./vaulticd/build-musl.sh
```

The script requires `x86_64-unknown-linux-musl` and writes the binary under
`dist/vaulticd/linux-amd64/`. It records the Rust, SlateDB, and target metadata
beside the binary.
