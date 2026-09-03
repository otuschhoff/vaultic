# vaulticdb

`vaulticdb` is the Rust process boundary for Vaultic's optional SlateDB metadata engine.
It provides the versioned protobuf lifecycle service plus bounded point reads,
multi-get, prefix scans, durable write batches, and serializable transactions.
Vaultic owns the binary record schema under `internal/index/schema`; the daemon
stores opaque keys and values and owns SlateDB durability. Vaultic's Go schema
adapter validates record families and immutability before writing them.

## Local development

Prerequisites:

- Rust toolchain compatible with the pinned SlateDB revision
- `protoc`
- C toolchain for any generated binding checks

Run the daemon on the default Unix socket:

```sh
cargo run --manifest-path vaulticdb/Cargo.toml
```

Local object storage is the default. Data is stored below
`$VAULTICDB_DATA_DIR/<repository-hash>/`, or below the system temporary
directory when `VAULTICDB_DATA_DIR` is unset. For an S3-compatible store, set:

```sh
export VAULTICDB_OBJECT_STORE=s3
export VAULTICDB_S3_BUCKET=metadata
export VAULTICDB_S3_PREFIX=repositories/example
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_DEFAULT_REGION=us-east-1
# For S3-compatible services:
export AWS_ENDPOINT_URL_S3=https://s3.example.invalid
```

The daemon appends the repository identity hash to `VAULTICDB_S3_PREFIX`, so
the configured prefix is a shared namespace root rather than a complete
database path.

For synchronous multi-provider metadata replication, set
`VAULTICDB_OBJECT_STORE=replicated` and list replica IDs in
`VAULTICDB_REPLICATED_REPLICAS`. Each replica uses the same object-store
settings with a `VAULTICDB_REPLICATED_<ID>_` prefix; non-alphanumeric
characters in the ID are written as underscores. The first replica is the
primary read/list target, and reads fail over to later replicas when a replica
returns an error. Writes, copies, deletes, and multipart completions must
succeed on every replica before the operation is acknowledged.

```sh
export VAULTICDB_OBJECT_STORE=replicated
export VAULTICDB_REPLICATED_REPLICAS=aws,local2
export VAULTICDB_REPLICATED_AWS_OBJECT_STORE=s3
export VAULTICDB_REPLICATED_AWS_S3_BUCKET=metadata-a
export VAULTICDB_REPLICATED_AWS_S3_PREFIX=repositories/example
export VAULTICDB_REPLICATED_LOCAL2_OBJECT_STORE=local
export VAULTICDB_REPLICATED_LOCAL2_DATA_DIR=/srv/vaulticdb-secondary
```

Replica object stores may be `local`, `s3`, or `azure`. Azure replicas use
`VAULTICDB_REPLICATED_<ID>_AZURE_ACCOUNT`,
`VAULTICDB_REPLICATED_<ID>_AZURE_CONTAINER`, optional
`VAULTICDB_REPLICATED_<ID>_AZURE_PREFIX`, and either
`VAULTICDB_REPLICATED_<ID>_AZURE_ACCESS_KEY` or
`VAULTICDB_REPLICATED_<ID>_AZURE_BEARER_TOKEN`.

`VAULTICDB_OBJECT_STORE=memory` is available only for isolated development and
tests. It is never selected as a fallback for a failed local or S3 open.

Run the native SlateDB binding smoke test:

```sh
VAULTICDB_NATIVE_SMOKE=1 cargo run --manifest-path vaulticdb/Cargo.toml
```

The service reports protocol `vaulticdb.v1`, schema `0`, and does not expose TCP
unless `VAULTICDB_TRANSPORT=tcp`, a non-empty `VAULTICDB_TCP_ALLOWLIST`, and
`VAULTICDB_TCP_AUTH_TOKEN_FD` names a non-standard inherited descriptor
containing a non-empty bearer token. The daemon consumes and closes that
descriptor during startup and removes its number from the environment. Vaultic
opens the descriptor from the protected `--daemon-auth-token-file`; it never
places the token in either process's arguments or environment. Storage calls
are rejected after drain. Commits and writes requesting durability return only
after SlateDB's durability handle completes.

Abandoned transactions are reclaimed after five minutes of inactivity before
the active-transaction limit is enforced. Set
`VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS` to an integer of at least 10 seconds
to override that interval. In-flight transaction operations are never pruned.

Run the Phase 3 integration tests against a pre-created S3-compatible bucket:

```sh
VAULTICDB_TEST_S3_ENDPOINT="$AWS_ENDPOINT_URL_S3" \
VAULTICDB_TEST_S3_BUCKET=metadata \
go test ./internal/index/daemon -run TestS3CompatibleStorageRoundTrip
```

## Static Linux build

```sh
./vaulticdb/build-musl.sh
```

The script requires `x86_64-unknown-linux-musl` and writes the binary under
`dist/vaulticdb/linux-amd64/`. It records the Rust, SlateDB, and target metadata
beside the binary.
