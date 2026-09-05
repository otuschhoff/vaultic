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
- `protoc` 36.0
- `protoc-gen-go` v1.36.10
- `protoc-gen-go-grpc` 1.5.1
- C toolchain for any generated binding checks

Install the pinned Go generators before regenerating bindings:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.10
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
export PATH="$(go env GOPATH)/bin:$PATH"
```

Install `protoc` 36.0 from the official [protobuf release](https://github.com/protocolbuffers/protobuf/releases/tag/v36.0), then regenerate and verify the checked-in Go bindings with:

```sh
./vaulticdb/generate-proto.sh
git diff --exit-code -- internal/index/proto
```

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

## Daemon environment

All daemon-specific environment variables are parsed once by `Config::from_env`.

| Variable | Default | Requirement or effect |
|---|---|---|
| `VAULTICDB_REPOSITORY_ID` | empty | Required by `publish-capsule`; scopes storage and daemon identity checks. |
| `VAULTICDB_DAEMON_ID` | `vaulticdb-dev` | Instance ID reported by health and writer-role RPCs. |
| `VAULTICDB_RUNTIME_DIR` | `/tmp/vaulticdb` | Parent for default Unix and TCP metadata paths. |
| `VAULTICDB_TRANSPORT` | `unix` | `unix` or `tcp`. |
| `VAULTICDB_SOCKET` | `<runtime>/<repository-hash>.sock` | Unix socket path. |
| `VAULTICDB_TCP_ADDR` | `127.0.0.1:50051` | TCP listen address. |
| `VAULTICDB_TCP_ALLOWLIST` | none | Required, comma-separated CIDRs when TCP is enabled. |
| `VAULTICDB_TCP_AUTH_TOKEN_FD` | none | Required non-standard inherited descriptor when TCP is enabled; consumed and closed at startup. |
| `VAULTICDB_TCP_METADATA` | `<runtime>/vaulticdb-tcp` | TCP PID, capability, and singleton-lock path base. |
| `VAULTICDB_WRITER_MINIMUM_TENURE` | `30s` | Positive duration with `ms`, `s`, `m`, or `h` suffix. |
| `VAULTICDB_WRITER_IDLE_GRACE` | disabled | Duration suffix as above; empty, `0`, or `off` disables automatic demotion. |
| `VAULTICDB_WRITER_TRANSITION_TIMEOUT` | `30s` | Positive duration with `ms`, `s`, `m`, or `h` suffix. |
| `VAULTICDB_OBJECT_STORE` | `local` | `local`, `memory`, `s3`, or `replicated`. |
| `VAULTICDB_DATA_DIR` | system temp `vaulticdb/data` | Root for local storage; repository hash is appended. |
| `VAULTICDB_S3_BUCKET` | none | Required for S3 storage. |
| `VAULTICDB_S3_PREFIX` | none | Optional non-empty shared S3 prefix. |
| `VAULTICDB_REPLICATED_REPLICAS` | none | Required comma-separated IDs for replicated storage. |
| `VAULTICDB_FENCING_REPLICA` | none | Required configured replica ID for replicated writer fencing. |
| `VAULTICDB_REPLICATED_<ID>_OBJECT_STORE` | none | Required per replica; `local`, `memory`, `s3`, or `azure`. |
| `VAULTICDB_REPLICATED_<ID>_DATA_DIR` | none | Required for a local replica. |
| `VAULTICDB_REPLICATED_<ID>_S3_BUCKET` | none | Required for an S3 replica. |
| `VAULTICDB_REPLICATED_<ID>_S3_PREFIX` | none | Optional non-empty S3 replica prefix. |
| `VAULTICDB_REPLICATED_<ID>_AZURE_ACCOUNT` | none | Required for an Azure replica. |
| `VAULTICDB_REPLICATED_<ID>_AZURE_CONTAINER` | none | Required for an Azure replica. |
| `VAULTICDB_REPLICATED_<ID>_AZURE_PREFIX` | none | Optional non-empty Azure replica prefix. |
| `VAULTICDB_REPLICATED_<ID>_AZURE_ACCESS_KEY` | none | Optional Azure access key. |
| `VAULTICDB_REPLICATED_<ID>_AZURE_BEARER_TOKEN` | none | Optional Azure bearer token. |
| `VAULTICDB_TRANSACTION_IDLE_TIMEOUT_SECS` | `300` | Integer of at least 10 seconds. |
| `VAULTICDB_METADATA_REBUILD_INITIALIZE` | `false` | Requires brokered encryption and an empty candidate metadata store. |
| `VAULTICDB_BROKER_SOCKET` | none | Enables brokered metadata-DEK acquisition. |
| `VAULTICDB_RELEASE_MANIFEST` | none | Required with `VAULTICDB_BROKER_SOCKET`. |
| `VAULTICDB_BROKER_LEASE_SECONDS` | `3600` | Broker lease lifetime in seconds. |
| `VAULTICDB_ENCRYPTION` | `off` | `off`, `required`, or `initialize`. |
| `VAULTICDB_ENCRYPTION_PASSPHRASE_FILE` | none | Private file used for local recovery unlock/initialization. |
| `VAULTICDB_ENCRYPTION_RECOVERY_ACK` | `false` | Must be `true` when policy requires explicit recovery acknowledgement. |
| `VAULTICDB_AZURE_TOKEN_FILE` | none | Private Azure Key Vault bearer-token file. |
| `VAULTICDB_GCP_TOKEN_FILE` | none | Private Google Cloud KMS bearer-token file. |
| `VAULTICDB_VAULT_TOKEN_FILE` | none | Private Vault Transit token file. |
| `VAULTICDB_PKCS11_PIN_FILE` | none | Private PKCS#11 PIN file. |
| `VAULTICDB_YUBIKEY_PIV_PIN_FILE` | none | Private YubiKey PIV PIN file. |
| `VAULTICDB_FIDO2_SECRET_FILE` | none | Private FIDO2 hmac-secret output file. |
| `VAULTICDB_NATIVE_SMOKE` | unset | Any value runs the native SlateDB smoke path and exits. |

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
