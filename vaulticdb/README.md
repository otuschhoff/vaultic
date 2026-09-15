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
- `protoc-gen-go` v1.36.12
- `protoc-gen-go-grpc` 1.6.2
- C toolchain for any generated binding checks

Install the pinned Go generators before regenerating bindings:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.6.2
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

Replica object stores may be `local`, `s3`, `azure`, or native `rados`. RADOS
replicas are configured by sealed topology capsules and require a build with
the `rados` feature; see `../doc/046_native_rados.rst` for the platform,
credential, and operational requirements. Azure replicas use
`VAULTICDB_REPLICATED_<ID>_AZURE_ACCOUNT`,
`VAULTICDB_REPLICATED_<ID>_AZURE_CONTAINER`, optional
`VAULTICDB_REPLICATED_<ID>_AZURE_PREFIX`, and either
`VAULTICDB_REPLICATED_<ID>_AZURE_ACCESS_KEY` or
`VAULTICDB_REPLICATED_<ID>_AZURE_BEARER_TOKEN`.

`VAULTICDB_OBJECT_STORE=memory` is available only for isolated development and
tests. It is never selected as a fallback for a failed local or S3 open.

## SlateDB read-cache tiers

Set `VAULTICDB_READ_CACHE_TIERS` to a comma-separated ordered list of tier IDs.
An unset or empty list disables the cache. IDs may contain ASCII letters,
digits, `-`, and `_`; configuration variable names uppercase IDs and replace
`-` with `_`, so IDs must remain distinct after that conversion.

Each tier requires `VAULTICDB_READ_CACHE_<ID>_OBJECT_STORE` (`local`, `memory`,
`s3`, or `rados`) and `VAULTICDB_READ_CACHE_<ID>_MAX_BYTES`. `memory` is for
tests. Local tiers require `_DATA_DIR`; S3 tiers require `_S3_BUCKET` and accept
`_S3_PREFIX`, `_S3_ENDPOINT`, `_S3_REGION`, `_S3_PROVIDER`,
`_S3_BUCKET_LOOKUP`, `_S3_ACCESS_KEY_ID`, and `_S3_SECRET_ACCESS_KEY`. Cache
credentials are currently static. `_S3_SESSION_TOKEN` is rejected because this
cache path has no renewal broker; credential denial opens the tier circuit and
origin reads continue. RADOS tiers require `_RADOS_MONITORS`,
`_RADOS_CLUSTER_FSID`, `_RADOS_POOL`, `_RADOS_NAMESPACE`, `_RADOS_PREFIX`,
`_RADOS_CLIENT`, and `_RADOS_KEY`. Azure and GCS are not supported for Phase 29
cache tiers.

Each tier also accepts `_CONFIDENTIALITY=encrypted|decrypted`; the default is
`encrypted`. An encrypted tier stores raw authoritative ciphertext below the
metadata envelope-encryption layer. It is rejected when metadata encryption is
off because the cache does not provide independent encryption and must not
misrepresent plaintext as encrypted. A decrypted tier stores logical plaintext
above envelope encryption and requires
`VAULTICDB_READ_CACHE_<ID>_ACKNOWLEDGE_PLAINTEXT=true` at startup. This setting
is per tier, so encrypted and decrypted tiers may be mixed in one configuration.

**Decrypted cache tiers are only for highly trusted backends.** Cached bytes
remain readable to anyone with backend access, including while the broker or
repository is locked. Backups, snapshots, replication, or object versioning may
retain plaintext after eviction; secure erase is not promised. Backend
encryption at rest, TLS, and CephX protect different boundaries and do not
substitute for trusting every principal and system with cache-backend access.

Per-tier policy settings are `_ENABLED` (default `true`), `_IDLE_AGE` and
`_ABSOLUTE_AGE` (duration with `ms`, `s`, `m`, or `h`; default disabled),
`_READ_PRIORITY` and `_ADMISSION_PRIORITY` (default `100`, lower is preferred),
and `_TIMEOUT` (default `250ms`). Global settings are
`VAULTICDB_READ_CACHE_PART_SIZE_BYTES` (default 4 MiB),
`VAULTICDB_READ_CACHE_MAX_INFLIGHT_BYTES` (default eight parts), and optional
`VAULTICDB_READ_CACHE_AGGREGATE_MAX_BYTES`.

The configured part size is the largest admitted response in a tier's byte
domain: ciphertext for encrypted tiers and plaintext for decrypted tiers.
Larger responses still succeed from the authority but bypass caching. Fill and
promotion writes are bounded best-effort background work; cache hits update only
generation-aware in-memory recency and never persist access metadata. Cache
timeouts and write failures do not turn an origin-successful read into a failure.

`CacheStatus` reports policy revisions and synchronization errors, namespace,
shared and process-local global/per-tier accounting, metrics, circuit state,
and reconciliation lag. `UpdateCachePolicy` applies complete per-tier policies
with `expected_revision` compare-and-swap. The policy-document CAS is the commit
point; a lost response is resolved by reading back the exact document, and a
rollback is another CAS revision. Quota-ledger synchronization after commit is
asynchronous and observable. Heartbeats propagate changes to hit-only managers.
An unavailable or malformed policy disables every cache tier while origin reads
continue, and heartbeat recovery restores a complete validated snapshot.

Policy updates are authenticated operational mutations, are rejected while
draining, and do not require the writer role. Backend configuration and
credentials are never returned or accepted by these RPCs. Status reports each
tier's effective confidentiality, but the policy update message cannot change
it; confidentiality is startup topology. Mode is included in cache namespaces
and entry identities, so existing bytes are never reinterpreted after a mode
change. One manager owns both byte-domain views, while a persisted fenced CAS
ledger coordinates entry generations, deletion state, leases, reservations,
and aggregate capacity across all managers sharing the namespace. WAL,
manifests, fencing, coordination, policy, and untagged object-store operations
remain outside both cache layers.

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
| `VAULTICDB_RUNTIME_DIR` | `$XDG_RUNTIME_DIR/vaulticdb`, otherwise `/tmp/vaulticdb-<uid>` | Parent for default Unix and TCP metadata paths. Existing directories must be owned by the daemon user with mode `0700`. Managed services should set this to a service-owned directory under `/run`. |
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
| `VAULTICDB_WAL_STORE` | `inherit` | `inherit`, `local`, `memory` (tests only), `s3`, or `rados`; a separate target never falls back to metadata storage. |
| `VAULTICDB_WAL_DATA_DIR` | system temp `vaulticdb/wal` | Root for local WAL; repository hash is appended. |
| `VAULTICDB_WAL_S3_BUCKET` | none | Required for S3 WAL storage. |
| `VAULTICDB_WAL_S3_PREFIX` | none | Optional dedicated WAL namespace root. |
| `VAULTICDB_WAL_S3_ENDPOINT` | provider default | Optional S3-compatible endpoint. |
| `VAULTICDB_WAL_S3_REGION` | provider default | Optional signing region. |
| `VAULTICDB_WAL_S3_PROVIDER` | `generic` | Optional S3 provider profile. |
| `VAULTICDB_WAL_S3_BUCKET_LOOKUP` | provider default | `auto`, `dns`, or `path`. |
| `VAULTICDB_WAL_S3_ACCESS_KEY_ID` | none | Dedicated external-topology WAL access key; prefer broker leases in production. |
| `VAULTICDB_WAL_S3_SECRET_ACCESS_KEY` | none | Secret half of the dedicated WAL credential. |
| `VAULTICDB_WAL_S3_SESSION_TOKEN` | none | Optional temporary WAL credential token. |
| `VAULTICDB_WAL_RADOS_*` | none | Monitor, FSID, pool, namespace, prefix, client, and key for external-topology native RADOS WAL. |
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
| `VAULTICDB_SLATEDB_MULTIGET` | `false` | `true` enables SlateDB-native batch reads; set `false` and restart to restore serial point reads. |
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

Separate WAL targets use SlateDB's native WAL-store protocol. Durable calls
return only after an immutable WAL object is published. WAL objects use the
same metadata encryption keyring and are never placed in a cache tier. Local
WAL cannot survive host loss unless its directory is durable shared storage.
To change targets, drain and stop the writer, retain the old WAL, and create
and verify a new metadata generation with the new target before activation.
VaulticDB atomically binds the target identity to the metadata generation, so a
direct reopen with a mismatched WAL identity fails closed. Capabilities include
live flush latency, byte, backlog, failure, and retained-segment counters.

Run the Phase 3 integration tests against a pre-created S3-compatible bucket:

```sh
VAULTICDB_TEST_S3_ENDPOINT="$AWS_ENDPOINT_URL_S3" \
VAULTICDB_TEST_S3_BUCKET=metadata \
go test ./internal/index/daemon -run TestS3CompatibleStorageRoundTrip
```

## Build identity

Run `vaultic --version`, `vaulticdb --version`, `vaultic-key-broker --version`,
or `vaultic-key-custodian --version` to inspect an executable without loading
repository, topology, broker, or hardware configuration. The report includes
the application and toolchain versions plus selected storage, transport, TLS,
and cryptographic dependency versions. Rust dependency data is generated from
`Cargo.lock`; multiple versions are shown when the resolved graph contains
more than one version of a security-relevant crate.

## Static Linux build

```sh
./vaulticdb/build-musl.sh
```

The script requires `x86_64-unknown-linux-musl` and writes `vaulticdb`,
`vaultic-key-broker`, and `vaultic-key-custodian` under
`dist/vaulticdb/linux-amd64/`. All three executables are statically linked. The
custodian uses HIDAPI's pure-Rust `basic-udev` backend, so it does not load
`libudev`. The script rejects an artifact with any ELF dynamic dependency and
records the Rust, SlateDB, and target metadata beside the binaries.

CI packages these files once as the generic `vaulticdb-linux-amd64` artifact.
The exact same artifact is run without rebuilding in AlmaLinux 8, Debian stable,
and Ubuntu latest containers. Run the distribution-independent smoke test
locally with:

```sh
./vaulticdb/smoke-static-linux.sh dist/vaulticdb/linux-amd64
```

Tagged GitHub releases provide downloadable bundles for Linux amd64 and arm64
and macOS arm64, plus a best-effort Windows amd64 CLI bundle. Linux bundles
contain the `vaultic` CLI and all three statically linked `vaulticdb`
executables. The macOS bundle contains the CLI and native service executables;
macOS requires dynamic linkage to Apple system libraries. The Windows bundle
contains the self-contained `vaultic.exe` CLI. The `vaulticdb` service suite is
not built for Windows because its local transport and broker security model
currently require Unix sockets and Unix peer credentials.
