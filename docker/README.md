# Containers Are Test Infrastructure Only

Vaultic v0.3.0 publishes static Linux binary archives, not Docker images.
GitHub Actions no longer builds or pushes application images. The application
Docker recipes have been removed, and the legacy release-preparation helper
is disabled in favor of the static-only release workflow.

Build all four static RADOS-enabled executables without Docker:

```sh
make vaultic-rados-linux-amd64
make vaultic-rados-linux-arm64
```

Outputs are in `bin/linux-amd64-rados` and `bin/linux-arm64-rados`.
Vaultic uses a pure-Go client with CGO disabled. VaulticDB uses the pure-Rust
rados-rs client with musl; neither adapter links librados. Generic static
variants remain available through the usual Linux Make targets.

`Dockerfile.rados` supplies only development toolchains for the disposable
Ceph integration harness under `helpers/rados-integration`. Those local test
images are not application artifacts and are never published. CI may run
unchanged static executables inside existing distribution images to test
runtime compatibility.

See [the rebuild instructions](../vaulticdb/REBUILDING.md) for the toolchain,
source bundle, and LGPL-2.1-only RADOS client notices. Its release qualification
is incomplete; passing static smoke tests is not production acceptance.
