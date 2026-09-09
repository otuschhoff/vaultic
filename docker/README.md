# Docker image

## Build

From the root of this repository run:

```
./docker/build.sh
```

image name will be `otuschhoff/vaultic:latest`

## Run

Set environment variable `VAULTIC_REPOSITORY` and map volume to directories and
files like:

```
docker run --rm --hostname my-host -ti \
    -v $HOME/.vaultic/passfile:/pass \
    -v $HOME/importantdirectory:/data \
    -e VAULTIC_REPOSITORY=rest:https://user:pass@hostname/ \
    otuschhoff/vaultic -p /pass backup /data
```

Vaultic relies on the hostname for various operations. Make sure to set a static
hostname using `--hostname` when creating a Docker container, otherwise Docker
will assign a random hostname each time.

## Native RADOS image

The generic image contains only the Vaultic CLI. Two native RADOS build targets
are available for Linux amd64.

Build non-container executables for hosts that provide compatible glibc and
librados installations:

```
make vaultic-rados-linux-amd64
```

This writes all four executables to `bin/linux-amd64-rados`. Rust, Go, and their
language dependencies are linked into the executables. Glibc, librados, and
librados' native dependency closure remain dynamically linked. This is the
most-static practical configuration for the native Ceph client stack. The
binaries are built against Ceph Tentacle 20.2.4 on the CentOS Stream 9 glibc
2.34 baseline and require Tentacle 20 or newer on a native host.

Build an image containing Vaultic, VaulticDB, the key broker, the key custodian,
and the matching dynamically loaded librados runtime:

```
make vaultic-rados-image-linux-amd64
```

The default image name is `vaultic:rados-linux-amd64`. Override it with
`VAULTIC_RADOS_IMAGE=registry.example/vaultic:rados`.
The image includes the matching official Ceph Tentacle 20.2.4 runtime and does
not use librados libraries from the host.

The image defaults to the Vaultic CLI. Select another component by placing its
name first:

```
docker run --rm vaultic:rados-linux-amd64 version
docker run --rm vaultic:rados-linux-amd64 vaulticdb --help
docker run --rm vaultic:rados-linux-amd64 vaultic-key-broker --help
docker run --rm vaultic:rados-linux-amd64 vaultic-key-custodian --help
```

Mount the same configuration, sockets, credentials, and data paths that the
component would use on a native host. The image does not use host librados
libraries. The standalone `linux-amd64` release remains available for systems
that do not use native RADOS. It remains a fully static musl build and is not
replaced by either RADOS target.
