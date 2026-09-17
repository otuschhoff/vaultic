############################
Native Ceph RADOS storage
############################

Vaultic can use a Ceph pool directly through the RADOS protocol without an RGW
endpoint. Native RADOS is a separate provider named ``rados``; an existing
``s3`` or RGW location is never reinterpreted as RADOS.

Support matrix
==============

Native support is optional. The Vaultic Go backend is a pure-Go client and does
not require glibc or librados. The RADOS release package currently targets
Linux amd64 because the bundled VaulticDB adapter still uses librados. The
automated integration harness uses a Ceph Tentacle 20.2.4 cluster and runtime.

Vaultic does not currently build or test its RADOS adapter on Windows or macOS;
those releases remain generic builds without RADOS. Use the Linux amd64 image
for the supported packaged deployment.

The Go backend uses ``github.com/otuschhoff/rados-go`` for pure-Go RADOS
protocol access. The VaulticDB backend uses the stable librados C ABI because
the evaluated Rust wrappers did not expose both writes and atomic version
assertions. Both adapters verify the cluster FSID after connecting.

Build Vaultic directly with Go. Building VaulticDB's RADOS feature additionally
requires ``librados-dev``:

.. code-block:: console

    $ go build -tags rados ./cmd/vaultic
    $ cargo build --manifest-path vaulticdb/Cargo.toml --features rados

Builds without the tag or feature have no native dependency. Selecting a
sealed RADOS backend with such a build returns an explicit unsupported-backend
error.

Release builds provide two non-container Linux amd64 variants:

.. code-block:: console

    $ make vaultic-linux-amd64 vaulticdb-linux-amd64
    $ make vaultic-rados-linux-amd64

The first produces generic, fully static musl executables without RADOS. The
second writes RADOS-enabled executables to ``bin/linux-amd64-rados``. Vaultic
is built with ``CGO_ENABLED=0`` and does not load librados. VaulticDB requires
compatible glibc and librados installations; those native dependencies remain
dynamic. Statically combining glibc with the dynamic C++ Ceph client is not a
supported VaulticDB release configuration.

For a host without librados, or with an incompatible librados version, build
the Linux amd64 all-components image instead:

.. code-block:: console

    $ make vaultic-rados-image-linux-amd64
    $ docker run --rm vaultic:rados-linux-amd64 version
    $ docker run --rm vaultic:rados-linux-amd64 vaulticdb --help

The image contains Vaultic, VaulticDB, the key broker, the key custodian, and
the official Ceph Tentacle 20.2.4 ``librados2`` runtime for VaulticDB on CentOS
Stream 9. The image does not load librados from the host. Configuration,
sockets, credentials, and repository paths still need to be mounted explicitly.
The generic non-container Linux amd64 artifacts remain static and do not
include native RADOS support.

Topology and credentials
========================

A sealed RADOS endpoint contains exactly these identity fields:

.. code-block:: json

    {
        "provider": "rados",
        "endpoint": {
            "monitors": "mon-a.example:3300,mon-b.example:3300,mon-c.example:3300",
            "cluster_fsid": "2f525d6a-8f31-4f79-b731-82a6acb235f5",
            "pool": "vaultic",
            "namespace": "repository-7",
            "prefix": "packs/primary/"
        },
        "credential_ref": "cred:vaultic-primary"
    }

Monitor addresses, FSID, pool, namespace, and prefix are sealed. There are no
host-local endpoint overrides. The CephX credential uses kind
``cephx-static`` with ``client_id`` such as ``client.vaultic`` and its key in
``client_secret``. The credential stays in broker custody and is passed to the
RADOS client in-process, never through command arguments or exported
environment variables. Backend descriptions hash monitor and prefix values and
never include the key.

CephX authority
===============

Create a separate namespace and client for each repository authority boundary:

.. code-block:: console

    $ ceph osd pool create vaultic 128
    $ ceph auth get-or-create client.vaultic-repository-7 \
        mon 'allow r' \
        osd 'allow rwx pool=vaultic namespace=repository-7'

The data-read role can use ``allow r``. Repository writers, maintainers,
locks, SlateDB WAL, and SlateDB metadata require ``allow rwx`` because their
contracts include atomic create, conditional replacement, and deletion.
CephX can enforce pool and namespace boundaries, but cannot enforce a prefix
boundary inside one namespace. It also cannot express Vaultic's append-only,
create-only, or delete-only-on-cache-prefix policies. Those are client-side
restrictions and must be reported as static-authority compliance findings.
Use separate pools or namespaces, not prefixes, when cache eviction must be
unable to delete repository data or WAL.

The current credential is static. Native RADOS does not claim STS-like expiry,
provider-enforced create-only access, or automatic CephX rotation. Rotate a
client key in broker custody according to the cluster's operational procedure.

SlateDB WAL on RADOS
====================

An RADOS-enabled VaulticDB can select a separate native WAL target with sealed
``wal_target`` topology. Use ``provider: rados``, the standard monitor, FSID,
pool, namespace, and prefix fields, ``durability: shared-remote``, and a
dedicated CephX ``storage-maintain`` binding. VaulticDB leases this credential
under target ``wal`` rather than reusing metadata or repository credentials.

SlateDB publishes immutable WAL objects through librados before a durable write
handle resolves. The RADOS adapter's completed atomic write is the durability
boundary; errors and credential expiry fail the write and never select local
storage. WAL objects pass through metadata encryption before librados and stay
outside every cache-eviction namespace.

Changing the target is restart-required. Drain and stop the writer, retain the
old pool and namespace, create and verify a new metadata generation configured
with the new WAL, then activate it. VaulticDB's metadata-generation binding
makes a direct reopen with a different WAL-store identity fail closed. Keep the
old generation and WAL until observation and retirement checks finish.

Operations
==========

Production pools must use the site's replicated or erasure-coded durability
policy. For a replicated pool, use at least three replicas across independent
failure domains and keep ``min_size`` high enough that acknowledged writes meet
the durability objective. The one-replica memory OSD in the integration
harness is only for deterministic fault tests.

Before sealing an endpoint, check monitor reachability, cluster identity,
health, pool configuration, capacity, and the exact client caps:

.. code-block:: console

    $ ceph -s
    $ ceph fsid
    $ ceph osd pool ls detail
    $ ceph df detail
    $ ceph auth get client.vaultic-repository-7
    $ rados --id vaultic-repository-7 -p vaultic -N repository-7 stat test-object

Go repository objects larger than 4 MiB are split into immutable chunks. The
logical manifest is published atomically only after every chunk is durable;
failed publication rolls back chunks created by that attempt. Reads verify
chunk and whole-object SHA-256 values. Delete hides the manifest before
best-effort chunk cleanup. VaulticDB multipart parts are durable RADOS staging
objects; completion publishes the destination through one atomic full-object
write, abort removes its staging, and staging older than 24 hours is swept
before a new upload.

Native librados calls are blocking and are moved off VaulticDB async executor
threads. The Go backend stops waiting when its caller is canceled; an admitted
RADOS operation continues under its configured operation timeout so ambiguous
write outcomes can be handled consistently. Compound compare-and-write requests
remain atomic at the OSD.

Migration from RGW
==================

An RGW URL cannot be replaced by monitor addresses. Create and seal a distinct
RADOS placement, copy live repository objects through Vaultic's placement copy
workflow, run ``vaultic check`` against the destination, and compare the
expected object and byte counts before changing placement policy. Keep the RGW
copy until the new placement has passed a restore test and its retention window.
The encrypted Vaultic repository format is unchanged; only its physical object
transport differs.

Integration tests
=================

Docker 25 or later can run the isolated Tentacle cluster and both native suites:

.. code-block:: console

    $ helpers/rados-integration/run.sh

The harness tests atomic create and CAS, large-object publication, ranged reads,
listing, concurrent writers, reopen after publication, namespace denial,
bounded failure while the OSD is down, OSD restart, and successful reopen after
recovery. It is destructive only to its dedicated Docker network and container.