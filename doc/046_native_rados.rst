############################
Native Ceph RADOS storage
############################

Vaultic can use a Ceph pool directly through the RADOS protocol without an RGW
endpoint. Native RADOS is a separate provider named ``rados``; an existing
``s3`` or RGW location is never reinterpreted as RADOS.

Support matrix
==============

Native support is optional. Vaultic uses a pure-Go client and VaulticDB uses
a pure-Rust client; neither RADOS adapter requires librados. RADOS releases
provide fully static Linux amd64 and arm64 executables. The automated
integration harness uses a separate Ceph Tentacle 20.2.4 test cluster.

Tagged releases contain only static Linux binaries, matching debug symbols,
and source archives. No Docker images are built or published by CI. macOS
development builds remain available locally but cannot be fully static.

The Go backend uses ``github.com/otuschhoff/rados-go`` for pure-Go RADOS
protocol access. The VaulticDB backend uses ``github.com/otuschhoff/rados-rs``
pinned to revision ``8a724626e614dbedff836d98f27c81f0ed3b65ea`` for atomic
conditional writes, attributes, versioned reads, and namespace-scoped listing.
Both adapters verify the cluster FSID after connecting. The Rust client's
release qualification remains incomplete; offline adapter tests are not live
Ceph acceptance or production migration sign-off.

Build Vaultic directly with Go and VaulticDB with Rust 1.98 or newer. No Ceph
development libraries are needed. Cargo resolves the pinned public
``rados-rs`` repository when generating or using the lockfile:

.. code-block:: console

    $ go build -tags rados ./cmd/vaultic
    $ cargo build --manifest-path vaulticdb/Cargo.toml --features rados

Builds without the tag or feature have no native dependency. Selecting a
sealed RADOS backend with such a build returns an explicit unsupported-backend
error.

Release builds provide generic and RADOS-enabled static variants:

.. code-block:: console

    $ make vaultic-linux-amd64 vaulticdb-linux-amd64
    $ make vaultic-rados-linux-amd64
    $ make vaultic-rados-linux-arm64

The first produces generic executables without RADOS. The others write all
four RADOS-enabled executables to ``bin/linux-amd64-rados`` and
``bin/linux-arm64-rados``. Vaultic uses ``CGO_ENABLED=0``; VaulticDB, the key
broker, and the key custodian use musl. Release gates reject executables with
a dynamic interpreter or shared-library dependencies.

The pure-Rust client is LGPL-2.1-only. RADOS binary archives preserve its license
and notices; releases also include a matching Rust source bundle with vendored
dependencies and instructions for rebuilding with modified library sources.
See ``vaulticdb/REBUILDING.md``. Static linking does not remove hardware or
external service requirements of optional key providers.

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

For direct CLI operation, select native main storage with
``--daemon-object-store rados`` and the ``--daemon-rados-*`` endpoint flags.
A separate native WAL uses ``--daemon-wal-store rados`` and the
``--daemon-wal-rados-*`` flags. The main and WAL stores may use different pools,
namespaces, prefixes, clients, and credentials. For example, keep SlateDB SSTs
and manifests in a ``db-sst`` pool and WAL objects in a ``db-wal`` pool.

Both ``--daemon-rados-key-file`` and ``--daemon-wal-rados-key-file`` accept
either a raw CephX key or a standard keyring containing a ``key =`` entry in the
section named by the corresponding client flag. The files must pass
protected-file permission checks; do not pass keys in command arguments.

.. code-block:: console

    $ vaultic index import --force-reset-old-idx --start-daemon \
        --daemon-object-store rados \
        --daemon-rados-monitors mon-a.example:3300,mon-b.example:3300 \
        --daemon-rados-fsid 2f525d6a-8f31-4f79-b731-82a6acb235f5 \
        --daemon-rados-pool db-sst --daemon-rados-namespace repository-7 \
        --daemon-rados-prefix metadata --daemon-rados-client client.vaultic \
        --daemon-rados-key-file /run/secrets/vaultic-rados.keyring \
        --daemon-wal-store rados \
        --daemon-wal-rados-monitors mon-a.example:3300,mon-b.example:3300 \
        --daemon-wal-rados-fsid 2f525d6a-8f31-4f79-b731-82a6acb235f5 \
        --daemon-wal-rados-pool db-wal --daemon-wal-rados-namespace repository-7 \
        --daemon-wal-rados-prefix wal --daemon-wal-rados-client client.vaultic \
        --daemon-wal-rados-key-file /run/secrets/vaultic-rados.keyring

SlateDB publishes immutable WAL objects through the Rust RADOS client before a durable write
handle resolves. The RADOS adapter's completed atomic write is the durability
boundary; errors and credential expiry fail the write and never select local
storage. WAL objects pass through metadata encryption before RADOS transport and stay
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

VaulticDB bridges its blocking object-store driver to a dedicated Tokio runtime.
The Rust client uses secure messenger sessions and finite 30-second operation
timeouts. Writes combine data and attributes with create/version assertions in
one atomic request. The pinned client limits compound write payloads to 64 MiB
including attributes and operation metadata; larger objects fail rather than
being split into non-atomic writes. Account for this limit when choosing SlateDB
SST and WAL sizes. Reads use bounded chunks asserted against one object version
and retry version conflicts at most three times. Unknown write outcomes remain
errors and are not retried by the adapter.

The Go backend stops waiting when its caller is canceled; an admitted
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