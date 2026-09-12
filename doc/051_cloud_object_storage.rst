.. _cloud_object_storage:

Cloud object storage (S3 and GCS)
#################################

Vaultic can use Amazon S3 or an S3-compatible service for three independent
purposes:

* as the repository backend selected by ``-r``;
* as an additional backend for data-pack placement; and
* as the object store for VaulticDB metadata.

Use separate prefixes, and preferably separate buckets and credentials, for
these roles. Repository packs and VaulticDB objects have different access,
lifecycle, performance, and recovery requirements.

Endpoint and credentials
========================

Repository URLs use the form ``s3:ENDPOINT/BUCKET/PREFIX``. For AWS, select the
regional endpoint. For an S3-compatible service, include ``https://`` and an
optional port::

    $ export AWS_DEFAULT_REGION=eu-central-1
    $ export AWS_ACCESS_KEY_ID=...
    $ export AWS_SECRET_ACCESS_KEY=...
    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/repository init

    $ vaultic -r s3:https://objects.example.net:9000/example/repository init

The default bucket lookup mode is ``auto``. Override it for providers that
require a particular addressing style::

    $ vaultic -r s3:https://objects.example.net/example/repository \
        -o s3.bucket-lookup=path snapshots

Backblaze and Wasabi profiles
-----------------------------

Select an explicit profile with ``-o s3.provider=backblaze`` or
``-o s3.provider=wasabi``. A recognized production hostname also enables the
profile for diagnostics and safe defaults, unless ``s3.provider=generic`` was
selected explicitly::

    $ vaultic -r s3:https://s3.us-west-004.backblazeb2.com/example/repository \
        -o s3.provider=backblaze snapshots

    $ vaultic -r s3:https://s3.eu-central-2.wasabisys.com/example/repository \
        -o s3.provider=wasabi snapshots

Named profiles validate the provider hostname and signing region before
credentials are resolved, require HTTPS for recognized production endpoints,
and default to DNS bucket lookup. A custom CNAME requires an explicit provider
and region; set ``s3.bucket-lookup=path`` only when the provider or network
requires it. Explicit options always win, including ``s3.provider=generic``.
Redirects to a different hostname or port are rejected before authorization can
be forwarded.

Both profiles support Signature V4, multipart uploads, range reads, and
ListObjectsV2. Conditional-create behavior is reported as unverified until a
live endpoint proves it. Backblaze IAM roles are unsupported. Wasabi supports
native `AWS-compatible STS <https://docs.wasabi.com/docs/aws-sts-with-wasabi>`_
``AssumeRole`` at ``https://sts.wasabisys.com``;
Vaultic selects that endpoint automatically for the Wasabi profile. The caller
must use Wasabi sub-user credentials because root credentials cannot assume a
role. Both profiles reject Glacier restore and storage classes other than
``STANDARD``. Inspect the normalized provider, endpoint host, region,
addressing mode, and capability results with ``vaultic index backends --no-list
--json``.

Vaultic refuses anonymous S3 access unless
``-o s3.unsafe-anonymous-auth=true`` is explicitly set. Do not enable anonymous
access for a backup repository.

Credential sources
------------------

For a quorum repository, the preferred source is format-2 sealed topology.
Vaultic asks the unlocked broker for a backend ID and operation tier; the broker
prefers that tier's STS role and returns a temporary session. If the policy
explicitly permits ``static-on-unavailable``, a transport timeout, connection
failure, or STS 5xx response selects only that tier's static binding. Access
denial, invalid roles or policies, malformed responses, and other configuration
errors fail closed and never activate fallback. VaulticDB does
the same for each metadata replica, requesting ``storage-read`` for a read-only
replica and ``storage-maintain`` for a writable replica. Repository lock objects
use an independent ``storage-lock`` binding restricted to the lock prefix.
The broker lease payload carries the temporary access key, secret key, session
token, and provider expiry together; both Vaultic and VaulticDB pass all four
fields directly to their S3 client and do not export them to the environment.
Static keys are therefore absent from shell variables, launchd property lists,
repository configuration, and the credential-free bootstrap profile. Optional
``tls_sha256`` endpoint pins are checked against the peer leaf certificate.

Credential lifetime and renewal
-------------------------------

Vaultic and VaulticDB request storage credentials for one hour by default and
renew them before expiry. Configure Vaultic with ``--storage-token-ttl``,
``--storage-token-renew-margin``, and ``--broker-outage-grace``; the equivalent
VaulticDB settings are ``VAULTICDB_STORAGE_TOKEN_TTL``,
``VAULTICDB_STORAGE_TOKEN_RENEW_MARGIN``, and
``VAULTICDB_BROKER_OUTAGE_GRACE``. Vaultic environment equivalents use the
``VAULTIC_`` prefix. Durations use ``ms``, ``s``, ``m``, or ``h`` suffixes.

The renewal margin defaults to the larger of 20 minutes and one third of the
TTL. Outage grace defaults to the TTL. Size them so that
``renew_margin >= expected_ceremony_time + retry_backoff`` and
``ttl >= renew_margin + minimum_useful_work_window``. The broker rejects a
storage TTL above one hour rather than silently clamping it.

Renewal opens a replacement cloud client and atomically routes new operations
to it. An operation already using the old client may finish before that client
is closed. VaulticDB keeps its SlateDB state open while swapping the underlying
replica clients, and reacquires its metadata-DEK authorization without replacing
the active DEK. A changed DEK or topology path fails renewal closed.

When the broker is locked, unavailable, or restarting, consumers keep using the
current credential only until the earlier of its broker/provider expiry and the
outage-grace deadline. Renewal retries use bounded jitter. New writes stop five
minutes or one tenth of the TTL before that deadline, whichever is smaller, so
backup uploads and maintenance mutations are not admitted without a useful
validity window. Reads continue until the hard deadline. At hard expiry Vaultic
returns ``storage credential expired`` and VaulticDB stops admitting object
operations, drains the server, and emits ``credential_expired``. Renewal and
issuance events contain target, tier, source, TTL, issue/lease ID, and expiry,
but never credential values.

Every S3 profile can instead use exact-tier static bindings, either before STS
is configured or as an explicit STS outage fallback. Backblaze has no IAM roles
or STS and therefore always uses this static-only form. Create separate bucket-
and repository-prefix-restricted application keys and register these bindings:

* ``storage-read``: ``listFiles`` and ``readFiles``;
* ``storage-append``: the read capabilities plus ``writeFiles``;
* ``storage-maintain``: the append capabilities plus ``deleteFiles``; and
* ``storage-lock``: list, read, write, and delete restricted to the repository
  lock prefix.

Prune and garbage collection need ``storage-maintain``, not a read-and-delete
key, because they write replacement packs before deleting obsolete objects.
Backblaze ``writeFiles`` is not create-only and can create a newer version of an
existing name. Vaultic still sends conditional-create requests, but operators
must treat append overwrite prevention as client-only. Broker lease expiry also
does not revoke a copied Backblaze key; only provider expiry or deletion does.
The same limitation applies to static AWS, Wasabi, MinIO, Ceph, and other
S3-compatible keys.

Each static policy has a monotonically increasing ``generation``. If the broker
has released a static fallback, status reports
``static-fallback-active-provider-authority``. When STS later succeeds, the
broker retires every outstanding fallback generation for that target from
further checkout and reports
``static-fallback-provider-revocation-required``. Delete or disable the old keys
at the provider, install fresh exact-tier keys under a higher generation, and
list the old generation in ``revoked_generations``. This acknowledgement clears
the finding; it does not itself revoke provider credentials.

Fallback checkout and retirement are written synchronously to an atomically
replaced, broker-identity-signed lifecycle journal before the corresponding
credential lease is returned. By default the journal is stored beside local
capsule generations with a ``.state`` extension so capsule discovery cannot
mistake it for a generation. Set ``fallback_state_path`` in broker configuration
to move it. A missing write or invalid signature fails credential issuance
closed; the journal contains target IDs and generation numbers, never keys.

Operators who prefer one data key may bind the same bucket- and prefix-restricted
read/write/delete credential to all three data tiers. Do not use an account
master key or grant key-management and bucket-administration capabilities. The
broker never silently substitutes a stronger binding when an exact tier is
missing.

The environment-based procedures below are the legacy external-topology mode.
Select that mode explicitly with ``--topology-source external`` or
``VAULTICDB_TOPOLOGY_SOURCE=external``. Broker status reports
``topology: external`` as non-compliant. Retain this path for standalone and
migration tooling only; recovery capsules always use sealed topology.

Vaultic's repository S3 backend supports the AWS credential chain, including:

* ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY``;
* temporary credentials with ``AWS_SESSION_TOKEN``;
* ``AWS_PROFILE`` and ``AWS_SHARED_CREDENTIALS_FILE``;
* instance or workload identity credentials; and
* STS role assumption through ``VAULTIC_AWS_ASSUME_ROLE_ARN`` and the related
  ``VAULTIC_AWS_ASSUME_ROLE_*`` variables listed in :doc:`075_scripting`.

For Wasabi, set ``VAULTIC_AWS_ASSUME_ROLE_ARN`` to a Wasabi IAM role ARN and
provide the sub-user access key through the normal AWS credential chain. The
Wasabi profile defaults ``VAULTIC_AWS_ASSUME_ROLE_STS_ENDPOINT`` to
``https://sts.wasabisys.com`` and the session name to ``vaultic``. An explicit
session name remains supported, and Wasabi documents a maximum role session of
12 hours. For credential safety, a Wasabi profile rejects an STS endpoint
override that is not that exact HTTPS service endpoint.

Long-lived access keys are simple but carry permanent value if copied. If they
must be used, create a dedicated identity for each repository role, restrict it
to one bucket prefix, keep it out of command lines and repository configuration,
and rotate it regularly.

Ceph RGW STS
------------

Select ``-o s3.provider=ceph`` for Ceph Object Gateway and provide the RGW
endpoint, region, and addressing mode explicitly::

    $ vaultic -r s3:https://rgw.example.net/example/repository \
        -o s3.provider=ceph -o s3.region=us-east-1 snapshots

Ceph uses Vaultic's AWS-compatible Signature V4 and ``AssumeRole`` paths. In
sealed topology, set ``credential_policy.sts.endpoint`` to the deployment's RGW
STS endpoint and bind a distinct role ARN to each enabled tier. The broker
derives the inline policy from the sealed bucket, repository prefix, and tier;
callers cannot provide a policy. HTTP is accepted only when the storage endpoint
itself explicitly uses HTTP for a local test cluster. Ceph releases vary in STS
and session-policy support, so verify prefix conditions and multipart operations
against the deployed RGW version before enabling dynamic credentials.

Google Cloud Valet Key credentials
----------------------------------

Google Cloud Storage does not issue temporary S3 interoperability HMAC keys.
Its XML and JSON APIs accept OAuth bearer tokens, so Vaultic implements the
Valet Key pattern with Google's native Credential Access Boundary flow. The
broker signs a service-account OAuth assertion from a capsule-sealed issuer,
uses IAM Credentials to impersonate the configured runtime service account,
then exchanges that source token at Google Security Token Service. Consumers
receive only a ``gcp-access-token`` and its provider expiry. The private key,
bootstrap token, and impersonated source token remain broker-only.

Configure ``credential_policy.gcp_downscope`` only on a ``gcs`` endpoint:

* ``issuer_ref`` names a ``gcp-service-account-json`` broker credential;
* ``service_account`` names the runtime service account to impersonate;
* ``iam_credentials_endpoint`` is normally
  ``https://iamcredentials.googleapis.com``;
* ``token_exchange_endpoint`` is normally
  ``https://sts.googleapis.com/v1/token``;
* ``tiers`` lists unique enabled tiers in canonical order; and
* ``fallback`` is ``disabled`` or ``static-on-unavailable``.

Grant the issuer only ``iam.serviceAccounts.getAccessToken`` on the runtime
service account. Grant that runtime account the underlying bucket permissions;
a Credential Access Boundary subtracts permissions but cannot add them. The
broker generates the boundary from the sealed bucket and prefix. Read uses
``roles/storage.objectViewer``; append intersects object viewer and object
creator; maintain uses ``roles/storage.objectAdmin``. Lock leases use object
admin only under ``locks/``. The CEL condition restricts both object names and
Google's ``storage.googleapis.com/objectListPrefix`` attribute so list requests
cannot escape the repository prefix.

Only connection failures, timeouts, HTTP 408/429, and provider 5xx responses can
activate ``static-on-unavailable``. The fallback must be a separate, narrowly
scoped service-account key bound to every enabled exact tier; the dynamic issuer
cannot be a consumer binding. Authentication denial, redirects, malformed or
oversized responses, and invalid configuration fail closed. Dynamic recovery
retires checked-out fallback generations through the durable lifecycle journal.
Vaultic and VaulticDB pass downscoped tokens directly to their native GCS
clients in memory, without environment variables or credential files.

Prefer short-lived credentials from an instance profile, workload identity, or
STS role. Export all three values when supplying a fixed temporary credential::

    $ export AWS_ACCESS_KEY_ID=...
    $ export AWS_SECRET_ACCESS_KEY=...
    $ export AWS_SESSION_TOKEN=...

The credential lifetime must cover the operation. Replacing variables in a
parent shell does not update an already running Vaultic or VaulticDB process;
restart that process before fixed environment credentials expire. Credential
providers backed by instance or workload identity can refresh credentials
without placing a permanent secret on disk.

VaulticDB uses the standard ``AWS_*`` environment understood by its S3 client.
When Vaultic starts the daemon, it forwards ``AWS_*`` variables for an S3
object store. The repository-only ``VAULTIC_AWS_ASSUME_ROLE_*`` variables are
not VaulticDB configuration; provide the resulting temporary ``AWS_*``
credentials or use a workload identity for the daemon.

For external-mode Backblaze or Wasabi metadata, pass the same normalized profile
through the daemon CLI::

    $ vaultic index status --start-daemon --persistent-daemon \
        --daemon-object-store s3 --daemon-s3-bucket example-metadata \
        --daemon-s3-prefix vaulticdb/production \
        --daemon-s3-endpoint https://s3.eu-central-2.wasabisys.com \
        --daemon-s3-region eu-central-2 --daemon-s3-provider wasabi \
        --daemon-s3-bucket-lookup dns

Direct daemon launches use ``VAULTICDB_S3_ENDPOINT``,
``VAULTICDB_S3_REGION``, ``VAULTICDB_S3_PROVIDER``, and
``VAULTICDB_S3_BUCKET_LOOKUP`` with the existing bucket and prefix variables.
Replicated external stores use the corresponding
``VAULTICDB_REPLICATED_ID_S3_*`` names. Capsule topology carries ``provider``
and ``bucket_lookup`` in the structured S3 endpoint, so capsule mode needs none
of these endpoint or credential variables.

Permissions
-----------

Pre-create buckets so the runtime identity does not need ``s3:CreateBucket``.
A typical prefix-scoped policy needs bucket listing and object read, write, and
delete access. Multipart uploads require their corresponding list and abort
operations. The following is a starting point; adapt condition keys and actions
to the provider:

.. code-block:: json

    {
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": [
            "s3:GetBucketLocation",
            "s3:ListBucket",
            "s3:ListBucketMultipartUploads"
          ],
          "Resource": "arn:aws:s3:::example",
          "Condition": {
            "StringLike": {"s3:prefix": ["repository", "repository/*"]}
          }
        },
        {
          "Effect": "Allow",
          "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:AbortMultipartUpload",
            "s3:ListMultipartUploadParts"
          ],
          "Resource": "arn:aws:s3:::example/repository/*"
        }
      ]
    }

Add ``s3:RestoreObject`` when Vaultic performs Glacier restores. A read-only
restore identity can omit writes and deletes, but normal backup, prune, repair,
placement, and VaulticDB operation require them. Bucket versioning or Object
Lock may preserve deleted versions, but retention rules can also make
``prune``, placement eviction, and metadata compaction fail; test the complete
lifecycle before enforcing retention.

Backblaze and Wasabi may charge for minimum storage duration, retained versions,
API operations, or egress. A provider warning is informational only: Vaultic
still performs requested retention and deletion work and reports retention or
quota denial distinctly.

Storage tiers
=============

Treat storage as one of three operational tiers:

**Warm/online**
  Objects are immediately readable. ``STANDARD``, ``STANDARD_IA``,
  ``ONEZONE_IA``, and ``INTELLIGENT_TIERING`` are typical examples. Use an
  online class for repository metadata, current data, and all VaulticDB
  objects.

**Cold**
  Lower-cost storage remains immediately readable but may have retrieval fees
  or minimum-duration charges. It is suitable for an additional durable data
  copy after its cost and failure domain are represented in placement policy.

**Archive/Glacier**
  ``GLACIER`` and ``DEEP_ARCHIVE`` objects must be restored before reading and
  usually impose retrieval delay and minimum-retention charges. They are
  suitable only for data packs. Never transition repository metadata, tree
  packs, or VaulticDB objects to an archive class.

Set a repository S3 storage class with ``-o s3.storage-class=CLASS``. For an
archive class, Vaultic automatically writes only data packs with that class;
config, keys, snapshots, indexes, and tree packs remain online::

    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/repository \
        -o s3.storage-class=GLACIER backup /srv/data

To restore archived objects with the S3 backend, enable the feature and restore
support. Retrieval can take hours and can incur substantial charges::

    $ VAULTIC_FEATURES=s3-restore vaultic \
        -r s3:s3.eu-central-1.amazonaws.com/example/repository \
        -o s3.enable-restore=true \
        -o s3.restore-tier=Standard \
        -o s3.restore-days=7 \
        -o s3.restore-timeout=24h \
        restore latest --target /srv/restore

``restore-tier`` accepts ``Standard``, ``Bulk``, or ``Expedited`` when the
provider supports them. See :ref:`cold_storage` for provider-neutral warm-up
commands and hot/cold repositories.

VaulticDB metadata on S3
========================

VaulticDB can store its SlateDB objects in S3. This is metadata storage, not a
copy of repository data packs. It requires low-latency, immediately readable
object storage and must not use Glacier or a lifecycle rule that transitions
objects to an archive class.

When Vaultic starts the daemon, select S3 and provide a bucket and namespace
root::

    $ export AWS_DEFAULT_REGION=eu-central-1
    $ export AWS_ACCESS_KEY_ID=...
    $ export AWS_SECRET_ACCESS_KEY=...
    $ export AWS_SESSION_TOKEN=...  # only for temporary credentials

    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/repository \
        index import --from-legacy --activate \
        --start-daemon --persistent-daemon \
        --daemon-object-store s3 \
        --daemon-s3-bucket example-metadata \
        --daemon-s3-prefix vaulticdb/production

For a non-AWS endpoint, also set ``AWS_ENDPOINT_URL_S3``. The daemon appends a
hash of the repository identity to ``--daemon-s3-prefix``; the prefix is a
namespace root and may serve multiple repositories. Still use a dedicated,
non-empty production prefix to make IAM boundaries, recovery, and deletion
auditable.

Pass the same daemon selection flags when reconnecting unless a persistent
daemon is already running. Protect remote metadata with
``--metadata-encryption required`` and the configured key-management or broker
options; repository pack encryption does not by itself encrypt SlateDB's
operational metadata. See :doc:`070_encryption` for metadata encryption and
recovery procedures.

Before activation, verify that the bucket region, endpoint, credentials, and
prefix are correct. Never point two unrelated repositories or independent
writers at the same final database path. Preserve the previous metadata prefix
during migration or rebuild until the replacement has been independently
verified.

SlateDB WAL on S3
-----------------

VaulticDB can place SlateDB's WAL in a separate S3 bucket or prefix with
``--daemon-wal-store s3`` and the ``--daemon-wal-s3-*`` options. The daemon
appends the repository identity hash and keeps WAL objects separate from SSTs,
manifests, repository packs, and caches. A durable acknowledgement waits for
the immutable S3 WAL upload to complete; there is no local upload queue and no
fallback after an S3 error.

Use a dedicated credential with read, create, overwrite, list, and delete
authority limited to the WAL namespace. In capsule mode, configure
``wal_target`` with ``durability: shared-remote``; the broker issues its
credential as target ``wal`` at ``storage-maintain`` independently of metadata
replica credentials. WAL objects use the metadata DEK outside process memory.

Changing WAL placement is restart-required. Drain and stop the writer, preserve
the old WAL, create and verify a new metadata generation under the new target,
then activate that generation through the guarded workflow. VaulticDB rejects a
direct reopen when its metadata-generation WAL binding differs. Never delete
WAL by age; SlateDB cleanup follows manifest checkpoint and reader recovery
state.

Volatile in-memory WAL for bulk import
--------------------------------------

On a high-memory host, a rebuild from legacy indexes can avoid durable WAL I/O
by starting its temporary VaulticDB process with an in-memory WAL::

  $ vaultic -r /srv/repository index import --from-legacy \
    --start-daemon --daemon-object-store local \
    --daemon-data-dir /shared/vaulticdb \
    --daemon-wal-store memory --pack-workers 16

Use this only while the legacy indexes remain available as the recovery source.
The daemon reports this WAL durability as ``local-process``: a crash, kill, or
host restart can lose transactions that were acknowledged but not yet flushed
to the durable metadata store. Restart the import with ``--resume`` (the
default) after such an interruption. Do not combine this mode with
``--persistent-daemon`` or use the resulting daemon for normal authoritative
metadata writes. Complete and validate the import before ``--activate``.

For Backblaze migration from the native API, copy objects with the existing
backend migration workflow and run ``vaultic backend verify --compare`` against
the native ``b2:`` and S3-compatible locations. Verification reads and compares
every object name, length, and SHA-256 hash; do not activate the new topology
merely because both URLs refer to the same Backblaze bucket.

With a format-1 capsule, do not set ``VAULTICDB_REPLICATED_*``, ``AWS_*``, or
Azure credential variables. Configure only the broker socket, signed release
manifest, repository identity, and ``VAULTICDB_TOPOLOGY_SOURCE=capsule``.
VaulticDB acquires redacted topology plus the exact replica credential leases,
constructs local, S3, or Azure stores in memory, and shuts down if a lease
disconnects or expires. Read-only broker authorizations cannot lease
delete-capable credentials.

Secondary storage for data packs
================================

Placement policy
----------------

For a SlateDB-authoritative repository, placement policy is the preferred way
to add S3 as a secondary pack backend. Credentials stay in the process
environment; repository configuration contains locations and policy only.
For example:

.. code-block:: json

    {
      "placement_backends": [
        {
          "id": "primary",
          "role": "primary",
          "failure_domain": "datacenter-a"
        },
        {
          "id": "warm-s3",
          "location": "s3:s3.eu-central-1.amazonaws.com/example/warm",
          "role": "primary",
          "offsite": true,
          "failure_domain": "aws-eu-central-1",
          "retrieval_class": "standard",
          "max_requests_per_second": 20
        },
        {
          "id": "archive-s3",
          "location": "s3:s3.eu-central-1.amazonaws.com/example/archive",
          "role": "archival",
          "offsite": true,
          "failure_domain": "aws-eu-central-1",
          "retrieval_class": "deep-archive",
          "min_retention_seconds": 15552000,
          "target_pack_size_bytes": 536870912
        }
      ],
      "placement_policy": {
        "min_copies": 2,
        "min_domains": 2,
        "min_offsite": 1,
        "offsite_deadline_seconds": 14400,
        "promotion_crossover_seconds": 691200
      }
    }

Provision these fields through the repository-config deployment workflow and
inspect the result with ``vaultic config --json``. The current ``vaultic
config`` setters do not edit placement arrays. Do not download and hand-edit an
encrypted repository config object: an interrupted or malformed replacement
can make the repository unavailable.

In sealed topology, different providers retain the same structured S3 shape.
The ``credential_policy`` below makes Wasabi STS preferred while retaining
generation 3 static fallback keys. The Backblaze location is static-only and
maps each operation tier to a pre-provisioned application key:

.. code-block:: json

    {
      "pack_backends": [
        {
          "id": "wasabi-primary",
          "provider": "s3",
          "endpoint": {
            "url": "https://s3.eu-central-2.wasabisys.com",
            "bucket": "example-primary",
            "prefix": "repository",
            "region": "eu-central-2",
            "provider": "wasabi",
            "bucket_lookup": "dns"
          },
          "credential_policy": {
            "sts": {
              "issuer_ref": "cred:wasabi-issuer",
              "endpoint": "https://sts.wasabisys.com",
              "region": "eu-central-2",
              "session_name": "vaultic",
              "roles": {
                "storage-read": "arn:aws:iam::123456789012:role/vaultic-read",
                "storage-append": "arn:aws:iam::123456789012:role/vaultic-append",
                "storage-maintain": "arn:aws:iam::123456789012:role/vaultic-maintain",
                "storage-lock": "arn:aws:iam::123456789012:role/vaultic-lock"
              },
              "fallback": "static-on-unavailable"
            },
            "static": {
              "generation": 3,
              "revoked_generations": [1, 2],
              "bindings": {
                "storage-read": "cred:wasabi-read-g3",
                "storage-append": "cred:wasabi-append-g3",
                "storage-maintain": "cred:wasabi-maintain-g3",
                "storage-lock": "cred:wasabi-lock-g3"
              }
            }
          },
          "role": "primary",
          "offsite": false,
          "failure_domain": "wasabi-eu-central-2"
        },
        {
          "id": "backblaze-offsite",
          "provider": "s3",
          "endpoint": {
            "url": "https://s3.us-west-004.backblazeb2.com",
            "bucket": "example-offsite",
            "prefix": "repository",
            "region": "us-west-004",
            "provider": "backblaze",
            "bucket_lookup": "dns"
          },
          "credential_policy": {
            "static": {
              "generation": 1,
              "bindings": {
                "storage-read": "cred:backblaze-offsite-read",
                "storage-append": "cred:backblaze-offsite-append",
                "storage-maintain": "cred:backblaze-offsite-maintain",
                "storage-lock": "cred:backblaze-offsite-lock"
              }
            }
          },
          "role": "replica",
          "offsite": true,
          "failure_domain": "backblaze-us-west-004"
        }
      ]
    }

Credential objects remain in the encrypted capsule and are omitted here. Enroll
the backend JSON and a protected JSON object mapping each new ``cred:`` reference
to its credential atomically:

.. code-block:: console

  $ vaultic index keys --repository-id REPOSITORY-UUID quorum topology \
    set-backend-policy backblaze-backend.json backblaze-credentials.json \
    --capsule current.vrc --capsule-directory capsules \
    --member 'MEMBER=PROVIDER:MEMBER-CREDENTIAL-FILE'

The credentials file must be owner-only. The mutation rejects invalid or unused
references, provider mismatches, duplicate credential enrollment, zero or
reused generations, Backblaze STS policies, and non-standard Wasabi STS
endpoints. A
successful write is not treated as a durability proof: placement verification
and ``vaultic backend verify`` perform independent reads and hashes.

Azure Blob Valet Key with Entra
-------------------------------

Azure Blob uses Microsoft Entra and user delegation SAS rather than STS. The
broker is the only process that receives the capsule-sealed
``azure-entra-client-secret`` issuer credential. For each authorized request it
uses the OAuth client-credentials flow, obtains a user delegation key from Blob
Storage, signs a short-lived SAS locally, and leases only that SAS to
``vaultic`` or ``vaulticdb``. The client secret, OAuth bearer token, and user
delegation key never leave the broker.

Grant the issuer service principal both:

* ``Microsoft.Storage/storageAccounts/blobServices/generateUserDelegationKey/action``
  at storage-account, resource-group, or subscription scope. The built-in
  Storage Blob Delegator role is the narrow key-generation role.
* The required Blob data actions on the dedicated repository container. Azure
  authorizes a user delegation SAS by intersecting these RBAC or ACL rights
  with the permissions in the SAS, so the issuer role must not be broader than
  the intended repository authority.

Use the OAuth scope ``https://storage.azure.com/.default`` for public Azure, or
the corresponding Storage resource scope for the configured sovereign cloud.
The token URI must be HTTPS, must contain the credential's exact tenant ID as
``/{tenant_id}/oauth2/v2.0/token``, and cannot contain user information, a query,
or a fragment.

Dynamic Azure topology requires a dedicated container and an empty ``prefix``.
Ordinary data leases are container-scoped. A ``storage-lock`` lease is narrower:
it is a directory SAS for ``locks/`` with ``sr=d`` and ``sdd=1``. Consequently,
enabling ``storage-lock`` requires an Azure storage account with hierarchical
namespace enabled and ``hierarchical_namespace: true`` in policy. Without HNS,
omit that tier; the broker rejects a policy that would claim lock isolation it
cannot enforce.

.. code-block:: json

    {
      "id": "azure-primary",
      "provider": "azure",
      "endpoint": {
        "url": "https://ACCOUNT.blob.core.windows.net",
        "account": "ACCOUNT",
        "container": "DEDICATED_REPOSITORY_CONTAINER",
        "prefix": ""
      },
      "credential_policy": {
        "azure_user_delegation": {
          "issuer_ref": "cred:azure-issuer",
          "service_version": "2026-04-06",
          "signed_ip": "198.51.100.10-198.51.100.20",
          "hierarchical_namespace": true,
          "tiers": [
            "storage-read",
            "storage-append",
            "storage-maintain",
            "storage-lock"
          ],
          "fallback": "static-on-unavailable"
        },
        "static": {
          "generation": 2,
          "revoked_generations": [1],
          "bindings": {
            "storage-read": "cred:azure-read-g2",
            "storage-append": "cred:azure-append-g2",
            "storage-maintain": "cred:azure-maintain-g2",
            "storage-lock": "cred:azure-lock-g2"
          }
        }
      },
      "role": "primary",
      "offsite": false,
      "failure_domain": "azure-account-region"
    }

The protected credential bundle supplied to ``set-backend-policy`` contains the
issuer and every referenced fallback credential. The issuer has this shape:

.. code-block:: json

    {
      "cred:azure-issuer": {
        "kind": "azure-entra-client-secret",
        "tenant_id": "TENANT_ID",
        "client_id": "APPLICATION_CLIENT_ID",
        "client_secret": "CLIENT_SECRET",
        "token_uri": "https://login.microsoftonline.com/TENANT_ID/oauth2/v2.0/token",
        "scopes": ["https://storage.azure.com/.default"]
      }
    }

The broker refuses to lease an Entra issuer directly or accept one in a static
consumer binding. Dynamically issued permissions are exactly ``rl`` for
``storage-read``, ``rcwl`` for ``storage-append``, and ``rcwdl`` for
``storage-maintain`` and ``storage-lock``. All SAS tokens require HTTPS;
``signed_ip`` optionally restricts use to one IPv4 address or an inclusive IPv4
range. Azure's ``w`` permission can overwrite, so append safety also depends on
Vaultic's create-only request precondition and must not be represented as
provider-enforced append-only authority.

The SAS expiry is bounded by both the broker lease and the user delegation key;
Azure limits delegation keys to seven days. Network failures, timeouts, HTTP
408/429, and provider 5xx responses are availability failures eligible for an
exact-tier ``static-on-unavailable`` fallback. Authentication and authorization
4xx responses, invalid policy, and malformed or oversized provider responses
fail closed. A successful dynamic issuance permanently retires every checked-out
fallback generation for that target. Revoke those static SAS tokens or shared
keys at Azure, enroll a fresh generation, and list the old generation in
``revoked_generations`` before fallback can be compliant again.

To revoke dynamic access, revoke the storage account's user delegation keys
and/or remove the issuer's RBAC or ACL assignments. Azure caches delegation keys
and role assignments, so allow for propagation delay and keep lease lifetimes
short. Treat every SAS as a bearer secret until expiry or confirmed revocation.

Recent packs are copied to warm offsite storage first. Promotion to an archival
backend is delayed until the pack survives the configured crossover, avoiding
minimum-retention charges for short-lived data. The configured durability
predicate counts live copies, distinct failure domains, and offsite copies.
Backends in the same provider account or region should not be declared as
independent failure domains merely because they use different buckets.

Monitor and execute placement work explicitly::

    $ vaultic index placement --unsatisfied --json
    $ vaultic index placement --overdue --json
    $ vaultic index placement --pending-promotion --json
    $ vaultic index placement --explain PACK-ID --json
    $ vaultic index placement --execute --max-requests 100

Use ``--max-bytes`` as well as backend bandwidth and request-rate limits to
bound transfer cost. A backup records missing placement work and performs one
bounded best-effort action; monitor overdue work rather than assuming every
backup immediately satisfies the offsite policy.

Hot/cold compatibility mode
---------------------------

The simpler ``--repo-hot`` model does not require SlateDB placement policy. The
repository selected by ``-r`` is the complete cold repository and receives all
data packs. The hot repository receives metadata and tree packs, which are also
mirrored to cold::

    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/cold init
    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/cold \
        --repo-hot /srv/vaultic-hot init --hot-only

    $ vaultic -r s3:s3.eu-central-1.amazonaws.com/example/cold \
        --repo-hot /srv/vaultic-hot backup /srv/data

This is a fixed split, not asynchronous replication: data packs are written
only to the cold backend, while metadata writes must succeed on both. Use
placement policy when you need multiple independent copies, offsite deadlines,
or delayed archival promotion.

Hardening checklist
===================

* Block public access and require TLS. Use a private endpoint where practical.
* Use separate identities and prefixes for repository packs, placement
  backends, and VaulticDB metadata. For dynamic Azure credentials, use dedicated
  containers and HNS directory scope for ``locks/``. Deny access outside each
  provider-enforced boundary.
* Prefer workload identity or short-lived STS credentials over permanent keys.
  Never place secrets in repository URLs, repository config, shell history, or
  service arguments.
* Enforce provider-side encryption as a bucket default, ideally with a
  dedicated KMS key and narrowly scoped key policy. This complements, rather
  than replaces, Vaultic repository and metadata encryption.
* Keep VaulticDB and repository metadata online. Scope lifecycle transitions to
  known data-pack prefixes only; test rules against a disposable repository.
* Enable versioning and retention only with a documented cleanup procedure.
  Account for retained versions and failed deletes in capacity and cost plans.
* Log object access and IAM changes, alert on denied operations and unexpected
  prefixes, and monitor placement ``--overdue`` results.
* Set request and bandwidth limits for secondary backends. Estimate restore,
  early-deletion, API-request, and egress charges before selecting an archive
  class.
* Test credential rotation, daemon restart, backup, ``check``, placement,
  prune, and a full restore. A successful upload alone does not prove that
  archive retrieval or disaster recovery works.
* Preserve an independent recovery path. Object-store durability does not
  protect against credential loss, account suspension, malicious deletion, or
  a policy that grants one principal control of every copy.
