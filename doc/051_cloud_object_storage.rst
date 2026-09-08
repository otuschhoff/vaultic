.. _cloud_object_storage:

Cloud object storage (S3)
#########################

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

Vaultic refuses anonymous S3 access unless
``-o s3.unsafe-anonymous-auth=true`` is explicitly set. Do not enable anonymous
access for a backup repository.

Credential sources
------------------

For a format-1 quorum repository, the preferred source is sealed topology.
Vaultic obtains the structured S3 endpoint and one authorized credential lease
from the unlocked broker; VaulticDB does the same for each metadata replica.
Static keys are therefore absent from shell variables, launchd property lists,
repository configuration, and the credential-free bootstrap profile. Optional
``tls_sha256`` endpoint pins are checked against the peer leaf certificate.

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

Long-lived access keys are simple but carry permanent value if copied. If they
must be used, create a dedicated identity for each repository role, restrict it
to one bucket prefix, keep it out of command lines and repository configuration,
and rotate it regularly.

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
  backends, and VaulticDB metadata. Deny access outside each prefix.
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
