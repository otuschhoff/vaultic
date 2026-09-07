########################################
Taking over a large Rustic repository
########################################

Vaultic can open an existing Rustic repository in place. Taking over the
repository therefore does not require copying, decrypting, re-encrypting, or
repacking its data. This distinction matters for a repository containing, for
example, 120 TB of pack files on storage capable of 300 MB/s: one ideal
sequential read takes about 4.6 days, while reading and writing the same amount
through the same array takes at least about 9.3 days.

There are two independent decisions:

* **CLI takeover** means using ``vaultic`` instead of ``rustic`` while the
  existing Restic-compatible JSON indexes remain authoritative. This is
  immediate and does not require ``vaulticdb`` or ``vaultic index import``.
* **Metadata takeover** means importing repository metadata into SlateDB and
  eventually making ``vaulticdb`` authoritative. This is optional, resumable,
  and does not copy pack bodies, but a complete rollout has significant
  metadata and source-crawl costs.

Do not use ``vaultic copy`` merely to change clients. Copy creates a separate
repository with different encryption keys and reads and rewrites all referenced
data. Use it only when isolation, a new repository identity, or new encryption
keys justify that cost.

Safety rules
============

Before the first Vaultic command:

1. Stop Rustic backup, forget, prune, and repository-maintenance jobs.
2. Ensure that only one product writes the repository at a time. Read-only
   Rustic use remains possible while the legacy JSON index is authoritative,
   but mixed writers complicate locking and configuration ownership.
3. Take a storage-level snapshot or independent copy of repository metadata.
   Preserve at least ``config``, ``keys``, ``index``, and ``snapshots``. A full
   repository snapshot is preferable when the backend supports it cheaply.
4. Record the exact source paths, snapshot hostname, tags, excludes, and Rustic
   repository options. Parent selection depends on these values.
5. Test with the Vaultic version that will run in production.

The examples below use environment variables so credentials do not appear in
the process list:

.. code-block:: console

   $ export VAULTIC_REPOSITORY=/srv/backup/repository
   $ export VAULTIC_PASSWORD_FILE=/etc/vaultic/repository-password
   $ export VAULTIC_FEATURES=slatedb-authoritative=true

``VAULTIC_REPOSITORY`` is equivalent to ``--repo``. ``VAULTIC_PASSWORD_FILE``
is equivalent to ``--password-file`` and names a protected file containing the
existing repository password. ``VAULTIC_FEATURES`` enables the currently alpha
SlateDB authority path; it is not required while the repository remains in
legacy mode, but every Vaultic process needs it after activation.

Stage 1: prove zero-copy compatibility
======================================

Start with read-only commands against the existing repository:

.. code-block:: console

   $ vaultic snapshots
   $ vaultic check
   $ vaultic restore latest \
         --include /small/representative/path \
         --target /srv/restore-test

``snapshots`` proves that Vaultic can decrypt snapshot metadata. ``check``
validates repository structure and index references; it does not replace a
separately scheduled full pack-data verification. ``restore latest`` performs
an end-to-end sample restore. ``--include`` limits the selected snapshot paths,
and ``--target`` chooses an empty restore destination.

At this point Vaultic can take over backup jobs without any SlateDB migration.
This is the fastest route when the immediate goal is replacing the executable
rather than enabling Vaultic's large-repository metadata features.

Backup and import ordering
==========================

``vaultic backup`` and ``vaultic index import`` do different work. Neither is a
prerequisite for running the other while the legacy JSON index remains
authoritative.

.. list-table:: Ordering choices
   :header-rows: 1
   :widths: 24 35 41

   * - Order
     - What happens
     - Consequence
   * - Backup before import
     - Vaultic writes a normal compatible snapshot, tree packs, data packs, and
       JSON indexes. It does not populate SlateDB.
     - The later import includes that new snapshot and index. This is useful as
       a production confidence step, but the crawl does not count as the first
       authoritative VaulticDB baseline crawl.
   * - Pack-only import before backup
     - ``index import --snapshot-depth 0`` imports JSON index locations and pack
       statistics without traversing snapshot trees. Authority remains legacy.
     - The next backup behaves exactly like a legacy backup. Rerun the resumable
       import afterward to pick up indexes created by that backup.
   * - Complete import before backup, without activation
     - Pack metadata and retained snapshot trees are imported, but repository
       commands still use the legacy JSON index.
     - This permits comparison before committing to VaulticDB. A backup in this
       state still does not reconcile live inode metadata into SlateDB.
   * - Complete import, activate, then backup
     - SlateDB becomes authoritative before the next backup.
     - The next backup performs the mandatory live baseline reconciliation. It
       can be very expensive, but subsequent verified backups can use the
       VaulticDB fast path.

The recommended low-risk sequence is:

1. Perform the read-only checks and, if desired, one production backup in
   legacy mode.
2. Initialize an empty, encrypted SlateDB store.
3. Import pack metadata without activation to measure index scale and
   VaulticDB performance.
4. Import snapshot trees and run differential checks.
5. Put ``vaulticdb`` under service management.
6. Activate SlateDB authority.
7. Run one complete, non-pathdiff baseline backup.
8. After validation, migrate the metadata DEK and existing repository master
   key into a quorum recovery capsule and retire the legacy key routes.

Stage 2: initialize encrypted SlateDB
=====================================

For a large import, enable metadata encryption before writing any imported
records. Encrypting an existing plaintext SlateDB is supported, but it rewrites
the existing metadata objects and is avoidable migration work.

Obtain the identity of the existing repository and prepare a high-entropy
recovery passphrase with organization-approved secret tooling:

.. code-block:: console

   $ export REPOSITORY_ID=$(vaultic cat config | jq -r .id)
   $ chmod 600 /etc/vaultic/metadata-recovery
   $ vaultic index encrypt \
         --repository-id "$REPOSITORY_ID" \
         --metadata-recovery-passphrase-file /etc/vaultic/metadata-recovery \
         --daemon-data-dir /srv/vaulticdb \
         --start-daemon

``index encrypt`` starts the empty store in one-time ``initialize`` mode. It
creates the SlateDB metadata DEK and its authenticated key envelope before
opening the database, writes the persistent encryption-required policy, and
mirrors the envelope into the repository. It does not yet place the existing
repository master key in the envelope or in SlateDB.

``--repository-id`` binds the envelope and encrypted objects to this repository.
``--metadata-recovery-passphrase-file`` creates the initial local Argon2id
recovery slot; the file must not be readable by group or other users.
``--daemon-data-dir`` selects the durable metadata-store root, and
``--start-daemon`` starts a temporary daemon when one is not already running.

Every subsequent daemon start must use ``required`` mode. It must never use
``initialize`` again. For on-demand commands, pass
``--metadata-encryption required`` and the recovery passphrase file. For a
service, use the equivalent daemon environment shown below.

The metadata envelope and repository encryption key have different roles. The
envelope initially protects only the SlateDB metadata DEK. Stage 8 moves both
that DEK and the existing repository master key into a broker-controlled quorum
recovery capsule, after the takeover has been validated.

Stage 3: fast pack-metadata import
==================================

Place the VaulticDB data directory on redundant SSD or NVMe storage rather than
on the pack array. The pack-only import reads legacy JSON indexes, writes the
blob-to-pack catalog, and performs approximately one backend ``Stat`` per pack.
It does not stream pack contents:

.. code-block:: console

   $ vaultic index import \
         --from-legacy \
         --snapshot-depth 0 \
         --start-daemon \
         --daemon-data-dir /srv/vaulticdb \
         --metadata-encryption required \
         --metadata-recovery-passphrase-file /etc/vaultic/metadata-recovery

Flags used here:

``--from-legacy``
   Selects the Restic/Rustic JSON indexes as the import source. This is
   currently the required source mode and defaults to true.

``--snapshot-depth 0``
   Disables snapshot-tree traversal. This is what makes the first import fast
   for a repository with billions of inodes. Do not combine this abbreviated
   import with ``--activate``.

``--start-daemon``
   Connects to a compatible daemon if one is already listening, otherwise
   starts ``vaulticdb`` and waits for it to become ready.

``--daemon-data-dir /srv/vaulticdb``
   Stores local SlateDB objects below this directory. The daemon appends a
   repository-identity hash. Use durable, backed-up storage with ample free
   space and metadata IOPS.

``--metadata-encryption required``
   Refuses plaintext metadata and refuses to initialize a replacement
   envelope. It must match the policy written by ``index encrypt``.

``--metadata-recovery-passphrase-file``
   Unlocks the initial metadata-DEK envelope. This bootstrap route is replaced
   by a broker lease after the quorum migration in Stage 8.

``--resume``
   Defaults to true. Completed source-index and snapshot checkpoints are
   skipped on later runs, so interruption does not restart completed work.

Without ``--persistent-daemon``, a daemon started by this command is shut down
when the command closes. An already-running daemon is never stopped by a client
that merely attached to it.

Inspect the staged result by starting the same local store again:

.. code-block:: console

   $ vaultic index check \
         --start-daemon \
         --daemon-data-dir /srv/vaulticdb \
         --metadata-encryption required \
         --metadata-recovery-passphrase-file /etc/vaultic/metadata-recovery \
         --include-crawl-debt

``index check`` compares legacy locations and snapshots with SlateDB and checks
pack aggregates. ``--include-crawl-debt`` includes individual deferred or
unknown facts in the report. Add ``--fail-on-warning`` in automation when
expected incompleteness should produce exit status 2.

For an unattended import, ``--batch-size N`` limits mutations per daemon
transaction; zero uses the negotiated daemon limit. ``--work-budget N`` limits
blob records examined, ``--snapshot-work-budget N`` limits snapshot nodes, and
``--max-errors N`` stops after that many source findings. A reached budget exits
as incomplete and is suitable for controlled windows, but a partially traversed
snapshot is not checkpointed until that snapshot completes. Avoid a very small
snapshot budget when one snapshot itself contains billions of nodes.

``--dry-run`` validates without writing SlateDB. It adds another full metadata
pass, so a storage snapshot plus a resumable real import is usually faster for a
very large, trusted repository.

Stage 4: import snapshot metadata
=================================

Rerun import without ``--snapshot-depth 0``:

.. code-block:: console

   $ vaultic index import \
         --from-legacy \
         --start-daemon \
         --daemon-data-dir /srv/vaulticdb \
         --metadata-encryption required \
         --metadata-recovery-passphrase-file /etc/vaultic/metadata-recovery

The default snapshot depth is unlimited. Existing pack/index checkpoints are
reused, while every retained snapshot tree is traversed. This reads tree blobs,
not every data-pack payload, but it can create a very large number of inode,
directory, content-manifest, and reverse-reference records. Runtime and
VaulticDB capacity depend on the number of retained snapshots and nodes, not
only on the 120 TB physical pack size.

Imported tree metadata is marked ``imported``, not ``verified``. Snapshot tree
records describe what Rustic observed during an old backup; they cannot prove
the current filesystem identity, parent inode, ``ctime``, or freshness. Missing
identities and incomplete relationships remain explicit crawl debt.

Run ``index check`` again and preserve its output. Do not activate while it
reports actual legacy/SlateDB differences. Crawl-debt warnings are expected
until a live baseline backup reconciles them.

Stage 5: manage the VaulticDB process
=====================================

Both on-demand and service-managed operation use the same daemon binary and
database. Their lifecycle differs.

Temporary on-demand daemon
--------------------------

``--start-daemon`` starts VaulticDB only when no compatible daemon is already
available. Without ``--persistent-daemon``, the starting command sends a
shutdown request when it closes. This is convenient for import experiments and
pre-activation checks.

Persistent on-demand daemon
---------------------------

Adding ``--persistent-daemon`` leaves a newly started daemon running:

.. code-block:: console

   $ vaultic index import \
         --from-legacy \
         --start-daemon \
         --persistent-daemon \
         --daemon-data-dir /srv/vaulticdb \
         --metadata-encryption required \
         --metadata-recovery-passphrase-file /etc/vaultic/metadata-recovery

This works for migration and later commands can attach to it, but it is not a
service manager: it does not restart VaulticDB after a crash or host reboot.

Systemd service
---------------

For production authority, run VaulticDB as a supervised service before
activation. The daemon and Vaultic CLI must run as the same OS account because
Vaultic requires the Unix socket and its parent directory to be owner-only.
Obtain the repository ID with:

.. code-block:: console

   $ vaultic cat config | jq -r .id

Create a mode-0600 environment file such as
``/etc/vaultic/vaulticdb-archive.env``:

.. code-block:: text

   VAULTICDB_REPOSITORY_ID=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef
   VAULTICDB_OBJECT_STORE=local
   VAULTICDB_DATA_DIR=/srv/vaulticdb
   VAULTICDB_ENCRYPTION=required
   VAULTICDB_ENCRYPTION_PASSPHRASE_FILE=/etc/vaultic/metadata-recovery

The repository ID makes VaulticDB derive the same repository-scoped default
socket that ordinary Vaultic commands use. A system unit can be:

.. code-block:: ini

   [Unit]
   Description=VaulticDB metadata service for archive
   After=local-fs.target

   [Service]
   Type=simple
   User=vaultic
   Group=vaultic
   UMask=0077
   EnvironmentFile=/etc/vaultic/vaulticdb-archive.env
   Environment=VAULTICDB_RUNTIME_DIR=/run/vaulticdb
   RuntimeDirectory=vaulticdb
   RuntimeDirectoryMode=0700
   ExecStart=/usr/local/bin/vaulticdb
   Restart=on-failure
   RestartSec=5s
   LimitCORE=0
   NoNewPrivileges=yes
   ProtectSystem=strict
   ReadWritePaths=/run/vaulticdb /srv/vaulticdb

   [Install]
   WantedBy=multi-user.target

``RuntimeDirectory=`` makes systemd create ``/run/vaulticdb`` as the service
account with mode ``0700`` and remove it when the unit stops. Set
``VAULTICDB_RUNTIME_DIR=/run/vaulticdb`` for ordinary Vaultic commands that
connect to this service as well. VaulticDB refuses to adopt an existing runtime
path that is a symlink, has another owner, or is not mode ``0700``. Adjust the
account, binary path, and data path to the installation. These settings use the
initial recovery-passphrase slot; Stage 8 replaces that operational route with
broker settings after the takeover is validated.

After installing the unit, stop any persistent on-demand daemon so it releases
the repository endpoint, then start and enable the service. The singleton lock
prevents two daemons from owning the same endpoint.

Stage 6: activate SlateDB authority
===================================

Activation writes the authoritative repository marker last. From then on,
Vaultic fails closed if a compatible daemon is unavailable; it does not silently
fall back to stale JSON metadata.

With the systemd service running, reuse the completed import checkpoints and
activate:

.. code-block:: console

   $ vaultic index import --from-legacy --activate

``--activate`` is accepted only after a complete import with no source errors.
It writes the repository marker that selects SlateDB on subsequent opens. It
cannot be combined with ``--dry-run``. Keep the legacy JSON indexes: Vaultic
continues to publish them as a deterministic compatibility and recovery view.

Immediately verify daemon-backed operation:

.. code-block:: console

   $ vaultic index check --include-crawl-debt
   $ vaultic snapshots
   $ vaultic restore latest \
         --include /small/representative/path \
         --target /srv/restore-test-after-activation

Do not run prune or VaulticDB garbage collection until import, activation, the
baseline backup, differential checks, and representative restores have all
succeeded.

Stage 7: the mandatory authoritative baseline
==============================================

If VaulticDB's inode and path metadata will be authoritative, one complete live
source crawl after activation is mandatory. A backup performed before
activation does not populate VaulticDB, and imported snapshot metadata is not a
live freshness proof.

Run the first authoritative backup without ``--use-pathdiff``:

.. code-block:: console

   $ vaultic backup \
         --host original-rustic-hostname \
         --use-cwalk \
         --cwalk-concurrency 32 \
         /exact/original/source/path

``--host`` preserves parent-snapshot selection when the current machine name
differs from the hostname stored by Rustic. Use the actual old hostname; omit
the flag when it is unchanged. The source path must match the original backup
path exactly.

``--use-cwalk`` enables the parallel filesystem walker. It accelerates metadata
enumeration but does not weaken correctness. ``--cwalk-concurrency 32`` sets
the traversal worker count; benchmark values such as 16, 32, and 64 against the
source filesystem instead of assuming that more workers are faster.

This first authoritative run is more than a directory listing. Imported or
missing inode records are not ``verified``, so Vaultic deliberately rejects the
unchanged-file fast path and follows the normal file read and rechunk path. For
a 500 TB logical dataset, plan for source reads approaching that scale.
Content-addressed deduplication should prevent unchanged content from creating
another 120 TB of pack uploads, but source throughput, hashing CPU, and metadata
IO can make this the longest migration stage.

After the backup, run ``vaultic index check`` again. Pending debt for sources
outside that backup scope or nodes that failed during the crawl remains pending
and must not be treated as verified.

Stage 8: move both keys to the key broker
==========================================

Do this only after the authoritative baseline, differential checks, and sample
restores succeed. The final step deliberately removes every repository password
key, so Rustic and password-based Vaultic access stop working. From then on, a
running and unlocked broker is required to obtain both the metadata DEK and the
repository master key.

First copy the existing repository master key into encrypted SlateDB while the
repository password and metadata recovery slot still work:

.. code-block:: console

   $ vaultic index keys store-master-key \
      --repository-id "$REPOSITORY_ID" \
      --confirm
   $ vaultic index keys status --repository-id "$REPOSITORY_ID"

``store-master-key`` decrypts the existing repository key through the normal
password route and stores one immutable copy inside encrypted SlateDB.
``--confirm`` acknowledges that sensitive key material will be added to the
metadata database. This is an intermediate migration state, not the final
broker-only state. Do not remove the repository password keys yet.

Next create separate broker-identity and release-signing keypairs, and sign the
installed ``vaultic`` and ``vaulticdb`` executables. The integer release version
must increase whenever an authorized executable changes:

.. code-block:: console

   $ vaultic-key-broker identity-init \
      /secure/broker-identity.key /etc/vaultic/broker-identity.pub
   $ vaultic-key-broker identity-init \
      /secure/release-signing.key /etc/vaultic/release-signing.pub
   $ vaultic-key-broker release-sign /secure/release-signing.key \
      /usr/local/bin/vaultic vaultic 1 production \
      /etc/vaultic/vaultic.release.json
   $ vaultic-key-broker release-sign /secure/release-signing.key \
      /usr/local/bin/vaulticdb vaulticdb 1 production \
      /etc/vaultic/vaulticdb.release.json

``identity-init`` creates a mode-0600 Ed25519 private key and a public key. The
broker identity signs unlock sessions; the separate release key authorizes
specific client binaries. ``release-sign`` binds the executable digest,
component name, monotonically increasing version, and release identity into a
manifest. Configure the broker with the repository ID, capsule directory,
identity key, socket, trusted release public key, service UID, and only the
capabilities each executable requires. At minimum, authorize ``vaulticdb`` for
``metadata-dek`` and authorize ``vaultic`` for ``repository-master-key`` and
``metadata-loss-recovery``; topology discovery and policy mutation require
their separately named capabilities. Use the shipped
``contrib/systemd/vaultic-key-broker.service`` and the full configuration and
authorization procedure in :doc:`070_encryption`.

Prepare a 2-of-3 offline quorum capsule while the temporary key-in-DB route is
still available:

.. code-block:: console

   $ vaultic index keys quorum migrate-prepare \
      --repository-id "$REPOSITORY_ID" \
      --capsule-directory /var/lib/vaultic/capsules \
      --generation 1 \
      --group operators \
      --threshold 2 \
      --broker-public-key /etc/vaultic/broker-identity.pub \
      --member alice=offline-argon2id:/secure/alice.passphrase \
      --member bob=offline-keyfile:/media/bob/member.key \
      --member carol=offline-keyfile:/media/carol/member.key \
      --state-file /secure/capsule-migration.json

``migrate-prepare`` reads the metadata DEK and database copy of the repository
master key, wraps both in a new recovery capsule, publishes immutable local and
repository copies, verifies reconstruction, and records the exact capsule
digest in the mode-0600 ``--state-file``. ``--threshold 2`` requires any two of
the three listed members. ``--generation 1`` is the first immutable capsule
generation. Preparation is intentionally non-destructive: all old access routes
remain available if broker setup or quorum testing fails.

Install and start the broker with that capsule, then unlock it through a fresh
authenticated contribution session. Each custodian must independently verify
the displayed fingerprint before contributing. The complete ceremony and
cloud, YubiKey, FIDO2, and Secure Enclave alternatives are documented in
:doc:`070_encryption`.

Before finalization, prove that both services can use broker leases. Configure
VaulticDB with these replacements for the passphrase settings:

.. code-block:: text

   VAULTICDB_ENCRYPTION=required
   VAULTICDB_BROKER_SOCKET=/run/vaultic/key-broker.sock
   VAULTICDB_RELEASE_MANIFEST=/etc/vaultic/vaulticdb.release.json
   VAULTICDB_BROKER_LEASE_SECONDS=3600

Restart VaulticDB during a maintenance window and verify it opens the encrypted
store. Then remove ``VAULTIC_PASSWORD_FILE`` from a test shell and prove direct
repository access through the broker:

.. code-block:: console

   $ unset VAULTIC_PASSWORD_FILE
   $ vaultic snapshots \
      --key-broker-socket /run/vaultic/key-broker.sock \
      --key-broker-release-manifest /etc/vaultic/vaultic.release.json

``--key-broker-socket`` requests a short-lived ``repository-master-key`` lease.
``--key-broker-release-manifest`` proves that the exact running Vaultic binary
is authorized. Brokered unlock is mutually exclusive with password, direct-key,
Azure-secret, and ``--metadata-key-in-db`` routes.

Inventory and destroy any externally copied standalone escrow or plaintext
master-key files. Only after the capsule, quorum combinations, broker restart,
VaulticDB restart, and sample repository access have all been tested, finalize:

.. code-block:: console

   $ vaultic index keys quorum migrate-finalize \
      --repository-id "$REPOSITORY_ID" \
      --state-file /secure/capsule-migration.json \
      --confirm \
      --retire-legacy-routes \
      --confirm-standalone-escrow-destroyed \
      --key-broker-socket /run/vaultic/key-broker.sock \
      --key-broker-release-manifest /etc/vaultic/vaultic.release.json

Finalization first authenticates the repository configuration and a pack through
the capsule lease. Only after that proof does it remove every repository
password-key record and mirrored standalone escrow, then atomically remove the
database copy of the master key. ``--retire-legacy-routes`` requests that
irreversible removal. ``--confirm-standalone-escrow-destroyed`` asserts that no
external standalone escrow or plaintext export remains; it does not destroy
external files itself. If retirement fails partway, the database key remains so
the same finalization can be retried.

After finalization, configure every Vaultic job with ``--key-broker-socket`` and
``--key-broker-release-manifest`` or their ``VAULTIC_KEY_BROKER_SOCKET`` and
``VAULTIC_KEY_BROKER_RELEASE_MANIFEST`` environment equivalents. A broker or
host restart starts locked and requires a new quorum ceremony. Losing every
valid capsule copy or enough independent member credentials makes both SlateDB
metadata and repository data unrecoverable. "Broker-only" means that an
authorized Vaultic process receives the master-key bytes in protected memory
for the bounded lease; the broker is not on the bulk pack-encryption data path.

Pathdiff during and after takeover
==================================

Pathdiff cannot replace the first complete authoritative baseline. Selective
reuse intentionally does not claim that descendants of reused directories were
observed, so it cannot establish complete live inode state or clear all import
debt. Run the baseline backup with full ``cwalk`` traversal.

After a successful baseline, pathdiff can accelerate later backups when its
service proves continuous observation from no later than the selected parent
snapshot through backup start:

.. code-block:: console

   $ vaultic backup \
         --host original-rustic-hostname \
         --use-cwalk \
         --cwalk-concurrency 32 \
         --use-pathdiff \
         --pathdiff-endpoint /run/pathdiff/control.sock \
         --pathdiff-svm-map /etc/vaultic/archive-svm-map.json \
         --pathdiff-require-coverage \
         /exact/original/source/path

``--use-pathdiff`` requests selective parent-subtree reuse. It requires
``--use-cwalk``. ``--pathdiff-endpoint`` identifies the running pathdiff Unix
socket, and ``--pathdiff-svm-map`` supplies the exact source-to-LIF, SVM, and
volume mapping described in :doc:`040_backup`. The map identifies topology; it
does not prove event coverage.

``--pathdiff-require-coverage`` makes the command fail if the service cannot
prove uninterrupted coverage. Without it, Vaultic safely falls back to a full
cwalk. During normal operation this fallback is conservative and useful; during
the planned baseline, omit pathdiff entirely so the full crawl is explicit.

Rollback boundary
=================

Before activation, rollback is simply stopping the staged daemon and continuing
with Rustic or Vaultic's legacy engine. Pack files and JSON indexes were not
changed by import.

After activation, do not delete the authoritative marker or guess at a rollback.
Vaultic intentionally fails closed to prevent divergent metadata histories. Use
the documented metadata-loss recovery and healing procedures in
:doc:`078_operational_resilience`, preserve both the SlateDB store and legacy
indexes, and investigate before allowing another writer.

After quorum migration finalization there is an additional boundary: the
repository password-key records no longer exist. Rustic and password-based
Vaultic access cannot be restored by stopping the broker or switching metadata
engines. Recovery requires a valid capsule, enough member credentials, an
authorized client, and an unlocked broker. Keep independently protected copies
of every current capsule and test the quorum recovery procedure before
finalization.
