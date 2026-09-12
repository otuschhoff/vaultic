Operational resilience
======================

Writer ownership
----------------

VaulticDB can remain available as a read-only metadata service after it closes
and relinquishes its SlateDB writer. Inspect and control the role with::

    vaultic index writer status --repository-id REPOSITORY_ID
    vaultic index writer demote --repository-id REPOSITORY_ID --reason MAINTENANCE
    vaultic index writer promote --repository-id REPOSITORY_ID --reason COMPLETE

Demotion rejects new mutations, drains admitted work and transactions, flushes
and closes the writer, and only then releases ownership. Promotion acquires an
exclusive epoch through the configured strongly consistent coordination store
before opening a writer. Replicated metadata stores require a single designated
``VAULTICDB_FENCING_REPLICA``; metadata replicas never elect writers
independently.

A process crash can leave an active-writer claim. Confirm that the former
writer cannot return, record the epoch shown by ``writer status``, and use the
explicit conditional takeover::

    vaultic index writer promote --repository-id REPOSITORY_ID \
      --force-takeover --expected-active-epoch EPOCH \
      --reason "confirmed failed host"

The takeover fails if the active object or epoch changed after inspection. It
must not be used merely to resolve ordinary contention.

Recover a crashed encrypted writer on Linux
-------------------------------------------

VaulticDB releases its active-writer claim during a graceful shutdown. A crash,
forced termination, or interrupted ``--start-daemon`` command can leave that
claim behind. The next VaulticDB process then starts read-only. Before taking
over the claim, prove that the former process cannot return. Run these checks as
the same OS account that owns the VaulticDB service and socket::

    pgrep -a -x vaulticdb
    ss -xlpn | grep -F /run/vaulticdb-rustic/vaulticdb.sock

Both commands must show that no former VaulticDB process owns the repository or
socket. Stop and resolve any live or supervised process before continuing. Do
not delete ``_vaultic/active-writer`` or any object below
``_vaultic/writer-epochs``; those objects provide the fencing history required
for a safe takeover.

Set the repository identity and the socket used for recovery, then create its
owner-only runtime directory::

    export REPOSITORY_ID="$(vaultic cat config | jq -r .id)"
    export VAULTICDB_SOCKET=/run/vaulticdb-rustic/vaulticdb.sock
    install -d -m 0700 "$(dirname "$VAULTICDB_SOCKET")"

Start VaulticDB in the foreground with the same storage and encryption settings
as the failed process. ``VAULTICDB_TOPOLOGY_SOURCE=external`` is required when
the topology comes from these environment variables::

    VAULTICDB_REPOSITORY_ID="$REPOSITORY_ID" \
    VAULTICDB_SOCKET="$VAULTICDB_SOCKET" \
    VAULTICDB_TOPOLOGY_SOURCE=external \
    VAULTICDB_OBJECT_STORE=local \
    VAULTICDB_DATA_DIR=/volume2/NASDA2/rustic/db \
    VAULTICDB_ENCRYPTION=required \
    VAULTICDB_ENCRYPTION_PASSPHRASE_FILE=/volume2/NASDA2/rustic/etc/metadata-recovery \
    RUST_BACKTRACE=1 \
    vaulticdb 2>&1 | tee /tmp/vaulticdb-startup.log

Keep that process running. In another shell, inspect its writer status through
the recovery socket::

    vaultic index writer \
      --repository-id "$REPOSITORY_ID" \
      --daemon-socket "$VAULTICDB_SOCKET" \
      status

A stale claim reports a read-only role, current epoch zero, and a non-zero
observed epoch. Use that exact observed value for the conditional takeover::

    export EPOCH=OBSERVED_EPOCH
    vaultic index writer \
      --repository-id "$REPOSITORY_ID" \
      --daemon-socket "$VAULTICDB_SOCKET" \
      promote \
      --force-takeover \
      --expected-active-epoch "$EPOCH" \
      --reason "confirmed previous vaulticdb process terminated"

Run ``writer status`` again and require a read-write role with matching current
and observed epochs. Commands such as ``vaultic index import`` can then attach
with ``--daemon-socket "$VAULTICDB_SOCKET"`` and must omit ``--start-daemon``.
The foreground terminal and ``/tmp/vaulticdb-startup.log`` retain startup
diagnostics. After recovery, install the same environment in a service manager
so future daemon exits and logs are supervised.

Deferred ingest journals
------------------------

An authenticated ``sealed-pending`` journal proves that its referenced packs
and journal segments met the configured staging durability policy when sealed.
It is not a committed backup: it has no normal snapshot visibility, must not be
used as an incremental basis, and does not satisfy an ordinary backup success
or restorability guarantee. Normal restorability begins only after successful
Plan A reconciliation publishes the snapshot and completion record.

The repository configuration names staging mirrors with the additive
``staging_backends`` field. Entries must name opened ``placement_backends``.
Credentials are never stored in this list or in journal objects; backend
credential providers resolve them at runtime.

Inspect authenticated jobs and their complete segment chains with::

    vaultic index staging status
    vaultic index staging inspect JOB_ID

Sealed and expired jobs protect referenced packs from prune and GC. Expiry does
not delete data or remove protection. Abandonment is a high-severity operation
that publishes an immutable audit record and starts a safety delay::

    vaultic index staging abandon JOB_ID \
      --reason "source backup superseded and verified" \
      --acknowledge-data-loss --safety-delay 24h

No pack is deleted by this command. Destructive maintenance rereads journal
state immediately before deletion and fails closed if staging state cannot be
authenticated.

Bootstrap material
------------------

Bootstrap profiles and topology manifests contain repository identities and
credential-free backend locators only. Generation anchors and protected
offline exports must be stored as mode-0600 files in a protected location.
Startup rejects foreign repository identities, authentication failures,
same-generation conflicts, and generations below the trusted anchor.