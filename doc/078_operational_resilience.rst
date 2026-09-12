Operational resilience
======================

Daemon lifecycle status
-----------------------

VaulticDB starts serving its gRPC endpoint immediately after the transport is
bound, before SlateDB has necessarily finished opening. The ``Health`` response
reports ``ready``, ``state``, ``state_detail``, and ``state_since_unix_ms`` so a
supervisor can distinguish a live daemon that is loading storage from an
unreachable daemon. Storage-dependent RPCs return ``Unavailable`` with the
current lifecycle state until storage is ready.

The complete state vocabulary is ``loading_storage``, ``read_only``,
``read_write``, ``promoting``, ``demoting``, ``fenced``, ``draining``,
``failed``, and ``stopping``. ``read_only``, ``read_write``, and ``fenced``
report ``ready=true``; fenced instances remain reachable for inspection and
controlled recovery while rejecting writes. A storage-open failure leaves the
gRPC status endpoint available in ``failed`` state until the daemon is shut
down, preserving the diagnostic in ``state_detail``.

Client startup distinguishes endpoint absence from an existing daemon that
rejects the connection. Missing or refused endpoints may start a configured
daemon. Authentication failure, repository mismatch, incompatible protocol or
schema, unsafe socket ownership or permissions, and a terminal ``failed``
lifecycle are returned immediately; ``vaultic`` does not launch a replacement
process over those errors.

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
        --force-takeover \
      --reason "confirmed failed host"

    ``--expected-active-epoch`` defaults to ``auto``, which retrieves the observed
    active epoch from the daemon immediately before requesting takeover. Promotion
    still uses an exact conditional update and fails if the active object or epoch
    changes between those operations. A positive integer can be supplied instead
    when an operator must authorize a previously recorded epoch explicitly.

Promotion opens SlateDB as the newly fenced writer before it returns. Large WAL
    replays on remote filesystems can take significant time, so promotion defaults
    to a one-hour timeout. A timeout after the active epoch advances is an uncertain
    outcome: inspect the claim and daemon state instead of immediately issuing
    another takeover.

    Writer transition failures report the state that remains, not the role that was
    requested. A failure before writer close can leave the daemon ``read_write``.
    A failure after writer close leaves it ``fenced`` when a reader remains usable,
    or ``failed`` when storage is unavailable. A failed promotion releases only the
    epoch it acquired and returns to ``read_only`` when it can reopen a reader;
    otherwise it becomes ``fenced`` or ``failed``. Never infer ownership from the
    failed command alone: inspect both lifecycle and writer status before retrying.

    Client cancellation does not cancel an admitted promotion, demotion,
    transaction finalization, or generation rollback. The daemon completes the
    state transition and its accounting in the background. After a deadline or
    disconnect, query status before deciding whether to retry. Writer claim release
    uses a version-conditional released marker rather than an unconditional delete,
    so a stale daemon cannot remove a successor's claim.

The takeover fails if the active object or epoch changed after inspection. It
must not be used merely to resolve ordinary contention.

Generation rollback reconciliation
----------------------------------

A generation rollback first commits generation authority and then advances the
writer epoch. If authority commits but epoch refresh fails, VaulticDB fences the
writer and returns the structured, retryable
``generation_reconciliation_pending`` error. Retrying the same acknowledged
rollback with the same expected decision and report digest resumes fence
refresh; it does not create another generation decision. Startup also advances
the fence before reporting ``read_write`` when durable authority remains in
``rollback-observation`` state.

Do not submit a different rollback while reconciliation is pending. Retry the
same request or restart the daemon with the same repository configuration, then
require matching current and observed writer epochs before resuming mutations.

Recovery capsule migration
--------------------------

Capsule migration persists the exact serialized capsule and its digest before
publishing either the local or mirrored artifact. Completion of each
destination is recorded durably. A retry with the same repository, generation,
and capsule directory resumes the first incomplete publication using the exact
stored bytes; it does not generate a new capsule. A different migration is
rejected while an intention is pending.

Finalization is rejected until both local and mirror publication markers are
present. Only then may VaulticDB remove the stored repository master key and
mark the migration digest finalized.

Structured failures and cleanup
-------------------------------

Expected daemon failures include a machine-readable ``ErrorDetail``. Its
``retryable`` field indicates whether the same request can reasonably succeed
without operator correction. Availability and transition failures are normally
retryable; authentication, authorization, identity, integrity, and validation
failures are not.

Native RADOS multipart completion can publish the final object and then fail to
delete staging objects. That condition is returned explicitly with text stating
that the final object was published. Do not blindly repeat a non-idempotent
publication; inspect the final object and clean the named staging objects.
Explicit multipart abort also reports staged-object deletion failures. Startup
stale-staging cleanup and destructor cleanup remain best-effort so cleanup does
not prevent repository availability.

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
observed epoch. Retrieve that value and use it for the conditional takeover::

    vaultic index writer \
      --repository-id "$REPOSITORY_ID" \
      --daemon-socket "$VAULTICDB_SOCKET" \
      promote \
      --force-takeover \
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