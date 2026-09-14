############################
Read-only NFS snapshot server
############################

``vaultic serve nfs`` publishes one immutable snapshot, or one subfolder of a
snapshot, through NFSv3 over TCP. It is intended for browsing and copying
backup data with a native NFS client when FUSE is unavailable or undesirable.

Starting an export
==================

The snapshot selector accepts ``latest``, a full or abbreviated snapshot ID,
and the ``snapshot:subfolder`` syntax used by restore and dump:

.. code-block:: console

    $ vaultic -r /srv/vaultic-repo serve nfs latest
    $ vaultic -r /srv/vaultic-repo serve nfs 79766175
    $ vaultic -r /srv/vaultic-repo serve nfs latest:/home/user/work

The selector is resolved once. A running ``latest`` export does not change when
a newer snapshot is created. By default, Vaultic listens only on
``127.0.0.1``, with NFS on TCP port ``20490`` and the mount protocol on TCP
port ``20491``. The default export name is ``/snapshot``. The server prints
mount commands containing the effective addresses and ports.

Native client commands
======================

Create an empty mountpoint, then use the command for the client platform. These
examples use the fixed default ports and request a read-only NFSv3 TCP mount.

macOS:

.. code-block:: console

    $ sudo mkdir -p /Volumes/vaultic-snapshot
    $ sudo mount_nfs -o vers=3,proto=tcp,port=20490,mountport=20491,ro 127.0.0.1:/snapshot /Volumes/vaultic-snapshot
    $ sudo umount /Volumes/vaultic-snapshot

Linux:

.. code-block:: console

    $ sudo mkdir -p /mnt/vaultic-snapshot
    $ sudo mount -t nfs -o vers=3,proto=tcp,port=20490,mountport=20491,ro 127.0.0.1:/snapshot /mnt/vaultic-snapshot
    $ sudo umount /mnt/vaultic-snapshot

FreeBSD and other supported BSD clients:

.. code-block:: console

    $ sudo mkdir -p /mnt/vaultic-snapshot
    $ sudo mount_nfs -o vers=3,proto=tcp,port=20490,mountport=20491,ro 127.0.0.1:/snapshot /mnt/vaultic-snapshot
    $ sudo umount /mnt/vaultic-snapshot

The client needs permission to mount, but Vaultic itself does not require root
when using the default unprivileged ports. The NFS and mount ports must be
different and fixed for the lifetime of the process. Vaultic does not use
``rpcbind``.

Network security
================

NFSv3 AUTH_SYS is not authentication: a client controls the UID, GID, and
supplementary groups it sends. NFSv3 traffic is unencrypted. Never expose this
service directly to the Internet or an untrusted LAN.

For remote use, prefer an authenticated VPN and restrict both TCP ports with a
host firewall. Binding a non-loopback address requires both one or more
``--allow-cidr`` options and ``--acknowledge-insecure-nfsv3``:

.. code-block:: console

    $ vaultic -r /srv/vaultic-repo serve nfs \
        --listen 10.8.0.5 --allow-cidr 10.8.0.0/24 \
        --acknowledge-insecure-nfsv3 latest

An SSH tunnel can keep the server on loopback. Forward both fixed ports, then
mount ``127.0.0.1:/snapshot`` on the client:

.. code-block:: console

    $ ssh -N \
        -L 20490:127.0.0.1:20490 \
        -L 20491:127.0.0.1:20491 backup-host

Allow only the configured NFS and mount TCP ports through firewalls. No UDP
listener is opened. Source CIDR filtering is defense in depth and does not make
AUTH_SYS identities trustworthy.

Filesystem behavior and limits
==============================

The export is always read-only. Writes, creates, deletes, renames, ownership or
mode changes, and timestamp changes fail. NFSv2, NFSv4, UDP, rpcbind, NLM file
locking, TLS, Kerberos, writable exports, and NFS extended attributes are not
implemented. ``COMMIT`` is accepted only as a compatibility no-op; it cannot
make a write possible.

Stored UID/GID and mode bits are reported by default. ``--owner server`` maps
displayed ownership to the Vaultic process, while ``--owner root`` reports
root. ``--permissions readable`` adds read and directory traversal bits for
recovery. These presentation options do not make the export writable and do
not strengthen AUTH_SYS.

NFS clients cache file attributes and data. The exported snapshot is immutable,
so cache-friendly behavior is expected, but data already delivered to a client
cannot be revoked. NFS does not expose the archived xattrs; use restore when
those must be recreated.

Vaultic uses bounded process-local caches controlled by ``--tree-cache-size``
and ``--blob-cache-size``. Bound reads and RPC resource use with
``--max-read-size``, ``--max-request-size``, ``--max-connections``,
``--max-requests-per-connection``, ``--connection-idle-timeout``, and
``--handle-limit``. Persistent read-cache targets and quotas are shared with
FUSE and other repository readers. Enabling an NFS export does not implicitly
permit plaintext persistence. A target configured ``plaintext-allowed`` may
retain readable backup data after this process exits; kernel and NFS client
caches may also retain plaintext and cannot be securely erased by Vaultic.

Lifecycle and services
======================

The foreground process owns both listeners and the repository read lock. Stop
it with the normal service-manager stop action or ``Ctrl-C``. Shutdown closes
listeners, drains requests for ``--graceful-drain-timeout``, and releases the
repository. ``--idle-timeout`` stops the server after the configured period
without mounted clients or RPC activity.

``--readiness-file PATH`` atomically writes a mode-0600 JSON record after both
listeners are ready. It contains the PID, repository and snapshot IDs,
subfolder, addresses, export name, and start time, but no password or key. Use
it for service readiness checks; treat stale files as advisory.

For unattended operation, use launchd, systemd, rc.d, or another supervisor to
run the foreground command, restart it according to local policy, restrict its
network and filesystem access, and stop it gracefully. Supply repository
credentials through the existing protected password-file, command, or secret
manager mechanisms. Do not put passwords in a unit file, process arguments, or
this readiness record.

Choosing NFS, FUSE, or restore
==============================

Use ``vaultic serve nfs`` to expose one snapshot or subfolder to native NFS
clients. Use ``vaultic mount`` for the local multi-snapshot FUSE namespace and
xattr browsing. Use ``vaultic restore`` for a durable copy, full metadata and
xattr restoration, large restores, or any workflow that must modify the result.

Troubleshooting
===============

* ``Connection refused`` usually means the Vaultic process is not ready, the
  address or one of the two ports differs, or a firewall blocks TCP traffic.
  Check the readiness file and the effective commands printed at startup.
* A mount that tries ``rpcbind``, UDP, NFSv4, or a dynamic mount port is using
  the wrong options. Specify NFS version 3, TCP, ``port``, and ``mountport``.
* ``permission denied`` while mounting can be a local client privilege issue,
  an excluded source CIDR, or a mismatched ``--export-name``.
* ``read-only file system`` for a mutation is expected. Copy data to writable
  storage or use ``vaultic restore``.
* Numeric owners or inaccessible files reflect archived metadata. Select an
  ``--owner`` or ``--permissions`` presentation policy if appropriate for a
  trusted recovery session.
* If a client remains busy during unmount, stop processes using the mountpoint
  and retry a normal unmount before stopping the server. Avoid forced unmounts
  unless required by the client operating system.