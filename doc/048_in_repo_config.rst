.. _in_repo_config:

In-repository configuration
###########################

Besides the settings passed per invocation, vaultic can store configuration
**inside the repository**. These settings are written once (at ``init`` or via
the ``config`` command) and then apply to every client that opens the
repository. This is compatible with the repository format used by restic and
rustic: both ignore unknown config keys, and vaultic uses the same key names as
rustic so the tools agree on a repository's settings.

Precedence
==========

When several sources provide the same setting, the following order applies
(highest first):

1. Command line flag (e.g. ``--compression max``)
2. Environment variable (e.g. ``VAULTIC_COMPRESSION``)
3. In-repository configuration
4. Built-in default

So a value stored in the repository is a *default* that any invocation can
still override.

Reading the configuration
=========================

``vaultic show-config`` prints the stored configuration without modifying the
repository (no lock is taken). ``vaultic config`` prints it as well, and
``--json`` gives machine-readable output.

Modifying the configuration
===========================

Use ``vaultic config --set-*`` to change settings. The command takes an
exclusive lock, validates the result, and writes it back::

    $ vaultic config --set-compression 19 --set-datapack-size 67108864
    saved new config
    version: 2
    ...
    compression: 19
    datapack_size: 67108864

Most options accept the value ``unset`` to remove a setting and fall back to
the default.

The same ``--set-*`` options are available on ``vaultic init`` to configure a
new repository from the start::

    $ vaultic init --set-compression 19 --set-datapack-size 33554432

Available settings
==================

``--set-compression LEVEL``
    zstd compression level, ``-7``..``22`` (``0`` disables compression).
    Overrides the named modes: ``-7``..``-1`` ≙ ``fastest``, ``1``..``3`` ≙
    ``auto``, ``4``..``9`` ≙ ``better``, ``10``..``22`` ≙ ``max``.

``--set-append-only true|false``
    Enable :ref:`append-only mode <append_only>`.

``--set-extra-verify true|false``
    Verify data before uploading (default ``true``). Corresponds to the
    ``--no-extra-verify`` command line flag.

``--set-chunker rabin|fixed_size``, ``--set-chunk-size``,
``--set-chunk-min-size``, ``--set-chunk-max-size``
    Chunker configuration for newly written data (sizes in bytes). Changing
    these only affects data written afterwards; existing data is unchanged and
    stays readable.

``--set-treepack-size`` / ``--set-datapack-size``,
``--set-*-growfactor``, ``--set-*-size-limit``
    Target pack size for tree and data packs (bytes), an optional grow factor
    that increases the target with the repository size, and a hard size limit
    (``0`` = unlimited, capped by the implementation maximum of 4 GiB).

``--set-min-packsize-tolerate-percent`` / ``--set-max-packsize-tolerate-percent``
    Tolerated pack size deviation in percent of the target, used by
    ``prune --repack-small``.

.. _append_only:

Append-only mode
================

A repository whose config sets ``append_only`` rejects every operation that
modifies or deletes existing data: ``forget``, ``prune``, and changing the
config itself are refused, while ``backup`` and other read/append operations
keep working. This is useful for repositories that ransomware or a compromised
client must not be able to destroy.

To disable append-only mode again you need direct access to the repository
storage (or another client that does not enforce it); this is intentional.

Opening a repository with a master key
======================================

For automation and recovery, a repository can be opened directly with its
master key instead of a password, mirroring rustic's ``--key`` options:

- ``--key KEY`` — base64-encoded JSON master key (``VAULTIC_KEY``)
- ``--key-file FILE`` — read the key from a file (``VAULTIC_KEY_FILE``)
- ``--key-command CMD`` — obtain the key from a command (``VAULTIC_KEY_COMMAND``)

The master key is printed by ``vaultic cat masterkey --json`` (base64-encode
the JSON). **Anyone holding the master key can decrypt the whole repository**,
so protect it like the password.
