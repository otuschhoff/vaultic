.. _cold_storage:

Cold storage
############

Cold storage is a storage tier that trades lower storage cost for slower and
more expensive retrieval: data must be "restored" (warmed up) before it can be
read. Examples are Amazon S3 Glacier storage classes and OVH Cold Archive.

vaultic supports cold storage in two complementary ways, mirroring rustic.

Hot/cold repositories
=====================

A hot/cold repository splits a repository over two locations: the **cold** part
holds all data packs (and is a complete repository on its own once fully warmed
up), the **hot** part additionally holds all metadata (config, keys, snapshots,
indexes) and the tree packs. Because every command except ``restore`` only
needs metadata, normal operations read from the fast hot part and never touch
the cold storage.

Set it up by pointing ``--repo-hot`` (or ``VAULTIC_REPO_HOT``) at the hot
location and ``-r`` at the cold one::

    # create the cold repository first
    $ vaultic -r /cold/repo init
    # then create the hot part (shares the cold repo's identity and key)
    $ vaultic -r /cold/repo --repo-hot /hot/repo init --hot-only

    # everyday commands use both:
    $ vaultic -r /cold/repo --repo-hot /hot/repo backup /data
    $ vaultic -r /cold/repo --repo-hot /hot/repo snapshots

``init --hot-only`` mirrors the existing metadata (keys, snapshots, indexes and
tree packs) into the hot part; it can also be used to turn a normal repository
into a hot/cold one. The reverse (back to a normal repository) is done by
warming up and removing ``--repo-hot``.

The ``check --check-hot-cold`` command verifies that the hot and cold parts
agree (all metadata present and identical on both).

Warming up cold data
====================

Before reading data packs (e.g. during ``restore``), vaultic must warm them up.
There is no vendor-neutral protocol for that, so vaultic invokes a warm-up
program you supply. This requires the ``warmup-command`` feature (enabled by
default) and is configured with:

- ``--warm-up-command CMD`` — the program to call for the packs that are about
  to be read. The following variables are substituted: ``%id``/``%path`` (one
  pack per invocation) and ``%ids``/``%paths`` (a batch per invocation).
- ``--warm-up-batch N`` — batch size: packs per ``%ids``/``%paths`` invocation,
  or the number of parallel ``%id``/``%path`` invocations.
- ``--warm-up-wait DURATION`` — wait up to this long for the warm-up to take
  effect after the command returned.
- ``--warm-up-wait-command CMD`` — a command that blocks until the requested
  packs are available (alternative to a fixed wait).

Environment variable equivalents: ``VAULTIC_WARM_UP_COMMAND``,
``VAULTIC_WARM_UP_BATCH``, ``VAULTIC_WARM_UP_WAIT`` and
``VAULTIC_WARM_UP_WAIT_COMMAND``.

For S3 Glacier specifically, the built-in ``-o s3.enable-restore=true`` backend
option performs warm-up directly (no external command needed); see the S3
backend section of the documentation.

The warm-up program can report progress on stdout as JSON Lines; each line
``{"type":"pack-progress","warm":N}`` reports how many of the packs in the
current invocation are warm. Other output is logged.

Example (AWS S3 Glacier)::

    $ vaultic -r s3:s3.amazonaws.com/bucket/cold --repo-hot s3:s3.amazonaws.com/bucket/hot \
        --warm-up-command 'aws s3api restore-object --bucket bucket-cold --key %path --restore-request ...' \
        restore latest --target /tmp/restore
