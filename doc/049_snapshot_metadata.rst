.. _snapshot_metadata:

Snapshot metadata, labels and filtering
########################################

Snapshots can carry additional metadata beyond the classic host/paths/tags:

- **label** — a short free-form label, set at backup time with
  ``--label LABEL``. Labels are shown by ``snapshots`` (a ``Label`` column
  appears when any snapshot is labeled) and can be used to group
  (``--group-by label``) and filter snapshots.
- **description** — a longer free-form description, set with
  ``--description TEXT`` or read from a file with ``--description-from FILE``.
- **delete protection** — mark a snapshot so ``forget``/``prune`` will not
  remove it (see below).

These fields are stored in the snapshot in the same JSON keys rustic uses, so
labels, descriptions and delete protection are understood by both tools.

Delete protection
=================

Mark a snapshot as protected at backup time::

    $ vaultic backup --delete-never /data
    $ vaultic backup --delete-after 30d /data   # not deletable for 30 days

``--delete-never`` protects the snapshot indefinitely; ``--delete-after``
protects it until the given duration has elapsed. ``forget`` keeps protected
snapshots (reporting how many were kept) regardless of the retention policy;
use ``--override-delete-protection`` to delete them anyway.

Protection can be changed afterwards with ``tag``::

    $ vaultic tag --set-delete never SNAPSHOT_ID
    $ vaultic tag --set-delete none SNAPSHOT_ID     # clear protection
    $ vaultic tag --set-label quarterly SNAPSHOT_ID
    $ vaultic tag --set-description "..." SNAPSHOT_ID

Filtering snapshots
===================

All commands that select snapshots (``snapshots``, ``forget``, ``restore``,
``ls``, ``find``, ``diff``, ``dump``, ``copy``, ``tag``, ``stats``, ``check``)
accept the classic ``--host``, ``--tag`` and ``--path`` filters plus the
rustic-compatible extended filters:

- ``--filter-label LABEL`` — match a label
- ``--filter-paths-exact a,b`` / ``--filter-tags-exact t1,t2`` — match
  snapshots whose path/tag set is exactly the given list (no subset matching)
- ``--filter-after TIME`` / ``--filter-before TIME`` — match by date/time
  (RFC3339 or ``YYYY-MM-DD[ HH:MM:SS]``)
- ``--filter-size MIN[:MAX]`` / ``--filter-size-added MIN[:MAX]`` — match by
  total size / bytes added to the repository (suffixes k/m/g/t)
- ``--filter-last N`` — only the newest ``N`` matching snapshots

Snapshot references additionally accept rustic's ``latest~N`` syntax to address
the N-th latest matching snapshot, e.g.::

    $ vaultic restore latest~2 --target /tmp/restore
    $ vaultic diff latest~1 latest
