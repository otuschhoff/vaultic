Command Parity Additions
========================

Merge snapshots
---------------

``vaultic merge SNAPSHOT...`` creates a new snapshot that contains the union
of its source trees. If two snapshots contain the same path, the node from the
newest source snapshot wins. The command never changes or deletes the source
snapshots, packs, or indexes: it only writes the new tree blobs and snapshot
under an append lock. The snapshot records its source IDs in additive
``merged_snapshots`` metadata. ``--label`` sets the new snapshot label.

Repository information
----------------------

``vaultic repoinfo`` reports stored object counts and sizes for data packs,
keys, snapshots, and indexes. It is read-only and works with the lock-free
feature mode. Use ``--json`` for stable machine-readable output.

Retention additions
-------------------

``forget`` additionally supports ``--keep-minutely``,
``--keep-quarter-yearly``, ``--keep-half-yearly`` and their corresponding
``--keep-within-*`` duration forms. ``--keep-none`` is an alias of
``--unsafe-allow-remove-all``; it remains an explicit acknowledgement before
a policy may remove every snapshot in a group.

Dump and selector compatibility
-------------------------------

``dump --archive tar.gz`` writes gzip-compressed tar output. With
``--archive auto``, vaultic selects ``tar.gz`` for ``.tar.gz``/``.tgz`` targets,
``zip`` for ``.zip`` targets, and ``tar`` otherwise. ``diff`` accepts
``latest`` and ``latest~N`` selectors like other snapshot commands.

``completions`` is an alias of ``generate`` for rustic-compatible shell
completion and man-page generation.

WebDAV
------

The WebDAV server command is deliberately not implemented. It is excluded
from this parity batch; no WebDAV-related endpoint or server is exposed by
vaultic.