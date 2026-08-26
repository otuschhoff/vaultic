Profiles, hooks, and observability
===================================

Profiles
--------

vaultic can load local TOML profiles before running a command. Select one or
more profiles with ``-P`` / ``--use-profile``. Without ``-P``, vaultic looks
for ``vaultic.toml`` in the current directory, then
``$XDG_CONFIG_HOME/vaultic`` (or ``~/.config/vaultic``), and finally
``/etc/vaultic``. A named profile such as ``-P nightly`` resolves to
``nightly.toml`` in those locations; an explicit path may also be used.

Profiles can include other profiles with ``use-profiles``. Includes are loaded
first and cycles are rejected. Values merge in this order, from lowest to
highest priority: included profiles, earlier ``-P`` profiles, later ``-P``
profiles, ``VAULTIC_*`` (with ``RESTIC_*`` fallback), and explicit CLI flags.

The ``[global]`` and ``[repository]`` tables contain global flags. A command
table uses the command's flag names:

.. code-block:: toml

   use-profiles = ["base.toml"]

   [global]
   repo = "/srv/vaultic/repository"
   no-progress = true
   log-file = "/var/log/vaultic.log"

   [backup]
   tag = ["server", "nightly"]
   read-concurrency = 4

   [forget]
   keep-last = 14

Rustic-style aliases are accepted for existing deployment profiles:
``[repository].repository`` maps to ``--repo``;
``set-compression = N`` maps its zstd level to vaultic's closest runtime
compression mode; ``packsize-default = "128MiB"`` maps to ``--pack-size``;
and ``packsize-tree = "8MiB"`` maps to ``--tree-pack-size``. These pack-size
profile settings are runtime overrides for the invoking command; use
``vaultic config --set-*`` when the values must be persisted in repository
configuration. A ``[global].group-by`` value is applied to backup, forget, and
snapshots, which own that flag.

In ``[backup]`` and ``[[backup.snapshots]]``, rustic ``globs`` are accepted.
Entries beginning with ``!`` are translated to vaultic exclusion patterns, so
for example ``"!**/.snapshot/**"`` excludes NetApp snapshot directories.
TOML arrays map to repeated command flags; this applies to
``exclude-if-present``, ``globs``, and similar array-valued options.

Named backup jobs
-----------------

``[[backup.snapshots]]`` describes one or more no-argument backup jobs. Each
job must have a ``name`` and ``sources``; it may set normal backup flags such
as ``label``, ``tag``, or ``exclude``. Running ``vaultic backup`` without
paths runs all configured jobs. Use ``vaultic backup --name name`` to run one
or more named jobs.

.. code-block:: toml

   [[backup.snapshots]]
   name = "home"
   sources = ["/home/alice"]
   label = "home"
   tag = ["daily"]

``backup --init`` initializes the repository when it does not exist before
running the backup. ``backup --ls`` lists the contents of the snapshot after
it is created.

Hooks
-----

Put hooks in ``[global.hooks]``, ``[repository.hooks]``, a command's
``[backup.hooks]`` table, or an individual
``[backup.snapshots.hooks]`` table. Hooks run in global, repository, then
command/job order. Supported lists are ``run-before``, ``run-after``,
``run-failed``, and ``run-finally``. A hook can be a command string or a table
with ``command``, ``args``, and ``on-failure`` (``error``, ``warn``, or
``ignore``):

.. code-block:: toml

   [backup.hooks]
   run-before = ["/usr/local/bin/backup-window-open"]

   [[backup.hooks.run-after]]
   command = "/usr/local/bin/notify-backup"
   args = ["--channel", "ops"]
   on-failure = "warn"

Commands are split into executable plus arguments and are **not** passed to a
shell. Use ``sh -c`` explicitly when shell syntax is required. Hooks receive
``VAULTIC_HOOK_TYPE``, ``VAULTIC_ACTION``, ``VAULTIC_BACKUP_LABEL``,
``VAULTIC_BACKUP_SOURCES`` (newline separated), ``VAULTIC_BACKUP_TAGS``, and
``VAULTIC_SNAPSHOT_ID``. Matching ``RUSTIC_*`` variables are exported too.

Logging and progress
--------------------

``--log-file FILE`` writes standard-library and dependency log messages to a
file as well as stderr. ``--log-level`` accepts ``debug``, ``info``, ``warn``,
or ``error`` and validates profile/CLI configuration. ``--no-progress``
disables periodic progress updates; ``--progress-interval DURATION`` controls
their frequency. These options are also profile-settable and accept matching
``VAULTIC_*`` variables.

Telemetry
---------

After a fully successful backup, vaultic can publish the snapshot summary.
Publishing failures are reported but never turn a durable backup into a failed
one.

``--prometheus URL`` sends Prometheus text metrics to a Pushgateway at
``URL/metrics/job/vaultic``. Use ``--prometheus-user`` and
``--prometheus-pass`` for HTTP Basic authentication.

InfluxDB v2 and newer compatible servers use the native v2 line-protocol write
API. Configure ``--influxdb-url``, ``--influxdb-token``, ``--influxdb-org``,
and ``--influxdb-bucket`` (or the matching ``VAULTIC_INFLUXDB_*`` variables).
The URL can be the server base URL or the direct ``/api/v2/write`` endpoint.
Vaultic writes a ``vaultic_backup`` measurement with repository, snapshot, and
label tags plus success, duration, file, byte, and data-added fields.

``--opentelemetry`` creates command spans through the process-wide OpenTelemetry
provider. It is a no-op unless the environment or embedding process configures
an OpenTelemetry SDK/exporter.