********************************************************
Taking over a Rustic repo from Google Drive on macOS
********************************************************

Goal
=====

This example shows a realistic takeover of an existing Rustic repository that
backs up a macOS user's home directory directly to Google Drive through an
``rclone:`` backend. No FUSE or filesystem mount is required. The repository is
already populated and trusted, so the goal is to switch the client to Vaultic
without copying or repacking the packs.

The intended end state is:

* keep the existing Google Drive-backed repository as the primary warm copy;
* index the repository locally with VaulticDB on the Mac for fast metadata
  lookups and daemon-assisted operations;
* later run VaulticDB with one local SlateDB replica and one replica on a Google
   Cloud Storage S3-compatible endpoint;
* keep a second cold tier in the cheapest archive class available to the chosen
  provider; and
* protect access with Touch ID, MFA, and least-privilege cloud credentials.

Security model and account hardening
====================================

Before you begin, secure your cloud identities and local credentials.

* Turn on MFA for your Google account and for the GCP project or cloud account
  that owns the S3-compatible bucket.
* Prefer a hardware-backed passkey, security key, or Touch ID-protected login
  for the Mac that stores the repository.
* Store the repository password in the macOS Keychain rather than in shell
  history or a plaintext document. If you must use a file, keep it under a
  private path with ``0700`` permissions and remove it from shell history.
* Keep separate identities for the Google Drive ``rclone`` remote, the GCP S3
   bucket, and
  the archive/storage account. Do not reuse a single long-lived credential across
  all workloads.

A typical setup looks like this:

.. code-block:: console

   $ rclone config
   $ rclone lsd biz-drive:
   $ export VAULTIC_REPOSITORY='rclone:biz-drive:Backup/Hosts/mbp'
   $ export VAULTIC_PASSWORD_FILE="$HOME/.config/vaultic/repository-password"
   $ chmod 700 "$HOME/.config/vaultic"
   $ chmod 600 "$HOME/.config/vaultic/repository-password"

Here ``biz-drive`` is the Google Drive remote configured in ``rclone``. Vaultic
invokes ``rclone`` as its transport and accesses the repository directly; it
does not require ``rclone mount``, FUSE, or Google Drive for desktop. First,
verify that Vaultic can read the current Rustic metadata without changing the
repository itself:

.. code-block:: console

   $ vaultic snapshots
   $ vaultic check
   $ vaultic restore latest --include "$HOME/Library" --target /tmp/vaultic-restore-check

If those commands succeed, the repository is compatible with Vaultic in place.
This is the zero-copy takeover path described in the larger Rustic takeover
guide and does not require rewriting any packs.

The ``rclone`` configuration is a temporary migration input. It is not the
intended steady state because its refresh token remains outside quorum custody.
After the format-3 capsule and broker are active, enroll the same Drive through
Vaultic's native backend. Put only the installed-application OAuth client secret
in an owner-only file; the refresh token returned by the loopback flow is sent
straight to the broker and is neither printed nor written:

.. code-block:: console

   $ chmod 600 "$HOME/.config/vaultic/google-client-secret"
   $ vaultic --key-broker-socket "$VAULTIC_BROKER_SOCKET" \
      --key-broker-release-manifest /usr/local/etc/vaultic/vaultic.release.json \
      backend enroll gdrive \
      --client-id CLIENT.apps.googleusercontent.com \
      --client-secret-file "$HOME/.config/vaultic/google-client-secret" \
      --backend-id drive --credential-ref cred:drive-oauth \
      --drive-id SHARED-DRIVE-ID --root-folder-id ROOT-FOLDER-ID \
      --path Backup/Hosts/mbp --role primary --offsite \
      --failure-domain google-drive \
      --capsule "$VAULTIC_CAPSULE_DIR/ACTIVE-GENERATION.json" \
      --capsule-directory "$VAULTIC_CAPSULE_DIR" \
      --member recovery-password=offline-argon2id:"$HOME/.config/vaultic/quorum/recovery.passphrase" \
      --external-member-file "$HOME/.config/vaultic/quorum/mac-touchid.json" \
      --external-member-file "$HOME/.config/vaultic/quorum/yubikey-piv.json"

Enrollment requests full Google Drive scope because a ``drive.file`` token from
Vaultic's OAuth client cannot inspect files created by rclone's different OAuth
client. Restrict the OAuth identity and shared drive operationally, and use a
dedicated Drive for this repository.

Before switching topology, compare the rclone and native inventories. The
native side obtains the newly sealed credential by broker lease:

.. code-block:: console

   $ vaultic --key-broker-socket "$VAULTIC_BROKER_SOCKET" \
      --key-broker-release-manifest /usr/local/etc/vaultic/vaultic.release.json \
      backend verify \
      --credential-ref cred:drive-oauth \
      --compare 'rclone:biz-drive:Backup/Hosts/mbp,gdrive:SHARED-DRIVE-ID/Backup/Hosts/mbp'

The command compares every repository file type by object name and size. After
it reports a match and normal ``snapshots``, ``check``, and restore tests pass
through capsule topology, remove the Google remote from ``rclone.conf``. Keep
rclone only when another non-Drive backend still requires it.

Step 1: index locally on the Mac with VaulticDB
===============================================

For a repository that is already large or read-heavy, keep the metadata index on
local NVMe or SSD storage instead of sending metadata operations through the
Google Drive ``rclone`` backend. That reduces remote API traffic and makes
reindex and comparison work far more predictable.

Example local metadata directory:

.. code-block:: console

   $ mkdir -p "$HOME/Library/Application Support/vaulticdb"
   $ export VAULTICDB_DATA_DIR="$HOME/Library/Application Support/vaulticdb"
   $ export VAULTICDB_REPOSITORY_ID=$(vaultic cat config | jq -r .id)
   $ export VAULTIC_METADATA_RECOVERY_KEY="$HOME/.config/vaultic/metadata-recovery-key"
   $ chmod 700 "$HOME/.config/vaultic"
   $ chmod 600 "$HOME/.config/vaultic/metadata-recovery-key"

   $ vaultic index encrypt \
         --repository-id "$VAULTICDB_REPOSITORY_ID" \
         --metadata-recovery-passphrase-file "$VAULTIC_METADATA_RECOVERY_KEY" \
         --daemon-data-dir "$VAULTICDB_DATA_DIR" \
         --start-daemon

   $ vaultic index import \
         --from-legacy \
         --snapshot-depth 0 \
         --daemon-data-dir "$VAULTICDB_DATA_DIR" \
         --metadata-encryption required \
         --metadata-recovery-passphrase-file "$VAULTIC_METADATA_RECOVERY_KEY" \
         --start-daemon

This builds the VaulticDB index from the legacy Rustic JSON indexes without
rewriting the data packs. The local database becomes the fast metadata source for
future snapshots, prune, restore, and comparison runs. The Google Drive
``rclone`` backend remains the authoritative warm repository object store and
the macOS local user still has a fully transparent backup history.

Step 1a: require two of Touch ID, YubiKey, and a password
========================================================

Vaultic's 2-of-3 control is implemented by the quorum recovery capsule, not by
adding three independent slots to the lower-level SlateDB key envelope. The
capsule protects both the SlateDB metadata DEK and the repository master key
used for data packs. Finalization can make the capsule the only supported route
to the data-pack key. Making it the only possible route to SlateDB additionally
requires retirement of every pre-quorum metadata envelope and its passphrase;
the current limitation is documented below.

This example uses three independent members:

* ``mac-touchid``: a non-exportable P-256 key in this Mac's Secure Enclave,
   authorized with Touch ID;
* ``yubikey-piv``: an RSA key in YubiKey PIV slot ``9a``, with PIN and touch
   required by the token policy; and
* ``recovery-password``: a new offline Argon2id passphrase, distinct from the
   existing Rustic repository password.

Any two members can unlock the broker. One member alone cannot recover either
key. Touch ID proves use of an enrolled biometric on this Mac, not a particular
human identity, so keep the YubiKey and recovery password in failure domains
separate from the Mac.

Prepare protected working files
-------------------------------

Create an owner-only directory and a new quorum passphrase. Do not reuse the
Rustic repository password or the temporary metadata-recovery passphrase:

.. code-block:: console

    $ install -d -m 0700 "$HOME/.config/vaultic/quorum"
    $ umask 077
    $ read -s 'QUORUM_PASSWORD?New quorum recovery password: '
    $ printf '%s\n' "$QUORUM_PASSWORD" > "$HOME/.config/vaultic/quorum/recovery.passphrase"
    $ unset QUORUM_PASSWORD

Provision the YubiKey RSA key with Yubico tooling and YKCS11 before continuing.
The PIV management key, PIN, and PUK must be changed from their defaults, and
slot ``9a`` must require touch. Record the token serial and calculate the public
key fingerprint as described in :doc:`070_encryption`. Put the PIN in a mode
``0600`` file; Vaultic never accepts the PIN itself on the command line.

Create the protected YubiKey member definition using the real YKCS11 module
path, slot ID, token/slot credential ID, and lowercase public-key SHA-256:

.. code-block:: json

    {
       "member_id": "yubikey-piv",
       "provider": "yubikey-piv",
       "key_reference": "pkcs11:module-path=/usr/local/lib/libykcs11.dylib;slot-id=SLOT-ID;id=9a;public-key-sha256=HEX;type=rsa-key-pair",
       "hardware": {
          "credential_id": "YUBIKEY-SERIAL-9a",
          "public_key": "sha256:HEX",
          "user_presence_required": true
       },
       "bearer_token_file": "/Users/oli/.config/vaultic/quorum/yubikey.pin",
       "custodian_path": "/usr/local/bin/vaultic-key-custodian"
    }

Save that JSON as ``$HOME/.config/vaultic/quorum/yubikey-piv.json`` with mode
``0600``. Despite the generic field name, ``bearer_token_file`` points to the
PIV PIN file during enrollment; it does not contain a cloud bearer token.

Enroll the Secure Enclave member. Touch ID protects the generated private key;
the JSON file contains its application tag and public definition, not the
private key:

.. code-block:: console

    $ vaultic index keys quorum enroll-macos-secure-enclave \
             --member mac-touchid \
             --custodian-path /usr/local/bin/vaultic-key-custodian \
             --output "$HOME/.config/vaultic/quorum/mac-touchid.json"

Bootstrap and activate the final policy
---------------------------------------

Install and configure ``vaultic-key-broker`` as described in
:doc:`070_encryption`, using a protected broker identity, signed Vaultic release
manifest, owner-only socket, and immutable capsule directory. Prepare an initial
1-of-1 capsule with the new recovery password while the existing key-in-database
route still works:

.. code-block:: console

    $ export VAULTIC_CAPSULE_DIR="$HOME/.config/vaultic/quorum/capsules"
    $ export VAULTIC_BROKER_SOCKET="$HOME/.config/vaultic/quorum/key-broker.sock"
    $ install -d -m 0700 "$VAULTIC_CAPSULE_DIR"

    $ vaultic index keys \
             --repository-id "$VAULTICDB_REPOSITORY_ID" \
             quorum migrate-prepare \
             --capsule-directory "$VAULTIC_CAPSULE_DIR" \
             --generation 1 \
             --group bootstrap \
             --threshold 1 \
             --broker-public-key "$HOME/.config/vaultic/quorum/broker-identity.pub" \
             --member recovery-password=offline-argon2id:"$HOME/.config/vaultic/quorum/recovery.passphrase" \
             --state-file "$HOME/.config/vaultic/quorum/capsule-migration.json"

Start the broker on generation 1 and unlock it with ``recovery-password``.
Before finalization, inventory and destroy every exported repository master key,
metadata DEK, standalone escrow record, and other plaintext key copy. Retain the
generation-1 capsule and its recovery password. Finalization is irreversible
and its state file is bound to the exact generation-1 capsule digest, so it must
run before creating generation 2:

.. code-block:: console

   $ vaultic --key-broker-socket "$VAULTIC_BROKER_SOCKET" \
          --key-broker-release-manifest /usr/local/etc/vaultic/vaultic.release.json \
          index keys --repository-id "$VAULTICDB_REPOSITORY_ID" \
          quorum migrate-finalize \
          --state-file "$HOME/.config/vaultic/quorum/capsule-migration.json" \
          --confirm \
          --retire-legacy-routes \
          --confirm-standalone-escrow-destroyed

This authenticates repository configuration and a pack through generation 1,
proves the same repository key to VaulticDB, removes all ordinary repository
password-key objects and mirrored standalone escrow records, and atomically
removes the database master-key record. A failure leaves the old database route
available for retry rather than silently completing halfway.

Now replace the bootstrap policy with the final 2-of-3 group. Policy creation
validates the YubiKey against the token and asks Touch ID to use the enrolled
Secure Enclave key:

.. code-block:: console

    $ vaultic --key-broker-socket "$VAULTIC_BROKER_SOCKET" \
             index keys --repository-id "$VAULTICDB_REPOSITORY_ID" \
             quorum create-group operators \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000001.json" \
             --capsule-directory "$VAULTIC_CAPSULE_DIR" \
             --threshold 2 \
             --member recovery-password=offline-argon2id:"$HOME/.config/vaultic/quorum/recovery.passphrase" \
             --external-member "$HOME/.config/vaultic/quorum/mac-touchid.json" \
             --external-member "$HOME/.config/vaultic/quorum/yubikey-piv.json"

Activation publishes generation 2 and relocks the broker. Preserve both capsule
generations only during validation, and configure the broker to load only the
newest active generation. Generation 1 is a temporary 1-of-1 password route and
must be destroyed after generation 2 passes every test below.

Prove all three valid pairs
---------------------------

Perform three fresh locked-broker ceremonies: Touch ID plus YubiKey, Touch ID
plus password, and YubiKey plus password. Also verify that each member alone
leaves the broker locked. Each ceremony needs a newly prepared signed session
and an independently compared fingerprint. Generation 1 remains available only
for rollback during these tests; do not configure the broker to accept it as
the active policy after generation 2 succeeds.

For example, test Touch ID plus YubiKey:

.. code-block:: console

    $ vaultic index unlock contribute --prepare \
             --broker-socket "$VAULTIC_BROKER_SOCKET" \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json" \
             --session-file "$HOME/.config/vaultic/quorum/session-touch-yubi.json"

    $ vaultic index unlock contribute \
             --broker-socket "$VAULTIC_BROKER_SOCKET" \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json" \
             --session-file "$HOME/.config/vaultic/quorum/session-touch-yubi.json" \
             --member mac-touchid --macos-secure-enclave \
             --generation-anchor "$HOME/.config/vaultic/quorum/mac-touchid.generation" \
             --confirm-fingerprint FINGERPRINT

    $ vaultic index unlock contribute \
             --broker-socket "$VAULTIC_BROKER_SOCKET" \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json" \
             --session-file "$HOME/.config/vaultic/quorum/session-touch-yubi.json" \
             --member yubikey-piv \
             --yubikey-piv-pin-file "$HOME/.config/vaultic/quorum/yubikey.pin" \
             --custodian-path /usr/local/bin/vaultic-key-custodian \
             --generation-anchor "$HOME/.config/vaultic/quorum/yubikey-piv.generation" \
             --confirm-fingerprint FINGERPRINT

Run the same ceremony with fresh session files for the other pairs. The password
contribution replaces the hardware-specific flags with:

.. code-block:: console

    $ vaultic index unlock contribute \
             --broker-socket "$VAULTIC_BROKER_SOCKET" \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json" \
             --session-file SESSION-FILE \
             --member recovery-password \
             --passphrase-file "$HOME/.config/vaultic/quorum/recovery.passphrase" \
             --generation-anchor "$HOME/.config/vaultic/quorum/recovery-password.generation" \
             --confirm-fingerprint FINGERPRINT

After each successful pair, exercise both capabilities before relocking: run
``vaultic index check`` through the metadata-DEK lease and authenticate a pack
or perform a test restore through the repository-key lease. Then end the unlock
epoch explicitly:

.. code-block:: console

    $ vaultic index unlock lock \
             --broker-socket "$VAULTIC_BROKER_SOCKET" --confirm

Finally unlock generation 2 with any valid pair and verify the active policy:

.. code-block:: console

    $ vaultic index keys quorum verify \
             --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json" \
             --broker-socket "$VAULTIC_BROKER_SOCKET"
    $ vaultic --key-broker-socket "$VAULTIC_BROKER_SOCKET" \
             index keys --repository-id "$VAULTICDB_REPOSITORY_ID" \
             status --capsule "$VAULTIC_CAPSULE_DIR/00000000000000000002.json"

Retire every legacy access route
--------------------------------

The generation-1 finalization above retires the database and repository-key
routes. After all generation-2 pair tests succeed, complete the filesystem,
credential-store, history, snapshot, and backup cleanup below. Keep the final
capsule and all three final member credentials.

.. warning::

   Finalization does not remove the pre-quorum SlateDB key-envelope objects.
   Brokered VaulticDB ignores them, but an attacker holding an old envelope and
   its recovery passphrase can still unwrap the old metadata DEK. The current
   release has no supported atomic broker-mode operation that rotates and
   rewrites the metadata DEK and then removes all legacy envelope generations
   from every metadata replica and repository mirror. Do not claim that
   SlateDB is cryptographically capsule-only until that operation exists or an
   independently reviewed offline retirement has removed every copy.

After successful finalization, remove or revoke all copies of these obsolete
credentials:

* the old Rustic/Vaultic repository password file, including the file referenced
   by the earlier ``VAULTIC_PASSWORD_FILE``;
* the old ``VAULTIC_METADATA_RECOVERY_KEY`` passphrase file used to initialize
   the pre-quorum metadata envelope;
* exported plaintext repository master-key or metadata-DEK files;
* standalone escrow JSON, recovery exports, temporary QR/paper exports, and
   removable-media copies that contain a complete key;
* obsolete Keychain entries and password-manager records for the old repository
   password or metadata-recovery passphrase;
* the generation-1 bootstrap capsule and every local, mirrored, versioned, and
   backup copy of it; its 1-of-1 recovery password policy bypasses the final
   threshold;
* temporary unlock session files and the completed capsule migration state file;
   these are not key bypasses, but no longer need to remain online; and
* shell history, automation secrets, launchd environment files, logs, and old
   machine backups that captured any retired plaintext credential.

Also inventory the old metadata-envelope ciphertext itself. It normally exists
under ``_vaultic/key-envelope.json`` and versioned
``_vaultic/key-envelopes/*.json`` objects in every local or remote VaulticDB
replica. Copies explicitly mirrored into the data repository are named
``key-envelope-<generation>.json`` in its SlateDB metadata namespace. These
objects are not plaintext keys, but any one of them together with its old
passphrase may preserve metadata access. Object versions, bucket retention,
APFS snapshots, Time Machine, and cloud backups count as copies.

Do not manually remove those objects from a running replicated database. Until
Vaultic provides the guarded broker-mode retirement workflow, either retain and
protect them as a documented legacy recovery route or perform an offline,
independently reviewed retirement of every replica, mirror, object version, and
backup. Merely deleting ``VAULTIC_METADATA_RECOVERY_KEY`` from the current Mac
is not evidence that no recoverable pair remains elsewhere.

Do **not** delete the generation-2 capsule, broker identity/configuration,
release manifest, Touch ID member definition, Secure Enclave key, YubiKey PIV
key, YubiKey member definition and PIN custody path, recovery-password file, or
the three monotonic generation-anchor files. Those are parts of the new access
system. Store the password separately from the Mac and YubiKey; losing any two
of the three members makes both metadata and pack recovery impossible.

Overwriting a file with ``shred`` or ``srm`` is not reliable on APFS, SSDs,
snapshots, copy-on-write filesystems, cloud synchronization, or backups. Prefer
cryptographic erasure by deleting the enclosing encrypted volume/key, revoke
Keychain and password-manager records, delete every synchronized and backup
copy, and rotate a key if its destruction cannot be demonstrated. Preserve a
signed inventory of what was removed and the successful finalization output,
without recording secret values.

Step 2: configure the real production layout
=============================================

The intended topology for this machine is:

* warm data-pack backend 1: Google Drive, accessed directly through the
   ``rclone:biz-drive:Backup/Hosts/mbp`` backend. This is the primary repository
   backend and should stay within a total budget of roughly 5 TiB. It is the
   cheapest warm data-pack storage because it is already included in the
   existing Google plan and adds no API cost.
* cold data-pack backend 2: a deep-archive S3 tier, such as AWS S3 Glacier or a
  comparable archive class in another provider. This stores older packs that are
  rarely accessed.
* SlateDB metadata backends: one local replica on the Mac and one replica in a
   dedicated S3-compatible GCP bucket. Google Drive and its ``rclone`` transport
   are not S3-compatible and should not become a metadata replica. Optionally add
   another provider as a third metadata replica or secondary archive backend.

This separation matters because the data packs and the metadata index are not the
same workload. Vaultic can use Google Drive for the actual pack data, but it
should not use Google Drive as the metadata database backend. The metadata store
needs a proper object API and predictable performance.

Where to configure the backends
------------------------------

The pack backends are configured in the repository's placement policy, which is
stored in the repository config and shown via ``vaultic show-config`` and
``vaultic config --json``. This repository config is authoritative; a sealed
bootstrap-topology manifest may carry a recovery projection of it. The SlateDB
replicas are separate and are configured in the VaulticDB service environment,
not as ``placement_backends`` entries.

A practical example:

.. code-block:: json

   {
     "placement_backends": [
       {
         "id": "drive",
         "location": "rclone:biz-drive:Backup/Hosts/mbp",
         "role": "primary",
         "offsite": true,
         "failure_domain": "google-drive",
         "retrieval_class": "standard",
         "max_bandwidth_bytes": 104857600,
         "max_requests_per_second": 10
       },
       {
         "id": "archive",
         "location": "s3:s3.us-east-1.amazonaws.com/alice-deep-archive",
         "role": "archival",
         "offsite": true,
         "failure_domain": "aws-archive",
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
      "promotion_crossover_seconds": 2592000
     }
   }

Here the primary pack backend is the Google Drive location and the archive
backend holds low-frequency data packs. The separately configured VaulticDB
deployment writes synchronously to local and GCP S3 replicas. Neither metadata
replica is a pack-placement backend, and the GCP replica must remain immediately
readable rather than using an archive storage class.

Thresholds and capacity limits
-----------------------------

The 5 TiB limit is an operational limit, not a single explicit Vaultic backend
option. In practice, use ``rclone size biz-drive:Backup/Hosts/mbp`` in a monitor
or local script to warn when the Google Drive repository crosses about 5 TiB.
Keep the archive backend as the overflow target for stale packs and use the
placement policy to hold the durable copy set in a predictable way.

The limits that Vaultic does support are the request and bandwidth caps in the
backend configuration, such as ``max_bandwidth_bytes`` and
``max_requests_per_second``. These make it possible to limit the rate at which
Vaultic pushes data to a backend without preventing the work from remaining queued.

The effective retention trigger comes from the ``promotion_crossover_seconds``
setting and from the archival backend's ``min_retention_seconds``. In this
example the crossover is 2,592,000 seconds, or 30 days, and the archive minimum
is 15,552,000 seconds, or 180 days. Those values delay archival promotion until
an existing pack is old enough and its retained bytes are expected to survive
the archive provider's minimum-retention period.

Migration steps
---------------

1. Confirm the existing Rustic repository is readable and the current backup
   schedule still works.
2. Export the current Rustic configuration into a Vaultic-friendly environment.
3. Import the legacy Rustic JSON indexes into a local VaulticDB database.
4. Populate and verify a GCP S3 metadata replica, then switch VaulticDB to the
   replicated local-plus-GCP configuration.
5. Validate snapshots and restore a representative sample.
6. Configure the placement backends so the primary Google Drive ``rclone:``
   backend is the warm pack store and the S3 archive backend handles old packs.
7. Add a second S3 provider only if you want redundancy for the archive or
   metadata layer.

At no point do you need to rewrite or re-encrypt the existing pack bodies. The
migration is metadata-first and zero-copy. The data remains in the same Rustic
format while Vaultic takes over the repository.

Migrating the original Rustic config
-----------------------------------

Your original Rustic config is a good starting point for the equivalent Vaultic
commands. The same settings translate naturally:

.. code-block:: ini

   [repository]
   repository = "rclone:biz-drive:Backup/Hosts/mbp"
   password-file = "/Users/oli/Library/Application Support/rustic/repo-password.txt"

   packsize-default = "128MiB"
   packsize-tree = "16MiB"

   [repository.options]
   "rclone.args" = "--transfers 2 --drive-chunk-size 64M --low-speed-limit 100k --low-speed-time 15s"
   "repository.compression" = "10"

   [forget]
   keep-daily = 7
   keep-weekly = 4
   keep-monthly = 12
   keep-yearly = 1

   [[backup.snapshots]]
   name = "mbp-disk"
   sources = ["/Users/oli"]
   one-file-system = true

   globs = [
       "!**/Library/Caches/**",
       "!**/Library/Logs/**",
       "!**/.Trash/**",
       "!**/node_modules/**",
       "!**/target/**",
       "!**/Library/Application Support/Spotify/PersistentCache/**",
       "!**/Library/Application Support/Spotify/Users/*-user/Storage/**",
       "!**/Library/Containers/com.docker.docker/**",
       "!**/Library/Developer/Xcode/DerivedData/**",
       "!**/Library/Developer/Xcode/Archives/**",
       "!**/Library/Metadata/**"
   ]

The equivalent Vaultic workflow is:

.. code-block:: console

   $ export VAULTIC_REPOSITORY='rclone:biz-drive:Backup/Hosts/mbp'
   $ export VAULTIC_PASSWORD_FILE='/Users/oli/Library/Application Support/rustic/repo-password.txt'
   $ export VAULTIC_COMPRESSION=10
   $ export VAULTIC_PACKSIZE=134217728
   $ export VAULTIC_TREEPACKSIZE=16777216

   $ cat >/Users/oli/Library/Application\ Support/vaultic/excludes.txt <<'EOF'
   !**/Library/Caches/**
   !**/Library/Logs/**
   !**/.Trash/**
   !**/node_modules/**
   !**/target/**
   !**/Library/Application Support/Spotify/PersistentCache/**
   !**/Library/Application Support/Spotify/Users/*-user/Storage/**
   !**/Library/Containers/com.docker.docker/**
   !**/Library/Developer/Xcode/DerivedData/**
   !**/Library/Developer/Xcode/Archives/**
   !**/Library/Metadata/**
   EOF

   $ vaultic init \
         --set-compression 10 \
         --set-datapack-size 134217728 \
         --set-treepack-size 16777216

   $ vaultic backup \
         --one-file-system \
         --host mbp-disk \
         --exclude-file '/Users/oli/Library/Application Support/vaultic/excludes.txt' \
         /Users/oli

Retention is applied by the separate ``forget`` command; running ``backup`` does
not remove old snapshots. Put the policy in ``~/.config/vaultic/vaultic.toml``
so Vaultic loads it automatically:

.. code-block:: toml

   [forget]
   keep-daily = 7
   keep-weekly = 4
   keep-monthly = 12
   keep-yearly = 1

Preview the policy before allowing it to remove snapshots:

.. code-block:: console

   $ vaultic forget --dry-run

Then run it for real. ``--prune`` also reclaims pack data that is no longer
referenced by any retained snapshot:

.. code-block:: console

   $ vaultic forget --prune
   $ vaultic check

Alternatively, keep the existing ``rustic.toml`` and select it explicitly as a
Vaultic profile. Vaultic accepts the Rustic-style repository, backup, exclude,
and ``[forget]`` settings used in this example:

.. code-block:: console

   $ vaultic -P '/Users/oli/Library/Application Support/rustic/rustic.toml' \
         forget --dry-run
   $ vaultic -P '/Users/oli/Library/Application Support/rustic/rustic.toml' \
         forget --prune

Schedule ``forget --prune`` independently from the backup job after validating
the dry-run output. The policy is applied separately to each snapshot group,
which is defined by host and source paths by default. The same exclude file can
be stored in a dotfiles repo or local config directory and reused for restore,
rewrite, and other commands that accept ``--exclude`` or ``--exclude-file``.

Step 3: replicate metadata and add the archive target
======================================================

Once the warm Google Drive backend is stable, replicate SlateDB between the
local Mac and a GCP S3 bucket. The deep archive target remains a separate cold
pack store, with an optional third provider for additional redundancy.

Do not repeat the Step 1 import against the same local database when adding a
pack backend. Extending SlateDB authority from that local database to a
local-plus-GCP replicated deployment is a separate metadata migration: populate
and verify the remote candidate before enabling synchronous replication. See
:doc:`070_encryption` for the guarded remote-candidate rebuild procedure.

The sealed topology's ``metadata_replicas`` object is the eventual VaulticDB
service configuration. Replica order is significant: ``local`` is the primary
read/list target and reads fail over to ``gcp``. Mutating operations must
succeed on both replicas before VaulticDB acknowledges them. GCP is the single
fencing replica used to coordinate writer authority.

.. code-block:: console

   $ export VAULTICDB_TOPOLOGY_SOURCE=capsule
   $ export VAULTICDB_BROKER_SOCKET="$VAULTIC_BROKER_SOCKET"
   $ export VAULTICDB_RELEASE_MANIFEST=/usr/local/etc/vaultic/vaulticdb.release.json
   $ export VAULTICDB_REPOSITORY_ID="$VAULTICDB_REPOSITORY_ID"

Do not set ``VAULTICDB_REPLICATED_*`` or ``AWS_*`` in this deployment. The
local path, GCP endpoint, bucket, prefix, order, and fencing replica come from
``topology-read``; the GCP HMAC or service-account credential comes from its
exact ``credential-lease``. VaulticDB appends a repository-identity hash beneath
each configured prefix, allowing the namespace root to serve multiple
repositories without mixing their SlateDB objects.

Do not point the existing local database at an empty GCP replica and assume it
will backfill automatically. Populate and validate the GCP candidate first,
preserve the original local database until restore checks pass, and only then
restart VaulticDB in replicated mode.

Replicate the existing local database to GCP
--------------------------------------------

The initial replication is an offline object-store copy. Do not copy a live
SlateDB directory: concurrent WAL or manifest writes can produce a remote tree
that never represented a valid database state.

First quiesce the writer, stop the supervised VaulticDB service, and verify that
the process has exited:

.. code-block:: console

   $ export VAULTICDB_REPOSITORY_ID=$(vaultic cat config | jq -r .id)
   $ vaultic index writer \
         --repository-id "$VAULTICDB_REPOSITORY_ID" \
         demote --timeout 2m --reason "metadata replica bootstrap"
   $ # Stop the launchd or other supervised vaulticdb service here.

VaulticDB appends the SHA-256 digest of the repository ID to both local and
remote replica roots. Compute that path and identify the source and destination.
This example assumes an ``rclone`` S3 remote named ``gcp-meta`` that uses the
GCP interoperability endpoint and protected credentials:

.. code-block:: console

   $ export VAULTICDB_REPOSITORY_HASH=$( \
         printf '%s' "$VAULTICDB_REPOSITORY_ID" | shasum -a 256 | awk '{print $1}' \
     )
   $ export LOCAL_METADATA="$HOME/Library/Application Support/vaulticdb/$VAULTICDB_REPOSITORY_HASH"
   $ export GCP_METADATA="gcp-meta:alice-vaulticdb/vaulticdb/production/$VAULTICDB_REPOSITORY_HASH"

Confirm that the destination prefix is empty, copy every object without
overwriting conflicts, and compare every source object with bytes downloaded
from GCP:

.. code-block:: console

   $ rclone lsf "$GCP_METADATA" --recursive
   $ rclone copy "$LOCAL_METADATA/" "$GCP_METADATA/" --immutable
   $ rclone check "$LOCAL_METADATA/" "$GCP_METADATA/" --download --one-way

The first command must produce no object names. If it reports existing objects,
choose a new generation-specific prefix rather than merging two database trees.
Do not continue unless ``rclone check`` succeeds with no differences.

Now install the replicated environment shown above in the supervised service
and restart VaulticDB. Check writer state, metadata consistency, snapshots, and
a representative restore before allowing backup or maintenance jobs to resume:

.. code-block:: console

   $ vaultic index writer \
         --repository-id "$VAULTICDB_REPOSITORY_ID" \
         status
   $ vaultic index check
   $ vaultic snapshots
   $ vaultic restore latest \
         --include "$HOME/Library" \
         --target /tmp/vaultic-replicated-restore-check

Keep the stopped local source and the previous service configuration as the
rollback point until these checks pass. After replicated startup, fencing state
is intentionally written through the configured GCP fencing replica; compare
database health through VaulticDB rather than expecting raw replica inventories
to remain identical forever.

Optional additional redundancy can use a third metadata replica or a second
archive bucket in another provider. The same metadata-replica pattern supports
AWS S3, another S3-compatible service, or Azure; it must not depend on the
Google Drive ``rclone`` backend.

Hydrate AWS Glacier from eligible existing packs
------------------------------------------------

Adding the ``archive`` placement backend does not immediately upload every old
pack. Vaultic treats packs as immutable, so "no churn for 30 days" is represented
by the 30-day ``promotion_crossover_seconds`` rather than by a mutable
last-modified timestamp. Promotion also requires known pack creation and usage
facts plus retention evidence from a completed ``forget`` policy evaluation.

Use an AWS identity dedicated to archive placement. The GCP endpoint and HMAC
credentials shown earlier belong in the supervised VaulticDB service only; do
not reuse them in this shell for the AWS pack backend:

.. code-block:: console

   $ unset AWS_ENDPOINT_URL_S3 AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN
   $ export AWS_PROFILE=vaultic-archive
   $ export AWS_DEFAULT_REGION=us-east-1

First preview and then apply the configured retention policy without pruning.
The real ``forget`` run records how long the blobs retained by surviving
snapshots must remain reachable:

.. code-block:: console

   $ vaultic forget --dry-run
   $ vaultic forget

Next preview packs that have crossed 30 days and satisfy the retention horizon.
``--dry-run`` computes the scheduler plan without writing placement requests:

.. code-block:: console

   $ vaultic -o s3.storage-class=DEEP_ARCHIVE \
      index placement --pending-promotion --dry-run --json

Inspect individual candidates before transferring data:

.. code-block:: console

   $ vaultic -o s3.storage-class=DEEP_ARCHIVE \
      index placement --explain PACK-ID --json

Hydrate the new backend in bounded batches. The first command processes at most
25 requests and 100 GiB; repeat it until no pending promotions remain:

.. code-block:: console

   $ vaultic -o s3.storage-class=DEEP_ARCHIVE \
      index placement --execute \
      --max-requests 25 \
      --max-bytes 107374182400

   $ vaultic index placement --pending-promotion --json
   $ vaultic index placement --unsatisfied --json

Promotion does not copy an existing encrypted pack object byte for byte. It
decrypts the source pack, selects only blobs still referenced by retained
snapshots, and writes new, larger pack objects to the ``archive`` backend with
the ``DEEP_ARCHIVE`` storage class. The new pack and its placement metadata are
published before the old source pack can become delete-pending. Interrupted
promotion is resumable from its durable lineage records.

Once the promotion queue is empty and placement durability is satisfied, run
``vaultic prune`` to reclaim unreferenced warm data. Packs that became
unreachable before the 30-day crossover are discarded by normal garbage
collection and are never uploaded to Glacier, avoiding minimum-storage-duration
charges for short-lived data.

This preserves the warm 5 TiB Google Drive tier, keeps one metadata replica
local and another on GCP S3, and makes the deep archive tier cheap while still
maintaining a replayable, restoreable backup history.

Run recurring backups after a 2-of-3 unlock
============================================

The recommended macOS arrangement separates scheduling from authorization:

* ``launchd`` probes every 15 minutes but never holds a repository password,
   master key, metadata DEK, YubiKey PIN, recovery password, or member share;
* a fresh 2-of-3 ceremony opens a bounded broker epoch;
* the next probe sees the unlocked broker and runs one backup in the background;
* a success stamp suppresses another backup for 23 hours; and
* the broker locks automatically when the configured epoch expires.

This makes unlocking the only recurring interactive step. If the broker stays
locked, the probe exits successfully without opening the repository. Unlocking
does not itself start a backup, but the next probe starts one within 15 minutes.

Choose one coherent macOS account model
---------------------------------------

The broker socket is mode ``0600``. For a personal Mac backup of
``/Users/oli``, run the broker and backup LaunchAgent as the ``oli`` login user
and authorize that numeric UID in the broker's signed client policy. Do not
install the system ``_vaultic`` LaunchDaemon template unchanged and then expect
a user LaunchAgent to reach its owner-only socket. A separate service account
also lacks ordinary access and macOS privacy consent for the login user's home
directory.

Run the supervised ``vaulticdb`` process under the same account and authorize
its separately signed release manifest for ``metadata-dek``, ``topology-read``,
and only the credential references used by its metadata replicas. If VaulticDB
or the broker restarts while locked, VaulticDB cannot reacquire its metadata
lease; let launchd retry it and complete the next 2-of-3 ceremony. Do not restore
the retired key-in-database route merely to make unattended restart succeed.

Keep ``vaultic``, ``vaulticdb``, and the wrapper below in root-owned,
non-writable installation paths even though their processes run as ``oli``.
Authorize the exact ``/usr/local/bin/vaultic`` digest and the
``repository-master-key`` capability in the broker release manifest. Grant that
installed Vaultic executable Full Disk Access in macOS Privacy & Security, then
test access from the LaunchAgent context; a Terminal test does not prove that a
background process has the same TCC authorization. The recurring backup also
uses ``tmutil`` and ``mount_apfs`` for its temporary read-only source. A missing
authorization falls back to a live read by default; set
``apfs-snapshot-require = true`` only after proving snapshot creation and mount
from the LaunchAgent itself.

The native ``gdrive:`` transport receives its refresh token through an exact
``credential-lease`` authorization on the signed Vaultic release. Do not put a
Google token, ``rclone`` configuration path, or cloud credential in the plist,
wrapper, or recurring profile. Broker lock or connection loss revokes the
broker-side lease; host integrity still matters while the authorized Vaultic
process holds the token in memory.

Bound the manual unlock window
------------------------------

Set a finite epoch lifetime in the broker config. Two hours gives the
15-minute probe enough time to observe an unlock without leaving the broker
open for the rest of the day:

.. code-block:: json

    {
       "maximum_unlocked_seconds": 7200
    }

Add this field to the existing broker JSON rather than replacing the complete
configuration. Restarting the broker applies the setting and starts it locked,
so perform another 2-of-3 ceremony afterward. Epoch expiry prevents new leases
and revokes broker-side lease state. It cannot retract key bytes already issued
to a running, authorized process, so host and authorized-client integrity still
matter during a backup.

Variant: keep the broker epoch open for one week
------------------------------------------------

If a daily 2-of-3 ceremony is too disruptive, configure a seven-day epoch:

.. code-block:: json

   {
     "maximum_unlocked_seconds": 604800
   }

One ceremony can then authorize up to seven recurring daily backups. The
LaunchAgent and wrapper do not change: each backup still requests only a
one-hour, connection-bound ``repository-master-key`` lease, and the 23-hour
success stamp still permits at most one successful backup per day. A job that
starts near the end of the week may continue with key bytes already delivered
to its process, but no new lease can be issued after epoch expiry.

This variant reduces ceremony frequency but materially enlarges the compromise
window. For the whole week, any correctly signed and broker-authorized client
running under the configured peer UID can request its allowed key capability
without another Touch ID, YubiKey, or password contribution. Screen lock,
logout, sleep, and closing Terminal do not lock a launchd-supervised broker.
Broker restart, host restart, explicit lock, or the seven-day expiry does.

Use the weekly variant only on a FileVault-protected, physically controlled Mac
with automatic screen lock, current endpoint security, tightly bounded release
authorizations, and no unnecessary local administrators. Prefer the two-hour
window on a shared, mobile, or higher-risk machine. Lock immediately before
travel, repair, account changes, suspected compromise, or handing the Mac to
another person:

.. code-block:: console

   $ vaultic index unlock lock \
         --broker-socket "$HOME/.config/vaultic/quorum/key-broker.sock" \
         --confirm

After a broker or Mac restart, or after seven days, perform a fresh 2-of-3
ceremony. Changing ``maximum_unlocked_seconds`` also requires a broker restart
and therefore another ceremony. Do not increase ``--key-broker-lease`` beyond
``1h``: the broker rejects longer leases, and a week-long lease would defeat the
separation between the unlock epoch and individual jobs even if it were
accepted.

Create a dedicated recurring profile
------------------------------------

First run ``vaultic bootstrap --from-capsule`` as described in
:doc:`070_encryption` to create
``$HOME/.config/vaultic/capsule-bootstrap.toml``. That profile contains only
repository identity, capsule directory, and broker socket. Save the backup
choices below as ``$HOME/.config/vaultic/recurring.toml`` with mode ``0600``;
it contains no repository location or unlock credential:

.. code-block:: toml

    [global]
    no-progress = true

    [backup]
    one-file-system = true
   use-fsevents = true
   fsevents-replay-timeout = "60s"
   fsevents-full-crawl-every = "168h"
   apfs-snapshot = true
    exclude-file = ["/Users/oli/Library/Application Support/vaultic/excludes.txt"]

    [[backup.snapshots]]
    name = "mbp-disk"
    sources = ["/Users/oli"]
    host = "mbp-disk"
    tag = ["automatic", "daily"]

Do not copy the old Rustic ``password-file`` into this production profile. The
finalized capsule migration deliberately makes password, direct-key,
Azure-secret, and key-in-database routes mutually exclusive with brokered
unlock.

Install the probe wrapper
-------------------------

Install the following as ``/usr/local/libexec/vaultic-backup-mbp``. Keep it
root-owned and mode ``0755``. The status query is local and does not acquire a
key lease. The backup requests a one-hour job lease, below the broker's maximum
allowed lease lifetime, and waits up to 30 minutes for an existing repository
operation to release its lock:

.. code-block:: zsh

    #!/bin/zsh
    set -u
    set -o pipefail
    umask 077

    readonly VAULTIC=/usr/local/bin/vaultic
    readonly PROFILE="$HOME/.config/vaultic/recurring.toml"
   readonly BOOTSTRAP_PROFILE="$HOME/.config/vaultic/capsule-bootstrap.toml"
    readonly BROKER_SOCKET="$HOME/.config/vaultic/quorum/key-broker.sock"
    readonly RELEASE_MANIFEST=/usr/local/etc/vaultic/vaultic.release.json
    readonly STATE_DIR="$HOME/Library/Application Support/vaultic/automation"
    readonly SUCCESS_STAMP="$STATE_DIR/last-backup-success"
    readonly MINIMUM_INTERVAL=82800

    /bin/mkdir -p "$STATE_DIR"

    unset VAULTIC_PASSWORD VAULTIC_PASSWORD_FILE VAULTIC_PASSWORD_COMMAND
    unset VAULTIC_KEY VAULTIC_KEY_FILE VAULTIC_KEY_COMMAND
    unset VAULTIC_METADATA_KEY_IN_DB
    unset RESTIC_PASSWORD RESTIC_PASSWORD_FILE RESTIC_PASSWORD_COMMAND

    status_json=$(
       "$VAULTIC" -P "$PROFILE" --json index unlock \
          --broker-socket "$BROKER_SOCKET" status 2>/dev/null
    ) || exit 0
    locked=$(
       /usr/bin/printf '%s' "$status_json" |
          /usr/bin/plutil -extract locked raw -o - - 2>/dev/null
    ) || exit 0
    [[ "$locked" == "false" ]] || exit 0

    now=$(/bin/date +%s)
    if [[ -f "$SUCCESS_STAMP" ]]; then
       last_success=$(/usr/bin/stat -f %m "$SUCCESS_STAMP") || exit 1
       (( now - last_success < MINIMUM_INTERVAL )) && exit 0
    fi

    "$VAULTIC" \
       --key-broker-socket "$BROKER_SOCKET" \
       --key-broker-release-manifest "$RELEASE_MANIFEST" \
       --key-broker-lease 1h \
       --retry-lock 30m \
      --bootstrap-profile "$BOOTSTRAP_PROFILE" \
       -P "$PROFILE" \
       backup --name mbp-disk || exit $?

    /usr/bin/touch "$SUCCESS_STAMP"

``launchd`` never starts a second instance of the same job while one is still
running. The repository lock and ``--retry-lock`` additionally coordinate this
job with manually invoked Vaultic maintenance. The success stamp is updated
only after exit status 0; an incomplete snapshot with exit status 3 or any
fatal failure remains eligible for retry during the current unlock epoch.

Install the user LaunchAgent
----------------------------

Create ``$HOME/Library/LaunchAgents/com.vaultic.backup.mbp.plist`` with mode
``0644``. Use absolute executable paths because a LaunchAgent does not inherit
the interactive shell's ``PATH``:

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
       "https://www.apple.com/DTDs/PropertyList-1.0.dtd">
    <plist version="1.0">
    <dict>
       <key>Label</key>
       <string>com.vaultic.backup.mbp</string>
       <key>ProgramArguments</key>
       <array>
          <string>/usr/local/libexec/vaultic-backup-mbp</string>
       </array>
       <key>RunAtLoad</key>
       <true/>
       <key>StartInterval</key>
       <integer>900</integer>
       <key>ProcessType</key>
       <string>Background</string>
       <key>LowPriorityIO</key>
       <true/>
       <key>StandardOutPath</key>
       <string>/Users/oli/Library/Logs/vaultic-backup.log</string>
       <key>StandardErrorPath</key>
       <string>/Users/oli/Library/Logs/vaultic-backup.log</string>
    </dict>
    </plist>

Validate and load it in the GUI domain of the logged-in user:

.. code-block:: console

    $ install -d -m 0700 "$HOME/Library/Logs" \
             "$HOME/Library/Application Support/vaultic/automation"
    $ chmod 0600 "$HOME/.config/vaultic/recurring.toml"
   $ chmod 0600 "$HOME/.config/vaultic/capsule-bootstrap.toml"
    $ plutil -lint "$HOME/Library/LaunchAgents/com.vaultic.backup.mbp.plist"
    $ launchctl bootstrap "gui/$(id -u)" \
             "$HOME/Library/LaunchAgents/com.vaultic.backup.mbp.plist"

After changing the plist, use ``launchctl bootout`` with the same GUI domain
and path before bootstrapping it again. ``launchctl print
gui/$(id -u)/com.vaultic.backup.mbp`` shows its state and last exit status.

Daily operation
---------------

When a backup is due, perform one of the three fresh ceremonies from Step 1a:
Touch ID plus YubiKey, Touch ID plus recovery password, or YubiKey plus recovery
password. Use a new signed session, compare its fingerprint independently, and
submit both contributions. Nothing from the ceremony belongs in the profile,
wrapper, plist, environment, logs, or success stamp.

Within 15 minutes the LaunchAgent acquires a repository-key lease and starts the
named backup. Check ``$HOME/Library/Logs/vaultic-backup.log``. While the broker
is still unlocked, confirm the new snapshot through the same brokered route:

.. code-block:: console

   $ vaultic \
      --key-broker-socket "$HOME/.config/vaultic/quorum/key-broker.sock" \
      --key-broker-release-manifest /usr/local/etc/vaultic/vaultic.release.json \
      --bootstrap-profile "$HOME/.config/vaultic/capsule-bootstrap.toml" \
      -P "$HOME/.config/vaultic/recurring.toml" \
      snapshots --latest 1

The 23-hour success interval prevents repeat snapshots during the same unlock
window. Broker expiry closes the window automatically. To end it immediately:

.. code-block:: console

   $ vaultic index unlock lock \
      --broker-socket "$HOME/.config/vaultic/quorum/key-broker.sock" \
      --confirm

If the Mac is asleep or offline, no backup is recorded and the stamp is not
advanced. The probes continue after wake or network recovery, but a new
2-of-3 ceremony is required if the broker epoch has already expired. Schedule
``forget --prune`` and archive promotion as separate, less frequent LaunchAgent
jobs; they also require an unlocked broker and must use distinct success stamps
so a successful backup cannot suppress maintenance.

Operational notes
=================

If you adopt this model, keep the following practices in place:

* Use the 2-of-3 broker ceremony to release repository and metadata keys; do not
   recreate a repository password file for scheduled jobs.
* Keep cloud access keys out of shell history and make sure they are rotated on a
  schedule.
* Prefer short-lived credentials from a workload identity or IAM role over long-
  lived static keys.
* Use a dedicated bucket or prefix for each repository role so archival,
  metadata, and active repository data never share the same retention policy.
* Verify restore tests from each tier before you rely on deep-archive recovery in
  an incident.

This pattern is especially useful for personal or small-team backup systems that
need an offsite mirror, local indexing, and a durable low-cost archival tier
without forcing all data through a single cloud vendor or a single retention
policy.


