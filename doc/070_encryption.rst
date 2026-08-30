..
  Normally, there are no heading levels assigned to certain characters as the structure is
  determined from the succession of headings. However, this convention is used in Python’s
  Style Guide for documenting which you may follow:

  # with overline, for parts
  * for chapters
  = for sections
  - for subsections
  ^ for subsubsections
  " for paragraphs

##########
Encryption
##########


*"The design might not be perfect, but it’s good. Encryption is a first-class feature,
the implementation looks sane and I guess the deduplication trade-off is worth
it. So… I’m going to use vaultic for my personal backups.*" `Filippo Valsorda`_

.. _Filippo Valsorda: https://words.filippo.io/vaultic-cryptography/

************************
Managing repository keys
************************

The ``key`` command allows you to set multiple access keys or passwords
per repository. In fact, you can use the ``list``, ``add``, ``remove``, and
``passwd`` (changes a password) sub-commands to manage these keys very precisely:

.. code-block:: console

    $ vaultic -r /srv/vaultic-repo key list
    enter password for repository:
     ID          User        Host        Created
    ----------------------------------------------------------------------
    *eb78040b    username    kasimir   2015-08-12 13:29:57

    $ vaultic -r /srv/vaultic-repo key add
    enter password for repository:
    enter password for new key:
    enter password again:
    saved new key as <Key of username@kasimir, created on 2015-08-12 13:35:05.316831933 +0200 CEST>

    $ vaultic -r /srv/vaultic-repo key list
    enter password for repository:
     ID          User        Host        Created
    ----------------------------------------------------------------------
     5c657874    username    kasimir   2015-08-12 13:35:05
    *eb78040b    username    kasimir   2015-08-12 13:29:57

Note that the currently used key is indicated by an asterisk (``*``).

*****************************
Encrypting SlateDB metadata
*****************************

SlateDB metadata encryption is separate from repository pack encryption. It
encrypts and authenticates SlateDB manifests, WALs, SSTs, and checkpoints with
a repository-specific AES-256-GCM data-encryption key (DEK). The DEK is wrapped
by one or more independent key-encryption-key slots and never stored in
plaintext.

Initialize or migrate metadata with a protected recovery passphrase file. On
Unix systems, secret and token files must not be accessible by group or other
users.

.. code-block:: console

  $ chmod 600 metadata-recovery
  $ vaultic -r /srv/vaultic-repo index encrypt \
    --repository-id REPOSITORY-UUID \
    --metadata-recovery-passphrase-file metadata-recovery \
    --start-daemon

Normal operation must use ``--metadata-encryption required``. The
``initialize`` mode is only for initial encryption and resumable migration. An
encrypted repository refuses to start if its envelope or persistent
``meta:encryption`` policy is missing, malformed, belongs to another
repository, or pins a different algorithm or object format.

Key slots
=========

``vaultic index keys status`` reports only slot identifiers, providers,
priorities, envelope generation, and the active DEK version. It never reports
keys or wrapped key blobs. Supported providers are:

* ``local-argon2id`` with a protected passphrase file;
* ``aws-kms`` with the AWS SDK credential chain and encryption context;
* ``azure-key-vault`` with a versioned Key Vault or Managed HSM key URL;
* ``gcp-kms`` with a CryptoKey resource name;
* ``vault-transit`` with an HTTPS ``/v1/<mount>/keys/<name>`` URL; and
* ``pkcs11`` with an AES secret key reference such as
  ``pkcs11:module-path=/usr/lib/pkcs11.so;slot-id=1;object=vaultic;type=secret-key``.

Azure, GCP, and Vault access tokens and PKCS#11 PINs are supplied in protected
files. Rotate those credentials according to the provider policy; they are
read when the daemon starts and are not persisted in the envelope. AWS uses
its normal credential chain.

Adding, removing, or rotating a slot publishes a new immutable envelope
generation and mirrors that exact generation to the repository backend. A
mirror failure makes the command fail but does not overwrite either immutable
copy; correct the backend error and rerun the operation or
``vaultic index keys mirror-envelope``. Never delete the latest local and
repository copies together.

Slots are tried by ascending priority. If cloud slots exist and only an
offline recovery slot can unlock the DEK, startup fails unless
``--metadata-recovery-unlock`` (or ``--metadata-recovery-ack`` for key-in-DB
startup) explicitly acknowledges the downgrade. An acknowledged recovery
unlock emits a warning-level ``auth`` event.

DEK rotation
============

The rotation command activates a new DEK, rewrites objects in bounded batches,
performs a second authenticated scan, and retires old read keys only after the
scan proves that no old-version, plaintext, or invalid object remains.

.. code-block:: console

  $ vaultic -r /srv/vaultic-repo index keys rotate-dek \
    --confirm --batch-size 128 --metadata-encryption required

If interrupted, rerun with ``--resume``. Both the activation generation and
the verified-retirement generation are mirrored. ``vaultic index check``
reports plaintext and invalid encrypted objects as dirty findings. Valid
objects still awaiting a DEK rewrite are warnings.

Repository master key in metadata
=================================

After metadata encryption is established, the repository master key can be
stored as an immutable encrypted metadata record:

.. code-block:: console

  $ vaultic -r /srv/vaultic-repo index keys store-master-key --confirm

Subsequent repository commands may use ``--metadata-key-in-db`` together with
the metadata daemon and provider options. Password keyfiles remain supported
in parallel. Keep an independent escrow before relying on key-in-DB: loss of
both SlateDB metadata and escrow leaves only the ordinary repository password
keys as recovery mechanisms.

Escrow and total metadata loss
==============================

Create at least one escrow under a separately administered provider key and
retain the standalone record as well as the automatic repository mirror:

.. code-block:: console

  $ vaultic -r /srv/vaultic-repo index keys escrow create \
    --escrow-id disaster-recovery --provider aws-kms \
    --key-reference arn:aws:kms:REGION:ACCOUNT:key/KEY-ID \
    --record-file escrow-disaster-recovery.json

An escrow record contains ciphertext and identifiers, not the plaintext master
key. Test recovery periodically. After total metadata loss, copy the mirrored
``escrow-<id>.json`` record out of the repository backend if the standalone
copy is unavailable. Start a temporary daemon with a fresh empty data
directory (the recovery RPC does not read SlateDB records), then recover a
mode-0600 direct-open key file:

.. code-block:: console

  $ vaultic index keys escrow recover \
    --repository-id REPOSITORY-UUID --record-file escrow-disaster-recovery.json \
    --output-key-file recovered-master-key \
    --daemon-data-dir /tmp/vaulticdb-recovery --daemon-object-store memory \
    --start-daemon
  $ vaultic -r /srv/vaultic-repo --key-file recovered-master-key \
    --metadata-loss-recovery snapshots

Destroy the recovered plaintext key file securely after the recovery. Provider
purpose and repository bindings prevent using an escrow record for another
repository. ``--metadata-loss-recovery`` is an explicit disaster mode: it
bypasses the authoritative SlateDB manifest and reads the legacy JSON index,
requires a direct master key, cannot use key-in-DB, and emits a warning-level
``lifecycle`` event. Its metadata engine rejects writes. Use it to restore or
inspect data, then rebuild authoritative metadata before resuming backups.

Monitoring and response
=======================

Route ``auth``, ``integrity``, and ``lifecycle`` observability categories to
the Phase 17 syslog exporter. Key unwraps and recovery use are ``auth`` events;
slot and rotation changes are ``lifecycle`` events; authenticated-object
failures are critical ``integrity`` events and surface to clients as gRPC
``data_loss``. Treat an unexpected integrity event as corruption or tampering:
stop writers, preserve the encrypted objects and envelope generations, run
``vaultic index check``, and restore only from a verified copy.
