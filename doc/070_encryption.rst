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

***********************************
Quorum recovery capsules and broker
***********************************

The maintainer-facing implementation map, transition invariants, state
machines, and failure-reconciliation matrix are maintained in
``doc/vaultic/02-architecture/08-quorum-key-broker.md``. This section is the
operator runbook and uses the same transition names.

The key-in-DB and standalone escrow procedures above are the migration
baseline for Phase 20. They are not quorum-compliant access routes. A migrated
repository uses one immutable recovery capsule as the sole managed holder of
authenticated wrapped copies of both the SlateDB metadata DEK and repository
master key. The capsule is available before SlateDB, stored in a deterministic
local generation directory, mirrored into
``_vaultic/recovery-capsules/<generation>.json``, and safe to export for
offline availability. Capsule ciphertext is not secret without a satisfying
set of member credentials, but loss of every capsule copy is unrecoverable.

Each capsule generation has a new random root wrapping secret. HKDF derives
independent wrapping keys for ``metadata-dek`` and
``repository-master-key``; AES-256-GCM authenticates each payload and the
complete logical header. Repository identity, generation, key versions,
policy hash, algorithm, and broker identity are bound independently of the
file location. Moving an unchanged capsule between its local, mirror, and
offline locations does not change authentication. Swapping payloads, members,
repositories, policies, or generations fails closed.

Unlock policies
===============

A policy is an expression of ``any_of``, ``all_of``, and threshold groups.
Offline passphrase members use Argon2id with per-member parameters; offline
keyfile members use an HKDF-derived member key. Threshold groups use Shamir
sharing, and every share is independently bound to the repository, capsule
generation, policy, group, member, index, threshold, share count, provider,
and root-key version.

``vaultic index unlock status`` reports the minimum effective number of
custodians across all alternatives. ``principal-verified`` means cloud seats
carry distinct validated immutable IAM principals. ``hardware-verified``
means hardware seats carry distinct pinned credentials with required user
presence. ``custody-assumed`` means offline files or passphrases are involved;
software cannot prove that different humans actually hold those values. A
retained 1-of-1 bootstrap alternative, ordinary repository password keyfile,
direct key, standalone escrow, duplicate cloud key, overlapping principal, or
duplicate hardware credential is a complete-key bypass and makes the overall
deployment non-compliant regardless of the advertised normal threshold.

Use ``vaultic index keys quorum verify`` for a strict comparison of a local
capsule with the running broker's authenticated repository, generation,
logical capsule ID, and policy hash. ``index keys status --capsule`` and
``index check --quorum-capsule`` additionally report configured password,
direct-key, Azure-secret, no-password, key-in-DB, metadata-passphrase, and
standalone metadata-slot bypasses. Routes that cannot be discovered from the
running process and repository require a signed operator inventory. Generate and
protect a dedicated Ed25519 keypair, with the private key held by the security
reviewer and the public key pinned on hosts that evaluate compliance::

  $ vaultic index keys quorum generate-attestation-key \
      --private-key /secure/bypass-attestation.key \
      --public-key /etc/vaultic/bypass-attestation.pub

After inventorying every storage system, custodian workstation, automation host,
backup, escrow destination, and restart mechanism, sign an attestation bound to the
exact repository, capsule generation, logical capsule identity, and policy hash::

  $ vaultic index keys quorum attest-bypasses \
      --capsule /secure/recovery-capsule.json \
      --private-key /secure/bypass-attestation.key \
      --output /secure/bypass-attestation.json \
      --valid-for 720h \
      --confirm-no-plaintext-key-exports \
      --confirm-no-external-standalone-escrow \
      --confirm-generation-anchors-current \
      --confirm-broker-credentials-protected \
      --confirm-no-warm-restart-material \
      --confirm-offline-custodian-separation

Pass both ``--bypass-attestation`` and ``--bypass-attestation-key`` with
``index keys status --capsule`` or ``index check --quorum-capsule``. Compliance
fails closed when either file is absent, unprotected, malformed, signed by another
key, expired, valid for more than 90 days, bound to another capsule, or missing any
statement. Reissue after every policy or capsule-generation change and rotate the
signing key through a separately reviewed public-key deployment. An attestation is
an auditable assertion, not proof by itself; retain the reviewed inventory and
destruction evidence with it.

The capsule library and online mutation workflow support mixed offline and
externally wrapped members. Custodian contribution adapters are available for
Azure Key Vault/Managed HSM, AWS KMS and CloudHSM-backed KMS keys, and Google
Cloud KMS/Cloud HSM.
External share framing binds the complete capsule and member context even when
the provider wrapping primitive has no native AAD. AWS role sessions are
normalized to stable IAM role ARNs; CloudHSM members require provider
attestation of their hardware-backed key origin. Azure contribution checks the
provider-accepted token's Key Vault audience, tenant, immutable object ID, and
expiry. Google contribution checks provider token introspection, project,
immutable subject, and remaining lifetime. These checks bind the principal
that the provider accepted; the client does not independently implement the
provider's JWT signing-key validation.

Online mutation accepts each cloud member through a separate mode-0600 JSON
definition supplied with repeatable ``--external-member`` flags. The file
contains no wrapped share, but names the member, provider, immutable key
reference, and expected principal binding. Azure and Google definitions name a
separate mode-0600 ``bearer_token_file``; AWS definitions omit it and use the
SDK credential chain. Tokens, credentials, PINs, and shares must never appear
in arguments or logs. For example:

.. code-block:: json

  {
    "member_id": "azure-alice",
    "provider": "azure-key-vault",
    "key_reference": "https://example.vault.azure.net/keys/recovery/KEY-VERSION",
    "principal": {
      "authority": "entra",
      "tenant_account_or_project": "TENANT-ID",
      "immutable_principal_id": "OBJECT-ID"
    },
    "bearer_token_file": "/secure/alice.azure-token"
  }

Grant each member identity access only to its own versioned key and keep the
broker service identity out of every custodian role. For Azure, assign a
Key Vault or Managed HSM data-plane role at the individual key scope that
permits ``wrapKey`` during enrollment and ``unwrapKey`` during contribution;
do not grant secret, certificate, key creation, deletion, purge, or vault-wide
administrator rights. Record the Entra tenant ID and immutable user or group
object ID in the member definition. For AWS, scope ``kms:Encrypt`` and
``kms:Decrypt`` to one key ARN, add ``kms:DescribeKey`` for a CloudHSM custom
key store, and permit ``sts:GetCallerIdentity`` so the contribution can bind
the accepted role. Map each IAM Identity Center duty group to a distinct role;
do not reuse one role or KMS key for two seats. For Google Cloud, grant only
``cloudkms.cryptoKeyVersions.useToEncrypt`` and
``cloudkms.cryptoKeyVersions.useToDecrypt`` on one CryptoKey and record the
project plus immutable principal subject. Use an HSM protection-level key for
``gcp-cloud-hsm`` members.

Before activation, have a different operator inspect the provider policy, key
version, tenant/account/project, and immutable principal recorded in every
definition. Exercise enrollment and contribution with the intended principal,
then prove that a different principal and a neighboring key are denied. Retain
the cloud provider's data-plane and identity audit records with the capsule
generation event. Live-provider validation and organization-specific group
mapping remain deployment gates.

YubiKey PIV members use the separately packaged ``vaultic-key-custodian``
executable and Yubico's YKCS11 module. Provision an RSA key in a PIV slot with
PIN and touch required by the token policy. The member key reference has this
form:

.. code-block:: text

  pkcs11:module-path=/absolute/path/libykcs11.so;slot-id=1;id=9a;public-key-sha256=HEX;type=rsa-key-pair

``public-key-sha256`` is the lowercase SHA-256 digest of the length-prefixed RSA
modulus and public exponent returned by PKCS#11. Set the hardware
``public_key`` field to ``sha256:HEX``, set ``credential_id`` to an
organization-unique token/slot identity, record the token serial where
available, and set ``user_presence_required`` to true. The broker verifies the
public key against the token while enrolling. Contribution uses
``index unlock contribute --yubikey-piv-pin-file FILE``; the helper verifies
the PIN and performs the private-key operation on the token. Use
``--custodian-path`` when the helper is not on ``PATH``. PIN files must be mode
0600 and are never passed as PIN values in command arguments or logs.
Wrapped hardware shares are sent to the custodian helper as bounded base64 on
standard input, not as command-line arguments. Vaultic starts every custodian
operation with an empty environment so unrelated parent credentials are not
inherited; only protected PIN-file paths and non-secret member context appear
in the argument list.

FIDO2 ``hmac-secret`` members use a non-resident credential scoped to a
DNS-style relying-party ID. Enroll one with:

.. code-block:: console

  $ vaultic index keys quorum enroll-fido2 \
      --member fido-alice --relying-party-id recovery.example.org \
      --pin-file /secure/alice.fido-pin --output /secure/fido-alice.json

The generated mode-0600 file contains the credential ID, relying-party ID,
pinned credential public-key hash, public verification key, and verified
attestation certificate fingerprint when the authenticator supplies one. It
contains neither the authenticator seed nor an ``hmac-secret`` output. During
policy mutation and contribution, the custodian helper performs a fresh
PIN-and-touch assertion, verifies UP, UV, credential ID, RP ID, and assertion
signature, and requests a domain-separated secret for the repository/member.
Only that transient 32-byte output enters process memory, where it wraps or
unwraps the bound share and is then zeroized. Use ``--fido2-pin-file`` when
contributing.

macOS Secure Enclave and Touch ID members
-----------------------------------------

The ``macos-secure-enclave`` provider creates a non-exportable P-256 private
key in the Mac's Secure Enclave and stores its reference in the Data Protection
Keychain. It is separate from WebAuthn passkeys and does not use iCloud
Keychain synchronization. Enroll a seat on the Mac that will contribute it:

.. code-block:: console

  $ vaultic index keys quorum enroll-macos-secure-enclave \
      --member mac-alice --output /secure/mac-alice.json

The native ``vaultic-key-custodian`` creates a random application tag and a
key protected by ``biometryCurrentSet`` plus private-key usage. The mode-0600
definition contains only that tag, the uncompressed P-256 public key and its
SHA-256 fingerprint, and the strict access-control mode. The private key never
leaves the Secure Enclave. If validation or protected definition-file creation
fails, enrollment deletes the exact tagged key after rechecking its public key.
The packaged helper is code-signed with the stable
``com.vaultic.key-custodian`` identifier and Data Protection Keychain
entitlements. Production packaging must replace the default ad hoc signature
with the organization's signing identity while retaining a compatible
application identifier and keychain access group across upgrades.

During policy creation, Vaultic generates an ephemeral P-256 key, performs
ECDH against the enrolled public key, derives a purpose-separated wrapping key
with HKDF-SHA256, and encrypts the member share with AES-256-GCM. The
authenticated context includes the repository, generation, policy, group,
member, share coordinates, root-key version, provider, key reference, hardware
binding, and purpose. During contribution, the custodian retrieves the exact
tagged private key, verifies its public key against the capsule, and asks macOS
to authorize the ECDH operation with Touch ID. The wrapped share travels to
the helper on bounded standard input; the helper environment is empty and the
derived shared secret and plaintext share are zeroized after use.

Contribute the enrolled member with the signed-session and fingerprint options
used by every custodian, selecting the local hardware route explicitly:

.. code-block:: console

  $ vaultic index unlock contribute \
      --capsule /secure/capsules/00000000000000000002.json \
      --broker-socket /run/vaultic/key-broker.sock \
      --session-file /secure/unlock-session.json \
      --member mac-alice --macos-secure-enclave \
      --confirm-fingerprint FINGERPRINT

Cancellation, biometric mismatch or lockout, changed biometric enrollment,
key deletion, unavailable Secure Enclave, malformed ciphertext, and public-key
or context substitution fail closed. The strict provider does not silently
fall back to a login password or device passcode. Changing the enrolled Touch
ID set invalidates the key, and loss of the Mac is unrecoverable for that seat.
Always combine it with an independent recovery member or alternative.

Touch ID proves that macOS authorized one locally enrolled biometric; it does
not prove which person supplied it. Status therefore reports this seat as
``hardware-verified``, never ``principal-verified``. Duplicate application
tags or public keys are non-compliant, and two keys on one Mac must not be
treated as two independent people.

A mixed local/cloud/offline 2-of-4 policy can combine a YubiKey, this Mac, a
GCP principal, and an offline key, for example:

.. code-block:: console

  $ vaultic index keys quorum create-group operators \
      --repository-id REPOSITORY-UUID \
      --capsule /secure/capsules/00000000000000000001.json \
      --capsule-directory /secure/capsules --threshold 2 \
      --external-member /secure/yubikey-alice.json \
      --external-member /secure/mac-alice.json \
      --external-member /secure/gcp-alice.json \
      --member offline-alice=offline-keyfile:/media/offline/member.key

Exercise every intended pair before activation evidence is accepted. In
particular, test Mac loss and biometric-set replacement by recovering through
a pair that does not contain the Secure Enclave seat. Physical Secure Enclave
and Touch ID validation on every advertised macOS release target remains a
production deployment gate; deterministic tests and code-signature checks do
not substitute for that exercise.

Model backup PIV or FIDO2 authenticators as separate members under an
``any_of`` policy for one hardware seat. Each token must have a distinct
credential ID and public key; reusing either is reported as a policy finding.
Linux release bundles keep the daemon and broker statically linked but build
the custodian helper for GNU libc so it can dynamically access YKCS11 and HID.
Linux hosts require the HID/udev runtime permissions needed by the service
account; macOS uses the native HID backend.

Custody progression and separation
===================================

Treat a bootstrap 1-of-1 member as temporary and non-compliant. Move first to
an offline threshold while the broker is unlocked. ``create-group`` requires
the complete resulting member set and each protected credential file, for
example:

.. code-block:: console

  $ vaultic index keys quorum create-group operators \
      --capsule /secure/capsules/00000000000000000001.json \
      --capsule-directory /secure/capsules --threshold 2 \
      --member alice=offline-argon2id:/secure/alice.passphrase \
      --member bob=offline-keyfile:/media/bob/member.key \
      --member carol=offline-keyfile:/media/carol/member.key

After activation the broker relocks. Perform a fresh ceremony proving each of
the three 2-member combinations succeeds and each single member fails, then
run ``index keys quorum verify`` and ``index keys status --capsule``. Destroy
the bootstrap credential only after those checks and include its absence in
the signed bypass inventory. Offline separation remains ``custody-assumed``;
store each credential with a different custodian and in a different failure
domain.

To move to a principal-verified cloud 2-of-4 policy, prepare four distinct
mode-0600 external-member definitions and run ``create-group`` with
``--threshold 2`` and four ``--external-member`` arguments. Every mutation
must supply all resulting member definitions or credentials, not only the
changed seat. To retain offline break-glass as a separate alternative, use a
mode-0600 ``--policy-file`` containing the complete ``any_of`` expression and
supply both the cloud and offline member material. A reduction in the minimum
effective threshold additionally requires
``--acknowledge-policy-downgrade``. Verify that status is
``principal-verified``, that no key or immutable principal is reused across
seats, and that every unwanted old alternative has disappeared before
destroying superseded credentials.

Online policy mutation
======================

``create-group``, ``add-member``, ``remove-member``, ``set-threshold``, and
``replace-member`` require an unlocked broker and a release manifest authorized
for ``policy-mutation``. Supply every resulting member protection on every
mutation. Simple threshold policies use repeatable ``--member`` and
``--external-member`` inputs. A complete JSON policy supplied with
``--policy-file`` enables composed ``member``, ``any_of``, ``all_of``, and
threshold expressions. The broker rejects missing, extra, or duplicate leaves.

The broker recovers the already authenticated metadata DEK and repository
master key, creates a fresh root and every share, and retains the exact new
capsule and digest. VaulticDB publishes the repository mirror before the local
immutable generation. Only then does the broker activate that exact digest and
relock. Reducing the minimum effective threshold requires
``--acknowledge-policy-downgrade`` and emits a critical event.

If publication or activation is interrupted, the candidate remains pending.
Do not start another mutation. Retry the same immutable bytes with:

.. code-block:: console

  $ vaultic index keys quorum resume-mutation \
      --repository-id REPOSITORY-UUID \
      --capsule-directory /secure/capsules

The signed resume operation retrieves the exact retained candidate, repeats
create-only publication idempotently, and activates only its recorded digest.
Use ``quorum cancel-mutation --confirm`` only after proving that no local or
repository mirror of the candidate generation was published; cancellation
cannot revoke immutable bytes that already escaped.

A membership or threshold change only reshapes custody of the existing keys.
After suspected metadata-DEK exposure, run the authenticated ``rotate-dek``
rewrite described above and do not retire the old version until the second scan
is clean. After suspected repository master-key exposure, there is no safe
in-place capsule-only repair: freeze writers, initialize a new repository with
a fresh master key and new Phase 20 capsule, copy every snapshot through an
authenticated source read and target write, and perform sampled full restores
from the target. Compare snapshot inventory and pack checks, then switch writers
and retain or destroy the old repository according to incident-retention
policy. Merely resharing the old key or deleting its latest capsule does not
revoke historical copies.

Broker lifecycle and trust boundary
===================================

``vaultic-key-broker`` is a separate local-only service. It starts locked,
reads the newest valid capsule generation without opening SlateDB, and creates
an unlock epoch only after the policy is satisfied and both payloads
authenticate. It never persists plaintext keys or warm-restart material.
Explicit lock, configured epoch expiry, broker exit or crash, and host restart
end the epoch and revoke all leases. Consequently, broker or host restart
requires another custodian ceremony; an authenticated application restart
during the same live epoch does not.

The broker socket and its parent directory are owner-only. The service checks
OS peer credentials, hashes the actual peer executable, requires a root-owned
non-writable installation path, verifies a signed release manifest, enforces
component and version authorization, and rejects software downgrade. Every
connection first negotiates ``vaultic-key-broker.v1``. Each lease request must
answer a one-time random challenge bound to that protocol and the executable
digest observed by the broker; the challenge cannot be replayed. Generate
the long-term broker identity offline and retain its public key for capsule
creation:

.. code-block:: console

  $ vaultic-key-broker identity-init /secure/broker-identity.key broker-identity.pub

The private output is created exclusively with mode 0600. Create release
manifests with a separate protected Ed25519 release key. The identity argument
must match a configured release authorization:

.. code-block:: console

  $ vaultic-key-broker release-sign release-signing.key /usr/local/bin/vaultic \
      vaultic 42 production-release vaultic.release.json
  $ vaultic-key-broker release-sign release-signing.key /usr/local/bin/vaulticdb \
      vaulticdb 42 production-release vaulticdb.release.json

Run the broker under a dedicated least-privilege account. Do not pass keys,
shares, PINs, tokens, or passphrases in command-line arguments, environment
variables, logs, or inherited descriptors. The broker disables core dumps,
locks recovered keys in memory where supported, marks Linux pages non-dumpable,
and zeroizes session, share, key, and lease buffers. These measures do not
protect against root, kernel, debugger, broker-process, or authorized-client
compromise during an unlock epoch. An authorized client receives key bytes in
protected process memory and can copy them while its lease is valid.

Service installation
====================

Deployment templates are provided as
``contrib/systemd/vaultic-key-broker.service`` and
``contrib/launchd/com.vaultic.key-broker.plist``. Install the broker executable
at the root-owned, non-writable path in the selected template. Create the
dedicated ``vaultic`` or ``_vaultic`` account without a login shell or home
directory. The broker config and identity key must be non-symlink regular files
owned for that account and mode 0600; capsule directories must be readable by
it. The service creates the socket parent and socket owner-only. Configure every
authorized client with the broker account's numeric UID because peer UID is
part of authorization.

The systemd template uses a read-only filesystem view except for
``/run/vaultic``, private temporary and device namespaces, no-new-privileges,
native system calls, and zero core size. It permits ``AF_INET`` and ``AF_INET6``
outbound connections because online Azure/AWS/GCP policy enrollment is brokered;
the executable still creates no network listener. Sites using offline-only
policies can reduce ``RestrictAddressFamilies`` to ``AF_UNIX``. Adjust
``ReadWritePaths`` only when the socket is deliberately placed elsewhere.

The launchd template is a system LaunchDaemon, sets umask 0077 and zero core
limits, and restarts only after unsuccessful exit. Install it root-owned under
``/Library/LaunchDaemons`` after replacing paths and account names. macOS does
not expose systemd's namespace controls; apply an organization-reviewed sandbox
profile or endpoint policy in addition to the template. On either platform,
service restart starts a new locked broker and therefore requires a new quorum.

Signed client upgrades
======================

Keep the broker identity key and client release-signing key separate. A second
``identity-init`` invocation can create the raw Ed25519 release key and its
base64 public key; name and custody those outputs for release signing rather
than broker identity. For each installed binary, sign the exact bytes with a
strictly increasing integer version:

.. code-block:: console

  $ vaultic-key-broker identity-init /secure/release-signing.key release-signing.pub
  $ vaultic-key-broker release-sign /secure/release-signing.key \
      /usr/local/bin/vaultic vaultic 43 production-release vaultic.release.json

Stage the binary and manifest together, update the broker authorization range
only after reviewing the component, release identity, UID, capabilities, and
digest, then atomically install them at their root-owned non-writable paths.
Keep the preceding version authorized only for a defined rollback window;
never lower ``minimum_version`` to recover from an unsigned deployment.

To rotate the release-signing key, generate a new pair offline, add a second
authorization entry for the new public key, sign every authorized component,
and restart the broker. The restart begins locked and requires a new quorum.
After all clients have connected with new-key manifests, remove the old
authorization, restart and unlock once more, and archive the overlap and
retirement events. Broker identity rotation is different: loss of that key
uses the identity-recovery procedure below and publishes a new capsule.

Unlock ceremony
===============

The broker creates a fresh one-time HPKE contribution session signed by the
Ed25519 identity pinned in the capsule. A custodian verifies the capsule and
signature locally, compares the displayed fingerprint with the other
participants over an independent channel, unwraps only their member share,
encrypts it to the session, and submits it. Never contribute when the
fingerprint differs.

.. code-block:: console

  $ vaultic index unlock contribute --prepare \
      --broker-socket /run/vaultic/key-broker.sock \
      --capsule /var/lib/vaultic/capsules/00000000000000000001.json \
      --session-file /secure/session.json
  $ vaultic index unlock contribute \
      --broker-socket /run/vaultic/key-broker.sock \
      --capsule /var/lib/vaultic/capsules/00000000000000000001.json \
      --session-file /secure/session.json --member alice \
      --passphrase-file /secure/alice.passphrase \
      --generation-anchor /secure/alice.generation \
      --confirm-fingerprint XXXX-XXXX-XXXX-XXXX-XXXX-XXXX-XXXX-XXXX

The generation anchor is mode 0600 and monotonic. Every contribution carries
the newest generation that custodian tooling has observed. If storage serves
an older capsule, contributions attesting a newer generation make the broker
abort, so rollback requires deceiving the configured quorum as well as
rewriting storage. Repository mirrors, offline exports, and remote audit logs
are supporting generation evidence.

Clients receive separate capabilities. ``vaulticdb`` may request only the
metadata DEK on a connection-bound lease. A Vaultic job may request the
repository master key or the read-only ``metadata-loss-recovery`` capability
under a short-lived job lease. Lease expiry limits duration and scope; it
cannot retract bytes copied by a compromised client. End an epoch explicitly
when work is complete:

.. code-block:: console

  $ vaultic index unlock lock --broker-socket /run/vaultic/key-broker.sock --confirm

Migration from key-in-DB
========================

Migration is deliberately two-phase. Preparation reads the active metadata
DEK and database master key while the old route still works, creates and
reconstructs the new capsule, authenticates both payloads, publishes local and
repository mirrors, and records the exact capsule digest without deleting the
old key:

.. code-block:: console

  $ vaultic index keys quorum migrate-prepare \
      --repository-id REPOSITORY-UUID --capsule-directory /secure/capsules \
      --generation 1 --group operators --threshold 2 \
      --broker-public-key broker-identity.pub \
      --member alice=offline-argon2id:/secure/alice.passphrase \
      --member bob=offline-keyfile:/media/bob/member.key \
      --member carol=offline-keyfile:/media/carol/member.key \
      --state-file /secure/capsule-migration.json

Start and unlock the broker from that capsule before finalization. Finalize
opens the repository through a ``metadata-loss-recovery`` lease, authenticates
the repository configuration and a pack header when packs exist, then obtains
a fresh broker lease and proves possession of the same repository key to
VaulticDB with a domain-separated HMAC over the repository and prepared
capsule digest. Preparing the same digest is idempotent, while a different
pending digest or a post-finalization preparation is rejected. Only an exact
pending digest and valid key proof atomically remove the database master-key
record. Wrong or repeated finalization fails without deleting the retained
route. ``index keys status`` exposes the redacted pending or finalized digest;
an unfinished migration is reported as non-compliant.

.. code-block:: console

  $ vaultic index keys quorum migrate-finalize \
      --repository-id REPOSITORY-UUID \
      --state-file /secure/capsule-migration.json --confirm \
      --retire-legacy-routes --confirm-standalone-escrow-destroyed \
      --key-broker-socket /run/vaultic/key-broker.sock \
      --key-broker-release-manifest vaultic.release.json

Finalization removes ordinary repository password-key records and mirrored
standalone escrow records after capsule and pack proof succeeds but before the
database master-key record is removed. A retirement failure therefore leaves
the database route available for a retry. The operator must separately destroy
exported escrow and plaintext key files and explicitly attest that fact.
Retaining any complete-key copy remains a non-compliant bypass. Preserve
historical capsules as
ciphertext evidence, but remember that resharing does not revoke a copied old
generation held together with enough old credentials. Rotate and rewrite the
metadata DEK after retiring a suspected metadata path; repository master-key
exposure requires repository rekeying or pack rewrite.

Metadata loss and recovery
==========================

The capsule is independent of SlateDB. Once unlocked, an explicitly authorized
Vaultic recovery job can request ``metadata-loss-recovery`` and inspect or
restore packs through the legacy index without writing a plaintext key file.
Recovery mode is read-only and must be explicitly selected. Preserve suspect
metadata and audit evidence before recovery. To rebuild a local candidate, use
``index import --metadata-rebuild-initialize`` with a new
``--daemon-data-dir``, brokered ``required`` metadata encryption,
``--metadata-loss-recovery``, ``--activate``, and
``--confirm-metadata-loss-rebuild``. The repository and candidate daemon must
use the same broker. The daemon rejects a candidate containing database
objects, writes authenticated recovery provenance before normal records, and
Vaultic compares the completed candidate against legacy metadata before
activating SlateDB authority.

For a remote candidate, select ``--daemon-object-store s3`` and provide a
bucket plus a dedicated non-empty ``--daemon-s3-prefix`` instead of
``--daemon-data-dir``. Use a new generation-specific prefix whose access policy
does not permit other writers. The daemon lists that prefix and rejects it if
any database object already exists; recovery-capsule mirrors under
``_vaultic/`` do not count as database objects. Activation occurs only after
the same encrypted provenance, full import, and consistency checks used for a
local candidate. Preserve the old prefix until the rebuilt authority has been
independently listed and restored.

Complete broker-host loss also loses the session-signing identity. Start the
replacement broker only with its explicit identity-recovery configuration and
a fresh identity key. Recovery sessions are visibly marked and normal clients
reject them. Every custodian must use ``--unverified-session`` and independently
compare the displayed fingerprint; this skips only verification against the
lost identity and does not skip endpoint, repository, generation, expiry,
transcript, or fingerprint checks. Critical authentication events record each
acknowledgement. The recovery broker grants no key leases. Its first successful
operation must publish a fresh capsule generation that pins the replacement
identity; the no-database publisher supports this even when SlateDB is absent.
Activation relocks the broker, after which ordinary signed sessions are
required again.

Create the replacement identity with ``vaultic-key-broker identity-init``.
Build a protected broker configuration that points at the surviving capsule,
the replacement private key, a new owner-only socket, and sets
``identity_recovery`` to ``true``; then start
``vaultic-key-broker RECOVERY-CONFIG.json``. Prepare a session normally and
require every contributor to add ``--unverified-session`` only after comparing
its fingerprint independently. Use an ordinary quorum policy mutation with the
complete resulting member set to publish the next generation; the recovery
broker automatically pins its replacement public identity. Stop the recovery
process, remove recovery mode from configuration, restart locked, and prove
that an unverified contribution is now rejected before completing a normal
signed ceremony. Preserve the critical recovery, publication, activation, and
relock events.

Local and S3 candidate rebuild plus no-database identity-repin paths are
implemented. The S3-compatible release lane destroys and lists an isolated
candidate prefix, rebuilds and reopens encrypted records, and scans raw objects
for known plaintext and the DEK. Repeat the exercise with production-provider
credentials and retention controls before treating a deployment as complete.

Audit ``auth``, ``integrity``, and ``lifecycle`` events remotely. Treat capsule
rollback, rejected contribution, client authorization denial, unexpected
lease, break-glass use, threshold downgrade, evidence of a persistent plaintext
key copy, and payload authentication failure as security-significant. Vaultic
does not provide a plaintext key-export command; authorized connection-bound
lease grants are the audited key-delivery boundary, while external copies are
covered by the signed bypass inventory. Run a periodic exercise from an empty,
isolated metadata directory: record the capsule generation and policy hash;
verify capsule discovery and fingerprints over an independent channel;
satisfy every supported normal and break-glass policy alternative; prove every
sub-threshold set fails; list and restore a sampled pack through a recovery
lease; run quorum verification and metadata consistency checks; and explicitly
relock the broker. Confirm the expected session, contribution, quorum, lease,
recovery, and lock events reached remote retention without secret fields.
Record date, repository, generation, participants, combinations exercised,
restored snapshot or object, event-query reference, deviations, and independent
sign-off. Do not reuse the recovery candidate as production authority unless
the documented activation checks are deliberately completed.

The broker writes one-line structured JSON security events to standard error,
which systemd and launchd route to their managed logs. Events cover capsule
discovery, session creation/expiry, accepted contributions and quorum
completion, lease grant/release/expiry and disconnect revocation, authorization
rejection, rollback rejection, payload-authentication failure, malformed
authenticated contribution payloads, identity-recovery
startup, policy mutation prepare/activate/cancel, explicit session closure and
lock, shutdown lock, and maximum-lifetime automatic lock. Event field names are allowlisted
and exclude credentials, tokens, PINs, shares, keys, and request bodies.
Client-side observability adds custodian member IDs and
publication/migration/recovery context that is not visible before an encrypted
contribution is authenticated. Configure remote collection and retention so a
lost broker host cannot erase the only generation evidence.
