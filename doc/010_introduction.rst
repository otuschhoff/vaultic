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

############
Introduction
############

Vaultic is a fast and secure backup program. The following sections present
typical workflows, starting with installing, preparing a new
repository, and making the first backup.

Quickstart guide
****************

To get started with a local repository, first define some environment variables:

.. code-block:: console

    export VAULTIC_REPOSITORY=/srv/vaultic-repo
    export VAULTIC_PASSWORD=some-strong-password

Initialize the repository (first time only):

.. code-block:: console

    vaultic init

Create your first backup:

.. code-block:: console

    vaultic backup ~/work

You can list all the snapshots you created with:

.. code-block:: console

    vaultic snapshots

You can restore a snapshot by noting the snapshot ID you want and running:

.. code-block:: console

    vaultic restore --target /tmp/restore-work your-snapshot-ID

It is a good idea to periodically check your repository's metadata:

.. code-block:: console

    vaultic check
    # or full data:
    vaultic check --read-data

For more details continue reading the next sections.
