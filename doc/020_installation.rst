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
Installation
############

Packages
********

Note that if at any point the package you’re trying to use is outdated, you
always have the option to use an official binary from the vaultic project.

These are up to date binaries, built in a reproducible and verifiable way, that
you can download and run without having to do additional installation work.

Please see the :ref:`official_binaries` section below for various downloads.
Official binaries can be updated in place by using the ``vaultic self-update``
command.

The environment variable ``$GITHUB_ACCESS_TOKEN`` can be set to use a personal 
access token when updating. This increases the rate limit through authenticated GitHub API 
requests, and prevents update failures when many 
unauthenticated requests have already been made from the same IP.

Alpine Linux
============

On `Alpine Linux <https://www.alpinelinux.org>`__ you can install the ``vaultic``
package from the official community repos, e.g. using ``apk``:

.. code-block:: console

    $ apk add vaultic

Arch Linux
==========

On `Arch Linux <https://archlinux.org/>`__, you can install the ``vaultic``
package from the official community repos, e.g. with ``pacman -S``:

.. code-block:: console

    $ pacman -S vaultic

Debian
======

On Debian, there's a package called ``vaultic`` which can be
installed from the official repos, e.g. with ``apt-get``:

.. code-block:: console

    $ apt-get install vaultic


Fedora
======

vaultic can be installed using ``dnf``:

.. code-block:: console

    $ dnf install vaultic

If you used vaultic from copr previously, remove the copr repo as follows to
avoid any conflicts:

.. code-block:: console

   $ dnf copr remove copart/vaultic

Gentoo Linux
============

On `Gentoo Linux <https://www.gentoo.org/>`__, you can install the ``vaultic``
package from the official repos using ``emerge``:

.. code-block:: console

    # emerge vaultic

macOS
=====

If you are using macOS, you can install vaultic using `Homebrew <https://brew.sh/>`__:

.. code-block:: console

    $ brew install vaultic

On Linux and macOS, you can also install it using `pkgx <https://pkgx.sh/>`__:

.. code-block:: console

    $ pkgx install vaultic

You may also install it using `MacPorts <https://www.macports.org/>`__:

.. code-block:: console

    $ sudo port install vaultic

Nix & NixOS
===========

If you are using `Nix / NixOS <https://nixos.org>`__,
there is a package named ``vaultic`` available in `nixpkgs <https://search.nixos.org/packages?query=vaultic>`__.
You can install it by adding this to your ``configuration.nix``:

.. code-block:: console

      environment.systemPackages = [
        pkgs.vaultic
      ];

Mise (Linux/macOS/Windows)
==========================

If you are using `Mise <https://mise.jdx.dev>`__,
you can install ``vaultic`` using this command:

.. code-block:: console

    $ mise use -g vaultic@latest

OpenBSD
=======

On OpenBSD 6.3 and greater, you can install vaultic using ``pkg_add``:

.. code-block:: console

    # pkg_add vaultic

FreeBSD
=======

On FreeBSD (11 and probably later versions), you can install vaultic using ``pkg install``:

.. code-block:: console

    # pkg install vaultic

openSUSE
========

On openSUSE (leap 15.0 and greater, and tumbleweed), you can install vaultic using the ``zypper`` package manager:

.. code-block:: console

    # zypper install vaultic

RHEL & CentOS Stream
====================

For supported RHEL / CentOS Stream releases, vaultic can be installed from the EPEL repository:

.. code-block:: console

    $ dnf install epel-release
    $ dnf install vaultic

Solus
=====

vaultic can be installed from the official repo of Solus via the ``eopkg`` package manager:

.. code-block:: console

    $ eopkg install vaultic

Windows
=======

vaultic can be installed using either `Scoop <https://scoop.sh/>`__ or `WinGet <https://learn.microsoft.com/en-us/windows/package-manager/>`__.

Regardless of the method, the ``vaultic.exe`` binary will be added to your ``PATH`` automatically, making the ``vaultic`` command accessible in PowerShell or CMD.

.. code-block:: console

    scoop install vaultic

.. code-block:: console

    winget install --exact --id vaultic.vaultic --scope Machine

By default, WinGet will install vaultic into the ``User`` scope, which is typically in your user's ``%LOCALAPPDATA%`` directory. This behavior may be undesirable for system-wide backups, so specifying ``--scope Machine`` is recommended so that vaultic is installed into ``%ProgramFiles%``. This requires elevation.

.. _official_binaries:

Official binaries
*****************

Stable releases
===============

You can download the latest stable release versions of vaultic from the `vaultic
release page <https://github.com/vaultic/vaultic/releases/latest>`__. These builds
are considered stable and releases are made regularly in a controlled manner.

There are both pre-compiled binaries for different platforms as well as the source
code available for download. Just download and run the one matching your system.

On your first installation, if you desire, you can verify the integrity of your
downloads by testing the SHA-256 checksums listed in ``SHA256SUMS`` and verifying
the integrity of the file ``SHA256SUMS`` with the PGP signature in ``SHA256SUMS.asc``.
The PGP signature was created using the key (`0x91A6868BD3F7A907 <https://vaultic.net/gpg-key-alex.asc>`__):

::

    pub   4096R/91A6868BD3F7A907 2014-11-01
          Key fingerprint = CF8F 18F2 8445 7597 3F79  D4E1 91A6 868B D3F7 A907
          uid                          Alexander Neumann <alexander@bumpern.de>
          sub   4096R/D5FC2ACF4043FDF1 2014-11-01

Once downloaded, the official binaries can be updated in place using the 
``vaultic self-update`` command (needs vaultic 0.9.3 or later):

.. code-block:: console

    $ vaultic version
    vaultic 0.9.3 compiled with go1.11.2 on linux/amd64

    $ vaultic self-update
    find latest release of vaultic at GitHub
    latest version is 0.9.4
    download file SHA256SUMS
    download SHA256SUMS
    download file SHA256SUMS
    download SHA256SUMS.asc
    GPG signature verification succeeded
    download vaultic_0.9.4_linux_amd64.bz2
    downloaded vaultic_0.9.4_linux_amd64.bz2
    saved 12115904 bytes in ./vaultic
    successfully updated vaultic to version 0.9.4

    $ vaultic version
    vaultic 0.9.4 compiled with go1.12.1 on linux/amd64

The ``self-update`` command uses the GPG signature on the files uploaded to
GitHub to verify their authenticity. No external programs are necessary.

.. note:: Please be aware that the user executing the ``vaultic self-update``
   command must have the permission to replace the vaultic binary.
   If you want to save the downloaded vaultic binary into a different file, pass
   the file name via the option ``--output``.

Unstable builds
===============

Another option is to use the latest builds for the master branch, available on
the `vaultic beta download site
<https://beta.vaultic.net/?sort=time&order=desc>`__. These too are pre-compiled
and ready to run, and a new version is built every time a push is made to the
master branch.

Docker container
****************

A minimal Docker image with just a few files and the vaultic
binary is available; you can get it with ``docker pull`` like this:

.. code-block:: console

    $ docker pull vaultic/vaultic

The container is also available on the GitHub Container Registry:

.. code-block:: console

    $ docker pull ghcr.io/vaultic/vaultic

Vaultic relies on the hostname for various operations. Make sure to set a static
hostname using ``--hostname`` when creating a Docker container, otherwise Docker
will assign a random hostname each time.

The container additionally honors traditional ``nice`` `(man page) <https://man7.org/linux/man-pages/man1/nice.1.html>`__ and ``ionice`` `(man page) <https://man7.org/linux/man-pages/man1/ionice.1.html#OPTIONS>`__ directives via the following environment variables.
This allows vaultic to be scheduled as a background process to reduce latency for the system while running.

* ``NICE``: If set, this adjusts the CPU prioritization via ``nice -n``. This defaults to no adjustment - standard CPU priority.
* ``IONICE_CLASS``: If set, this adjusts the IO prioritization of the vaultic process via ``ionice -c`` for the available classes.
  Note: if you attempt to set `real-time`, you *will* have to add the ``SYS_NICE`` capability (`see capabilities manpage <https://man7.org/linux/man-pages/man7/capabilities.7.html#DESCRIPTION>`__) to allow using this IO class.
* ``IONICE_PRIORITY``: This defaults to 4 (no prioritization or penalties); this is the prioritization within the given ``IONICE_CLASS``.

The following example runs vaultic such that other CPU and IO requests have higher priority. This effectively perturbs the system as minimally as possible while ``vaultic`` runs.

.. code-block:: console

    # docker run -e NICE=20 -e IONICE_CLASS=2 -e IONICE_PRIORITY=7 ghcr.io/vaultic/vaultic

*Remember* that this invocation is explicitly telling your CPU and IO scheduler to deprioritize vaultic.  This typically will result in a longer runtime.  For a system with heavy load, this can be drastically longer.


From source
***********

vaultic is written in the Go programming language and you need at least
Go version 1.25. Building vaultic may also work with older versions of Go,
but that's not supported. See the `Getting
started <https://go.dev/doc/install>`__ guide of the Go project for
instructions how to install Go.

In order to build vaultic from source, execute the following steps:

.. code-block:: console

    $ git clone https://github.com/vaultic/vaultic
    [...]

    $ cd vaultic

    $ go run build.go

You can easily cross-compile vaultic for all supported platforms, just
supply the target OS and platform via the command-line options like this
(for Windows and FreeBSD respectively):

.. code-block:: console

    $ go run build.go --goos windows --goarch amd64

    $ go run build.go --goos freebsd --goarch 386

    $ go run build.go --goos linux --goarch arm --goarm 6

    $ go run build.go --goos solaris --goarch amd64

The resulting binary is statically linked and does not require any
libraries.

At the moment, the only tested compiler for vaultic is the official Go
compiler. Building vaultic with gccgo may work, but is not supported.

Autocompletion
**************

Vaultic can write out man pages and bash/fish/zsh/powershell compatible autocompletion scripts:

.. code-block:: console

    $ ./vaultic generate --help

    The "generate" command writes automatically generated files (like the man pages
    and the auto-completion files for bash, fish, powershell and zsh).

    EXIT STATUS
    ===========

    Exit status is 0 if the command was successful.
    Exit status is 1 if there was any error.

    Usage:
      vaultic generate [flags]

    Flags:
          --bash-completion file         write bash completion file (`-` for stdout)
          --fish-completion file         write fish completion file (`-` for stdout)
      -h, --help                         help for generate
          --man directory                write man pages to directory
          --powershell-completion file   write powershell completion file (`-` for stdout)
          --zsh-completion file          write zsh completion file (`-` for stdout)

Example for using sudo to write a bash completion script directly to the system-wide location:

.. code-block:: console

    $ sudo ./vaultic generate --bash-completion /etc/bash_completion.d/vaultic
    writing bash completion file to /etc/bash_completion.d/vaultic

Example for using sudo to write a zsh completion script directly to the system-wide location:

.. code-block:: console

    $ sudo ./vaultic generate --zsh-completion /usr/local/share/zsh/site-functions/_restic
    writing zsh completion file to /usr/local/share/zsh/site-functions/_restic

.. note:: The path for the ``--bash-completion`` option may vary depending on
   the operating system used, e.g. ``/usr/share/bash-completion/completions/vaultic``
   in Debian and derivatives. Please look up the correct path in the appropriate
   documentation.

Example for setting up a powershell completion script for the local user's profile:

.. code-block:: pwsh-session

    # Create profile if one does not exist
    PS> If (!(Test-Path $PROFILE.CurrentUserAllHosts)) {New-Item -Path $PROFILE.CurrentUserAllHosts -Force}

    PS> $ProfileDir = (Get-Item $PROFILE.CurrentUserAllHosts).Directory

    # Generate Vaultic completions in the same directory as the profile
    PS> vaultic generate --powershell-completion "$ProfileDir\vaultic-completion.ps1"

    # Append to the profile file the command to load Vaultic completions
    PS> Add-Content -Path $PROFILE.CurrentUserAllHosts -Value "`r`nImport-Module $ProfileDir\vaultic-completion.ps1"
