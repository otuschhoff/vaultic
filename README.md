[![Documentation](https://readthedocs.org/projects/vaultic/badge/?version=latest)](https://vaultic.readthedocs.io/en/latest/?badge=latest)
[![Build Status](https://github.com/otuschhoff/vaultic/workflows/test/badge.svg)](https://github.com/otuschhoff/vaultic/actions?query=workflow%3Atest)
[![Go Report Card](https://goreportcard.com/badge/github.com/otuschhoff/vaultic)](https://goreportcard.com/report/github.com/otuschhoff/vaultic)

# Introduction

vaultic is a backup program that is fast, efficient and secure. It supports the three major operating systems (Linux, macOS, Windows) and a few smaller ones (FreeBSD, OpenBSD).

For detailed usage and installation instructions check out the [documentation](https://vaultic.readthedocs.io/en/latest).

You can ask questions in our [Discourse forum](https://forum.vaultic.net).

## Quick start

Once you've [installed](https://vaultic.readthedocs.io/en/latest/020_installation.html) vaultic, start
off with creating a repository for your backups:

    $ vaultic init --repo /tmp/backup
    enter password for new backend:
    enter password again:
    created vaultic backend 085b3c76b9 at /tmp/backup
    Please note that knowledge of your password is required to access the repository.
    Losing your password means that your data is irrecoverably lost.

and add some data:

    $ vaultic --repo /tmp/backup backup ~/work
    enter password for repository:
    scan [/home/user/work]
    scanned 764 directories, 1816 files in 0:00
    [0:29] 100.00%  54.732 MiB/s  1.582 GiB / 1.582 GiB  2580 / 2580 items  0 errors  ETA 0:00
    duration: 0:29, 54.47MiB/s
    snapshot 40dc1520 saved

Next you can either use `vaultic restore` to restore files or use `vaultic
mount` to mount the repository via fuse and browse the files from previous
snapshots.

For more options check out the [online documentation](https://vaultic.readthedocs.io/en/latest/).

# Backends

Saving a backup on the same machine is nice but not a real backup strategy.
Therefore, vaultic supports the following backends for storing backups natively:

- [Local directory](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#local)
- [sftp server (via SSH)](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#sftp)
- [HTTP REST server](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#rest-server) ([protocol](https://vaultic.readthedocs.io/en/latest/100_references.html#rest-backend), [rest-server](https://github.com/restic/rest-server))
- [Amazon S3](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#amazon-s3) (either from Amazon or using the [Minio](https://minio.io) server)
- [OpenStack Swift](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#openstack-swift)
- [BackBlaze B2](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#backblaze-b2)
- [Microsoft Azure Blob Storage](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#microsoft-azure-blob-storage)
- [Google Cloud Storage](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#google-cloud-storage)
- And many other services via the [rclone](https://rclone.org) [Backend](https://vaultic.readthedocs.io/en/latest/030_preparing_a_new_repo.html#other-services-via-rclone)

# Design Principles

Vaultic is a program that does backups right and was designed with the
following principles in mind:

-  **Easy**: Doing backups should be a frictionless process, otherwise
   you might be tempted to skip it. Vaultic should be easy to configure
   and use, so that, in the event of a data loss, you can just restore
   it. Likewise, restoring data should not be complicated.

-  **Fast**: Backing up your data with vaultic should only be limited by
   your network or hard disk bandwidth so that you can backup your files
   every day. Nobody does backups if it takes too much time. Restoring
   backups should only transfer data that is needed for the files that
   are to be restored, so that this process is also fast.

-  **Verifiable**: Much more important than backup is restore, so vaultic
   enables you to easily verify that all data can be restored.

-  **Secure**: Vaultic uses cryptography to guarantee confidentiality and
   integrity of your data. The location the backup data is stored is
   assumed not to be a trusted environment (e.g. a shared space where
   others like system administrators are able to access your backups).
   Vaultic is built to secure your data against such attackers.

-  **Efficient**: With the growth of data, additional snapshots should
   only take the storage of the actual increment. Even more, duplicate
   data should be de-duplicated before it is actually written to the
   storage back end to save precious backup space.

# Reproducible Builds

The binaries released with each vaultic version starting at 0.6.1 are
[reproducible](https://reproducible-builds.org/), which means that you can
reproduce a byte identical version from the source code for that
release. Instructions on how to do that are contained in the
[builder repository](https://github.com/vaultic/builder).

## News

You can follow the vaultic project on Mastodon [@vaultic](https://fosstodon.org/@vaultic) or subscribe to
the [project blog](https://vaultic.net/blog/).

## License

Vaultic is licensed under [BSD 2-Clause License](https://opensource.org/licenses/BSD-2-Clause). You can find the
complete text in [`LICENSE`](LICENSE).

## Sponsorship

Backend integration tests for Google Cloud Storage and Microsoft Azure Blob
Storage are sponsored by [AppsCode](https://appscode.com)!

[![Sponsored by AppsCode](https://cdn.appscode.com/images/logo/appscode/ac-logo-color.png)](https://appscode.com)
