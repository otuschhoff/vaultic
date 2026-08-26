#!/bin/bash

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 vaultic_version go_version"
    exit 1
fi

vaultic_version="$1"
go_version="$2"

# invalid if zero
is_valid=1
set_invalid() {
    echo $1
    is_valid=0
}

tmpdir="$(mktemp -d -p .)"
cd "${tmpdir}"
echo -e "Running checks in ${tmpdir}\n"

highlight() {
    echo "@@${1//?/@}@@"
    echo "@ ${1} @"
    echo "@@${1//?/@}@@"
}


highlight "Verifying release self-consistency"

curl -OLSs https://github.com/otuschhoff/vaultic/releases/download/v${vaultic_version}/vaultic-${vaultic_version}.tar.gz.asc
# tarball is downloaded while processing the SHA256SUMS
curl -OLSs https://github.com/otuschhoff/vaultic/releases/download/v${vaultic_version}/SHA256SUMS.asc
curl -OLSs https://github.com/otuschhoff/vaultic/releases/download/v${vaultic_version}/SHA256SUMS

export GNUPGHOME=$PWD/gnupg
mkdir -p 700 $GNUPGHOME
curl -OLSs https://vaultic.net/gpg-key-alex.asc
gpg --import gpg-key-alex.asc
gpg --verify SHA256SUMS.asc SHA256SUMS

for i in $(cat SHA256SUMS | cut -d " "  -f 3 ) ; do
    echo "Downloading $i"
    curl -OLSs https://github.com/otuschhoff/vaultic/releases/download/v${vaultic_version}/"$i"
done
shasum -a256 -c SHA256SUMS || set_invalid "WARNING: RELEASE BINARIES DO NOT MATCH SHA256SUMS!"
gpg --verify vaultic-${vaultic_version}.tar.gz.asc vaultic-${vaultic_version}.tar.gz
# TODO verify that the release does not contain any unexpected files


highlight "Verifying tarball matches tagged commit"

tar xzf "vaultic-${vaultic_version}.tar.gz"
git clone -b "v${vaultic_version}" https://github.com/otuschhoff/vaultic.git
rm -rf vaultic/.git
diff -r vaultic vaultic-${vaultic_version}


highlight "Regenerating builder container"

git clone https://github.com/vaultic/builder.git
docker pull debian:stable
docker build --no-cache -t vaultic/builder:tmp --build-arg GO_VERSION=${go_version} builder


highlight "Reproducing release binaries"

mkdir output
docker run --rm \
    --volume "$PWD/vaultic-${vaultic_version}:/vaultic" \
    --volume "$PWD/output:/output" \
    vaultic/builder:tmp \
    go run helpers/build-release-binaries/main.go --version "${vaultic_version}"

cp "vaultic-${vaultic_version}.tar.gz" output
cp SHA256SUMS output

# check that all release binaries have been reproduced successfully
(cd output && shasum -a256 -c SHA256SUMS) || set_invalid "WARNING: REPRODUCED BINARIES DO NOT MATCH RELEASE BINARIES!"
# and that the SHA256SUMS files does not miss binaries
for i in output/vaultic* ; do grep "$(basename "$i")" SHA256SUMS > /dev/null || set_invalid "WARNING: $i MISSING FROM RELEASE SHA256SUMS FILE!" ; done


extract_docker() {
    image=$1
    docker_platform=$2
    vaultic_platform=$3
    out=vaultic_${vaultic_version}_linux_${vaultic_platform}.bz2

    # requires at least docker 25.0
    docker image pull --platform "linux/${docker_platform}" ${image}:${vaultic_version} > /dev/null
    docker image save ${image}:${vaultic_version} -o docker.tar

    mkdir img
    tar xvf docker.tar -C img --wildcards blobs/sha256/\* > /dev/null
    rm docker.tar
    for i in img/blobs/sha256/*; do
        tar -xvf "$i" -C img usr/bin/vaultic 2> /dev/null 1>&2 || true
        if [[ -f img/usr/bin/vaultic ]]; then
            if [[ -f vaultic-docker ]]; then
                set_invalid "WARNING: CONTAINER CONTAINS MULTIPLE RESTIC BINARIES"
            fi
            mv img/usr/bin/vaultic vaultic-docker
        fi
    done
    
    rm -rf img
    bzip2 vaultic-docker
    mv vaultic-docker.bz2 docker/${out}
    grep ${out} SHA256SUMS >> docker/SHA256SUMS
}

ctr=0
for img in otuschhoff/vaultic ghcr.io/otuschhoff/vaultic; do
    highlight "Verifying binaries in docker containers from $img"
    mkdir docker

    extract_docker "$img" arm/v7 arm
    extract_docker "$img" arm64 arm64
    extract_docker "$img" 386 386
    extract_docker "$img" amd64 amd64

    (cd docker && shasum -a256 -c SHA256SUMS) || set_invalid "WARNING: DOCKER CONTAINER DOES NOT CONTAIN RELEASE BINARIES!"

    mv docker docker-$(( ctr++ ))
done


if [[ $is_valid -ne 1 ]]; then
    highlight "Failed to reproduce some binaries, check the script output for details"
    exit 1
else
    cd ..
    rm -rf "${tmpdir}"
fi
