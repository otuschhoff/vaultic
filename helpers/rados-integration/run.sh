#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname "$0")/../.." && pwd)
network=vaultic-ceph-test
cluster=vaultic-ceph-test
target=vaultic-ceph-test-target
image=quay.io/ceph/ceph:v17.2.7

cleanup() {
    docker rm -f "$cluster" >/dev/null 2>&1 || true
    docker network rm "$network" >/dev/null 2>&1 || true
    docker volume rm "$target" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
cleanup
docker network create "$network" >/dev/null
docker volume create "$target" >/dev/null
docker run -d --rm --name "$cluster" --network "$network" --hostname ceph-test --privileged \
    -v "$root:/work" "$image" sh /work/helpers/rados-integration/start-cluster.sh >/dev/null
attempts=0
until docker exec "$cluster" test -f /tmp/vaultic-ready 2>/dev/null; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 120 || { echo "Ceph cluster readiness timed out" >&2; exit 1; }
    sleep 1
done

key=$(docker exec "$cluster" ceph auth get-key client.vaultic)
common="--rm --network $network -e VAULTIC_RADOS_TEST_MONITORS=ceph-test:6789 -e VAULTIC_RADOS_TEST_KEY=$key -v $root:/work"

docker run $common -w /work golang:1.26-bookworm sh -c \
    'apt-get update -qq && apt-get install -y -qq librados-dev >/dev/null && go test -tags rados ./internal/backend/rados -run "TestNativeLiveRADOS|TestLiveEncryptedRepositoryLifecycle" -v'
docker run $common -v "$target:/work/vaulticdb/target" -w /work/vaulticdb rust:1-bookworm sh -c \
    'apt-get update -qq && apt-get install -y -qq librados-dev protobuf-compiler >/dev/null && cargo build --features rados && cargo test --features rados storage::rados::native::tests::live_rados_atomicity_reopen_and_isolation -- --nocapture'
docker run $common -v "$target:/work/vaulticdb/target" -w /work golang:1.26-bookworm sh -c \
    'apt-get update -qq && apt-get install -y -qq librados-dev >/dev/null && go test -tags rados ./internal/repository -run TestLivePlacePackCopiesToRADOS -v'

docker exec "$cluster" pkill -STOP ceph-osd
docker exec "$cluster" ceph osd down 0
attempts=0
until docker exec "$cluster" ceph osd stat | grep -q '0 up'; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 60 || { echo "Ceph OSD shutdown timed out" >&2; exit 1; }
    sleep 1
done
docker run $common -e VAULTIC_RADOS_TEST_OSD_DOWN=1 -w /work golang:1.26-bookworm sh -c \
    'apt-get update -qq && apt-get install -y -qq librados-dev >/dev/null && go test -tags rados ./internal/backend/rados -run TestNativeLiveOSDUnavailableIsBounded -v'
docker exec "$cluster" pkill -CONT ceph-osd
attempts=0
until docker exec "$cluster" ceph osd stat | grep -q '1 up'; do
    attempts=$((attempts + 1))
    test "$attempts" -lt 60 || { echo "Ceph OSD recovery timed out" >&2; exit 1; }
    sleep 1
done
docker run $common -w /work golang:1.26-bookworm sh -c \
    'apt-get update -qq && apt-get install -y -qq librados-dev >/dev/null && go test -tags rados ./internal/backend/rados -run TestNativeLiveRADOS -v'