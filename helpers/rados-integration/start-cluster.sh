#!/bin/sh
set -eu

fsid=${VAULTIC_RADOS_TEST_FSID:-2f525d6a-8f31-4f79-b731-82a6acb235f5}
ip=$(hostname -i)

mkdir -p /var/lib/ceph/mon/ceph-a /var/lib/ceph/osd/ceph-0 /run/ceph /etc/ceph
cat > /etc/ceph/ceph.conf <<EOF
[global]
fsid = $fsid
mon host = $ip:6789
auth cluster required = cephx
auth service required = cephx
auth client required = cephx
osd pool default size = 1
osd pool default min size = 1
EOF

ceph-authtool --create-keyring /tmp/mon.keyring --gen-key -n mon. --cap mon 'allow *'
ceph-authtool /tmp/mon.keyring --gen-key -n client.admin --cap mon 'allow *' --cap osd 'allow *' --cap mgr 'allow *'
ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring
ceph-authtool /etc/ceph/ceph.client.admin.keyring --import-keyring /tmp/mon.keyring
monmaptool --create --add a "$ip:6789" --fsid "$fsid" /tmp/monmap
ceph-mon --mkfs -i a --monmap /tmp/monmap --keyring /tmp/mon.keyring
chown -R ceph:ceph /var/lib/ceph /run/ceph
ceph-mon -i a --public-addr "$ip:6789" --setuser ceph --setgroup ceph
until ceph -s --connect-timeout 2 >/dev/null 2>&1; do sleep 1; done

ceph config set mon mon_allow_pool_size_one true
ceph osd create 0
ceph-authtool --create-keyring /var/lib/ceph/osd/ceph-0/keyring --gen-key -n osd.0 --cap mon 'allow profile osd' --cap osd 'allow *'
ceph auth add osd.0 -i /var/lib/ceph/osd/ceph-0/keyring
ceph-osd -i 0 --mkfs --osd-data /var/lib/ceph/osd/ceph-0 --osd-objectstore memstore
echo memstore > /var/lib/ceph/osd/ceph-0/type
chown -R ceph:ceph /var/lib/ceph/osd/ceph-0
ceph-osd -i 0 --osd-data /var/lib/ceph/osd/ceph-0 --osd-objectstore memstore --setuser ceph --setgroup ceph
until ceph osd stat | grep -q '1 up'; do sleep 1; done
ceph osd crush add osd.0 1.0 root=default host=ceph-test || true
ceph osd pool create vaultic 8
ceph auth get-or-create client.vaultic mon 'allow r' osd 'allow rwx pool=vaultic namespace=repo' -o /tmp/vaultic.keyring
touch /tmp/vaultic-ready
exec tail -f /dev/null