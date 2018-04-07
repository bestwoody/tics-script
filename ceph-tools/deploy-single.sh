dir="$1"
host="$2"

set -eu

if [ -z "$dir" ]; then
	dir="/data/ceph-storage"
fi

if [ -z "$host" ]; then
	host="h0"
fi

ceph-deploy install "$host"
ceph-deploy new "$host"
ceph-deploy mon create "$host"
ceph-deploy gatherkeys "$host"

mv ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring
mv ceph.bootstrap-mds.keyring /var/lib/ceph/bootstrap-mds/ceph.keyring
mv ceph.bootstrap-mgr.keyring /var/lib/ceph/bootstrap-mgr/ceph.keyring
mv ceph.bootstrap-rgw.keyring /var/lib/ceph/bootstrap-rgw/ceph.keyring

# mv ceph.mon.keyring
# mv ceph.client.admin.keyring
# mv ceph.conf

ceph-disk prepare "$dir"
ceph-disk activate "$dir"
