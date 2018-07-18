host="$1"
path="$2"
ceph_path="$3"

if [ -z "$ceph_path" ]; then
	echo "usage: <bin> ceph-mon-host mount-to-path cephfs-path" >&2
	exit 1
fi

if [ `whoami` != "root" ]
	echo "need 'sudo' to run mount, exiting" >&2
	exit 1
fi

key=`cat /etc/ceph/ceph.client.admin.keyring | grep key | awk -F '= ' '{print $2}'`
mount -t ceph $host:6789:ceph_path "$path" -o name=admin,secret=$key
