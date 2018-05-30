host="$1"
path="$2"
if [ -z "$host" ] || [ -z "$path" ]; then
	echo "usage: <bin> ceph-mon-host mount-to-path"
	exit 1
fi

sudo ceph-fuse -k /etc/ceph/ceph.client.admin.keyring -m $host:6789 "$path"
