dir="$1"
host="$2"

set -eu

wd="single-deploy"

if [ -z "$dir" ]; then
	dir="/data/ceph-storage"
fi

if [ -z "$host" ]; then
	host="h0"
fi

mkdir -p "$wd"
cd "$wd"

ceph-deploy new "$host"
ceph-deploy install --repo-url ceph-repo "$host"
ceph-deploy mon create-initial

ceph-deploy admin "$host"
ceph-deploy mgr create "$host"

ceph-deploy osd create --data "$dir" "$host"
