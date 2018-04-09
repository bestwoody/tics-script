dir="$1"
host="$2"

set -eu

source ./_env.sh

wd="single-deploy"

if [ -z "$dir" ]; then
	dir="/data/ceph-storage"
fi

if [ -z "$host" ]; then
	host="h0"
fi

mkdir -p "$wd"
cd "$wd"

ceph-deploy --username "$user" new "$host"
ceph-deploy --username "$user" install "$host"
ceph-deploy --username "$user" --overwrite-conf mon create-initial

ceph-deploy --username "$user" admin "$host"
ceph-deploy --username "$user" mgr create "$host"

ceph-deploy --username "$user" osd create --data "$dir" "$host"
