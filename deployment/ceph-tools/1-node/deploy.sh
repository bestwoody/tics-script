dir="$1"

set -eu

source ./_env.sh

if [ -z "$dir" ]; then
	echo "usage: <bin> data-path(vg/lv)" >&2
	exit 1
fi

if [ -z "$host" ]; then
	echo "host not defined in '_env.sh', exiting" >&2
	exit 1
fi

../common-scripts/lo-prepare.sh "$host"

ceph-deploy --username "$user" new "$host"
ceph-deploy --username "$user" install "$host"
ceph-deploy --username "$user" --overwrite-conf mon create-initial

ceph-deploy --username "$user" --overwrite-conf admin "$host"
ceph-deploy --username "$user" --overwrite-conf mgr create "$host"

ceph-deploy --username "$user" --overwrite-conf osd create --data "$dir" "$host"
