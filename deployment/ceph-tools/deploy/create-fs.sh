name="$1"
pg="$2"

set -eu

if [ -z "$name" ]; then
	echo "usage: <bin> fs-name(eg: cephfs) [pg(default: 64+64)]" >&2
	exit 1
fi

if [ -z "$pg" ]; then
	pg="64"
fi

name_data=$name"_data"
name_meta=$name"_metadata"

ceph osd pool create "$name_data" "$pg"
ceph osd pool create "$name_meta" "$pg"
ceph fs new "$name" "$name_meta" "$name_data"
