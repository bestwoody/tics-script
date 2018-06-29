name="$1"
pg="$2"

set -eu

if [ -z "$name" ]; then
	name="cephfs"
fi

if [ -z "$pg" ]; then
	pg="64"
fi

name_data=$name"_data"
name_meta=$name"_metadata"

ceph osd pool create "$name_data" "$pg"
ceph osd pool create "$name_meta" "$pg"
ceph fs new "$name" "$name_meta" "$name_data"
