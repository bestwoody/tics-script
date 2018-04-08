host="$1"

set -eu

if [ -z "$host" ]; then
	host="h0"
fi

ceph-deploy purge h0
ceph-deploy purgedata h0
ceph-deploy forgetkeys
rm ceph.*
