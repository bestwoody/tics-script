host="$1"

set -eu

source ./_env.sh

if [ -z "$host" ]; then
	host="h0"
fi

cd single-deploy

ceph-deploy --username "$user" purge "$host"
ceph-deploy --username "$user" purgedata "$host"
ceph-deploy --username "$user" forgetkeys
rm ceph.*
