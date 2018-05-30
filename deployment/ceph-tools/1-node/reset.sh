set -eu

if [ -z "$host" ]; then
	echo "host not defined in '_env.sh', exiting" >&2
	exit 1
fi

source ./_env.sh

ceph-deploy --username "$user" purge "$host"
ceph-deploy --username "$user" purgedata "$host"
ceph-deploy --username "$user" forgetkeys

rm -rf ceph.*
