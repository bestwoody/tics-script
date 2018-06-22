set -eu

source ./_env.sh

cho ceph-deploy purge "$h0" "$h1" "$h2"
ceph-deploy --username "$user" purge "$h0" "$h1" "$h2"
echo

echo ceph-deploy purgedata "$h0" "$h1" "$h2"
ceph-deploy --username "$user" purgedata "$h0" "$h1" "$h2"
echo

echo ceph-deploy forgetkeys
ceph-deploy --username "$user" forgetkeys

echo rm -rf ceph.*
rm -rf ceph.*
