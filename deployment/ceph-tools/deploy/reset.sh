set -eu

source ./_env.sh

cho ceph-deploy purge ${nodes[@]}
ceph-deploy --username "$user" purge ${nodes[@]}
echo

echo ceph-deploy purgedata ${nodes[@]}
ceph-deploy --username "$user" purgedata ${nodes[@]}
echo

echo ceph-deploy forgetkeys
ceph-deploy --username "$user" forgetkeys

echo rm -rf ceph.*
rm -rf ceph.*