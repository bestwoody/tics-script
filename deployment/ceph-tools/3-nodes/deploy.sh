et -eu

source ./env.sh

echo "=> ../common-scripts/lo-prepare.sh $h0"
../common-scripts/lo-prepare.sh "$h0"
echo "=> ../common-scripts/lo-prepare.sh $h1"
../common-scripts/lo-prepare.sh "$h1"
echo "=> ../common-scripts/lo-prepare.sh $h2"
../common-scripts/lo-prepare.sh "$h2"

echo "=> ceph-deploy --username $user new --public-network $public_network $h0 $h1 $h2"
ceph-deploy --username "$user" new --public-network "$public_network" "$h0" "$h1" "$h2"

echo "=> ceph-deploy --username $user install --repo-url https://download.ceph.com/debian-luminous xenial $h0"
ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h0"
echo "=> ceph-deploy --username $user install --repo-url https://download.ceph.com/debian-luminous xenial $h1"
ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h1"
echo "=> ceph-deploy --username $user install --repo-url https://download.ceph.com/debian-luminous xenial $h2"
ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h2"

echo "=> ceph-deploy --username $user --overwrite-conf mon create-initial"
ceph-deploy --username "$user" --overwrite-conf mon create-initial

echo "=> ceph-deploy --username $user --overwrite-conf admin $h0"
ceph-deploy --username "$user" --overwrite-conf admin "$h0"
echo "=> ceph-deploy --username $user --overwrite-conf admin $h1"
ceph-deploy --username "$user" --overwrite-conf admin "$h1"
echo "=> ceph-deploy --username $user --overwrite-conf admin $h2"
ceph-deploy --username "$user" --overwrite-conf admin "$h2"

echo "=> ceph-deploy --username $user --overwrite-conf mgr create $h0"
ceph-deploy --username "$user" --overwrite-conf mgr create "$h0"
echo "=> ceph-deploy --username $user --overwrite-conf mgr create $h1"
ceph-deploy --username "$user" --overwrite-conf mgr create "$h1"
echo "=> ceph-deploy --username $user --overwrite-conf mgr create $h2"
ceph-deploy --username "$user" --overwrite-conf mgr create "$h2"

echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $lo_name $h0"
ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h0"
echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $lo_name $h1"
ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h1"
echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $lo_name $h2"
ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h2"

echo "=> ceph-deploy --username $user --overwrite-conf mds create $h0"
ceph-deploy --username "$user" --overwrite-conf mds create "$h0"
echo "=> ceph-deploy --username $user --overwrite-conf mds create $h1"
ceph-deploy --username "$user" --overwrite-conf mds create "$h1"
echo "=> ceph-deploy --username $user --overwrite-conf mds create $h2"
ceph-deploy --username "$user" --overwrite-conf mds create "$h2"
