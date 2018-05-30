et -eu

source ./env.sh

../common-scripts/lo-prepare.sh "$h0"
../common-scripts/lo-prepare.sh "$h1"
../common-scripts/lo-prepare.sh "$h2"

ceph-deploy --username "$user" new --public-network "$public_network" "$h0" "$h1" "$h2"

ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h0"
ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h1"
ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$h2"

ceph-deploy --username "$user" --overwrite-conf mon create-initial

ceph-deploy --username "$user" --overwrite-conf admin "$h0"
ceph-deploy --username "$user" --overwrite-conf admin "$h1"
ceph-deploy --username "$user" --overwrite-conf admin "$h2"

ceph-deploy --username "$user" --overwrite-conf mgr create "$h0"
ceph-deploy --username "$user" --overwrite-conf mgr create "$h1"
ceph-deploy --username "$user" --overwrite-conf mgr create "$h2"

ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h0"
ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h1"
ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$h2"

exit
