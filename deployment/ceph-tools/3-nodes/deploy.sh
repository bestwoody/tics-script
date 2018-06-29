et -eu
source ./_env.sh

for node in ${nodes[@]}; do
	echo "=> ../common-scripts/lo-prepare.sh $node"
	../common-scripts/lo-prepare.sh "$node"
done

echo "=> ceph-deploy --username $user new --public-network $public_network ${nodes[@]}"
ceph-deploy --username "$user" new --public-network "$public_network" ${nodes[@]}

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user install --repo-url https://download.ceph.com/debian-luminous xenial $node"
	ceph-deploy --username "$user" install --repo-url "https://download.ceph.com/debian-luminous xenial" "$node"
done

echo "=> ceph-deploy --username $user --overwrite-conf mon create-initial"
ceph-deploy --username "$user" --overwrite-conf mon create-initial

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user --overwrite-conf admin $node"
	ceph-deploy --username "$user" --overwrite-conf admin "$node"
done

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user --overwrite-conf mgr create $node"
	ceph-deploy --username "$user" --overwrite-conf mgr create "$node"
done

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $lo_name $node"
	ceph-deploy --username "$user" --overwrite-conf osd create --data $lo_name "$node"
done

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user --overwrite-conf mds create $node"
	ceph-deploy --username "$user" --overwrite-conf mds create "$node"
done
