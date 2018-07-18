set -eu
source ./_env.sh

if [ ! -z "$osd_img" ]; then
	for node in ${nodes[@]}; do
		echo "=> ./_lo-prepare.sh $node $osd_img $osd_mb $dev_name"
		./_lo-prepare.sh "$node" "$osd_img" "$osd_mb" "$dev_name"
	done
fi

mains=()
for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	mains[$i]=${nodes[$i]}
done

echo "=> ceph-deploy --username $user new --public-network $public_network ${mains[@]}"
ceph-deploy --username "$user" new --public-network "$public_network" ${mains[@]}

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user install $node"
	ceph-deploy --username "$user" install "$node"
done

echo "=> ceph-deploy --username $user --overwrite-conf mon create-initial"
ceph-deploy --username "$user" --overwrite-conf mon create-initial

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf admin $node"
	ceph-deploy --username "$user" --overwrite-conf admin "$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf mgr create $node"
	ceph-deploy --username "$user" --overwrite-conf mgr create "$node"
done

for node in ${nodes[@]}; do
	echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $dev_name $node"
	ceph-deploy --username "$user" --overwrite-conf osd create --data $dev_name "$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf mds create $node"
	ceph-deploy --username "$user" --overwrite-conf mds create "$node"
done
