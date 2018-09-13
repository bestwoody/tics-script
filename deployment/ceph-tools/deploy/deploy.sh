#!/bin/bash

set -eu
source ./_env.sh

if [ ! -z "$osd_img" ]; then
	for node in ${nodes[@]}; do
		echo "=> ./_lo-prepare.sh $node $osd_img $osd_mb $dev_name"
		confirm
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
confirm
ceph-deploy --username "$user" new --public-network "$public_network" ${mains[@]}

for node in ${nodes[@]}; do
	if [ "$update_yum" == "true" ]; then
		echo "=> ssh $user@$node \"sudo yum --skip-broken -y update\""
		confirm
		ssh $user@$node "sudo yum --skip-broken -y update"
	fi

	echo "=> ssh $user@$node \"sudo yum --skip-broken -y install ntp ntpdate ntp-doc\""
	confirm
	ssh $user@$node "sudo yum --skip-broken -y install ntp ntpdate ntp-doc"

	epel_installed=`rpm -qa | grep epel`
	if [ -z "$epel_installed" ]; then
		echo "ssh $user@$node \"sudo yum --skip-broken -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm\""
		confirm
		ssh $user@$node "sudo yum --skip-broken -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm"
	fi

	echo "=> ceph-deploy --username $user install $node"
	confirm
	ceph-deploy --username "$user" install "$node"
done

echo "=> ceph-deploy --username $user --overwrite-conf mon create-initial"
confirm
ceph-deploy --username "$user" --overwrite-conf mon create-initial

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf admin $node"
	confirm
	ceph-deploy --username "$user" --overwrite-conf admin "$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf mgr create $node"
	confirm
	ceph-deploy --username "$user" --overwrite-conf mgr create "$node"
done

# If things not going well, use `sudo dmsetup ls` and `sudo dmsetup remove_all` to fix it
for node in ${nodes[@]}; do
	echo "=> ssh ${user}@${node} \"sudo df -l | grep $dev_name | awk '{print \$1}' | xargs -I {} sudo umount -f {}\""
	ssh ${user}@${node} "sudo df -l | grep $dev_name | awk '{print \$1}' | xargs -I {} sudo umount -f {}"

	echo "=> ceph-deploy --username $user --overwrite-conf osd create --data $dev_name $node"
	ceph-deploy --username "$user" --overwrite-conf osd create --data $dev_name "$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	echo "=> ceph-deploy --username $user --overwrite-conf mds create $node"
	confirm
	ceph-deploy --username "$user" --overwrite-conf mds create "$node"
done
