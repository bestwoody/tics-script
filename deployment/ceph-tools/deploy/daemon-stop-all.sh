#!/bin/bash

set -eu
source ./_env.sh

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl stop ceph-mds@$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl stop ceph-mgr@$node"
done

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl stop ceph-mon@$node"
done

for i in ${!nodes[@]}; do
	ssh "${nodes[$i]}" "sudo systemctl stop ceph-osd@$i"
done

echo "status: (should be nothing)"
./daemon-status.sh
