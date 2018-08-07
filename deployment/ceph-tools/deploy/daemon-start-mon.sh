#!/bin/bash

set -u

source ./_env.sh

for i in ${!nodes[@]}; do
	if [ $i -gt 2 ]; then
		continue
	fi
	node=${nodes[$i]}
	ssh "$node" "sudo systemctl start ceph-mon@$node"
done
