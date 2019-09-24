#!/bin/bash

set -eu

source ./_env.sh

for node in ${nodes[@]}; do
	echo "$node:"
	ssh "$node" "ps -ef | grep ceph | grep cluster | grep -v grep"
done
