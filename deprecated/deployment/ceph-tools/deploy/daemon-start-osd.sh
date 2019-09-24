#!/bin/bash

set -u

source ./_env.sh

for i in ${!nodes[@]}; do
	ssh "${nodes[$i]}" "sudo systemctl start ceph-osd@$i"
done
