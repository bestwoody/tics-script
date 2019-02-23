#!/bin/bash

daemon_mode="$1"
ensure_ceph="$2"

set -eu
source ./_env.sh
dir=`pwd`

for server in ${storage_server[@]}; do
	host=`get_host $server`
	echo "=> $host starting"
	if [ "$ensure_ceph" == "true" ]; then
		echo "=> checking ceph"
		ceph_mounted=`ssh $host "df -h | grep ':6789'" | wc -l | awk '{print $1}'`
		if [ "$ceph_mounted" == "0" ]; then
			echo "cephfs not mounted, exiting ..." >&2
			exit 1
		fi
	fi
	ssh $host -t -q "set -m; cd $dir; $storage_bin server --config-file $storage_server_config &"
	echo "=> $host started"
done

./storages-pid.sh
