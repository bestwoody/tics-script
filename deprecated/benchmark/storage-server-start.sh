#!/bin/bash

daemon_mode="$1"

set -eu
source ./_env.sh

# Check cephfs is mounted when on cluster mode
if [ "${#storage_server[@]}" != "1" ]; then
	ceph_mounted=`df -h | grep ":6789" | wc -l | awk '{print $1}'`
	if [ "$ceph_mounted" == "0" ]; then
		echo "cephfs not mounted, exiting ..." >&2
		exit 1
	fi
fi

pid=`./storage-pid.sh`
if [ ! -z "$pid" ]; then
	echo "another storage server is running, skipped and exiting" >&2
	exit 1
fi

if [ "$daemon_mode" != "false" ]; then
	$storage_bin server --config-file "$storage_server_config" 2>/dev/null &
	sleep 2
	./storage-pid.sh
else
	$storage_bin server --config-file "$storage_server_config"
fi
