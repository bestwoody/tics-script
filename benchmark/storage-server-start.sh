#!/bin/bash

set -eu
source ./_env.sh

pid=`./storage-pid.sh`
if [ ! -z "$pid" ]; then
	echo "another storage server is running, skipped and exiting" >&2
	exit 1
fi

$storage_bin server --config-file "$storage_server_config" 2>/dev/null &

sleep 2

./storage-pid.sh
