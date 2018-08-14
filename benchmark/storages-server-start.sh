#!/bin/bash

daemon_mode="$1"

set -eu
source ./_env.sh
dir=`pwd`

for server in ${storage_server[@]}; do
	host=`get_host $server`
	echo "=> $host starting"
	ssh $host -t -q "set -m; cd $dir; $storage_bin server --config-file $storage_server_config &"
	echo "=> $host started"
done

./storages-pid.sh
