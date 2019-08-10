#!/bin/bash

source ./_cluster_env.sh

set -eu
set -o pipefail

server=$1

if [[ -z "$server" ]]; then
	echo "usage: <bin> server">&2
	exit 1
fi

delete_store "$server" "$deploy_dir" "$user" "$pd_port"

while true; do
    status=`get_store_status "$server" "$deploy_dir" "$user" "$pd_port"`
    echo $status
    if [[ "$status" == "Tomestone" ]]; then
        break
    fi
    sleep 120
done

execute_cmd "$server" "./stop_rngine_and_tiflash.sh" "$deploy_dir" "$user" "true"
execute_cmd "$server" "./cleanup_tiflash.sh" "$deploy_dir" "$user" "true"
