#!/bin/bash

set -eu
set -o pipefail

source ./_cluster_env.sh

./cluster_stop.sh
./cluster_cleanup_data.sh
./cluster_start.sh
cur=`pwd`
cd ./load_data/
./load_tpch.sh &
cd $cur

function restart_all_tiflash()
{
    for server in ${storage_server[@]}; do
		restart_tiflash "$server" "$tiflash_tcp_port" "$deploy_dir" "$user"
	done
}

# TODO need kill -9
for i in {1..10}; do
    echo "$i"
    restart_all_tiflash
    sleep 100
done

./cluster_wait_data_ready.sh
./cluster_check_result_consistency.sh

./cluster_stop.sh
