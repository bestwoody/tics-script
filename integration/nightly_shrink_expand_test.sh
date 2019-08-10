#!/bin/bash

set -eu
set -o pipefail

source ./_cluster_env.sh

function get_learner_region_count()
{
    sum=0
    for server in ${storage_server[@]}; do
        count=`get_store_region_count "$server" "$deploy_dir" "$user" "$pd_port" "true"`
        sum=$(( sum + count ))
    done
    echo $sum
}

function get_normal_region_count()
{
    sum=0
    for server in ${storage_server[@]}; do
        count=`get_store_region_count "$server" "$deploy_dir" "$user" "$pd_port" "false"`
        sum=$(( sum + count ))
    done
    echo $sum
}


./cluster_stop.sh
./cluster_cleanup_data.sh
./cluster_start.sh
cur=`pwd`
cd ./load_data/
./load_tpch.sh &
cd $cur

./cluster_wait_data_ready.sh
./cluster_check_result_consistency.sh

if [[ ${#storage_server[*]} -le 1 ]]; then
    exit 0
fi

down_server=${storage_server[0]}

# delete a tiflash node and wait for region transfer completed
./cluster_delete_single_tiflash.sh $down_server

sleep 300

completed="false"

while [[ "$completed" == "false" ]]; do
    learner_region_count=`get_learner_region_count`
    normal_region_count=`get_normal_region_count`
    echo $learner_region_count
    echo $normal_region_count
    if [[ ${learner_region_count} -eq ${normal_region_count} ]]; then
        completed="true"
    fi
done
echo "region transfer completed"

# start a tiflash node and wait for region transfer completed
echo "starting tiflash"
ssh "${user}@${down_server}" -q -t "set -m; cd ${deploy_dir}; ./run_tiflash.sh &"
sleep 30
ready="false"
while [[ $ready != "true" ]]; do
    ready="true"
    if [[ `port_ready "$down_server" "$tiflash_tcp_port" "$deploy_dir" "$user"` -eq 0 ]]; then
        ready="false"
    fi
    sleep 10
done
echo "start tiflash proxy"
ssh "${user}@${down_server}" -q -t "set -m; cd ${deploy_dir}; ./run_rngine.sh &"

sleep 300

for i in {1..10}; do
    ./cluster_check_result_consistency.sh
    sleep 240
done

./cluster_stop.sh