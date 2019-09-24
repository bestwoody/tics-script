#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

function run_start_shell_on_cluster()
{
    cmd="$1"
    for server in ${storage_server[@]}; do
        run_start_shell_on_host "$server" "$cmd" "$deploy_dir" "$user"
    done
}

run_start_shell_on_cluster "./run_pd.sh &"
sleep 10
run_start_shell_on_cluster "./run_tikv.sh &"
sleep 10
run_start_shell_on_cluster "./run_tidb.sh &"
sleep 40
run_start_shell_on_cluster "./run_tiflash.sh &"
sleep 30
# check tiflash tcp port is ready
ready="false"
while [[ "$ready" != "true" ]]; do
    ready="true"
    for server in ${storage_server[@]}; do
        if [[ `port_ready "$server" "$tiflash_tcp_port" "$deploy_dir" "$user"` -eq 0 ]]; then
            ready="false"
        fi
    done
    echo "tiflash not ready. wait.."
    sleep 10
done
run_start_shell_on_cluster "./run_rngine.sh &"
sleep 10
./cluster_dsh.sh "./check_all.sh"

for server in ${storage_server[@]}; do
    if [[ "$server" == "$spark_master" ]]; then
        ssh "${user}@${server}" "cd ${deploy_dir}; ./spark_start_all.sh" < /dev/null 2>&1
        sleep 10
    else
        ssh "${user}@${server}" "cd ${deploy_dir}; ./spark_start_slave.sh" < /dev/null 2>&1
    fi
done