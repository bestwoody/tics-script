#!/bin/bash

source ./_cluster_helper.sh

export deploy_dir="/data1/tiflash_deploy_dir/"

# Storage server list
#export storage_server=("172.16.5.59" "172.16.5.81" "172.16.5.82")
export storage_server=("172.16.5.59")

export scale="10"
export storage_db="tpch$scale"

export spark_master="172.16.5.59"

# pd related config
export pd_port=13499
export pd_addrs=
for i in ${!storage_server[@]}; do
    if [[ $i != 0 ]]; then
		pd_addrs+=,
	fi
	pd_addrs+=${storage_server[i]}:${pd_port}
done
export cluster_addrs=
for i in ${!storage_server[@]}; do
	if [[ $i != 0 ]]; then
		cluster_addrs+=,
	fi
	cluster_addrs+=pd_${storage_server[i]//./_}=http://${storage_server[i]}:$((pd_port+1))
done

# kv related config
export kv_port=20430

# tidb related config
export tidb_port=12490
export tidb_status_port=9880

# tiflash related config
export tiflash_bin_name="tiflash-ap"
export tiflash_tcp_port=9900
export tiflash_http_port=10023
export tiflash_log_level="debug"
export ignore_databases="test2"

# tiflash proxy related config
export tiflash_proxy_bin_name="tikv-server-rngine-ap"
export raft_port=9830
export rngine_port=20832

export spark_executor_memory="8G"

export user="root"
