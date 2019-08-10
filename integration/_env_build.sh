#!/bin/bash
# This is a generated file. Do not edit it. Changes will be overwritten.

source ./_vars.sh
source ./_helper.sh

export tiflash_bin_name=tiflash-ap
export tiflash_proxy_bin_name=tikv-server-rngine-ap

export deploy_dir=/data1/tiflash_deploy_dir/
export ip=172.16.5.59
export pd_port=13499
export pd_name=pd_172_16_5_59
export pd_addrs=172.16.5.59:13499
export cluster_addrs=pd_172_16_5_59=http://172.16.5.59:13500
export kv_port=20430
export kv_name=kv_172_16_5_59
export tidb_port=12490
export tidb_status_port=9880

export tiflash_tcp_port=9900
export tiflash_http_port=10023
export tiflash_log_level=debug
export raft_port=9830
export ignore_databases=test2

export rngine_port=20832

export spark_master=172.16.5.59
export spark_executor_memory=8G

export bin="bin/"
export log="log/"
export data="data/"
export config="conf/"

export storage_db="tpch10"

mkdir -p ./${bin}
mkdir -p ./${data}
mkdir -p ./${log}

