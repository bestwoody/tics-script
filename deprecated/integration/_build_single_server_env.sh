#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

server_ip=$1
env_name=$2

if [[ -z ${env_name} ]]; then
    env_name="_env_build.sh"
fi;

cat<<EOF>./${env_name}
#!/bin/bash
# This is a generated file. Do not edit it. Changes will be overwritten.

source ./_vars.sh
source ./_helper.sh

export tiflash_bin_name=${tiflash_bin_name}
export tiflash_proxy_bin_name=${tiflash_proxy_bin_name}

export deploy_dir=${deploy_dir}
export ip=${server_ip}
export pd_port=${pd_port}
export pd_name=pd_${server_ip//./_}
export pd_addrs=${pd_addrs}
export cluster_addrs=${cluster_addrs}
export kv_port=${kv_port}
export kv_name=kv_${server_ip//./_}
export tidb_port=${tidb_port}
export tidb_status_port=${tidb_status_port}

export tiflash_tcp_port=${tiflash_tcp_port}
export tiflash_http_port=${tiflash_http_port}
export tiflash_log_level=${tiflash_log_level}
export raft_port=${raft_port}
export ignore_databases=${ignore_databases}

export rngine_port=${rngine_port}

export spark_master=${spark_master}
export spark_executor_memory=${spark_executor_memory}

export bin="bin/"
export log="log/"
export data="data/"
export config="conf/"

export storage_db="${storage_db}"

mkdir -p ./\${bin}
mkdir -p ./\${data}
mkdir -p ./\${log}

EOF
