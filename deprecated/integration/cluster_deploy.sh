#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

if [[ ! -f "./load_data/bin/dbgen" ]]; then
    cur=`pwd`
    cd "../benchmark/tpch-dbgen"
    make
    cd "$cur"
    if [[ ! -d "./load_data/bin/" ]]; then
        mkdir -p "./load_data/bin/"
    fi
    cp ../benchmark/tpch-dbgen/dbgen ./load_data/bin/dbgen
    cp ../benchmark/tpch-dbgen/dists.dss ./load_data/bin/dists.dss
fi
./_build_load_data_env.sh

for server in ${storage_server[@]}; do
    ssh "${user}@${server}" "mkdir -p ${deploy_dir}" < /dev/null 2>&1
done

./_build_spark_defaults_conf.sh

for file in ./resources/*; do
    ./cluster_spread_file.sh ${file} ${deploy_dir}
done

build_env_file_name="_env_build.sh"
for server in ${storage_server[@]}; do
    ./_build_single_server_env.sh ${server} ${build_env_file_name}
    chmod a+x ${build_env_file_name}
    scp ${build_env_file_name} "${user}@${server}:${deploy_dir}"
done

./cluster_dsh.sh "./local_prepare.sh"


