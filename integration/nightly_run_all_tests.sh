#!/bin/bash

set -eu
set -o pipefail

source ./_cluster_env.sh

deploy_file_existed="true"

for server in ${storage_server[@]}; do
    result=`need_deploy "$server" "$deploy_dir" "$user"`
    if [[ ${result} -eq 1 ]]; then
        deploy_file_existed="false"
    fi
done

if [[ "$deploy_file_existed" == "false" ]]; then
    ./cluster_deploy.sh
fi

./nightly_restart_test.sh
sleep 10
./nightly_shrink_expand_test.sh