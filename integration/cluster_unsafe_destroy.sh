#!/bin/bash

set -ue
set -o pipefail

source ./_cluster_env.sh

echo "destroying cluster file..."

for server in ${storage_server[@]}; do
    ssh "${user}@${server}" "rm -rf ${deploy_dir}" < /dev/null 2>&1
done

echo "done"
