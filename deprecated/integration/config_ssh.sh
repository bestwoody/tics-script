#!/bin/bash

set -eu

source ./_cluster_env.sh

if [[ ! -f ~/.ssh/id_rsa ]]; then
    echo "id_rsa file not exists. creating..."
    ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
else
    echo "id_rsa file already exists."
fi

for server in "${storage_server[@]}"; do
    target=`get_host $server`
    echo $target
    ssh-copy-id -i ~/.ssh/id_rsa $target
done
