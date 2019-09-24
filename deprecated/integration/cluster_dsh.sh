#!/bin/bash

cmd="$1"
async="$2"

set -eu
set -o pipefail

source ./_cluster_env.sh

if [[ -z "$cmd" ]]; then
	echo "usage: <bin> cmd [async=false]" >&2
	exit 1
fi

if [[ -z "$async" ]]; then
	async="false"
fi

for server in ${storage_server[@]}; do
    if [[ "$async" == "true" ]]; then
		execute_cmd "$server" "$cmd" "$deploy_dir" "$user" "true" &
	else
		execute_cmd "$server" "$cmd" "$deploy_dir" "$user" "true"
	fi
done

wait
