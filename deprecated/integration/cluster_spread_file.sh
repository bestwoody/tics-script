#!/bin/bash

file="$1"
target="$2"
user="$3"

set -eu
set -o pipefail

source ./_cluster_env.sh

if [[ -z "$file" || -z "$target" ]]; then
	echo "usage: <bin> file target [user]" >&2
	exit 1
fi

if [[ ! -z "$user" ]]; then
	user="$user@"
fi

echo "=> target: $target"
for server in ${storage_server[@]}; do
	echo "=> copying to [$server]"
	if [[ -f ${file} ]]; then
	    scp ${file} ${user}${server}:${target}
	fi
	if [[ -d ${file} ]]; then
	    scp -r ${file} ${user}${server}:${target}
	fi
done

