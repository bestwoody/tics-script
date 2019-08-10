#!/bin/bash

arg="$1"

set -eu
set -o pipefail

source ./_cluster_env.sh

if [[ -z "$arg" ]]; then
	mysql -h ${storage_server[0]} -P ${tidb_port} -u root
else
	mysql -h ${storage_server[0]} -P ${tidb_port} -u root -e "$@"
fi
