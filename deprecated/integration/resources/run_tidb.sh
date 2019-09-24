#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

exec $bin/tidb-server \
    -P $tidb_port \
    --status="$tidb_status_port" \
    --advertise-address="$ip" \
    --path="$pd_addrs" \
    --config="$config/tidb.toml" \
    --log-file="$log/tidb.log" 2>> "$log/tidb_stderr.log"
