#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

exec $bin/tikv-server \
    --addr "$ip:$kv_port" \
    --advertise-addr "$ip:$kv_port" \
    --pd "$pd_addrs" \
    --data-dir "$data/$kv_name" \
    --log-level info \
    --config $config/tikv.toml \
    --log-file "$log/${kv_name}.log" 2>> "$log/${kv_name}_stderr.log"
