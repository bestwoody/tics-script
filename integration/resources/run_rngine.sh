#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

exec $bin/${tiflash_proxy_bin_name} \
    --addr "$ip:$rngine_port" \
    --advertise-addr "$ip:$rngine_port" \
    --pd "$pd_addrs" \
    --data-dir "$data/rngine" \
    --config $config/rngine.toml \
    --log-level info \
    --log-file "$log/rngine.log" 2>> "$log/rngine_stderr.log"
