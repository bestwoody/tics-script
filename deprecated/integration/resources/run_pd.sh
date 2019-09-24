#!/bin/bash

set -ue
set -o pipefail

source ./_env_build.sh

peer_port=$((pd_port+1))

exec $bin/pd-server \
    --name="$pd_name" \
    --client-urls="http://$ip:$pd_port" \
    --advertise-client-urls="http://$ip:$pd_port" \
    --peer-urls="http://$ip:$peer_port" \
    --advertise-peer-urls="http://$ip:$peer_port" \
    --data-dir="$data/$pd_name" \
    --initial-cluster="$cluster_addrs" \
    --config=$config/pd.toml \
    --log-file="$log/${pd_name}.log" 2>> "$log/${pd_name}_stderr.log"
