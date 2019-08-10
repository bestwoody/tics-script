#!/bin/bash

detail="$1"

set -ue
set -o pipefail

source _check.sh

len="65"

echo -e "   \033[34;7;1mDemo One @$ip\033[0m"

check_pid tidb-server tidb
check_log "${log}tidb.log" "ERRO" "error" "$len" false

check_pid pd-server "name=$pd_name"
check_log "${log}${pd_name}.log" "ERRO" "error" "$len" false
check_log "${log}${pd_name}.log" "fatal" "fatal" "$len" true

check_pid tikv-server "$kv_name"
check_log "${log}${kv_name}.log" "ERRO" "error" "$len" false

check_pid $tiflash_proxy_bin_name rngine
check_log "${log}rngine.log" "ERRO" "error" "$len" false

check_pid "$tiflash_bin_name server" flash
check_log "${data}theflash/server.log" "Error" "error" "$len" true
check_log "${data}theflash/server.log" "Warning" "warning" "$len" false
if [ "$detail" == "true" ]; then
	check_sync_status "${data}theflash/server.log"
	check_move_status "${data}theflash/server.log"
fi

show_load "nvme0n1"
show_top_regions 4

if [ "$detail" == "true" ]; then
	source ../tpch-scripts/_env.sh
	show_partitions "../theflash" "lineitem" "tpch${scale}"
	show_partitions "../theflash" "partsupp" "tpch${scale}"
	show_partitions "../theflash" "usertable" "test"
fi

show_region_partition 4
