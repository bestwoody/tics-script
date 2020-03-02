#!/bin/bash

function cmd_ti_global_region_leader_transfer()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/region/transfer'
	mkdir -p "${dir}"
	local file="${dir}/transfer.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-9' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up : sleep 60
	"${ti}" "${file}" parallel \
		GO: wait/syncing tpch_1 lineitem : pd/ctl scheduler add shuffle-leader-scheduler : sleep 120 \
		GO: tpch/load 1 lineitem true \
		GO: wait/syncing tpch_1 lineitem \
			LOOP: verify/consistency tpch_1 lineitem
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/leader_transfer OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_region_leader_transfer
