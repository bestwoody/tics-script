#!/bin/bash

function cmd_ti_global_region_learner_transfer()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/region/learner_transfer'
	mkdir -p "${dir}"
	local file="${dir}/transfer.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-76' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up : sleep 60
	"${ti}" "${file}" parallel \
		GO: sleep 60 : wait/available tpch_1 lineitem : pd/ctl scheduler add shuffle-region-scheduler : pd/ctl scheduler config shuffle-region-scheduler set-roles learner : sleep 120 \
		GO: tpch/load 1 lineitem true \
		GO: sleep 60 : wait/available tpch_1 lineitem \
			LOOP: verify/consistency tpch_1 lineitem
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/learner_transfer OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_region_learner_transfer
