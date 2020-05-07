#!/bin/bash

function cmd_ti_global_region_learner_transfer()
{
	local ti="${integrated}/ops/ti.sh"

	local scale='1'
	if [ ! -z "${1+x}" ]; then
		local scale="${1}"
	fi

	local dir='/tmp/ti/region/learner_transfer'
	mkdir -p "${dir}"
	local file="${dir}/transfer.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-76' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up : sleep 10
	"${ti}" "${file}" pd/ctl scheduler add shuffle-region-scheduler
	"${ti}" "${file}" pd/ctl scheduler config shuffle-region-scheduler set-roles learner
	"${ti}" "${file}" parallel \
		GO: tpch/load ${scale} lineitem true \
		GO: wait/available tpch_${scale} lineitem \
			LOOP: verify/consistency tpch_${scale} lineitem
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/learner_transfer OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
error_handle="$-"
set +eu
cmd_args=("${@}")
restore_error_handle_flags "${error_handle}"
cmd_ti_global_region_learner_transfer "${cmd_args[@]}"
