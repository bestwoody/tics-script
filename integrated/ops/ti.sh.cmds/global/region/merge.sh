#!/bin/bash

function cmd_ti_global_region_merge()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/region/merge'
	mkdir -p "${dir}"
	local file="${dir}/merge.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-29' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up : sleep 60

	"${ti}" "${file}" pd/ctl_raw scheduler add random-merge-scheduler
	"${ti}" "${file}" pd/ctl_raw config set merge-schedule-limit 8

	"${ti}" "${file}" parallel \
		GO: tpch/load 1 lineitem true : sleep 60 \
		GO: sleep 60 : wait/available tpch_1 lineitem \
			LOOP: verify/consistency tpch_1 lineitem

	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/merge OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_region_merge
