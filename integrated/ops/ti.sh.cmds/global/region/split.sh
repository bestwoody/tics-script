#!/bin/bash

function cmd_ti_global_region_split()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/region/split'
	mkdir -p "${dir}"
	local file="${dir}/split.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-9' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up : sleep 60
	"${ti}" "${file}" parallel \
		GO: wait/available tpch_1 lineitem : region/split_test 10 10 10 \
		GO: tpch/load 1 lineitem true \
		GO: wait/available tpch_1 lineitem \
			LOOP: verify/consistency tpch_1 lineitem
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/split OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_region_split
