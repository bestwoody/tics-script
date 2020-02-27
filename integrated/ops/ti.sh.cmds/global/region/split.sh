#!/bin/bash

function cmd_ti_global_region_split()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/region'
	mkdir -p "${dir}"
	local file="${dir}/region.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-9' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up
	"${ti}" "${file}" parallel \
		GO: tpch/load 10 lineitem true \
        	GO: repeat 20 region/split 20 : sleep 20 \
			GO: wait/table tpch_10 lineitem \
				LOOP: verify/consistency tpch_10 lineitem
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'region/split OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_region_split
