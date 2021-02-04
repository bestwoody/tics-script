#!/bin/bash

function cmd_ti_global_ci_tpch_diff()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/diff'
	mkdir -p "${dir}"
	local file="${dir}/diff.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-28' 'tikv=2' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up
	"${ti}" "${file}" ci/tpch_diff
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/tpch_diff OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_ci_tpch_diff
