#!/bin/bash

function cmd_ti_global_ci_tidb_test()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/tidb-test'
	mkdir -p "${dir}"
	local file="${dir}/tidb-test.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-33' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up
	"${ti}" "${file}" ci/tidb_test
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/tidb_test OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_ci_tidb_test
