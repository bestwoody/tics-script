#!/bin/bash

function cmd_ti_global_tidb_test_randgen()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/tidb/test/randgen'
	mkdir -p "${dir}"
	local file="${dir}/randgen.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-62' "dir=${dir}" 1>/dev/null
	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" tidb/test/randgen
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'tidb/test/randgen OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_tidb_test_randgen
