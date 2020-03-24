#!/bin/bash

function cmd_ti_global_tidb_test_sqllogic()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/tidb/test/sqllogic'
	mkdir -p "${dir}"
	local file="${dir}/sqllogic.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-60' "dir=${dir}" 1>/dev/null
	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" tidb/test/sqllogic
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'tidb/test/sqllogic OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_tidb_test_sqllogic
