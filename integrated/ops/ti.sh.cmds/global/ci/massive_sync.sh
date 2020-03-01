#!/bin/bash

function cmd_ti_global_ci_massive_sync()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/massive_sync'
	mkdir -p "${dir}"
	local file="${dir}/massive_sync.ti"
	rm -f "${file}"
	if [ -z "${1+x}" ]; then
		local case_num="5000"
	else
		local case_num="${1}"
	fi

	if [ -z "${2+x}" ]; then
		local insert_num="10"
	else
		local insert_num="${2}"
	fi

	"${ti}" new "${file}" 'delta=-12' "dir=${dir}" 1>/dev/null
	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" ci/massive_sync ${case_num} ${insert_num}
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/massive_sync OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
error_handle="$-"
set +eu
cmd_args=("${@}")
restore_error_handle_flags "${error_handle}"
cmd_ti_global_ci_massive_sync "${cmd_args[@]}"
