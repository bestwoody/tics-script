#!/bin/bash

function cmd_ti_global_ci_manual()
{
	local ti="${integrated}/ops/ti.sh"

	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local dir='/tmp/ti/ci/manual'
	else
		local dir="${1}"
	fi

	mkdir -p "${dir}"
	local file="${dir}/manual.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-100' "dir=${dir}" 'tikv=3' 'tiflash=3' 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" ver

	"${ti}" "${file}" ci/cases/consistency/all 10

	"${ti}" "${file}" ci/write_balance

	"${ti}" "${file}" ci/massive_sync 1000 100

	"${ti}" "${file}" must burn

	"${ti}" region/leader_transfer 10
	"${ti}" region/learner_transfer 10
	"${ti}" region/merge 10
	"${ti}" region/split 10

	rm -f "${file}"
	print_hhr
	echo 'ci/manual OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
error_handle="$-"
set +eu
cmd_args=("${@}")
restore_error_handle_flags "${error_handle}"
cmd_ti_global_ci_manual "${cmd_args[@]}"
