#!/bin/bash

function cmd_ti_global_schrodinger_bank()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local enable_region_merge='false'
	else
		local enable_region_merge="${1}"
		if [ "${enable_region_merge}" != 'false' ] && [ "${enable_region_merge}" != 'true' ]; then
			echo "[cmd schrodinger/bank] value of enable_region_merge should be true or false" >&2
			return 1
		fi
	fi
	if [ -z "${2+x}" ] || [ -z "${2}" ]; then
		local enable_shuffle_region='false'
	else
		local enable_shuffle_region="${2}"
		if [ "${enable_shuffle_region}" != 'false' ] && [ "${enable_shuffle_region}" != 'true' ]; then
			echo "[cmd schrodinger/bank] value of enable_shuffle_region should be true or false" >&2
			return 1
		fi
	fi
	if [ -z "${3+x}" ] || [ -z "${3}" ]; then
		local enable_shuffle_leader='false'
	else
		local enable_shuffle_leader="${3}"
		if [ "${enable_shuffle_leader}" != 'false' ] && [ "${enable_shuffle_leader}" != 'true' ]; then
			echo "[cmd schrodinger/bank] value of enable_shuffle_leader should be true or false" >&2
			return 1
		fi
	fi
	if [ -z "${4+x}" ] || [ -z "${4}" ]; then
		local dir='/tmp/ti/schrodinger/bank'
	else
		local dir="${4}"
	fi

	local ti="${integrated}/ops/ti.sh"

	mkdir -p "${dir}"
	local file="${dir}/bank.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=+10' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run

	if [ "${enable_region_merge}" == 'true' ]; then
		"${ti}" "${file}" 'pd/ctl_raw' 'scheduler' 'add' 'random-merge-scheduler' 1>/dev/null 2>&1
		"${ti}" "${file}" 'pd/ctl_raw' 'config' 'set' 'merge-schedule-limit' 8 1>/dev/null 2>&1
	fi
	if [ "${enable_shuffle_region}" == 'true' ]; then
		"${ti}" "${file}" 'pd/ctl_raw' 'scheduler' 'add' 'shuffle-region-scheduler' 1>/dev/null 2>&1
	fi
	if [ "${enable_shuffle_leader}" == 'true' ]; then
		"${ti}" "${file}" 'pd/ctl_raw' 'scheduler' 'add' 'shuffle-leader-scheduler' 1>/dev/null 2>&1
	fi
	echo "Enable region merge: ${enable_region_merge}"
	echo "Enable shuffle region: ${enable_shuffle_region}"
	echo "Enable shuffle leader: ${enable_shuffle_leader}"

	"${ti}" "${file}" schrodinger/bank

	print_hhr
	echo 'schrodinger/bank FINISHED'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
error_handle="$-"
set +eu
cmd_args=("${@}")
restore_error_handle_flags "${error_handle}"
cmd_ti_global_schrodinger_bank "${cmd_args[@]}"
