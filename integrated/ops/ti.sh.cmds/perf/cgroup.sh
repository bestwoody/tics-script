#!/bin/bash

function cmd_ti_cgroup_add()
{
	set -uo pipefail

	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	shift 5

	if [ -z "${1+x}" ]; then
		echo "usage: <bin> cgroup-to-be-added" >&2
		return 1
	fi

	echo "=> ${mod_name} #${index} (${dir})"

	local pid=`get_value "${dir}/proc.info" 'pid'`
	if [ -z "${pid}" ]; then
		echo "   not running"
		return 1
	fi

	local group="${1}"
	local procs="/sys/fs/cgroup/cpuset/${group}/cgroup.procs"
	if [ ! -f "${procs}" ]; then
		echo "   cgroup not exists: ${procs}"
		return 1
	fi

	for ((; 1 == 1; )); do
		local pid_l=`cat "${procs}"`
		if [ "${pid_l}" != "${pid}" ]; then
			echo "   limiting ${pid}, got ${pid_l}"
			echo "${pid}" > "${procs}"
		else
			echo "   ${pid} limited"
			break
		fi

		sleep 2
	done
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
cmd_ti_cgroup_add "${@}"
