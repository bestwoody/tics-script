#!/bin/bash

function cmd_ti_pack_tispark()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo "[cmd pack/tispark] usage: <cmd> repo_path [branch] [commit]" >&2
		return 1
	fi
	local repo_path="${1}"

	if [ -z "${2+x}" ]; then
		local branch="master"
	else
		local branch="${2}"
	fi
	if [ -z "${3+x}" ]; then
		local commit=""
	else
		local commit="${3}"
	fi
	if [ -z "${4+x}" ]; then
		local binary_rel_path="./assembly/target/tispark-assembly-2.3.0-SNAPSHOT.jar"
	else
		local binary_rel_path="${4}"
	fi

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	source "${here}/_env.sh"
	auto_error_handle
	if [ ! -d "${repo_path}" ]; then
		echo "[cmd pack/tispark] ${repo_path} is not a dir" >&2
		return 1
	fi
	
	_build_and_update_mod "${repo_path}" "${branch}" "${commit}" "${binary_rel_path}" "mvn clean install -Dmaven.test.skip=true" "tispark"

	echo "pack binary done"
}

cmd_ti_pack_tispark "${@}"
