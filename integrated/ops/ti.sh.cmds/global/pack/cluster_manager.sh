#!/bin/bash

function cmd_ti_pack_cluster_manager()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo "[cmd pack/cluster_manager] usage: <cmd> repo_path [branch] [commit]" >&2
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
		local binary_rel_path="dist/flash_cluster_manager"
	else
		local binary_rel_path="${4}"
	fi

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	source "${here}/_env.sh"
	auto_error_handle
	if [ ! -d "${repo_path}" ]; then
		echo "[cmd pack/cluster_manager] ${repo_path} is not a dir" >&2
		return 1
	fi
	
	_build_and_update_mod "${repo_path}" "${branch}" "${commit}" "${binary_rel_path}" "./release.sh" "cluster_manager"

	echo "pack binary done"
}

cmd_ti_pack_cluster_manager "${@}"
