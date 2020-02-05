#!/bin/bash

function cmd_ti_pack_tiflash()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo "[cmd pack/tiflash] usage: <cmd> repo_path [branch] [commit]" >&2
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
		if [ `uname` == "Darwin" ]; then
			local binary_rel_path="build_clang/dbms/src/Server/tiflash"
		else
			local binary_rel_path="build/dbms/src/Server/tiflash"
		fi
	else
		local binary_rel_path="${4}"
	fi

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	source "${here}/_env.sh"
	auto_error_handle
	if [ ! -d "${repo_path}" ]; then
		echo "[cmd pack/tiflash] ${repo_path} is not a dir" >&2
		return 1
	fi

	if [ `uname` == "Darwin" ]; then
		local build_command="./build_clang.sh"
	else
		local build_command="./build.sh"
	fi
	_build_and_update_mod "${repo_path}" "${branch}" "${commit}" "${binary_rel_path}" "${build_command}" "tiflash" "tiflash"

	echo "pack binary done"
}

cmd_ti_pack_tiflash "${@}"
