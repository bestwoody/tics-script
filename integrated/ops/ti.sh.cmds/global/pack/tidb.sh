#!/bin/bash

function cmd_ti_pack_tidb()
{
	if [ -z "${GOPATH+x}" ] || [ -z "${GOPATH}" ]; then
		echo "[cmd pack/tidb] GOPATH not set" >&2
		return 1
	fi
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local repo_path="${GOPATH}/src/github.com/pingcap/tidb"
	else
		local repo_path="${1}"
	fi
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
		local binary_rel_path="bin/tidb-server"
	else
		local binary_rel_path="${4}"
	fi

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	source "${here}/_env.sh"
	auto_error_handle
	if [ ! -d "${repo_path}" ]; then
		echo "[cmd pack/tidb] ${repo_path} is not a dir" >&2
		return 1
	fi

	_build_and_update_mod "${repo_path}" "${branch}" "${commit}" "${binary_rel_path}" "make" "tidb"

	echo "pack binary done"
}

cmd_ti_pack_tidb "${@}"
