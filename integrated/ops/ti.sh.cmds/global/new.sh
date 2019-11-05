#!/bin/bash

function cmd_ti_new()
{
	local path="${1}"
	shift 1

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	source "${here}/_env.sh"
	local cmd_args=("${@}")
	auto_error_handle

	if [ -z "${path}" ]; then
		echo "[cmd new] example: <cmd> file_path_to_be_created.ti tikv=3 tiflash=3 spark=2" >&2
		return 1
	fi

	local ext=`print_file_ext "${path}"`
	if [ "${ext}" != 'ti' ]; then
		echo "[cmd new] the file name must have '.ti' ext name" >&2
		return 1
	fi

	if [ -f "${path}" ]; then
		echo "[cmd new] ${path} exists, burn and rm it first" >&2
		return 1
	fi

	if [ -z "${cmd_args+x}" ]; then
		python "${here}"/new.py > "${path}"
	else
		python "${here}"/new.py "${cmd_args[@]}" > "${path}"
	fi

	echo "${path} created"
}

cmd_ti_new "${@}"
