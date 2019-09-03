#!/bin/bash

function cmd_ti_burn()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	local doit="${6}"

	if [ "${dir}" == "/" ]; then
		echo "=> DENIED: rm -f /" >&2
		return 1
	fi
	if [ ! -d "${dir}" ]; then
		echo "=> skipped: ${dir}, not a dir"
		return
	fi
	if [ "${doit}" == "doit" ]; then
		local up_status=`ti_file_mod_status "${dir}" "${conf_rel_path}"`
		local ok=`echo "${up_status}" | grep ^OK`
		if [ ! -z "${ok}" ]; then
			ti_file_cmd_fstop "${index}" "${mod_name}" "${dir}" "${conf_rel_path}"
		fi
		echo "=> burned:  ${dir}"
		rm -rf "${dir}"
	else
		echo "=> dry run: ${dir}, append 'doit' to burn"
	fi
}

cmd_ti_burn "${@}"
