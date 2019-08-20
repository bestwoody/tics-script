#!/bin/bash

function cmd_ti_burn()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"

	local doit="${5}"

	if [ "${dir}" == "/" ]; then
		echo "=> DENIED: rm -f /" >&2
		return 1
	fi
	if [ ! -d "${dir}" ]; then
		echo "=> Skipped, not a dir: ${dir}" >&2
		return
	fi
	if [ "${doit}" == "doit" ]; then
		local up_status=`ti_file_mod_status "${dir}" "${conf_rel_path}"`
		local ok=`echo "${up_status}" | grep ^OK`
		if [ ! -z "${ok}" ]; then
			ti_file_mod_stop "${index}" "${mod_name}" "${dir}" "${conf_rel_path}" 'true'
		fi
		echo "=> Burning: ${dir}"
		rm -rf "${dir}"
	else
		echo "=> Burning(dry run, append 'doit' to execute): ${dir}"
	fi
}

cmd_ti_burn "${@}"
