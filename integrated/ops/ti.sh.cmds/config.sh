#!/bin/bash

function cmd_ti_config()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	echo "${mod_name} #${index} (${dir})"
	if [ -f "${dir}/${conf_rel_path}" ]; then
		cat "${dir}/${conf_rel_path}" | awk '{print "['${mod_name}' #'${index}'] "$0}'
	else
		echo "    MISSED"
	fi
}

cmd_ti_config "${@}"
