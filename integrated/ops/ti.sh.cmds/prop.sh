#!/bin/bash

function cmd_ti_prop()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5


	echo "${mod_name} #${index} (${dir})"
	if [ -f "${dir}/proc.info" ]; then
		cat "${dir}/proc.info" | awk -F '\t' '{print "    "$1": "$2}'
	else
		echo "    MISSED"
	fi
}

cmd_ti_prop "${@}"
