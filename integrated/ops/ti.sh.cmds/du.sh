#!/bin/bash

function cmd_ti_du()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"

	if [ -d "${dir}" ]; then
		local res=`du -sh "${dir}" | awk '{print $1}'`
	else
		local res="MISSED"
	fi
	echo "${mod_name} #${index} (${dir}) ${res}"
}

cmd_ti_du "${@}"
