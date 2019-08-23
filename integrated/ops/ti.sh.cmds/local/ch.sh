#!/bin/bash

function cmd_ti_ch()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ "${mod_name}" != 'tiflash' ]; then
		return
	fi

	local format='PrettyCompactNoEscapes'
	local port=`get_value "${dir}/proc.info" 'tcp_port'`
	"${dir}/tiflash" client --host="${host}" --port="${port}" "${format}" "${@}"
}

cmd_ti_ch "${@}"
