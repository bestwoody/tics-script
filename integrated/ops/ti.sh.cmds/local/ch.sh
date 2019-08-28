#!/bin/bash

function cmd_ti_ch()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'tiflash' ]; then
		return
	fi

	if [ -z "${6+x}" ]; then
		echo '[cmd ch] <cmd> query [database] [print-format]' >&2
		return
	fi

	local query="${6}"
	if [ -z "${query}" ]; then
		return
	fi

	if [ -z "${7+x}" ]; then
		local db='default'
	else
		local db="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local format='PrettyCompactNoEscapes'
	else
		local format="${78}"
	fi

	local port=`get_value "${dir}/proc.info" 'tcp_port'`
	"${dir}/tiflash" client --host="${host}" --port="${port}" "${format}" -d "${db}" --query="${query}"
}

cmd_ti_ch "${@}"
