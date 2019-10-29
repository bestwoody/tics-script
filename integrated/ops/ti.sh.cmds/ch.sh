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

	if [ -z "${1+x}" ]; then
		echo '[cmd ch] <cmd> query_str_or_file_path [database] [print_format=(tab|title|pretty)] [ch_args]' >&2
		return
	fi

	local query="${1}"
	if [ -z "${query}" ]; then
		return
	fi

	if [ ! -z "${2+x}" ] && [ ! -z "${2}" ]; then
		local db="${2}"
	else
		local db='default'
	fi

	if [ ! -z "${3+x}" ] && [ ! -z "${3}" ]; then
		local format="${3}"
	else
		local format='TabSeparatedWithNames'
	fi
	if [ "${format}" == 'pretty' ]; then
		local format='PrettyCompactNoEscapes'
	elif [ "${format}" == 'tab' ]; then
		local format='TabSeparated'
	elif [ "${format}" == 'title' ]; then
		local format='TabSeparatedWithNames'
	fi

	shift 3
	if [ -f "${query}" ]; then
		local query_str="`cat ${query}`"
	else
		local query_str="${query}"
	fi
	local port=`get_value "${dir}/proc.info" 'tcp_port'`
	if [ -z "${1+x}" ]; then
		"${dir}/tiflash" client --host="${host}" --port="${port}" -d "${db}" -f "${format}" --query="${query_str}"
	else
		"${dir}/tiflash" client --host="${host}" --port="${port}" -d "${db}" -f "${format}" --query="${query_str}" "${@}"
	fi
}

cmd_ti_ch "${@}"
