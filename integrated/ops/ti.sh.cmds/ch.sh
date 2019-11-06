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

	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo '[cmd ch] <cmd> query_str_or_file_path [database] [print_format=(tab|title|pretty)] [show_elapsed=true] [ch_args]' >&2
		return
	fi

	local query="${1}"
	shift 1

	if [ ! -z "${1+x}" ] && [ ! -z "${1}" ]; then
		local db="${1}"
		shift 1
	else
		local db='default'
	fi

	if [ ! -z "${1+x}" ] && [ ! -z "${1}" ]; then
		local format="${1}"
		shift 1
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

	if [ ! -z "${1+x}" ] && [ ! -z "${1}" ]; then
		local show_elapsed="${1}"
		shift 1
	else
		local show_elapsed='true'
	fi
	if [ "${show_elapsed}" != 'false' ] && [ "${show_elapsed}" != 'true' ]; then
		echo "[cmd ch] show_elapsed should be 'true|false', got '${show_elapsed}'" >&2
		return 1
	fi

	if [ -f "${query}" ]; then
		local query_str="`cat ${query}`"
	else
		local query_str="${query}"
	fi
	local port=`get_value "${dir}/proc.info" 'tcp_port'`

	local start_time=`timer_start`
	if [ -z "${1+x}" ]; then
		LD_LIBRARY_PATH="`get_tiflash_lib_path`" "${dir}/tiflash" client --host="${host}" \
			--port="${port}" -d "${db}" -f "${format}" --query="${query_str}"
	else
		LD_LIBRARY_PATH="`get_tiflash_lib_path`" "${dir}/tiflash" client --host="${host}" \
			--port="${port}" -d "${db}" -f "${format}" --query="${query_str}" "${@}"
	fi
	local elapsed=`timer_end "${start_time}"`
	if [ "${show_elapsed}" == 'true' ]; then
		echo "elapsed: ${elapsed}"
	fi
}

set -euo pipefail
cmd_ti_ch "${@}"
