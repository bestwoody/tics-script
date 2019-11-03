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
		echo '[cmd ch] <cmd> query_str_or_file_path [database] [print_format=(tab|title|pretty)] [show_elapsed=false] [ch_args]' >&2
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
		local show_elapsed='false'
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

	if [ `uname` != "Darwin" ]; then
		# TODO: remove hard code file name from this func
		local target_lib_dir="tiflash_lib"
		if [ ! -d "${dir}/${target_lib_dir}" ]; then
			echo "[cmd ch] cannot find library dir ${dir}/${target_lib_dir}"
			return 1
		fi
		# TODO: check whether the following path exists before use it
		local lib_path="/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:${dir}/${target_lib_dir}"
		if [ -z "${LD_LIBRARY_PATH+x}" ]; then
			export LD_LIBRARY_PATH="$lib_path"
		else
			export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$lib_path"
		fi
	fi

	local start_time=`date +%s%N`
	if [ -z "${1+x}" ]; then
		"${dir}/tiflash" client --host="${host}" --port="${port}" -d "${db}" -f "${format}" --query="${query_str}"
	else
		"${dir}/tiflash" client --host="${host}" --port="${port}" -d "${db}" -f "${format}" --query="${query_str}" "${@}"
	fi
	local end_time=`date +%s%N`
	if [ "${show_elapsed}" == 'true' ]; then
		echo "elapsed: $(( (end_time - start_time) / 1000000 ))ms"
	fi
}

set -euo pipefail
cmd_ti_ch "${@}"
