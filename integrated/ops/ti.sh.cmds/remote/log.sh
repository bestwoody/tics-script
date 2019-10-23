#!/bin/bash

function cmd_ti_log()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ -z "${1+x}" ]; then
		echo "[cmd log] usage: <cmd> count-of-matched-log [grep-string] [upload]" >&2
		return
	fi
	local cnt="${1}"

	local grep_str=''
	if [ ! -z "${2+x}" ]; then
		local grep_str="${2}"
	fi

	local upload_log='false'
	if [ ! -z "${3+x}" ]; then
		local upload_log="${3}"
	fi

	local log_file=''
	if [ "${mod_name}" == 'pd' ]; then
		local log_file='pd.log'
	elif [ "${mod_name}" == 'tikv' ]; then
		local log_file='tikv.log'
	elif [ "${mod_name}" == 'tidb' ]; then
		local log_file='tidb.log'
	elif [ "${mod_name}" == 'tiflash' ]; then
		local log_file='log/server.log'
	elif [ "${mod_name}" == 'rngine' ]; then
		local log_file='rngine.log'
	elif [ "${mod_name}" == 'spark_m' ]; then
		local log_file='logs/spark_master.log'
	elif [ "${mod_name}" == 'spark_w' ]; then
		local log_file='logs/spark_worker.log'
	fi
	local log_file="${dir}/${log_file}"

	if [ ! -f "${log_file}" ]; then
		echo "[${mod_name} #${index} ${dir}] NO LOG" >&2
		return
	fi

	if [ "${upload_log}" == "false" ]; then
		cat "${log_file}" | { grep -i "${grep_str}" || test $? = 1; } | tail -n "${cnt}" | awk '{print "['${mod_name}' #'${index}' '${dir}'] "$0}'
	else
		local file_name=`basename "${log_file}"`
		# TODO: move this to a new cmd
		curl --upload-file "${log_file}" "http://139.219.11.38:8000/${file_name}" "http://139.219.11.38:8000/66nb8/${file_name}"
	fi
}

cmd_ti_log "${@}"
