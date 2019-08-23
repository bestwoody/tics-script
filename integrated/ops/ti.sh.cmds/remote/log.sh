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
		echo "[cmd log] usage: <cmd> count-of-matched-log [grep-string]" >&2
		return
	fi
	local cnt="${1}"

	local grep_str=''
	if [ ! -z "${2+x}" ]; then
		local grep_str="${2}"
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
	fi
	local log_file="${dir}/${log_file}"

	if [ ! -f "${log_file}" ]; then
		echo "[${mod_name} #${index} ${dir}] NO LOG" >&2
		return
	fi

	cat "${log_file}" | grep -i "${grep_str}" | tail -n "${cnt}" | awk '{print "['${mod_name}' #'${index}' '${dir}'] "$0}'
}

cmd_ti_log "${@}"
