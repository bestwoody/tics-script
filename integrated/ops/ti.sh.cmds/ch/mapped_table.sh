#!/bin/bash

function cmd_ti_mapped_table()
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

	if [ -z "${2+x}" ]; then
		echo "[cmd ch/mapped_table] usage: <cmd> database table [qualify=true]" >&2
		return 1
	fi
    
	local db_name="${1}"
	local tab_name="${2}"
    shift 2
	if [ ! -z "${1+x}" ] && [ ! -z "${1}" ]; then
        local query_str="DBGInvoke mapped_table(${db_name}, ${tab_name}, '${1}')"
	else
        local query_str="DBGInvoke mapped_table(${db_name}, ${tab_name})"
	fi

	local port=`get_value "${dir}/proc.info" 'tcp_port' 2>/dev/null`
	if [ -z "${port}" ]; then
		echo "[cmd ch/queries] getting port from proc.info failed" >&2
		return 1
	fi

	run_query_through_ch_client "${dir}/tiflash" --host="${host}" --port="${port}" \
		-f "TabSeparated" --query="${query_str}"
}

set -euo pipefail
cmd_ti_mapped_table "${@}"
