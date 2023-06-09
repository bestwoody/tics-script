#!/bin/bash

function cmd_ti_ch_kill_queries()
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

	local port=`get_value "${dir}/proc.info" 'tcp_port' 2>/dev/null`
	if [ -z "${port}" ]; then
		echo "[cmd ch/queries] getting port from proc.info failed" >&2
		return 1
	fi

	local query_str='kill query where 1'
	run_query_through_ch_client "${dir}/tiflash/tiflash" --host="${host}" --port="${port}" \
		-f "PrettyCompactNoEscapes" --query="${query_str}"
}

cmd_ti_ch_kill_queries "$@"
