#!/bin/bash

function cmd_ti_mysql()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'tidb' ]; then
		return
	fi

	if [ -z "${6+x}" ]; then
		echo '[cmd mysql/learner] usage: <cmd> query_str_or_file_path [database] [show_elapsed=true]' >&2
		return 1
	fi

	local query="${6}"
	if [ -z "${query}" ]; then
		return 1
	fi

	if [ -z "${7+x}" ]; then
		local db='mysql'
	else
		local db="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local show_elapsed='true'
	else
		local show_elapsed="${8}"
	fi
	if [ "${show_elapsed}" != 'false' ] && [ "${show_elapsed}" != 'true' ]; then
		echo "[cmd mysql/learner] show_elapsed should be 'true|false', got '${show_elapsed}'" >&2
		return 1
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql/learner] get port failed' >&2
		return 1
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		mysql_explain "${host}" "${port}" "${query}" 'tiflash'
		local start_time=`timer_start`
	fi

	if [ -f "${query}" ]; then
		mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments < "${query}"
	else
		local si="set @@session.tidb_isolation_read_engines=\"tiflash\"; "
		mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments -e "${si} ${query}"
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		local elapsed=`timer_end "${start_time}"`
		echo "elapsed: ${elapsed}"
	fi
}

set -euo pipefail
cmd_ti_mysql "${@}"
