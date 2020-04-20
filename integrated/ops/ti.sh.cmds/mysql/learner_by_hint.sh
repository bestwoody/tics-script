#!/bin/bash

function cmd_ti_learner()
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
		echo '[cmd mysql/learner_by_hint] usage: <cmd> query_str_or_file_path [database] [show_elapsed=true]' >&2
		return 1
	fi

	local query="${6}"
	if [ -z "${query}" ]; then
		echo '[cmd mysql/learner_by_hint] query is empty'
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

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql/learner_by_hint] get port failed' >&2
		return 1
	fi

	if [ -f "${query}" ]; then
		local query=`cat "${query}" | grep -v "^--" | tr -s "\n" " "`
	fi

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	local query=`python "${here}/to_learner_query.py" "${query}"`

	if [ "${show_elapsed}" == 'true' ]; then
		# Plan don't affected by hint
		#mysql_explain "${host}" "${port}" "${query}" ''
		local start_time=`timer_start`
	fi

	mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments -e "${query}"

	if [ "${show_elapsed}" == 'true' ]; then
		local elapsed=`timer_end "${start_time}"`
		echo "elapsed: ${elapsed}"
	fi
}

set -euo pipefail
cmd_ti_learner "${@}"
