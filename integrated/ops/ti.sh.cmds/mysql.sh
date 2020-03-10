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
		echo '[cmd mysql] usage: <cmd> query_str_or_file_path [database] [show_elapsed=true] [pretty=false] [show-warnings=false] [user=root]' >&2
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
		echo "[cmd mysql] show_elapsed should be 'true|false', got '${show_elapsed}'" >&2
		return 1
	fi

	if [ -z "${9+x}" ]; then
		local pretty='false'
	else
		local pretty="${9}"
	fi

	if [ -z "${10+x}" ]; then
		local show_warnings='false'
	else
		local show_warnings="${10}"
	fi

	if [ -z "${11+x}" ]; then
		local user='root'
	else
		local user="${11}"
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql] get port failed' >&2
		return 1
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		local start_time=`timer_start`
	fi

	if [ "${pretty}" == 'false' ]; then
		local pretty_opt=''
	else
		local pretty_opt=' --table'
	fi

	if [ "${show_warnings}" == 'false' ]; then
		local show_warnings_opt=''
	else
		local show_warnings_opt=' --show-warnings'
	fi

	local mysql_cmd="mysql -h ${host} -P ${port} -u ${user} --database=${db} --comments${pretty_opt}${show_warnings_opt}"

	if [ -f "${query}" ]; then
		${mysql_cmd} < "${query}"
	else
		${mysql_cmd} -e "${query}"
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		local elapsed=`timer_end "${start_time}"`
		echo "elapsed: ${elapsed}"
	fi
}

set -euo pipefail
cmd_ti_mysql "${@}"
