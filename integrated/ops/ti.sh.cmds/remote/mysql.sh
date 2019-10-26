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
		echo '[cmd mysql] usage: <cmd> query_str_or_file_path [database]' >&2
		return
	fi

	local query="${6}"
	if [ -z "${query}" ]; then
		return
	fi

	if [ -z "${7+x}" ]; then
		local db='mysql'
	else
		local db="${7}"
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql] get port failed' >&2
		return 1
	fi

	if [ -f "${query}" ]; then
		mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments < "${query}"
	else
		mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments -e "${query}"
	fi
}

cmd_ti_mysql "${@}"
