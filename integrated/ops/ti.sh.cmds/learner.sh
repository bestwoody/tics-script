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
		echo '[cmd mysql] usage: <cmd> query_str_or_file_path [database] [show_elapsed=false]' >&2
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

	if [ -z "${8+x}" ]; then
		local show_elapsed='false'
	else
		local show_elapsed="${8}"
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql] get port failed' >&2
		return 1
	fi

	if [ -f "${query}" ]; then
		local query=`cat "${query}" | tr -s "\n" " "`
	fi

	local query=`echo "${query}" | tr 'A-Z' 'a-z'`
	local query=`replace_substr "${query}" 'select' 'select /*+ read_from_storage(tiflash[lineitem]) */'`

	local start_time=`date +%s%N`
	mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments -e "${query}"
	local end_time=`date +%s%N`
	if [ "${show_elapsed}" == 'true' ]; then
		echo "elapsed: $(( (end_time - start_time) / 1000000 ))ms"
	fi
}

cmd_ti_mysql "${@}"
