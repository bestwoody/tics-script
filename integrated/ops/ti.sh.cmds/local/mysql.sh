#!/bin/bash

function cmd_ti_mysql()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ "${mod_name}" != 'tidb' ]; then
		return
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	mysql -h "${host}" -P "${port}" -u root "${@}"
}

cmd_ti_mysql "${@}"
