#!/bin/bash

function cmd_ti_mysql_raw()
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

	local port=`ssh_get_value_from_proc_info "${host}" "${dir}" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo "[${host}] ${dir}: getting tidb port failed" >&2
		return 1
	fi

	mysql -h "${host}" -P "${port}" -u root "${@}"
}
cmd_ti_mysql_raw "${@}"
