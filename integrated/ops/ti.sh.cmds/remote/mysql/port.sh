#!/bin/bash

function cmd_ti_mysql_port()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'tidb' ]; then
		return
	fi

	echo `get_value "${dir}/proc.info" "tidb_port"`
}

cmd_ti_mysql_port "${@}"
