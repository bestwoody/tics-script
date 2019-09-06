#!/bin/bash

function cmd_ti_beeline()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'spark_m' ]; then
		return
	fi

	if [ -z "${6+x}" ]; then
		echo '[cmd beeline] <cmd> query' >&2
		return
	fi

	local query="${6}"
	if [ -z "${query}" ]; then
		return
	fi

	local port=`get_value "${dir}/proc.info" 'thriftserver_port'`
	${dir}/spark/bin/beeline -u "jdbc:hive2://${host}:${port}" -e "${query}"
}

cmd_ti_beeline "${@}"