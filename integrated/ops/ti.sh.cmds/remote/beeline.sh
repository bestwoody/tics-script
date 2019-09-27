#!/bin/bash

function cmd_ti_beeline()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ "${mod_name}" != 'spark_m' ]; then
		return
	fi

	if [ -z "${2+x}" ]; then
		echo '[cmd beeline] <cmd> database beeline-args' >&2
		return
	fi
	local db="${1}"
	if [ "${db}" == "-e" ]; then
		echo '[cmd beeline] <cmd> database beeline-args' >&2
		return
	fi

	shift 1

	local port=`get_value "${dir}/proc.info" 'thriftserver_port'`
	${dir}/spark/bin/beeline -u "jdbc:hive2://${host}:${port}/${db}" "${@}"
}

cmd_ti_beeline "${@}"
