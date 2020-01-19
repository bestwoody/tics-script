#!/bin/bash

function cmd_ti_beeline()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ "${mod_name}" != 'spark_m' ] &&  [ "${mod_name}" != 'chspark_m' ]; then
		return
	fi

	if [ -z "${1+x}" ]; then
		echo '[cmd beeline] usage: <cmd> query_str_or_file_path [database] [show_elapsed=true]' >&2
		return 1
	fi

	local query="${1}"
	if [ -z "${query}" ]; then
		return 1
	fi

	if [ -z "${2+x}" ]; then
		local db='test'
	else
		local db="${2}"
	fi

	if [ -z "${3+x}" ]; then
		local show_elapsed='true'
	else
		local show_elapsed="${3}"
	fi
	if [ "${show_elapsed}" != 'false' ] && [ "${show_elapsed}" != 'true' ]; then
		echo "[cmd mysql] show_elapsed should be 'true|false', got '${show_elapsed}'" >&2
		return 1
	fi

	local port=`get_value "${dir}/proc.info" 'thriftserver_port'`
	if [ -z "${port}" ]; then
		echo '[cmd mysql] get port failed' >&2
		return 1
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		local start_time=`timer_start`
	fi

	if [ -f "${query}" ]; then
		${dir}/spark/bin/beeline --verbose=false --silent=true -u "jdbc:hive2://${host}:${port}/${db}" -f "${query}"
	else
		${dir}/spark/bin/beeline --verbose=false --silent=true -u "jdbc:hive2://${host}:${port}/${db}" -e "${query}"
	fi

	if [ "${show_elapsed}" == 'true' ]; then
		local elapsed=`timer_end "${start_time}"`
		echo "elapsed: ${elapsed}"
	fi
}

set -euo pipefail
cmd_ti_beeline "${@}"
