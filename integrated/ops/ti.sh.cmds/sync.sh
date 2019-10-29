#!/bin/bash

function cmd_ti_sync()
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

	if [ -z "${2+x}" ]; then
		echo '[cmd sync] <cmd> database table [replica]' >&2
		return
	fi

	local db="${1}"
	if [ -z "${db}" ]; then
		return
	fi

	local table="${2}"
	if [ -z "${table}" ]; then
		return
	fi

	if [ -z "${3+x}" ]; then
		local replica='1'
	else
		local replica="${3}"
	fi

	local port=`get_value "${dir}/proc.info" 'tidb_port'`
	if [ -z "${port}" ]; then
		echo '[cmd sync] get port failed' >&2
		return 1
	fi

	local query="ALTER TABLE ${db}.${table} SET TIFLASH REPLICA ${replica}"

	if [ "${replica}" -eq 0 ]; then
		echo "remove ${db}.${table} from tiflash and stop syncing it from tikv to tiflash"
	elif [ "${replica}" -ge 1 ]; then
		echo "keep syncing ${db}.${table} from tikv to tiflash"
	else
		echo "[cmd sync] unsupported replica setting: ${replica}" >&2
		return 1
	fi
	mysql -h "${host}" -P "${port}" -u root --database="${db}" --comments -e "${query}"
}

cmd_ti_sync "${@}"
