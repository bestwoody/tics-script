#!/bin/bash

function cmd_ti_tikv_ctl()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'tikv' ]; then
		return
	fi
	if [ "${index}" != '0' ]; then
		return
	fi

	if [ -z "${6+x}" ] || [ -z "${7+x}" ]; then
		echo '[cmd tikv_ctl] <cmd> online(true|false) command' >&2
		return
	fi
	local cmd_type="${6}"
	if [ "${cmd_type}" != "true" ] && [ "${cmd_type}" != "false" ]; then
		echo '[cmd tikv_ctl] <cmd> online(true|false) command' >&2
		return
	fi

	shift 6
	if [ "${cmd_type}" == "true" ]; then
		local port=`get_value "${dir}/proc.info" 'tikv_port'`
		"${dir}/tikv-ctl" --host "${host}:${port}" "${@}"
	else
		"${dir}/tikv-ctl" --db "${dir}/data/db" "${@}"
	fi	
}

cmd_ti_tikv_ctl "${@}"
