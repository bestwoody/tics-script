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

	if [ -z "${6+x}" ]; then
		echo '[cmd tikv_ctl] <cmd> command' >&2
		return
	fi

	local command="${6}"
	if [ -z "${command}" ]; then
		return
	fi

	local port=`get_value "${dir}/proc.info" 'tikv_port'`
	"${dir}/tikv-ctl" --host "http://${host}:${port}" "${command}"
}

cmd_ti_tikv_ctl "${@}"
