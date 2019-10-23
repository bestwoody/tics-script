#!/bin/bash

function cmd_ti_pd_ctl()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ "${mod_name}" != 'pd' ]; then
		return
	fi
	if [ "${index}" != '0' ]; then
		return
	fi

	if [ -z "${6+x}" ]; then
		echo '[cmd pd-ctl] usage: <cmd> command' >&2
		return
	fi

	local command="${6}"
	if [ -z "${command}" ]; then
		return
	fi

	local port=`get_value "${dir}/proc.info" 'pd_port'`
	"${dir}/pd-ctl" -u "http://${host}:${port}" <<< "${command}"
}

cmd_ti_pd_ctl "${@}"
