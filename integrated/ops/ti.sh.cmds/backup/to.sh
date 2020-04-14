#!/bin/bash

function cmd_ti_backup_to()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ -z "${1+x}" ]; then
		echo "[cmd backup/to] usage: <cmd> tag" >&2
		return 1
	fi
	local tag="${1}"

	echo "=> ${mod_name} #${index} (${dir})"

	if [ ! -f "${dir}/proc.info" ]; then
		if [ "${mod_name}" == 'tidb' ]; then
			echo "   missed, ignore"
			return
		else
			echo "   missed, exit..." >&2
			return 1
		fi
	fi

	local up_status=`ti_file_mod_status "${dir}" "${conf_rel_path}"`
	local ok=`echo "${up_status}" | { grep ^OK || test $? = 1; }`
	if [ ! -z "${ok}" ]; then
		echo "   still running, stop service first, exit..." >&2
		return 1
	fi

	if [ -d "${dir}.${tag}" ] || [ -f "${dir}.${tag}" ]; then
		echo "   ${dir}.${tag} exists, exit..." >&2
		return 1
	fi

	echo "   backup to '${tag}'"
	cp -r "${dir}" "${dir}.${tag}"
	echo "   done"
}

set -euo pipefail
cmd_ti_backup_to "${@}"
