#!/bin/bash

function cmd_ti_backup_restore()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ -z "${1+x}" ]; then
		echo "[cmd backup/restore] usage: <cmd> tag" >&2
		return 1
	fi
	local tag="${1}"

	echo "=> ${mod_name} #${index} (${dir})"

	if [ ! -d "${dir}.${tag}" ]; then
		if [ "${mod_name}" == 'tidb' ]; then
			echo "   ${dir}.${tag} not exists, ignore"
			return
		else
			echo "   ${dir}.${tag} not exists, exit..." >&2
			return 1
		fi
	fi

	if [ -d "${dir}" ]; then
		local has_files=`ls -A ${dir} | wc -w`
		if [ "${has_files}" != '0' ]; then
			echo "   ${dir} not empty, exit..." >&2
			return 1
		fi
		rmdir -f "${dir}"
	fi

	echo "   restore from '${tag}'"
	cp -rf "${dir}.${tag}" "${dir}"
	echo "   done"
}

set -euo pipefail
cmd_ti_backup_restore "${@}"
