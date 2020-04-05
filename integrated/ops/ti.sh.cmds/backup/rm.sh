#!/bin/bash

function cmd_ti_backup_rm()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ -z "${1+x}" ]; then
		echo "[cmd backup/rm] usage: <cmd> tag" >&2
		return 1
	fi
	local tag="${1}"

	echo "=> ${mod_name} #${index} (${dir})"

	if [ ! -d "${dir}.${tag}" ]; then
		echo "   ignore: ${dir}.${tag} not exists" >&2
		return
	fi

	echo "   deleting ${tag}"
	rm -rf "${dir}.${tag}"
	echo "   done"
}

set -euo pipefail
cmd_ti_backup_rm "${@}"
