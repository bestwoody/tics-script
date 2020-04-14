#!/bin/bash

function cmd_ti_backup_rename()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	if [ -z "${2+x}" ]; then
		echo "[cmd backup/rename] usage: <cmd> src_tag dest_tag" >&2
		return 1
	fi
	local src_tag="${1}"
	local dest_tag="${2}"

	echo "=> ${mod_name} #${index} (${dir})"

	if [ ! -d "${dir}.${src_tag}" ]; then
		echo "   ${dir}.${src_tag} not exists, exit..." >&2
		return 1
	fi
	if [ -d "${dir}.${dest_tag}" ] || [ -f "${dir}.${dest_tag}" ]; then
		echo "   ${dir}.${dest_tag} exists, exit..." >&2
		return 1
	fi

	echo "   rename ${src_tag} to ${dest_tag}"
	mv "${dir}.${src_tag}" "${dir}.${dest_tag}"
	echo "   done"
}

set -euo pipefail
cmd_ti_backup_rename "${@}"
