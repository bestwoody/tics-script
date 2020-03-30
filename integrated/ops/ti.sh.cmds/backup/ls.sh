#!/bin/bash

function _print_backup_names()
{
	local dir="${1}"
	for dir in "${dir}."*; do
		if [ -d "${dir}" ]; then
			print_file_ext ${dir}
		fi
	done
}
export -f _print_backup_names

function cmd_ti_backup_ls()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	echo "=> ${mod_name} #${index} (${dir})"
	local names=`_print_backup_names "${dir}"`
	if [ ! -z "${names}" ]; then
		echo "${names}" | awk '{print "   "$0}'
	else
		echo "   (no backups)"
	fi
}

set -euo pipefail
cmd_ti_backup_ls "${@}"
