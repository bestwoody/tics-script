#!/bin/bash

function cmd_ti_compaction()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	shift 5

	if [ "${mod_name}" != 'tiflash' ]; then
		return
	fi

	if [ -z "${2+x}" ]; then
		echo "[cmd ch/compaction] usage: <cmd> database table" >&2
		return 1
	fi

	local db_name="${1}"
	local table="${2}"

	echo "=> ${mod_name} #${index} ${dir}"
	if [ ! -d "${dir}" ]; then
		echo "   missed"
		return
	fi

	local config="${dir}/conf/config.toml"
	if [ ! -f "${config}" ]; then
		echo "   error: config file missed"
		return 1
	fi

	local data_path=`grep '<path>' "${config}" | awk -F '>' '{print $2}' | awk -F '<' '{print $1}'`
	local table_path="${dir}/db/data/${table}"
	if [ ! -d "${table_path}" ]; then
		echo "   error: table ${db_name}.${table} table data path missed, \"${table_path}\" should exist"
		return 1
	fi

	local curr_path=`pwd`
	cd "${table_path}"

	local here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
	du -sk * | python "${here}/analyze_compaction.py" | awk '{print "   "$0}'

	cd "${curr_path}"
}

set -euo pipefail
cmd_ti_compaction "${@}"
