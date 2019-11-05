#!/bin/bash

function cmd_ti_perf_fio()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	shift 5

	local cmd_args=("${@}")
	set -euo pipefail

	mkdir -p "${dir}/tmp"

	echo "=> ${mod_name} #${index} ${dir}"
	fio -filename="${dir}/tmp/fio.tmp" "${cmd_args}" | awk '{print "   "$0}'
}

cmd_ti_perf_fio "${@}"
