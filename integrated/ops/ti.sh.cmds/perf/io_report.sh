#!/bin/bash

function cmd_ti_perf_io_report()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"
	shift 5

	local sec='10'
	if [ ! -z "${1+x}" ]; then
		local sec="${1}"
	fi

	local size='1000m'
	if [ ! -z "${2+x}" ]; then
		local size="${2}"
	fi

	local threads='8'
	if [ ! -z "${3+x}" ]; then
		local threads="${3}"
	fi

	mkdir -p "${dir}/tmp"
	local file="${dir}/tmp/fio.tmp"

	local log="${file}.log"
	rm -f "${log}"

	echo "=> ${mod_name} #${index} ${dir}"
	fio_report "${file}" "${log}" "${sec}" "${size}" "${threads}" | awk '{print "   "$0}'
}

set -euo pipefail
cmd_ti_perf_io_report "${@}"
