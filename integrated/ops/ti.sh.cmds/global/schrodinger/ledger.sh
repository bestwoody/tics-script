#!/bin/bash

function cmd_ti_global_schrodinger_ledger()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local dir='/tmp/ti/schrodinger/ledger'
	else
		local dir="${1}"
	fi
	local ti="${integrated}/ops/ti.sh"

	mkdir -p "${dir}"
	local file="${dir}/ledger.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=+16' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" schrodinger/ledger

	print_hhr
	echo 'schrodinger/ledger FINISHED'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_schrodinger_ledger
