#!/bin/bash

function cmd_ti_global_schrodinger_bank()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local dir='/tmp/ti/schrodinger/bank'
	else
		local dir="${1}"
	fi
	local ti="${integrated}/ops/ti.sh"

	mkdir -p "${dir}"
	local file="${dir}/bank.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=+10' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" schrodinger/bank

	print_hhr
	echo 'schrodinger/bank FINISHED'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_schrodinger_bank
