#!/bin/bash

function cmd_ti_global_schrodinger_sqllogic()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local dir='/tmp/ti/schrodinger/sqllogic'
	else
		local dir="${1}"
	fi
	local ti="${integrated}/ops/ti.sh"

	mkdir -p "${dir}"
	local file="${dir}/sqllogic.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=+18' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" schrodinger/sqllogic

	print_hhr
	echo 'schrodinger/sqllogic FINISHED'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_schrodinger_sqllogic
