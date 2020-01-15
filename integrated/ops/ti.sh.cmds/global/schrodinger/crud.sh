#!/bin/bash

function cmd_ti_global_schrodinger_crud()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		local dir='/tmp/ti/schrodinger/crud'
	else
		local dir="${1}"
	fi
	local ti="${integrated}/ops/ti.sh"

	mkdir -p "${dir}"
	local file="${dir}/crud.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=+14' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" schrodinger/crud

	print_hhr
	echo 'schrodinger/crud FINISHED'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_schrodinger_crud
