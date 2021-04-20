#!/bin/bash

function cmd_ti_ci_fullstack()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/fullstack'
	mkdir -p "${dir}"
	local file="${dir}/fullstack.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-10' 'tiflash=3' "dir=${dir}" 1>/dev/null
	"${ti}" "${file}" must burn
	"${ti}" "${file}" run
	"${ti}" "${file}" ci/fullstack ci
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/fullstack OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_ci_fullstack
