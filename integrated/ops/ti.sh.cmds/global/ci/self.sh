#!/bin/bash

function cmd_ti_global_ci_self()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" help 1>/dev/null
	"${ti}" help 'stop' 1>/dev/null
	"${ti}" flags 1>/dev/null
	"${ti}" example 1>/dev/null
	"${ti}" procs 1>/dev/null
	# TODO: take too long
	#"${ti}" io_report 1>/dev/null
	# TODO: need sudo
	#"${ti}" hw 1>/dev/null

	local dir='/tmp/ti/ci/self'
	mkdir -p "${dir}"
	local file="${dir}/self.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 'spark=1' 1>/dev/null
	"${ti}" "${file}" 'burn:up:sleep 5'
	"${ti}" "${file}" ci/self
	"${ti}" "${file}" reset:restart
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/self OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_ci_self
