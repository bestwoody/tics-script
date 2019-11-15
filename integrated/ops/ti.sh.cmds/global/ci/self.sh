#!/bin/bash

function cmd_ti_global_ci_self()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" 'help:help stop:flags:example:procs'

	# TODO: take too long, or need sudo
	#"${ti}" io_report:hw

	local dir='/tmp/ti/ci/self'
	mkdir -p "${dir}"
	local file="${dir}/self.ti"
	rm -f "${file}"

	# TODO: test with spark
	#"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 'spark=1' 1>/dev/null
	"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 1>/dev/null

	"${ti}" "${file}" must burn
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
