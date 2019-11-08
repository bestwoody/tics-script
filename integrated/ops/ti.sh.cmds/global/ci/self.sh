#!/bin/bash

function cmd_ti_global_ci_self()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" help 1>/dev/null
	"${ti}" help 'stop' 1>/dev/null
	"${ti}" flags 1>/dev/null
	"${ti}" example 1>/dev/null
	"${ti}" procs

	local dir='/tmp/ti/ci/self'
	mkdir -p "${dir}"
	local file="${dir}/self.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 'spark=1'
	"${ti}" "${file}" 'burn doit:up:sleep 5'
	"${ti}" "${file}" ci/self
	"${ti}" "${file}" reset:restart
	"${ti}" "${file}" must 'burn:burn doit'

	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_global_ci_self
