#!/bin/bash

function cmd_ti_global_ci_release()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/release'
	mkdir -p "${dir}"
	local file="${dir}/release.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-8' "dir=${dir}"
	"${ti}" "${file}" must 'burn doit:up:sleep 5'
	"${ti}" "${file}" ci/release
	"${ti}" "${file}" must burn doit

	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_global_ci_release
