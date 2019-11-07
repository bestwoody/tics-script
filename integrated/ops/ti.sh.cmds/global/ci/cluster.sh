#!/bin/bash

function cmd_ti_ci_cluster()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/cluster'
	mkdir -p "${dir}"
	local file="${dir}/cluster.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-6' "dir=${dir}"
	"${ti}" "${file}" 'burn doit:up:sleep 5'
	"${ti}" "${file}" ci/cluster
	"${ti}" "${file}" must burn doit

	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_ci_cluster
