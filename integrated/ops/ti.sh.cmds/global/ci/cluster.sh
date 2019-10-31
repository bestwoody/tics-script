#!/bin/bash

function cmd_ci_cluster()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/cluster'
	mkdir -p "${dir}"
	local file="${dir}/cluster.ti"

	"${ti}" new "${file}" 'delta=-6' "dir=${dir}"

	"${ti}" "${file}" 'burn doit:up:tpch/load 0.01 all:tpch/ch:tpch/tikv'

	"${ti}" "${file}" must burn doit

	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ci_cluster "${@}"
