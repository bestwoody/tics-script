#!/bin/bash

function cmd_ti_ci_cluster()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/cluster'
	mkdir -p "${dir}"
	local file="${dir}/cluster.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-6' "dir=${dir}" 1>/dev/null
	"${ti}" "${file}" 'burn:up:sleep 5'
	"${ti}" "${file}" ci/cluster
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/cluster OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_ci_cluster
