#!/bin/bash

function cmd_ci_release()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" ci/jenkins

	local dir='/tmp/ti/ci/release'
	mkdir -p "${dir}"
	local file="${dir}/cluster.ti"
	rm -f "${file}"
	"${ti}" new "${file}" 'delta=-6' "dir=${dir}"

	"${ti}" repeat 10 'up:tpch/load 0.1 all:tpch/ch:tpch/tikv'
	# TODO: remote 'sleep' after FLASH-635 is addressed
	"${ti}" repeat 10 'kill/storage:up:sleep 300:tpch/ch:tpch/tikv'

	"${ti}" "${file}" must burn doit
	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ci_release
