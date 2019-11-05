#!/bin/bash

function cmd_ti_ci_release()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" ci/jenkins

	local dir='/tmp/ti/ci/release'
	mkdir -p "${dir}"
	local file="${dir}/release.ti"
	rm -f "${file}"
	"${ti}" new "${file}" 'delta=-8' "dir=${dir}"
	"${ti}" "${file}" must burn doit

	# TODO: remote 'sleep' after FLASH-635 is addressed
	"${ti}" "${file}" repeat 20 'up:sleep 5:tpch/load 0.01 all:tpch/ch:tpch/tikv'
	"${ti}" "${file}" repeat 20 'up:sleep 5:kill/storage:tpch/ch:tpch/tikv'

	"${ti}" "${file}" must burn doit
	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_ci_release
