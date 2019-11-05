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

	"${ti}" "${file}" learner 'show databases' 1>/dev/null
	"${ti}" "${file}" learner 'show databases' 'test' 1>/dev/null
	"${ti}" "${file}" learner 'show databases' 'test' 'true' 1>/dev/null
	"${ti}" "${file}" learner 'show databases' 'test' 'false' 1>/dev/null

	"${ti}" "${file}" 'tpch/create 0.01 region:tpch/load 0.01 region:tpch/load 0.01 all:tpch/tikv'
	# TODO: remote 'sleep' after FLASH-635 is addressed
	"${ti}" "${file}" 'tpch/ch:tpch/db:ch/compaction tpch_0_01 lineitem'
	"${ti}" "${file}" 'verify/rows tpch_0_01.lineitem:tpch/rows'
	"${ti}" "${file}" 'syncing/test:sleep 3:syncing/show'
	"${ti}" "${file}" 'burn doit:up:tpch/load_no_raft 0.01 lineitem tmt'
	"${ti}" "${file}" 'verify/balance tpch_0_01_tmt_no_raft.lineitem:fstop'

	"${ti}" "${file}" must burn doit
	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_ci_cluster "${@}"
