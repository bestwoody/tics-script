#!/bin/bash

function cmd_ti_global_ci_release()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/release'
	mkdir -p "${dir}"
	local file="${dir}/release.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-8' 'tikv=2' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up
	"${ti}" "${file}" ci/release
	## randgen-mpp only runs daily due to heavy workloads
	"${ti}" "${file}" ci/fullstack randgen-mpp false
	## change parameters to run the test again
	## force shuffle hash join rather than broadcast join
	"${ti}" "${file}" mysql "set @@global.tidb_broadcast_join_threshold_size=0; set @@global.tidb_broadcast_join_threshold_count=0;" test false
	"${ti}" "${file}" ci/fullstack randgen-mpp false
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/release OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_ci_release
