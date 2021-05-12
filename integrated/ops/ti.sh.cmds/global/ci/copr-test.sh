#!/bin/bash

function cmd_ti_global_ci_copr_test()
{
	local ti="${integrated}/ops/ti.sh"

	local dir='/tmp/ti/ci/copr-test'
	mkdir -p "${dir}"
	local file="${dir}/copr-test.ti"
	rm -f "${file}"

	"${ti}" new "${file}" 'delta=-33' 'tikv=3' 'tiflash=3' "dir=${dir}"
	"${ti}" "${file}" burn : up
	"${ti}" "${file}" ci/copr-test
	"${ti}" "${file}" must burn

	rm -f "${file}"
	print_hhr
	echo 'ci/copr-test OK'
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_global_ci_copr_test
