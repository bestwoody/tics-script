#!/bin/bash

function cmd_ci_self()
{
	local ti="${integrated}/ops/ti.sh"

	echo '============'
	echo 'help'
	echo '------------'
	"${ti}" help
	"${ti}" help 'stop'

	echo '============'
	echo 'flags'
	echo '------------'
	"${ti}" flags

	echo '============'
	echo 'example'
	echo '------------'
	"${ti}" example

	echo '============'
	echo 'procs'
	echo '------------'
	"${ti}" procs

	local dir='/tmp/ti/ci/self'
	mkdir -p "${dir}"

	local file="${dir}/self.ti"
	echo '============'
	echo 'new'
	echo '------------'
	rm -f "${file}"
	"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 'spark=1'

	# 'reset' and 'restart' are not tested
	"${ti}" "${file}" fstop:stop:up:status:prop:du:'log 5':ver:'pd_ctl --help':'tikv_ctl true --help':'sleep 1'

	echo '============'
	echo 'ch "show databases"'
	echo '------------'
	"${ti}" "${file}" ch "show databases"
	echo '============'
	echo 'mysql "show databases"'
	echo '------------'
	"${ti}" "${file}" mysql "show databases"
	echo '============'
	echo 'beeline -e "show databases"'
	echo '------------'
	"${ti}" "${file}" beeline -e "show databases"

	# TODO test: loop, floop

	"${ti}" "${file}" must burn doit
	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ci_self "${@}"
