#!/bin/bash

# TODO test: loop, floop
# TODO need sudo to test: perf/drop_page_cache, perf/hw
function cmd_ti_ci_self()
{
	local ti="${integrated}/ops/ti.sh"

	"${ti}" help 1>/dev/null
	"${ti}" help 'stop' 1>/dev/null
	"${ti}" flags 1>/dev/null
	"${ti}" example 1>/dev/null
	"${ti}" procs

	local dir='/tmp/ti/ci/self'
	mkdir -p "${dir}"

	local file="${dir}/self.ti"
	rm -f "${file}"
	"${ti}" new "${file}" 'delta=-4' "dir=${dir}" 'spark=1'

	"${ti}" "${file}" 'fstop:stop:up:status:prop:du:log 1:log 1 info'

	"${ti}" "${file}" pd/ctl --help 1>/dev/null
	"${ti}" "${file}" tikv/ctl true --help 1>/dev/null
	#"${ti}" "${file}" perf/fio --help 1>/dev/null

	"${ti}" "${file}" ch "show databases" 1>/dev/null
	"${ti}" "${file}" ch "show databases" 'default' 1>/dev/null
	"${ti}" "${file}" ch "show databases" 'default' 'tab' 1>/dev/null
	"${ti}" "${file}" ch "show databases" 'default' 'title' 1>/dev/null
	"${ti}" "${file}" ch "show databases" 'default' 'pretty' 1>/dev/null
	"${ti}" "${file}" ch 'show databases' 'default' 'pretty' 'true' 1>/dev/null
	"${ti}" "${file}" ch 'show databases' 'default' 'pretty' 'false' 1>/dev/null

	"${ti}" "${file}" mysql 'show databases' 1>/dev/null
	"${ti}" "${file}" mysql 'show databases' 'test' 1>/dev/null
	"${ti}" "${file}" mysql 'show databases' 'test' 'true' 1>/dev/null
	"${ti}" "${file}" mysql 'show databases' 'test' 'false' 1>/dev/null
	"${ti}" "${file}" mysql/host:mysql/port

	local res=`"${ti}" "${file}" beeline default -e 'show databases' | grep test`
	test ! -z "${res}"

	"${ti}" "${file}" 'ver:ver ver:ver githash:ver full:sleep 1'
	"${ti}" "${file}" 'perf/top:perf/top true:perf/top false'
	#"${ti}" -m tiflash "${file}" 'perf/flame:perf/flame 5:perf/io_report'

	"${ti}" "${file}" repeat 2 status
	# TODO install ssh and test:
	#"${ti}" "${file}" 'byhost/do uptime:byhost/local/do date:byhost/procs'
	"${ti}" "${file}" 'byhost/do uptime:byhost/procs'
	"${ti}" "${file}" must reset:restart
	"${ti}" "${file}" must 'burn:burn doit'

	rm -f "${file}"
	echo '------------'
	echo 'OK'
}

set -euo pipefail
cmd_ti_ci_self "${@}"
