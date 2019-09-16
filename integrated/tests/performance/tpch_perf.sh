#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function tpch_perf()
{
	if [ -z "${1+x}" ]; then
		echo "[func tpch_perf] usage: <func> ports [scale]" >&2
		return 1
	fi

	local ports="${1}"

	if [ ! -z "${2+x}" ]; then
		local scale="${2}"
	else
		local scale='1'
	fi

	local title='<tpch perf>'
	local data="${BASH_SOURCE[0]}.data"
	local report="${BASH_SOURCE[0]}.report"

	test_cluster_prepare "${ports}" ''
	local vers=`test_cluster_vers "${ports}"`
	if [ -z "${vers}" ]; then
		echo "[func tpch_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tpch_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	test_cluster_load_tpch_data "${ports}" "${scale}" "${data}.load" "${vers}"
	echo "[func tpch_perf] load tpch data to test cluster done"

	to_table "${title}" 'cols:tiflash_ver|notag; rows:table; cell:limit(20)|avg|~|duration' 9999 "${data}.load" > "${report}.tmp"

	echo '---'

	test_cluster_run_tpch "${ports}" "${scale}" "${data}.queries" "${vers}"

	echo '---' >> "${report}.tmp"
	to_table "${title}" 'cols:tiflash_ver|notag; rows:query; cell:limit(20)|avg|~|duration' 9999 "${data}.queries" >> "${report}.tmp"

	mv -f "${report}.tmp" "${report}"

	test_cluster_burn "${ports}"
	echo '[func tpch_perf] done'
}

tpch_perf 46
