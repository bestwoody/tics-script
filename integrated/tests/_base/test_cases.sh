#!/bin/bash

function tpch_perf_report()
{
	if [ -z "${2+x}" ]; then
		echo "[func tpch_perf_report] usage: <func> test_entry_file scale" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local scale="${2}"

	local report="${test_entry_file}.report"
	local title='            '

	echo "<tpch ${scale} loading time>" > "${report}.tmp"
	to_table "${title}" 'cols:; rows:table|notag; cell:limit(20)|avg|~|duration' 9999 "${test_entry_file}.load.data" >> "${report}.tmp"

	echo '' >> "${report}.tmp"
	echo "<tpch ${scale} execute time>" >> "${report}.tmp"
	to_table "${title}" 'cols:cat|notag; rows:query; cell:limit(20)|avg|~|cnt|duration' 9999 "${test_entry_file}.queries.data" >> "${report}.tmp"

	mv -f "${report}.tmp" "${report}"
}
export -f tpch_perf_report

function tpch_perf()
{
	if [ -z "${2+x}" ]; then
		echo "[func tpch_perf] usage: <func> test_entry_file ports [scale]" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"

	if [ ! -z "${3+x}" ]; then
		local scale="${3}"
	else
		local scale='1'
	fi

	local report="${test_entry_file}.report"

	test_cluster_prepare "${ports}" ''
	local vers=`test_cluster_vers "${ports}"`
	if [ -z "${vers}" ]; then
		echo "[func tpch_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tpch_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	test_cluster_load_tpch_data "${ports}" "${scale}" "${test_entry_file}.load.data" "${vers}"
	echo "[func tpch_perf] load tpch data to test cluster done"

	echo '---'

	test_cluster_run_tpch "${ports}" "${scale}" "${test_entry_file}.queries.data" "${vers}"

	tpch_perf_report "${test_entry_file}" "${scale}"

	test_cluster_burn "${ports}"
	echo '[func tpch_perf] done'
}
export -f tpch_perf
