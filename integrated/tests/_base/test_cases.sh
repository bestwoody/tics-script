#!/bin/bash

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

	echo "<tpch ${scale} loading time>" > "${report}.tmp"
	to_table '' 'cols:|notag; rows:table|notag; cell:limit(20)|avg|~|duration' 9999 "${test_entry_file}.load.data" >> "${report}.tmp"

	echo '---'

	test_cluster_run_tpch "${ports}" "${scale}" "${test_entry_file}.queries.data" "${vers}"

	echo '---' >> "${report}.tmp"
	to_table "<tpch ${scale}>" 'cols:cat|notag; rows:query; cell:limit(20)|avg|~|duration' 9999 "${test_entry_file}.queries.data" >> "${report}.tmp"

	mv -f "${report}.tmp" "${report}"

	test_cluster_burn "${ports}"
	echo '[func tpch_perf] done'
}
