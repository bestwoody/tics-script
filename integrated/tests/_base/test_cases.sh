#!/bin/bash

function tpch_perf_report()
{
	if [ -z "${2+x}" ]; then
		echo "[func tpch_perf_report] usage: <func> test_entry_file scale" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local scale="${2}"

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"

	local report="${entry_dir}/report"
	local title='            '

	echo "<tpch ${scale} loading time>" > "${report}.tmp"
	to_table "${title}" 'cols:; rows:table|notag; cell:limit(20)|avg|~|duration' 9999 "${entry_dir}/load.data" >> "${report}.tmp"

	echo '' >> "${report}.tmp"
	echo "<tpch ${scale} execute time>" >> "${report}.tmp"
	to_table "${title}" 'cols:cat,round; rows:query; cell:limit(20)|avg|~|cnt|duration' 9999 "${entry_dir}/queries.data" >> "${report}.tmp"

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

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"
	local report="${entry_dir}/report"

	test_cluster_prepare "${ports}" ''
	local vers=`test_cluster_vers "${ports}"`
	if [ -z "${vers}" ]; then
		echo "[func tpch_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tpch_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	test_cluster_load_tpch_data "${ports}" "${scale}" "${entry_dir}/load.data" "${vers}"
	echo "[func tpch_perf] load tpch data to test cluster done"

	echo '---'

	# TODO: remove this
	test_cluster_cmd "${ports}" '' ch "DBGInvoke refresh_schemas()"

	for ((r = 0; r < 3; ++r)); do
		echo "=> tidb, round ${r}"
		test_cluster_run_tpch "${ports}" "${scale}" "${entry_dir}" "round:${r},${vers}"
		# TODO: calculate sleep-sec with scale
		sleep 16
	done

	# TODO: BUG: no data
	#for ((r = 0; r < 3; ++r)); do
	#	echo "=> spark, round ${r}"
	#	test_cluster_spark_run_tpch "${ports}" "${scale}" "${entry_dir}" "round:${r},${vers}"
		# TODO: calculate sleep-sec with scale
	#	sleep 16
	#done

	tpch_perf_report "${test_entry_file}" "${scale}"

	test_cluster_burn "${ports}"
	echo '[func tpch_perf] done'
}
export -f tpch_perf
