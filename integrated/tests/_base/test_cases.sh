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

	rm -f "${report}.tmp"

	if [ -f "${entry_dir}/queries.data" ]; then
		echo "<tpch ${scale} loading time>" >> "${report}.tmp"
		to_table "${title}" 'rows:; cols:table|notag; cell:limit(20)|avg|~|cnt|duration' 9999 "${entry_dir}/load.data" >> "${report}.tmp"
	fi

	if [ -f "${entry_dir}/queries.data" ]; then
		echo "<tpch ${scale} execute time>" >> "${report}.tmp"
		to_table "${title}" 'cols:cat,round; rows:query; cell:limit(20)|avg|~|cnt|duration' 9999 "${entry_dir}/queries.data" >> "${report}.tmp"
	fi

	if [ -f "${report}.tmp" ]; then
		mv -f "${report}.tmp" "${report}"
	fi
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

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" '' "${test_ti_file}"
	local vers=`test_cluster_vers "${test_ti_file}"`
	if [ -z "${vers}" ]; then
		echo "[func tpch_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tpch_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	test_cluster_load_tpch_data "${test_ti_file}" "${scale}" "${entry_dir}/load.data" "${vers}" | while read line; do
		tpch_perf_report "${test_entry_file}" "${scale}"
		echo "${line}"
	done
	echo "[func tpch_perf] load tpch data to test cluster done"

	echo '---'

	# TODO: remove this
	"${integrated}/ops/ti.sh" "${test_ti_file}" "ch" "DBGInvoke refresh_schemas()"

	for ((r = 0; r < 3; ++r)); do
		for ((i = 1; i < 23; ++i)); do
			test_cluster_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "round:${r},${vers}" "${i}"
			tpch_perf_report "${test_entry_file}" "${scale}"
			test_cluster_spark_run_tpch "${test_ti_file}" "${scale}" "${entry_dir}" "round:${r},${vers}" "${i}"
			tpch_perf_report "${test_entry_file}" "${scale}"
		done
		sleep_by_scale "${scale}"
	done

	tpch_perf_report "${test_entry_file}" "${scale}"

	"${integrated}/ops/ti.sh" "${test_ti_file}" "burn" "doit"
	echo '[func tpch_perf] done'
}
export -f tpch_perf

function tidb_tpcc_perf_report()
{
	if [ -z "${1+x}" ]; then
		echo "[func tidb_tpcc_perf_report] usage: <func> test_entry_file" >&2
		return 1
	fi

	local test_entry_file="${1}"

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"

	local report="${entry_dir}/report"
	local title='<tpcc performance test with/without tiflash>'

	rm -f "${report}.tmp"

	if [ -f "${entry_dir}/results.data" ]; then
		to_table "${title}" 'cols:test; rows:type|notag; cell:limit(20)|avg|~' 9999 "${entry_dir}/results.data" > "${report}.tmp"
	fi

	if [ -f "${report}.tmp" ]; then
		mv -f "${report}.tmp" "${report}"
	fi
}
export -f tidb_tpcc_perf_report

function tidb_tpcc_perf()
{
	if [ -z "${2+x}" ]; then
		echo "[func tidb_tpcc_perf] usage: <func> test_entry_file ports [warehouses] [minutes]" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"

	if [ ! -z "${3+x}" ]; then
		local warehouses="${3}"
	else
		local warehouses='1'
	fi

	if [ ! -z "${4+x}" ]; then
		local minutes="${4}"
	else
		local minutes='1'
	fi

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"
	local report="${entry_dir}/report"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" 'pd,tikv,tidb' "${test_ti_file}"
	local vers=`test_cluster_vers "${test_ti_file}"`
	if [ -z "${vers}" ]; then
		echo "[func tidb_tpcc_perf] test cluster prepare failed" >&2
		return 1
	else
		echo "[func tidb_tpcc_perf] test cluster prepared: ${vers}"
	fi

	echo '---'

	local benchmark_repo="${entry_dir}/benchmarksql"
	if [ ! -d "${benchmark_repo}" ]; then
		temp_benchmark_repo="${benchmark_repo}.`date +%s`.${RANDOM}"
		mkdir -p "${temp_benchmark_repo}"
		git clone -b 5.0-mysql-support-opt-2.1 https://github.com/pingcap/benchmarksql.git "${temp_benchmark_repo}"
		(
			cd "${temp_benchmark_repo}" && ant
		)
		wait
		mv "${temp_benchmark_repo}" "${benchmark_repo}"
	fi
	local benchmark_dir="${benchmark_repo}/run"

	load_tpcc_data "${benchmark_dir}" "${test_ti_file}" "${warehouses}" "${minutes}"
	test_cluster_run_tpcc "tidb_only" "${benchmark_dir}" "${entry_dir}" "${vers}"
	"${integrated}/ops/ti.sh" -m 'pd,tikv,tidb,tiflash,rngine' "${test_ti_file}" "run"
	test_cluster_run_tpcc "with_tiflash" "${benchmark_dir}" "${entry_dir}" "${vers}"
	tidb_tpcc_perf_report "${test_entry_file}"

	"${integrated}/ops/ti.sh" -m 'pd,tikv,tidb,tiflash,rngine' "${test_ti_file}" "burn" "doit"
	echo '[func tidb_tpcc_perf] done'
}
export -f tidb_tpcc_perf
