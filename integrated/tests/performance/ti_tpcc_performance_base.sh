#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"

function load_tpcc_data()
{
	local benchmark_dir="${1}"
	local test_ti_file="${2}"
	local warehouses="${3}"
	local minutes="${4}"

	local ti="${integrated}/ops/ti.sh"

	local tidb_host=`"${ti}" "${test_ti_file}" "mysql/host"`
	local tidb_port=`"${ti}" "${test_ti_file}" "mysql/port"`
	local prop_file="${benchmark_dir}/props.mysql"

	"${ti}" "${test_ti_file}" "mysql" "create database tpcc"
	echo "db=mysql" > "${prop_file}"
	echo "driver=com.mysql.jdbc.Driver" >> "${prop_file}"
	echo "conn=jdbc:mysql://${tidb_host}:${tidb_port}/tpcc?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance" >> "${prop_file}"
	echo "user=root" >> "${prop_file}"
	echo "password=" >> "${prop_file}"
	echo "warehouses=${warehouses}" >> "${prop_file}"
	echo "loadWorkers=4" >> "${prop_file}"
	echo "terminals=1" >> "${prop_file}"
	echo "runTxnsPerTerminal=0" >> "${prop_file}"
	echo "runMins=${minutes}" >> "${prop_file}"
	echo "limitTxnsPerMin=0" >> "${prop_file}"
	echo "terminalWarehouseFixed=true" >> "${prop_file}"
	echo "newOrderWeight=45" >> "${prop_file}"
	echo "paymentWeight=43" >> "${prop_file}"
	echo "orderStatusWeight=4" >> "${prop_file}"
	echo "deliveryWeight=4" >> "${prop_file}"
	echo "stockLevelWeight=4" >> "${prop_file}"
	echo "resultDirectory=my_result_%tY-%tm-%td_%tH%tM%tS" >> "${prop_file}"

	(
		cd "${benchmark_dir}"
		./runSQL.sh props.mysql sql.mysql/tableCreates.sql 2>&1 1>/dev/null
		./runSQL.sh props.mysql sql.mysql/indexCreates.sql 2>&1 1>/dev/null
		./runLoader.sh props.mysql 2>&1
	)
	wait
}

function test_cluster_run_tpcc()
{
	local type="${1}"
	local benchmark_dir="${2}"
	local entry_dir="${3}"
	local tags="${4}"

	local start_time=`date +%s`
	(
		cd "${benchmark_dir}"
		./runBenchmark.sh props.mysql 2>&1 1>"./test.log"
	)
	wait
	local end_time=`date +%s`
	local tags="type:${type},test:tpcc,start_ts:${start_time},end_ts:${end_time},${tags}"
	local result=`grep "Measured tpmC" "${benchmark_dir}/test.log" | awk -F '=' '{print $2}' | tr -d ' ' | awk -F '.' '{print $1}'`
	echo "${result} ${tags}" >> "${entry_dir}/results.data"
}

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
	"${integrated}/ops/ti.sh" -m 'pd,tikv,tidb,tiflash' "${test_ti_file}" "run"
	test_cluster_run_tpcc "with_tiflash" "${benchmark_dir}" "${entry_dir}" "${vers}"
	tidb_tpcc_perf_report "${test_entry_file}"

	"${integrated}/ops/ti.sh" -m 'pd,tikv,tidb,tiflash' "${test_ti_file}" "burn" "doit"
	echo '[func tidb_tpcc_perf] done'
}