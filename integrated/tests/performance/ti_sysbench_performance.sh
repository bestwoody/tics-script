#!/bin/bash
# sysbench tools: https://github.com/akopytov/sysbench/tree/1.0.14
source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function generate_sysbench_config()
{
	local entry_dir="${1}"
	local test_ti_file="${2}"
	local config_file="${3}"
	local test_time="${4}"
	local threads="${5}"

	local mysql_host=`"${integrated}/ops/ti.sh" "${test_ti_file}" mysql/host`
	local mysql_port=`"${integrated}/ops/ti.sh" "${test_ti_file}" mysql/port`

	echo "mysql-host=${mysql_host}" > "${config_file}"
	echo "mysql-port=${mysql_port}" >> "${config_file}"
	echo "mysql-user=root" >> "${config_file}"
	echo "mysql-db=sbtest" >> "${config_file}"
	echo "time=${test_time}" >> "${config_file}"
	echo "threads=${threads}" >> "${config_file}"
	echo "report-interval=10" >> "${config_file}"
	echo "db-driver=mysql" >> "${config_file}"
}

function run_sysbench()
{
	local type="${1}"
	local tags="${2}"
	local entry_dir="${3}"
	local table_num="${4}"
	local table_size="${5}"
	sysbench --config-file="${sysbench_config_file}" oltp_point_select --tables=${table_num} --table-size=${table_size} run > "${entry_dir}/result"
	local transactionPerSecond=`cat "${entry_dir}/result" | { grep "transactions:" || test $? = 1; } | awk -F "(" '{print $2}' | awk -F " " '{print $1}' | awk -F "." '{print $1}'`
	local queryPerSecond=`cat "${entry_dir}/result" | { grep "queries:" || test $? = 1; } | awk -F "(" '{print $2}' | awk -F " " '{print $1}' | awk -F "." '{print $1}'`
	local avgLatency=`cat "${entry_dir}/result" | { grep "avg:" || test $? = 1; } | awk -F " " '{print $2}'`
	local maxLatency=`cat "${entry_dir}/result" | { grep "max:" || test $? = 1; } | awk -F " " '{print $2}'`
	local tags="type:${type},test:sysbench,queryPerSecond:${queryPerSecond},avgLatency:${avgLatency},maxLatency:${maxLatency},${tags}"
	local result="${transactionPerSecond}"
	echo "${result} ${tags}" >> "${entry_dir}/results.data"
}

function sysbench_perf_report()
{
	local entry_dir="${1}"

	local report="${entry_dir}/report"
	local title='<sysbench performance test>'

	rm -f "${report}.tmp"

	if [ -f "${entry_dir}/results.data" ]; then
		to_table "${title}" 'cols:test; rows:type|notag; cell:limit(20)|avg|~' 9999 "${entry_dir}/results.data" > "${report}.tmp"
	fi

	if [ -f "${report}.tmp" ]; then
		mv -f "${report}.tmp" "${report}"
	fi
}

function sysbench_test()
{
	if [ -z "${6+x}" ]; then
		echo "[func sysbench_test] usage: <func> test_entry_file ports table_num table_size test_time thread_num" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"
	local table_num="${3}"
	local table_size="${4}"
	local test_time="${5}"
	local thread_num="${6}"

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" "pd,tikv,tidb" "${test_ti_file}"
	local vers=`test_cluster_vers "${test_ti_file}"`

	local ti="${integrated}/ops/ti.sh"
	local sysbench_config_file="${entry_dir}/config"
	generate_sysbench_config "${entry_dir}" "${test_ti_file}" "${sysbench_config_file}" "${test_time}" "${thread_num}"

	"${ti}" "${test_ti_file}" "mysql" "create database sbtest;"
	sysbench --config-file="${sysbench_config_file}" oltp_point_select --tables=${table_num} --table-size=${table_size} prepare
	for ((i=1; i<=${table_num}; i++)); do
		"${ti}" "${test_ti_file}" "mysql" "SELECT COUNT(pad) FROM sbtest${i} USE INDEX (k_${i});" "sbtest"
		"${ti}" "${test_ti_file}" "mysql" "ANALYZE TABLE sbtest${i};" "sbtest"
	done
	run_sysbench "tidb_only" "${vers}" "${entry_dir}" "${table_num}" "${table_size}"
	"${ti}" -m "tiflash,rngine" "${test_ti_file}" "run"
	run_sysbench "with_tiflash" "${vers}" "${entry_dir}" "${table_num}" "${table_size}"
	sysbench_perf_report "${entry_dir}"
}

sysbench_test "${BASH_SOURCE[0]}" "102" "32" "100000" "300" "8"
