#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1+spark_x.ti"
args="ports=+33#dir=nodes/33"

entry_dir="${BASH_SOURCE[0]}.data"
mkdir "${entry_dir}"

data="${entry_dir}/data"
report="${entry_dir}/report"
title='<tpcc performance test with/without tiflash>'

function load_tpcc_data() {
    local benchmark_dir="${1}"
	local tidb_host=`${ti} -k "${args}" -m tidb "${ti_file}" mysql_host`
	local tidb_port=`${ti} -k "${args}" -m tidb "${ti_file}" mysql_port`

	local prop_file="${benchmark_dir}/props.mysql"

	"${ti}" -k "${args}" "${ti_file}" "mysql" "create database tpcc"
	echo "db=mysql" > "${prop_file}"
	echo "driver=com.mysql.jdbc.Driver" >> "${prop_file}"
	echo "conn=jdbc:mysql://${tidb_host}:${tidb_port}/tpcc?useSSL=false&useServerPrepStmts=true&useConfigs=maxPerformance" >> "${prop_file}"
	echo "user=root" >> "${prop_file}"
	echo "password=" >> "${prop_file}"
	echo "warehouses=1" >> "${prop_file}"
	echo "loadWorkers=4" >> "${prop_file}"
	echo "terminals=1" >> "${prop_file}"
	echo "runTxnsPerTerminal=0" >> "${prop_file}"
	echo "runMins=1" >> "${prop_file}"
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

function run_benchmark_test() {
	local type="${1}"
	local benchmark_dir="${2}"

	local start_time=`date +%s`
	(
	    cd "${benchmark_dir}"
	    ./runBenchmark.sh props.mysql 2>&1 &> "${benchmark_dir}/test.log"
	)
    wait
	local end_time=`date +%s`
    local version="`get_mod_ver "pd" "${ti_file}" "${args}"`,`get_mod_ver "tikv" "${ti_file}" "${args}"`,`get_mod_ver "tidb" "${ti_file}" "${args}"`,`get_mod_ver "tiflash" "${ti_file}" "${args}"`,`get_mod_ver "rngine" "${ti_file}" "${args}"`"
	local tags="type:${type},test:tpcc,start_ts:${start_time},end_ts:${end_time},${version}"
	local result=`grep "Measured tpmC" "${benchmark_dir}/test.log" | awk -F '=' '{print $2}' | tr -d ' ' | awk -F '.' '{print $1}'`
	echo "${result} ${tags}"
}

function run_tpcc_test() {
	"${ti}" -k "${args}" "${ti_file}" burn doit

    local benchmark_repo="${here}/benchmarksql"
	if [ ! -d "${benchmark_repo}" ]; then
		mkdir -p "${benchmark_repo}"
		git clone -b 5.0-mysql-support-opt-2.1 https://github.com/pingcap/benchmarksql.git "${benchmark_dir}"
		(
		    cd "${benchmark_repo}" && ant
		)
		wait
	fi
	local benchmark_dir="${benchmark_repo}/run"

	"${ti}" -k "${args}" -m pd,tikv,tidb "${ti_file}" up
	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l | awk '{print $1}'`
	if [ "${ok}" != '3' ]; then
		echo "${status}" >&2
		"${ti}" -k "${args}" "${ti_file}" burn doit
		return 1
	fi
	local error_handle="$-"
	set +e
	local timeout=100
	for ((i=0; i<${timeout}; i++)); do
		"${ti}" -k "${args}" "${ti_file}" "mysql" "show databases" 1>/dev/null 2>&1
		if [ "$?" == "0" ]; then
			break
		else
			if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
				echo "#${i} waiting for tidb ready"
			fi
			sleep 1
		fi
	done
	restore_error_handl_flags

	load_tpcc_data "${benchmark_dir}"
	echo `run_benchmark_test "tidb_only" "${benchmark_dir}"` >> "${data}"

	"${ti}" -k "${args}" -m pd,tikv,tidb,tiflash,rngine "${ti_file}" up
	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l | awk '{print $1}'`
	if [ "${ok}" != '5' ]; then
		echo "${status}" >&2
		"${ti}" -k "${args}" "${ti_file}" burn doit
		return 1
	fi

	echo `run_benchmark_test "with_tiflash" "${benchmark_dir}"` >> "${data}"

	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l | awk '{print $1}'`
	if [ "${ok}" != '5' ]; then
		echo "${status}" >&2
	else
		to_table "${title}" 'cols:type; rows:test; cell:limit(20)|avg' 9999 "${data}" > "${report}.tmp"
		mv -f "${report}.tmp" "${report}"
	fi

	"${ti}" -k "${args}" "${ti_file}" burn doit
	echo 'done'
}

run_tpcc_test
