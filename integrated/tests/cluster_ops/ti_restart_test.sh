#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function wait_load_data_ready()
{
	if [ -z "${3+x}" ]; then
		echo "[func wait_load_data_ready] usage: <func> test_ti_file db table" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local db="${2}"
	local table="${3}"

	local prev_mysql_result=""
	while true; do
		local mysql_result=`"${integrated}/ops/ti.sh" "${test_ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
		if [ ! -z "${prev_mysql_result}" ] && [ "${mysql_result}" == "${prev_mysql_result}" ]; then
			break
		fi
		local prev_mysql_result="${mysql_result}"
		sleep $((10 + (${RANDOM} % 5)))
	done
}

function check_tiflash_and_tidb_result_consistency()
{
	if [ -z "${4+x}" ]; then
		echo "[func check_tiflash_and_tidb_result_consistency] usage: <func> test_ti_file db table entry_dir" >&2
		return 1
	fi

	local test_ti_file="${1}"
	local db="${2}"
	local table="${3}"
	local entry_dir="${4}"

	local ti="${integrated}/ops/ti.sh"

	"${ti}" -m "spark_m,spark_w" "${test_ti_file}" "run"
	local mysql_result=`"${ti}" "${test_ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
	for (( i=0; i<100; i++ )); do
		local beeline_raw_result=`"${ti}" "${test_ti_file}" "beeline" "" -e "select count(*) from ${db}.${table}" 2>&1`
		local beeline_result=`echo "${beeline_raw_result}" | grep -A 2 "count" | tail -n 1 | awk -F '|' '{ print $2 }' | tr -d ' '`
		if [ "${beeline_result}" != "" ]; then
			break
		fi
		sleep 1
	done

	if [ "${mysql_result}" != "${beeline_result}" ]; then
		echo `date +"%Y-%m-%d %H:%m:%S"` >&2
		echo "mysql_result" "${mysql_result}" >&2
		echo "beeline_result" "${beeline_result}" >&2
		echo "beeline_raw_result" "${beeline_raw_result}" >&2
		return 1
	fi
}

function restart_tiflash_test()
{
	if [ -z "${2+x}" ]; then
		echo "[func restart_tiflash_test] usage: <func> test_entry_file ports [load_tpch_scale] [restart_times] [restart_interval]" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"

	if [ ! -z "${3+x}" ]; then
		local load_tpch_scale="${3}"
	else
		local load_tpch_scale='1'
	fi

	if [ ! -z "${4+x}" ]; then
		local restart_times="${4}"
	else
		local restart_times='1'
	fi

	if [ ! -z "${5+x}" ]; then
		local restart_interval="${5}"
	else
		local restart_interval='120'
	fi

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_prepare "${ports}" "pd,tikv,tidb,tiflash,rngine" "${test_ti_file}"

	echo '---'

	local table="lineitem"

	"${integrated}/ops/ti.sh" -l "${test_ti_file}" "tpch/load" "${load_tpch_scale}" "${table}" &

	for i in {1..${restart_times}}; do
		sleep ${restart_interval}
		test_cluster_restart_tiflash "${test_ti_file}" "${entry_dir}"
	done

	wait_load_data_ready "${test_ti_file}" `_db_name_from_scale "${load_tpch_scale}"` "${table}"
	check_tiflash_and_tidb_result_consistency "${test_ti_file}" `_db_name_from_scale "${load_tpch_scale}"` "${table}" "${entry_dir}"
}

function test_main()
{
	local test_entry_file="${1}"
	restart_tiflash_test "${test_entry_file}" 32 1 2 120
}

test_main "${BASH_SOURCE[0]}"


