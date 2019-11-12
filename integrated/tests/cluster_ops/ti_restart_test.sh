#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function restart_tiflash()
{
	if [ -z "${2+x}" ]; then
		echo "[func restart_tiflash] usage: <func> test_ti_file entry_dir [interval_between_stop_and_start]"  >&2
		return 1
	fi

	local test_ti_file="${1}"
	local entry_dir="${2}"

	if [ ! -z "${3+x}" ]; then
		local interval="${3}"
	else
		local interval="10"
	fi

	local ti="${integrated}/ops/ti.sh"

	"${ti}" -m "tiflash" "${test_ti_file}" "fstop"
	sleep ${interval}
	"${ti}" -m "tiflash,rngine" "${test_ti_file}" "run"
	sleep $((5 + (${RANDOM} % 5)))
	local status=`"${ti}" "${test_ti_file}" "status"`
	local ok_cnt=`echo "${status}" | { grep 'OK' || test $? = 1; } | wc -l | awk '{print $1}'`
	if [ "${ok_cnt}" != "5" ]; then
		echo "${status}" >&2
		echo `date +"%Y-%m-%d %H:%m:%S"` >&2
		return 1
	fi
}

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
	echo "load data for ${db}.${table} complete"
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

	local tikv_result=`"${ti}" "${test_ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
	local tiflash_result=`"${ti}" "${test_ti_file}" "mysql" "select /*+ read_from_storage(tiflash[${table}]) */ count(*) from ${table}" "${db}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`

	if [ "${tikv_result}" != "${tiflash_result}" ]; then
		echo `date +"%Y-%m-%d %H:%m:%S"` >&2
		echo "tikv_result" "${tikv_result}" >&2
		echo "tiflash_result" "${tiflash_result}" >&2
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
	local ti="${integrated}/ops/ti.sh"
	
	"${ti}" "${test_ti_file}" 'pd/ctl_raw' "config set learner-schedule-limit 256"
	"${ti}" -l "${test_ti_file}" "tpch/load" "${load_tpch_scale}" "${table}" &

	for (( i = 0; i < "${restart_times}"; i++ )); do
		sleep ${restart_interval}
		restart_tiflash "${test_ti_file}" "${entry_dir}"
	done

	wait_load_data_ready "${test_ti_file}" `_db_name_from_scale "${load_tpch_scale}"` "${table}"
	check_tiflash_and_tidb_result_consistency "${test_ti_file}" `_db_name_from_scale "${load_tpch_scale}"` "${table}" "${entry_dir}"
}

restart_tiflash_test "${BASH_SOURCE[0]}" 32 10 15 120
