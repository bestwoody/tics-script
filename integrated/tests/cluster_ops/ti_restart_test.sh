#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function wait_load_data_ready() 
{
	if [ -z "${3+x}" ]; then
		echo "[func wait_load_data_ready] usage: <func> ports db table" >&2
		return 1
	fi

	local ports="${1}"
	local db="${2}"
	local table="${3}"

	local prev_mysql_result=""
	while true; do
		local mysql_result=`test_cluster_cmd "${ports}" "" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
		if [ ! -z "${prev_mysql_result}" ] && [ "${mysql_result}" == "${prev_mysql_result}" ]; then
			break
		fi
		local prev_mysql_result="${mysql_result}"
		sleep $((5 + (${RANDOM} % 5)))
	done
}

function check_tiflash_and_tidb_result_consistency() 
{
	if [ -z "${4+x}" ]; then
		echo "[func check_tiflash_and_tidb_result_consistency] usage: <func> ports db table entry_dir" >&2
		return 1
	fi

	local ports="${1}"
	local db="${2}"
	local table="${3}"
	local entry_dir="${4}"

	test_cluster_cmd "${ports}" "spark_m,spark_w" "run"
	local mysql_result=`test_cluster_cmd "${ports}" "" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
	for (( i=0; i<100; i++ )); do
		local beeline_result_orig=`test_cluster_cmd "${ports}" "" "beeline" -e "select count(*) from ${db}.${table}" 2>&1`
		local beeline_result=`echo "${beeline_result_orig}" | grep -A 2 "count" | tail -n 1 | awk -F '|' '{ print $2 }' | tr -d ' '`
		if [ "${beeline_result}" != "" ]; then
			break
		fi
		sleep 1
	done

	if [ "${mysql_result}" != "${beeline_result}" ]; then
		local error_time=`date +%s`
		local error_time=`date -d "1970-01-01 ${error_time} seconds" +"%Y_%m_%d_%H_%m_%S"`
		echo "mysql_result" ${mysql_result} >&2
		echo "beeline_result" ${beeline_result_orig} >&2
		echo "${error_time}" >&2
		local failed_test_envs="${entry_dir}/failed_test_envs"
		mkdir -p "${failed_test_envs}"
		test_cluster_cmd "${ports}" "" "down"
		local cluster_dir=`test_cluster_args | awk -F '#' '{ print $2 }' | awk -F '=' '{ print $2 }'`
		tar -czPf "${failed_test_envs}/${error_time}_env.tar.gz" "${cluster_dir}"
		test_cluster_cmd "${ports}" "pd,tikv,tidb,tiflash,rngine" log 100000 > "${failed_test_envs}/${error_time}.log"
		return 1
	else
		echo "mysql_result" ${mysql_result}
		echo "beeline_result" ${beeline_result}
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

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"

	test_cluster_prepare "${ports}" "pd,tikv,tidb,tiflash,rngine" "${entry_dir}/test.ti"
	local vers=`test_cluster_vers "${ports}"`
	if [ -z "${vers}" ]; then
		echo "[func restart_tiflash_test] test cluster prepare failed" >&2
		return 1
	else
		echo "[func restart_tiflash_test] test cluster prepared: ${vers}"
	fi

	echo '---'

	local table="lineitem"
	local blocks="4"
	local data_dir="${entry_dir}/load_data"

	_test_cluster_gen_and_load_tpch_table "${ports}" "${table}" "${load_tpch_scale}" "${blocks}" "${data_dir}" 1>/dev/null &

	for i in {1..${restart_times}}; do
		sleep ${restart_interval}
		test_cluster_restart_tiflash "${ports}" "${entry_dir}"
	done
	wait_load_data_ready "${ports}" `_db_name_from_scale ${load_tpch_scale}` "${table}"

	local error_handle="$-"
	set +e
	check_tiflash_and_tidb_result_consistency "${ports}" `_db_name_from_scale ${load_tpch_scale}` "${table}" "${entry_dir}"
	if [ "${?}" != 0 ]; then
		test_cluster_burn "${ports}"
		return 1
	fi
	restore_error_handle_flags "${error_handle}"
	test_cluster_burn "${ports}"
	echo 'done'
}

restart_tiflash_test "${BASH_SOURCE[0]}" 32 1 2 120
