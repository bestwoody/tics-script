#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

function remove_store()
{
	local index="${1}"
	local test_ti_file="${2}" 
	local ti="${integrated}/ops/ti.sh"
	local prop=`"${ti}" -i "${index}" -m "rngine" "${test_ti_file}" 'prop'`
	local host=`echo "${prop}" | grep "listen_host" | awk -F 'listen_host:' '{print $2}' | tr -d ' '`
	local port=`echo "${prop}" | grep "rngine_port" | awk -F 'rngine_port:' '{print $2}' | tr -d ' '`
	echo "remove store ${host}:${port}"
	"${ti}" -i "${index}" -m "tiflash,rngine" "${test_ti_file}" 'stop'
	test_cluster_remove_store "${test_ti_file}" "${host}" "${port}"
}

function add_store()
{
	local index="${1}"
	local test_ti_file="${2}" 
	local ti="${integrated}/ops/ti.sh"
	"${ti}" -i "${index}" -m "tiflash,rngine" "${test_ti_file}" 'burn' 'doit'
	"${ti}" -i "${index}" -m "tiflash,rngine" "${test_ti_file}" 'run'
}

function wait_learner_count_to_target()
{
	local target="${1}"
	local test_ti_file="${2}"
	local direction="${3}"
	while true; do
		local learner_count=`test_cluster_get_learner_store_count "${test_ti_file}"`
		echo "wait learner count to target: ${target}"
		echo "current learner count: ${learner_count}"
		if [ "${learner_count}" -eq "${target}" ]; then
			break
		fi
		if [ "${direction}" == "down" ] && [ "${learner_count}" -lt "${target}" ]; then
			echo "learner less than expected num" >&2
			return 1
		fi
		if [ "${direction}" == "up" ] && [ "${learner_count}" -gt "${target}" ]; then
			echo "learner greater than expected num" >&2
			return 1
		fi
		sleep 1
	done
}

function get_learner_store_region_count()
{
	local learner_index="${1}"
	local test_ti_file="${2}"
	local prop=`"${ti}" -i "${learner_index}" -m "rngine" "${test_ti_file}" 'prop'`
	local host=`echo "${prop}" | grep "listen_host" | awk -F 'listen_host:' '{print $2}' | tr -d ' '`
	local port=`echo "${prop}" | grep "rngine_port" | awk -F 'rngine_port:' '{print $2}' | tr -d ' '`
	echo `test_cluster_get_store_region_count "${test_ti_file}" "${host}" "${port}"`
}

function wait_region_balance_complete()
{
	local test_ti_file="${1}"
	local prev_count0=""
	local prev_count1=""
	local prev_count2=""
	while true; do
		local count0=`get_learner_store_region_count 0 "${test_ti_file}"`
		local count1=`get_learner_store_region_count 1 "${test_ti_file}"`
		local count2=`get_learner_store_region_count 2 "${test_ti_file}"`
		echo "index 0 store prev region count: ${prev_count0}"
		echo "index 0 store region count: ${count0}"
		echo "index 1 store prev region count: ${prev_count1}"
		echo "index 1 store region count: ${count1}"
		echo "index 2 store prev region count: ${prev_count2}"
		echo "index 2 store region count: ${count2}"
		if [ "${count0}" == "${prev_count0}" ] && [ "${count1}" == "${prev_count1}" ] && [ "${count2}" == "${prev_count2}" ]; then
			break
		fi
		local prev_count0="${count0}"
		local prev_count1="${count1}"
		local prev_count2="${count2}"
		sleep 1
	done
}

function get_query_result()
{
	local test_ti_file="${1}"
	local query="${2}"
	local db="${3}"

	echo `"${ti}" -i 0 "${test_ti_file}" "mysql" "${query}" "${db}" | tail -n 1 | tr -d ' '`
}

function expand_shrink_test()
{
	local test_entry_file="${1}"
	local load_tpch_scale="${2}"
	local expand_shrink_times="${3}"
	local ports1="${4}"
	local ports2="${5}"
	local ports3="${6}"
	
	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"
	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_multi_node_prepare "127.0.0.1" "127.0.0.1" "127.0.0.1" "${ports1}" "${ports2}" "${ports3}" '' "${test_ti_file}"

	local ti="${integrated}/ops/ti.sh"
	"${ti}" -l -i 0 "${test_ti_file}" 'tpch/load' "${load_tpch_scale}" 'lineitem'

	local query="select /*+ read_from_storage(tiflash[t]) */ count(*) from lineitem"
	local db=`_db_name_from_scale "${load_tpch_scale}"`
	local target=`get_query_result "${test_ti_file}" "${query}" "${db}"`

	local query_times="10"
	for ((i=0; i<${expand_shrink_times}; i++)); do
		for ((j=0; j<3; j++)); do
			remove_store "${j}" "${test_ti_file}"
			for ((k=0; k<${query_times}; k++)); do
				local result=`get_query_result "${test_ti_file}" "${query}" "${db}"`
				if [ ! -z "${result}" ] && [ "${result}" != "${target}" ]; then
					echo "result error when shrink cluster" >&2
					echo "result: ${result}" >&2
					echo "target: ${target}" >&2
					return 1
				fi
				sleep 1
			done
			wait_learner_count_to_target 2 "${test_ti_file}" "down"
			add_store "${j}" "${test_ti_file}"
			for ((k=0; k<${query_times}; k++)); do
				local result=`get_query_result "${test_ti_file}" "${query}" "${db}"`
				if [ ! -z "${result}" ] && [ "${result}" != "${target}" ]; then
					echo "result error when expand cluseter" >&2
					echo "result: ${result}" >&2
					echo "target: ${target}" >&2
					return 1
				fi
				sleep 1
			done
			wait_learner_count_to_target 3 "${test_ti_file}" "up"
			wait_region_balance_complete "${test_ti_file}"
		done
	done

	local timeout=180
	for ((i=0; i<${timeout}; i++)); do
		local final_result=`"${ti}" -i 0 "${test_ti_file}" "mysql" "${query}" "${db}" | tail -n 1 | tr -d ' '`
		if [ ! -z "${final_result}" ]; then
			break
		fi
		sleep 1
	done

	if [ "${result}" != "${final_result}" ]; then
		echo "expand_shrink_test failed" >&2
		echo "target result: ${target}" >&2
		echo "final result: ${final_result}" >&2
		return 1
	fi
	echo "final result: ${final_result}"
}

expand_shrink_test "${BASH_SOURCE[0]}" "5" "10" "104" "106" "108" 
