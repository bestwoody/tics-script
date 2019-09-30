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
		if [ "${learner_count}" == "${target}" ]; then
			break
		fi
		if [ "${direction}" == "down" ] && [ ${learner_count} -lt ${target} ]; then
			echo "learner less than expected num" >&2
			return 1
		fi
		if [ "${direction}" == "up" ] && [ ${learner_count} -gt ${target} ]; then
			echo "learner greater than expected num" >&2
			return 1
		fi
		sleep 1
	done
}

function get_learner_store_region_count()
{
	local index="${1}"
	local test_ti_file="${2}"
	local prop=`"${ti}" -i "${index}" -m "rngine" "${test_ti_file}" 'prop'`
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

function expand_shrink_test()
{
	local ip1="${1}"
	local ip2="${2}"
	local ip3="${3}"
	local ports1="${4}"
	local ports2="${5}"
	local ports3="${6}"
	local test_entry_file="${7}"
	local expand_shrink_times="${8}"

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"
	local report="${entry_dir}/report"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_multi_host_prepare "${ip1}" "${ip2}" "${ip3}" "${ports1}" "${ports2}" "${ports3}" '' "${test_ti_file}"

	local ti="${integrated}/ops/ti.sh"
	"${ti}" -l -i 0 "${test_ti_file}" tpch/load 0.1 lineitem

	local result=`"${ti}" "${test_ti_file}" "beeline" "tpch_0_1" -e "select count(*) from lineitem" | grep -A 2 "count" | tail -n 1 | awk -F '|' '{ print $2 }' | tr -d ' '`

	for ((i=0; i<${expand_shrink_times}; i++)); do
		remove_store 0 "${test_ti_file}"
		wait_learner_count_to_target 2 "${test_ti_file}" "down"
		add_store 0 "${test_ti_file}"
		wait_learner_count_to_target 3 "${test_ti_file}" "up"
		wait_region_balance_complete "${test_ti_file}"
	done

	local timeout=180
	for ((i=0; i<${timeout}; i++)); do
		local final_result=`"${ti}" "${test_ti_file}" "beeline" "tpch_0_1" -e "select count(*) from lineitem" | grep -A 2 "count" | tail -n 1 | awk -F '|' '{ print $2 }' | tr -d ' '`
		if [ ! -z "${final_result}" ]; then
			break
		fi
		sleep 1
	done

	if [ "${result}" != "${final_result}" ]; then
		echo "expand_shrink_test failed" >&2
		echo "result ${result}" >&2
		echo "final_result ${final_result}" >&2
		return 1
	fi
}

function test_main()
{
	local test_entry_file="${1}"
	set_is_running "${test_entry_file}"
	expand_shrink_test "172.16.5.82" "172.16.5.81" "172.16.5.85" "101" "101" "101" "${test_entry_file}" "1"
	clean_is_running "${test_entry_file}"
}

test_main "${BASH_SOURCE[0]}"
