#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

function get_learner_id_for_region()
{
	local region_id="${1}"
	local test_ti_file="${2}"

	echo `"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" 'pd/ctl' "region ${region_id}" | { grep -B 1 "is_learner" || test $? = 1; } | { grep "store_id" || test $? = 1; } | awk '{print $2}' | tr -d ','`
}

function transfer_learner()
{
	local region_id="${1}"
	local learner_id="${2}"
	local test_ti_file="${3}"
	if [ -z "${4+x}" ]; then
		local timeout="240"
	else
		local timeout="${4}"
	fi

	local ti="${integrated}/ops/ti.sh"

	"${ti}" -i 0 "${test_ti_file}" 'pd/ctl' "operator add add-learner ${region_id} ${learner_id}"
	for ((k=0; k<${timeout}; k++)); do
		echo "region id ${region_id}"
		echo "target learner id ${learner_id}"
		local learner_count=`"${ti}" -i 0 "${test_ti_file}" 'pd/ctl' "region ${region_id}" | { grep "is_learner" || test $? = 1; } | wc -l`
		echo "learner count ${learner_count}"
		if [ "${learner_count}" -eq 1 ]; then
			local learner_store_id=`get_learner_id_for_region "${region_id}" "${test_ti_file}"`
			echo "current learner id ${learner_store_id}"
			if [ "${learner_store_id}" == "${learner_id}" ]; then
				break
			else
				"${ti}" -i 0 "${test_ti_file}" 'pd/ctl' "operator add add-learner ${region_id} ${learner_id}"
			fi
		fi
		sleep 1
	done
}

function dump_cluster_region_count()
{
	local test_ti_file="${1}"
	local store_count=`test_cluster_get_normal_store_count "${test_ti_file}"`
	for ((i=0; i<${store_count}; i++)); do
		local store_id=`test_cluster_get_learner_store_id_by_index "${test_ti_file}" "${i}"`
		local region_count=`"${integrated}/ops/ti.sh" -i 0 "${test_ti_file}" "pd/ctl" "store ${store_id}" | { grep "region_count" || test $? = 1; } | awk -F ':' '{print $2}' | tr -cd '[0-9]'`
		echo "index ${i} store id ${store_id} region count ${region_count}"
	done
}

function choose_target_learner_id()
{
	local region_id="${1}"
	local test_ti_file="${2}"

	local store_count=`test_cluster_get_normal_store_count "${test_ti_file}"`
	local current_store_of_region=`get_learner_id_for_region "${region_id}" "${test_ti_file}"`
	for ((i=0; i<${store_count}; i++)); do
		local store_id=`test_cluster_get_learner_store_id_by_index "${test_ti_file}" "${i}"`
		if [ ! -z "${store_id}" ] && [ "${store_id}" != "${current_store_of_region}" ]; then
			echo "${store_id}"
			break
		fi
	done
}

function ti_manual_schedule()
{
	local test_entry_file="${1}"
	local ports1="${2}"
	local ports2="${3}"
	local ports3="${4}"
	local load_tpch_scale="${5}"

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"

	local test_ti_file="${entry_dir}/test.ti"
	test_cluster_multi_node_prepare "127.0.0.1" "127.0.0.1" "127.0.0.1" "${ports1}" "${ports2}" "${ports3}" '' "${test_ti_file}"

	echo '---'

	local db=`_db_name_from_scale "${load_tpch_scale}"`
	local table="lineitem"
	local ti="${integrated}/ops/ti.sh"

	"${ti}" -l -i 0 "${test_ti_file}" "tpch/load" "${load_tpch_scale}" "${table}"
	echo "load tpch${load_tpch_scale} ${table} done."

	local cluster_region_count=`test_cluster_get_cluster_region_count "${test_ti_file}"`
	echo "cluster total region count: ${cluster_region_count}"

	for ((i=0; i<${cluster_region_count}; i++)); do
		local region_info=`"${ti}" "${test_ti_file}" 'pd/ctl' "region ${i}"`
		if [ "${region_info}" != "null" ]; then
			local target_learner_id=`choose_target_learner_id "${i}" "${test_ti_file}"`
			echo "${i}"
			dump_cluster_region_count "${test_ti_file}"
			transfer_learner "${i}" "${target_learner_id}" "${test_ti_file}"
			dump_cluster_region_count "${test_ti_file}"
			local tiflash_result=`"${ti}" -i 0 "${test_ti_file}" "mysql" "select /*+ read_from_storage(tiflash[${table}]) */ count(*) from ${table}" "${db}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
			local tikv_result=`"${ti}" -i 0 "${test_ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
			echo "tiflash result: ${tiflash_result}"
			echo "tikv result: ${tikv_result}"
			local tiflash_num_in_query_plan=`"${ti}" -i 0 "${test_ti_file}" "mysql" "desc select /*+ read_from_storage(tiflash[${table}]) */ count(*) from ${table}" "${db}" | { grep "tiflash" || test $? = 1; } | wc -l`
			if [ "${tiflash_num_in_query_plan}" -eq 0 ]; then
				echo "query plan doesn't contain tiflash" 2>&1
				return 1
			fi
			if [ "${tiflash_result}" != "${tikv_result}" ]; then
				echo "result not match" 2>&1
				return 1
			fi
		fi
		sleep 0.5
	done
}

ti_manual_schedule "${BASH_SOURCE[0]}" "124" "126" "128" "5"
