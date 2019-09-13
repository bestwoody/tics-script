#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1+spark_x.ti"
dir="nodes/32"
args="ports=+32#dir=${dir}"

db="tpch10"
table="lineitem"
schema_dir="/data1/data/tpch/schema"
data_dir="/data1/data/tpch/tpch10_16"

function restart_tiflash() {
	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l | awk '{print $1}'`
	if [ "${ok}" != '5' ]; then
		echo "${status}" >&2
		local error_time=`date +%s`
		local error_time=`date -d "1970-01-01 ${error_time} seconds" +"%Y_%m_%d_%H_%m_%S"`
		mkdir -p "${here}/failed_test_envs"
		echo "${status}" > "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" -m pd "${ti_file}" log 100000 | awk '{ print "[pd] " $0 }' >> "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" -m tikv "${ti_file}" log 100000 | awk '{ print "[tikv] " $0 }' >> "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" -m tidb "${ti_file}" log 100000 | awk '{ print "[tidb] " $0 }' >> "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" -m tiflash "${ti_file}" log 100000 | awk '{ print "[tiflash] " $0 }' >> "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" -m rngine "${ti_file}" log 100000 | awk '{ print "[rngine] " $0 }' >> "${here}/failed_test_envs/${error_time}.log"
		"${ti}" -k "${args}" "${ti_file}" 'down'
		tar -czPf "${here}/failed_test_envs/${error_time}_env.tar.gz" "${integrated}/${dir}"
		"${ti}" -k "${args}" "${ti_file}" burn doit
		return 1
	fi

	sleep $((100 + (${RANDOM} % 100)))
	"${ti}" -k "${args}" -m tiflash "${ti_file}" fstop

	sleep $((10 + (${RANDOM} % 10)))
	"${ti}" -k "${args}" -m tiflash,rngine "${ti_file}" up
	sleep $((10 + (${RANDOM} % 10)))
}

function wait_load_data_ready() {
	local target_result=`cat ${data_dir}/${table}* | wc -l`
	while true; do
		local completed="true"
		local mysql_result=`"${ti}" -k "${args}" "${ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
	if [ "${mysql_result}" == "${target_result}" ]; then
		break
	else
		echo "waiting for load data completed"
		echo "current: " "${mysql_result}"
		echo "target: " "${target_result}"
	fi
	sleep $((5 + (${RANDOM} % 5)))
	done
}

function check_tiflash_and_tidb_result_consistency() {
	"${ti}" -k "${args}" "${ti_file}" up
	local mysql_result=`"${ti}" -k "${args}" "${ti_file}" "mysql" "select count(*) from ${db}.${table}" | tail -n 1 | awk '{ print $1 }' | tr -d ' '`
	local beeline_result=`"${ti}" -k "${args}" "${ti_file}" "beeline" "select count(*) from ${db}.${table}" 2>&1 | grep -A 2 "count" | tail -n 1 | awk -F '|' '{ print $2 }' | tr -d ' '`

	if [ "${mysql_result}" != "${beeline_result}" ]; then
		echo "mysql_result" ${mysql_result} >&2
		echo "beeline_result" ${beeline_result} >&2
		mkdir -p "${here}/failed_test_envs"
		local error_time=`date +%s`
		local error_time=`date -d "1970-01-01 ${error_time} seconds" +"%Y_%m_%d_%H_%m_%S"`
		tar -czPf "${here}/failed_test_envs/result_not_match.${error_time}_env.tar.gz" "${integrated}/${dir}"
		"${ti}" -k "${args}" -m pd,tikv,tidb,tiflash,rngine "${ti_file}" log 100000 > "${here}/failed_test_envs/result_not_match.${error_time}.log"
	fi
}

function test_abnormal_kill_tiflash() {
	"${ti}" -k "${args}" "${ti_file}" burn doit
	"${ti}" -k "${args}" -m pd,tikv,tidb,tiflash,rngine "${ti_file}" up

	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l | awk '{print $1}'`
	if [ "${ok}" != '5' ]; then
		echo "${status}" >&2
		"${ti}" -k "${args}" "${ti_file}" burn doit
		return 1
	fi

	load_tpch_data_to_ti_cluster "${ti_file}" "${schema_dir}" "${data_dir}" "${db}" "${table}" "${args}" 1>/dev/null &
	for i in {1..5}; do
		restart_tiflash
	done
	wait_load_data_ready
	check_tiflash_and_tidb_result_consistency

	"${ti}" -k "${args}" "${ti_file}" burn doit
	echo 'done'
}

test_abnormal_kill_tiflash
