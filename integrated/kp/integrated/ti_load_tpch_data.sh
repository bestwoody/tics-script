#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
args="ports=+43#dir=nodes/43"

title='<cluster load tpch data time>'
data="${BASH_SOURCE[0]}.data"
report="${BASH_SOURCE[0]}.report"

schema_dir="/data1/data/tpch/schema"
data_dir="/data1/data/tpch/tpch10_16"
db="tpch10"
table="lineitem"

function load_tpch_data_test()
{
	local schema_dir="${1}"
	local data_dir="${2}"
	local db="${3}"
	local table="${4}"

	"${ti}" -k "${args}" "${ti_file}" burn doit
	"${ti}" -k "${args}" "${ti_file}" 'run'

	local start_time=`date +%s`
	load_tpch_data_to_ti_cluster "${ti_file}" "${schema_dir}" "${data_dir}" "${db}" "${table}" "${args}"
	local end_time=`date +%s`

	local status=`"${ti}" -k "${args}" "${ti_file}" 'status'`
	local ok=`echo "${status}" | grep 'OK' | wc -l`
	if [ "${ok}" != '5'  ]; then
		echo "${status}" >&2
		"${ti}" -k "${args}" "${ti_file}" burn doit
		return 1
	fi
	local version="`get_mod_ver "pd" "${ti_file}" "${args}"`,`get_mod_ver "tikv" "${ti_file}" "${args}"`,`get_mod_ver "tidb" "${ti_file}" "${args}"`,`get_mod_ver "tiflash" "${ti_file}" "${args}"`,`get_mod_ver "rngine" "${ti_file}" "${args}"`"
	local tags="db:${db},table:${table},start_ts:${start_time},end_ts:${end_time},${version}"
	echo "$((end_time - start_time)) ${tags}" >> "${data}"
	"${ti}" -k "${args}" "${ti_file}" burn doit
	to_table "${title}" 'cols:table|notag; rows:db|notag; cell:limit(20)|avg|~|duration' 9999 "${data}" > "${report}.tmp"
	mv -f "${report}.tmp" "${report}"
	echo 'done'
}

load_tpch_data_test "${schema_dir}" "${data_dir}" "${db}" "${table}"