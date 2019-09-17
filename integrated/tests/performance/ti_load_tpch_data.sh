#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1_x.ti"
args="ports=+45#dir=nodes/45"

title='<cluster load tpch data time>'
data="${BASH_SOURCE[0]}.data"
report="${BASH_SOURCE[0]}.report"

schema_dir="${integrated}/resource/tpch/mysql/schema"
data_dir="${integrated}/resource/tpch/data"
scale="1"
table="lineitem"

function load_tpch_data_test()
{
	local db="tpch${scale}"
	local start_time=`date +%s`
	load_tpch_data_base_test "${schema_dir}" "${data_dir}" "${scale}" "${table}" "${ti}" "${ti_file}" "${args}"
	local end_time=`date +%s`
	local version="`get_mod_ver "pd" "${ti_file}" "${args}"`,`get_mod_ver "tikv" "${ti_file}" "${args}"`,`get_mod_ver "tidb" "${ti_file}" "${args}"`,`get_mod_ver "tiflash" "${ti_file}" "${args}"`,`get_mod_ver "rngine" "${ti_file}" "${args}"`"
	local tags="db:${db},table:${table},start_ts:${start_time},end_ts:${end_time},${version}"
	echo "$((end_time - start_time))" "${tags}" >> "${data}"
	to_table "${title}" 'cols:table|notag; rows:db|notag; cell:limit(20)|avg|~|duration' 9999 "${data}" > "${report}.tmp"
	mv -f "${report}.tmp" "${report}"
	echo 'done'
}

load_tpch_data_test