#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1+spark_x.ti"
args="ports=+47#dir=nodes/47"

function load_tpch()
{
	local scale="${1}"
	local table="${2}"
	local blocks="${3}"
	local ti_file="${4}"
	local ti_file_args="${5}"

	local schema_dir="${integrated}/tests/schema/tpch/mysql"
	local data_dir="${integrated}/data/tpch"

	load_and_generate_tpch_data "${schema_dir}" "${data_dir}" "${scale}" "${table}" "${blocks}" "${ti_file}" "${ti_file_args}"
}
export -f load_tpch

"${ti}" -k "${args}" "${ti_file}" "run"

"${here}/run_test.sh" "${here}/sample.test" "${ti_file}" "${ti}" "${args}"
"${here}/run_test.sh" "${here}/ddl_syntax" "${ti_file}" "${ti}" "${args}"
"${here}/run_test.sh" "${here}/dml_basic" "${ti_file}" "${ti}" "${args}"

"${ti}" -k "${args}" "${ti_file}" "burn" "doit"