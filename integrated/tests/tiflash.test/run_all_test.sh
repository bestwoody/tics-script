#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

ti="${integrated}/ops/ti.sh"
ti_file="${integrated}/ti/1+spark_x.ti"
args="ports=+97#dir=nodes/97"

function load_tpch()
{
	local scale="${1}"
	local table="${2}"
	local blocks="${3}"
	local ti_file="${4}"
	local ti_file_args="${5}"

	local schema_dir="${integrated}/resource/tpch/mysql/schema"
	local data_dir="${integrated}/resource/tpch/data"

	load_and_generate_tpch_data "${schema_dir}" "${data_dir}" "${scale}" "${table}" "${blocks}" "${ti_file}" "${ti_file_args}"
}
export -f load_tpch

function run_all_test()
{
    "${ti}" -k "${args}" "${ti_file}" "burn" "doit"
    "${ti}" -k "${args}" "${ti_file}" "run"

    local error_handle="$-"
    set +e
    "${here}/run_test.sh" "${here}/test_cases" "${ti_file}" "${ti}" "${args}"
    if [ "${?}" != 0 ]; then
        "${ti}" -k "${args}" "${ti_file}" "burn" "doit"
        exit 1
    fi
    restore_error_handle_flags "${error_handle}"

    "${ti}" -k "${args}" "${ti_file}" "burn" "doit"
}

run_all_test
