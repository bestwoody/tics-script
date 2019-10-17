#!/bin/bash

here=`cd $(dirname ${BASH_SOURCE[0]}) && pwd`
source "${here}/_env.sh"
auto_error_handle

function load_tpch()
{
	local scale="${1}"
	local table="${2}"
	local blocks="${3}"
	local test_ti_file="${4}"

	"${integrated}/ops/ti.sh" "${test_ti_file}" "tpch/load" "${scale}" "${table}" "" "${blocks}"
}
export -f load_tpch

function run_file()
{
	local target="${1}"
	local test_ti_file="${2}"
	local run_test_entry="${3}"

	python "${run_test_entry}" "" "${target}" "${integrated}/ops/ti.sh" "${test_ti_file}" "true"

	if [ "${?}" == 0 ]; then
		echo "${target}": OK
	else
		echo "${target}": Failed >&2
		return 1
	fi
}

function run_dir()
{
	local target="${1}"
	local test_ti_file="${2}"
	local run_test_entry="${3}"

	find "${target}" -maxdepth 1 -name "*.test" -type f | sort | while read file; do
		if [ -f "${file}" ]; then
			run_file "${file}" "${test_ti_file}" "${run_test_entry}"
			if [ "${?}" != 0 ]; then
				return 1
			fi
		fi
	done

	if [ "${?}" != 0 ]; then
		return 1
	fi

	find "${target}" -maxdepth 1 -type d | sort -r | while read dir; do
		if [ -d "${dir}" ] && [ "${dir}" != "${target}" ]; then
			run_dir "${dir}" "${test_ti_file}" "${run_test_entry}"
			if [ "${?}" != 0 ]; then
				return 1
			fi
		fi
	done

	if [ "${?}" != 0 ]; then
		return 1
	fi
}

function run_path()
{
	local target="${1}"
	local test_ti_file="${2}"
	local run_test_entry="${3}"

	if [ -f "${target}" ]; then
		run_file "${target}" "${test_ti_file}" "${run_test_entry}"
		if [ "${?}" != 0 ]; then
			return 1
		fi
	else
		if [ -d "${target}" ]; then
			run_dir "${target}" "${test_ti_file}" "${run_test_entry}"
			if [ "${?}" != 0 ]; then
				return 1
			fi
		else
			echo "error: ${target} not file nor dir." >&2
			return 1
		fi
	fi
}

function run_all_test()
{
	if [ -z "${2+x}" ]; then
		echo "[func run_all_test] usage: <func> test_entry_file ports run_test_sh test_cases" >&2
		return 1
	fi

	local test_entry_file="${1}"
	local ports="${2}"
	local run_test_entry="${3}"
	local test_cases="${4}"

	local entry_dir="${test_entry_file}.data"
	mkdir -p "${entry_dir}"

	local test_ti_file="${entry_dir}/test.ti"
	#test_cluster_prepare "${ports}" "pd,tikv,tidb,tiflash,rngine" "${test_ti_file}"

	echo '---'

	run_path "${test_cases}" "${test_ti_file}" "${run_test_entry}"
}

function generate_test()
{
	local target_dir="${1}"
	if [ -d "${target_dir}" ]; then
		return 0
	fi
	local temp_target_dir="${target_dir}.`date +%s`.${RANDOM}"
	python "${here}/generate-fullstack-test.py" "test" "t" "${temp_target_dir}"
	mv "${temp_target_dir}" "${target_dir}"
}

generate_test "${here}/test_cases/gen_tests"
run_all_test "${BASH_SOURCE[0]}" 34 "${here}/run_test.py" "${here}/test_cases"
