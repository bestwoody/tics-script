#!/bin/bash

function run_file()
{
	local target="${1}"
	local ti_file_path="${2}"
	local ti_sh_path="${3}"
	local ti_file_args="${4}"

	local here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
	python "${here}/run_test.py" "" "${target}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}" "true"

	if [ "${?}" == 0 ]; then
		echo "${target}": OK
	else
		echo "${target}": Failed >&2
	fi
}

function run_dir()
{
	local target="${1}"
	local ti_file_path="${2}"
	local ti_sh_path="${3}"
	local ti_file_args="${4}"

	find "${target}" -maxdepth 1 -name "*.test" -type f | sort | while read file; do
		if [ -f "${file}" ]; then
			run_file "${file}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}"
		fi
	done

	if [ "${?}" != 0 ]; then
		return 1
	fi

	find "${target}" -maxdepth 1 -type d | sort -r | while read dir; do
		if [ -d "${dir}" ] && [ "${dir}" != "${target}" ]; then
			run_dir "${dir}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}"
		fi
	done

	if [ "${?}" != 0 ]; then
		return 1
	fi
}

function run_path()
{
	local target="${1}"
	local ti_file_path="${2}"
	local ti_sh_path="${3}"
	local ti_file_args="${4}"

	if [ -f "${target}" ]; then
		run_file "${target}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}"
	else
		if [ -d "${target}" ]; then
			run_dir "${target}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}"
		else
			echo "error: ${target} not file nor dir." >&2
			return 1
		fi
	fi
}

function run_test()
{
	local target="${1}"
	local ti_file_path="${2}"
	local ti_sh_path="${3}"
	local ti_file_args="${4}"

	if [ -z "${target}" ]; then
		echo "[func run_test] target_test_file ti_sh_path ti_file_path ti_file_args"
		return 1
	fi

	if [ -z "${ti_file_path}" ]; then
		ti_file_path="${integrated}/ti/1.ti"
	fi

	if [ -z "${ti_sh_path}" ]; then
		ti_sh_path="${integrated}/ops/ti.sh"
	fi

	run_path "${target}" "${ti_file_path}" "${ti_sh_path}" "${ti_file_args}"
}

run_test "${@}"
