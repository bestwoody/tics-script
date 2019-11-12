#!/bin/bash

function get_test_entry_dir()
{
	if [ -z "${1+x}" ]; then
		echo "[func get_test_entry_dir] usage: <func> test_entry_file_or_stop_file" >&2
		return 1
	fi
	local test_entry_or_stop_file="${1}"
	local stop_file_suffix=".stop"
	local file_name_len="${#test_entry_or_stop_file}"
	local stop_file_suffix_len="${#stop_file_suffix}"
	if [ "${test_entry_or_stop_file:0-${stop_file_suffix_len}}" == '.stop' ]; then
		echo "${test_entry_or_stop_file:0:$((file_name_len-${stop_file_suffix_len}))}.data"
	else
		echo "${test_entry_or_stop_file}.data"
	fi
}
export -f get_test_entry_dir

function stop_test_cluster()
{
	if [ -z "${1+x}" ]; then
		echo "[stop_test_cluster] usage: <func> stop_script_file clean" >&2
		exit 1
	fi

	local stop_file="${1}"
	local clean="${2}"

	local test_entry_dir=`get_test_entry_dir "${stop_file}"`
	local ti_file="${test_entry_dir}/test.ti"
	if [ -f "${ti_file}" ]; then
		if [ "${clean}" == 'true' ]; then
			"${integrated}/ops/ti.sh" "${ti_file}" burn
		else
			"${integrated}/ops/ti.sh" "${ti_file}" fstop
		fi
	fi
}
export -f stop_test_cluster
