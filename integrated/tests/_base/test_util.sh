#!/bin/bash

function get_test_entry_dir()
{
	if [ -z "${1+x}" ]; then
		echo "[func get_test_entry_dir] usage: <func> test_entry_file_or_clean_file" >&2
		return 1
	fi
	local test_entry_or_clean_file="${1}"
	local clean_file_suffix=".clean"
	local file_name_len="${#test_entry_or_clean_file}"
	local clean_file_suffix_len="${#clean_file_suffix}"
	if [ "${test_entry_or_clean_file:0-${clean_file_suffix_len}}" == ".clean" ]; then
		echo "${test_entry_or_clean_file:0:$((file_name_len-${clean_file_suffix_len}))}.data"
	else
		echo "${test_entry_or_clean_file}.data"
	fi
}
export -f get_test_entry_dir

function set_is_running()
{
	if [ -z "${1+x}" ]; then
		echo "[func set_is_running] usage: <func> test_entry_file" >&2
		return 1
	fi
	local test_entry_file="${1}"

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"

	echo `date +"%Y-%m-%d %H:%m:%S"` > "${entry_dir}/RUNNING"
}
export -f set_is_running

function clean_is_running()
{
	if [ -z "${1+x}" ]; then
		echo "[func clean_is_running] usage: <func> test_entry_file" >&2
		return 1
	fi
	local test_entry_file="${1}"

	local entry_dir=`get_test_entry_dir "${test_entry_file}"`
	mkdir -p "${entry_dir}"

	rm -f "${entry_dir}/RUNNING"
}
export -f clean_is_running

function assert_prev_test_finished()
{
	if [ -z "${1+x}" ]; then
		echo "[func assert_prev_test_finished] usage: <func> test_clean_file force_clean" >&2
		return 1
	fi
	local test_clean_file="${1}"
	local force_clean="${2}"

	local entry_dir=`get_test_entry_dir "${test_clean_file}"`

	if [ "${force_clean}" == "true" ] && [ -f "${entry_dir}/RUNNING" ]; then
		rm -f "${entry_dir}/RUNNING"
	fi

	if [ -f "${entry_dir}/RUNNING" ]; then
		return 1
	else
		return 0
	fi
}
export -f assert_prev_test_finished
