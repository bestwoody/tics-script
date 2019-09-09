#!/bin/bash

function func_exists()
{
	local has=`LC_ALL=C type ${1} 2>/dev/null | grep 'is a function'`
	if [ -z "${has}" ]; then
		echo 'false'
	else
		echo 'true'
	fi
}
export -f func_exists

function echo_test()
{
	local error_handle="$-"
	set +u
	local args=("${@}")
	restore_error_handle_flags "${error_handle}"

	echo "=> echo test start"
	echo "args count: ${#args[@]}"
	echo "args: ${args[@]}"
	echo "=> echo test end"
}
export -f echo_test

function get_value()
{
	if [ -z "${2+x}" ]; then
		echo "[func get_value] usage: <func> from_file key"
		return 1
	fi

	local file="${1}"
	local key="${2}"

	if [ ! -f "${file}" ]; then
		echo "[func get_value] '${file}' not exists" >&2
		return 1
	fi

	local value=`grep "${key}" "${file}" | awk '{print $2}'`
	if [ -z "${value}" ]; then
		return 1
	fi
	echo "${value}"
}
export -f get_value
