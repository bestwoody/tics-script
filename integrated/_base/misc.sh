#!/bin/bash

function func_exists()
{
	local has=`LC_ALL=C type ${1} 2>/dev/null | { grep 'is a function' || test $? = 1; }`
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

	local value=`cat "${file}" | { grep "^${key}\b" || test $? = 1; } | awk '{print $2}'`
	if [ -z "${value}" ]; then
		return 1
	fi
	echo "${value}"
}
export -f get_value

# TODO: unused
function watch_file()
{
	if [ -z "${2+x}" ]; then
		echo "[func watch_file] usage: <func> file_path timeout" >&2
		return 1
	fi

	local file="${1}"
	local timeout="${2}"

	local latest_mtime=`file_mtime "${file}"`

	local unchanged_times='0'
	for (( i = 0; i < "${timeout}"; i++ )); do
		local mtime=`file_mtime "${file}"`
		if [ "${latest_mtime}" != "${mtime}" ]; then
			local unchanged_times='0'
			local latest_mtime="${mtime}"
			local i=0
			echo "changed!! #${i} < ${timeout} ${file}"
			continue
		fi
		local unchanged_times=$((unchanged_times + 1))
		echo "unchanged #${i} < ${timeout} ${file}"
		sleep 1
	done

	# TODO
}
export -f watch_file

# TODO: unused
function watch_files()
{
	if [ -z "${2+x}" ]; then
		echo "[func watch_files] usage: <func> dir_path timeout" >&2
		return 1
	fi

	local dir="${1}"
	local timeout="${2}"
	for file in "${dir}"; do
		watch_file "${file}" "${timeout}"
	done
}
export -f watch_files
