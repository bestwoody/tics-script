#!/bin/bash

# TODO: Test in Mac OS
function _print_file_dir()
{
	local path="${1}"
	local dir=$(dirname "${path}")
	if [ -z "${dir}" ] || [ "${dir}" == '.' ]; then
		echo "${path}"
	else
		echo "${dir}"
	fi
}
export -f _print_file_dir

# TODO: Test in Mac OS
function _print_file_parent_dir()
{
	local path="${1}"
	local dir=$(dirname $(dirname "${path}"))
	if [ -z "${dir}" ] || [ "${dir}" == '.' ]; then
		echo "${path}"
	else
		echo "${dir}"
	fi
}
export -f _print_file_parent_dir

function print_file_ext()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_file_ext] usage: <func> file" >&2
		return 1
	fi

	local file="${1}"

	local name="${file##*\/}"
	local ext="${file##*\.}"
	if [ ".""${ext}" == "${name}" ]; then
		ext=""
	fi
	echo "${ext}"
}
export -f print_file_ext
