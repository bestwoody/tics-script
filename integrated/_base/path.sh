#!/bin/bash

function print_file_ext()
{
	if [ -z ${1+x} ]; then
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
