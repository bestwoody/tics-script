#!/bin/bash

function run_file()
{
	local target="$1"
	local ti_sh_path="$2"
	local ti_file_path="$3"

	python tests/run-test.py "" "${target}" "${ti_sh_path}" "${ti_file_path}" "true"

	if [ $? == 0 ]; then
		echo "$target": OK
	else
		echo "$target": Failed
	fi
}

function run_dir()
{
	local target="$1"
	local ti_sh_path="$2"
	local ti_file_path="$3"

	find "$target" -maxdepth 1 -name "*.test" -type f | sort | while read file; do
		if [ -f "$file" ]; then
			run_file "$file" "$ti_sh_path" "$ti_file_path"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi

	find "$target" -maxdepth 1 -type d | sort -r | while read dir; do
		if [ -d "$dir" ] && [ "$dir" != "$target" ]; then
			run_dir "$dir" "$ti_sh_path" "$ti_file_path"
		fi
	done

	if [ $? != 0 ]; then
		exit 1
	fi
}

function run_path()
{
	local target="$1"
	local ti_sh_path="$2"
	local ti_file_path="$3"

	if [ -f "$target" ]; then
		run_file "$target" "$ti_sh_path" "$ti_file_path"
	else
		if [ -d "$target" ]; then
			run_dir "$target" "$ti_sh_path" "$ti_file_path"
		else
			echo "error: $target not file nor dir." >&2
			exit 1
		fi
	fi
}

set -e

target="$1"
ti_sh_path="$2"
ti_file_path="$3"

if [ -z "${target}" ]; then
	echo "<bin> test-file-path ti_sh_path ti_file_path"
	exit 1
fi

integrated="`cd $(dirname ${BASH_SOURCE[0]}) && cd .. && pwd`"

if [ -z "${ti_sh_path}" ]; then
	ti_sh_path="${integrated}/ops/ti.sh"
fi

if [ -z "${ti_file_path}" ]; then
	ti_file_path="${integrated}/ti/cluster/1.ti"
fi

run_path "$target" "$ti_sh_path" "$ti_file_path"
