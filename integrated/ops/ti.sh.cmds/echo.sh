#!/bin/bash

function cmd_ti_echo()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	shift 5

	local args=("${@}")
	local args_str="extra:[ "
	for it in "${args[@]}"; do
		local args_str="$args_str'$it' "
	done
	local args_str="$args_str]"

	echo "${index}" "${mod_name}" "${dir}" "${conf_rel_path}" "${host}" "${args_str}"
}

set -euo pipefail
cmd_ti_echo "${@}"
