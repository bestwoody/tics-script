#!/bin/bash

function cmd_ti_download()
{
	if [ -z "${2+x}" ]; then
		echo "[func cmd_ti_download] usage: <func> ti_file download_dir [dry] [cache_dir=/tmp/ti]" >&2
		return 1
	fi

	local ti_file="$1"
	local dir="$2"
	# If "dry" is set, output the download scripts instead of downloading
	local dry="${3:-""}"
	local cache_dir="${4:-"/tmp/ti"}"

	echo "Downloading components to ${dir} ..."
	mkdir -p "${dir}"
	local ti="${integrated}/ops/ti.sh"

	local conf_templ_dir="${integrated}/conf"
	local mod_names=""
	local cmd_hosts=""
	local indexes=""
	local ti_args=""
	local sh=$(python "${integrated}/_base/ti_file/ti_file.py" "download" \
		"${ti_file}" "${integrated}" "${conf_templ_dir}" \
		"${cache_dir}" "${mod_names}" "${cmd_hosts}" "${indexes}" "${ti_args}" "${dir}" )
	
	if [ ! -z "$dry" ]; then
		local output_file="${ti_file}.download.sh"
		echo "$sh" > "${output_file}"
		echo "Check ${output_file}"
	else
		eval "$sh"
		print_hhr
		echo 'Download OK'
	fi
}

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle
cmd_ti_download "${@:-""}"
