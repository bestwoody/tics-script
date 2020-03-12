#!/bin/bash

export download_server="http://fileserver.pingcap.net/download/builds/pingcap"

export upload_server="http://139.219.11.38:8000"

export upload_secret="66nb8"

function download_release_binary()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ]; then
		echo "[func download_test_binary] usage: <func> binary_url binary_file_name bin_dir" >&2
		return 1
	fi
	local binary_url="${1}"
	local binary_file_name="${2}"
	local bin_dir="${3}"

	mkdir -p "${bin_dir}"
	local download_path="${bin_dir}/${binary_file_name}.tar.gz.`date +%s`.${RANDOM}"
	# TODO: refine all download process in ops to avoid multi-thread hazard
	if [ ! -f "${bin_dir}/${binary_file_name}.tar.gz" ]; then
		wget --quiet -nd "${binary_url}" -O "${download_path}"
		mv "${download_path}" "${bin_dir}/${binary_file_name}.tar.gz"
	fi
}
export -f download_release_binary
