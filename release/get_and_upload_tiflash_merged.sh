#!/bin/bash

set -x
set -euo pipefail

source ./_env.sh

function get_and_upload_tiflash_merged()
{
	if [ -z "${2+x}" ]; then
		echo "must provide branch & commit hash" >&2
		exit 1
	fi

	branch="${1}"
	commit="${2}"

	local file="tiflash"
	local z_file="${file}.tar.gz"
	rm -f "${z_file}"
	download_release_binary "${download_server}/tiflash/${branch}/${commit}/centos7/${z_file}" "${file}" "./"

	if [ ! -f "${z_file}" ]; then
		echo "${z_file} not found" >&2
		exit 1
	fi

	curl --upload-file ./${z_file} ${upload_server}/${z_file} ${upload_server}/${upload_secret}/${z_file}
	md5sum ${z_file}
}

get_and_upload_tiflash_merged $*
