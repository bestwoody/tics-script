#!/bin/bash

set -x
set -euo pipefail

source ./_env.sh

function get_and_upload_pd()
{
	if [ -z "${2+x}" ]; then
		echo "must provide branch & commit hash" >&2
		exit 1
	fi

	branch="{1}"
	commit="${2}"

	local file="pd-server"
	local z_file="${file}.tar.gz"
	rm -f ${z_file}
	download_release_binary "${download_server}/pd/${commit}/centos7/${z_file}" "${file}" "./"

	if [ ! -f "${z_file}" ]; then
		echo "${z_file} not found" >&2
		exit 1
	fi

	rm -rf pd
	mkdir pd
	tar -zxf "${z_file}" -C pd
	rm -f "${z_file}"

	if [ ! -d "pd/bin" ]; then
		echo "unzip failed" >&2
		exit 1
	fi

	rm -rf bin/
	mkdir bin/
	mv pd/bin/${file} bin/
	rm -rf pd

	tar -zcvf ${z_file} bin/ >/dev/null 2>&1
	rm -rf bin

	curl --upload-file ./${z_file} ${uplaod_server}/${z_file} ${uplaod_server}/${upload_secret}/${z_file}
	md5sum ${z_file}
}

get_and_upload_pd $*
