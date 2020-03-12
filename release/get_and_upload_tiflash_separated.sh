#!/bin/bash

set -x
set -euo pipefail

source ./_env.sh

function get_and_upload_tiflash_separated()
{
	if [ -z "${2+x}" ]; then
		echo "must provide branch & commit hash" >&2
		exit 1
	fi

	branch="${1}"
	commit="${2}"

	local file="tiflash"
	local z_file="${file}.tar.gz"
	rm -f ${z_file}
	download_release_binary "${download_server}/tiflash/${branch}/${commit}/centos7/${z_file}" "${file}" "./"

	if [ ! -f "${z_file}" ]; then
		echo "${z_file} not found" >&2
		exit 1
	fi

	rm -rf ${file}
	tar -zxf "${z_file}"
	rm -f "${z_file}"

	if [ ! -d "${file}" ]; then
		echo "unzip failed" >&2
		exit 1
	fi

	here=`pwd`
	cd ${file}

	binary2="libtiflash_proxy.tar.gz"
	binary3="flash_cluster_manager.tgz"
	tar -zcvf ${z_file} ${file} >/dev/null 2>&1
	curl --upload-file ./${z_file} ${upload_server}/${z_file} ${upload_server}/${upload_secret}/${z_file}
	md5sum ${z_file}

	tar -zcvf ${binary2} libtiflash_proxy.so >/dev/null 2>&1
	curl --upload-file ./${binary2} ${upload_server}/${binary2} ${upload_server}/${upload_secret}/${binary2}
	md5sum "libtiflash_proxy.so"

	tar -zcvf ${binary3} flash_cluster_manager >/dev/null 2>&1
	curl --upload-file ./${binary3} ${upload_server}/${binary3} ${upload_server}/${upload_secret}/${binary3}
	md5sum ${binary3}
}

get_and_upload_tiflash_separated $*
