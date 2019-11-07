#!/bin/bash

function _update_mod_urls()
{
	local mod_name="${1}"
	local file_md5="${2}"
	local file_name="${3}"
	local file_url="${4}"
	local branch="${5}"
	local commit_hash="${6}"

	if [ `uname` == "Darwin" ]; then
		local url_file="${integrated}/conf/bin.urls.mac"
	else
		local url_file="${integrated}/conf/bin.urls"
	fi
	local temp_url_file="${url_file}.temp"

	local remove_target_line=`cat "${url_file}" | grep "^${mod_name}\b" | awk '{print $2}'`
	sed "/${remove_target_line}/{d;}" "${url_file}" > "${temp_url_file}"
	echo -e "${mod_name}\t${file_md5}\t${file_name}\t${file_url}\t${branch}\t${commit_hash}" >> "${temp_url_file}"
	mv "${temp_url_file}" "${url_file}"	
}
export -f _update_mod_urls

function _build_and_update_mod()
{
	local repo_path="${1}"
	local branch="${2}"
	local commit="${3}"
	local binary_rel_path="${4}"
	local build_command="${5}"
	local mod_name="${6}"
	if [ ! -z "${7+x}" ]; then
		local binary_alias="${7}"
	else
		local binary_alias=""
	fi
	(
		cd "${repo_path}"
		if [ "${mod_name}" == "tiflash" ]; then
			local here=`pwd`
			cd "./ch"
			git checkout "${branch}"
			git pull
			git submodule update --init --recursive
			if [ ! -z "${commit}" ]; then
				git reset --hard "${commit}"
			fi
			cd "${here}"
		else
			git checkout "${branch}"
			git pull
			if [ ! -z "${commit}" ]; then
				git reset --hard "${commit}"
			fi
		fi
		local commit_hash=`git log | head -n 1 | awk '{print $2}'`
		local binary_file_path="${repo_path}/${binary_rel_path}"
		local binary_file_name=`basename "${binary_file_path}"`
		rm -f "${binary_file_path}"
		${build_command}
		if [ ! -z "${binary_alias}" ]; then
			local binary_file_dir=`dirname "${binary_file_path}"`
			local alias_file_path="${binary_file_dir}/${binary_alias}"
			rm -f "${alias_file_path}"
			mv "${binary_file_path}" "${alias_file_path}"
			local binary_file_path="${alias_file_path}"
			local binary_file_name="${binary_alias}"
			local binary_rel_dir=`dirname "${binary_rel_path}"`
			local binary_rel_path="${binary_rel_dir}/${binary_alias}"
		fi
		if [ ! -f "${binary_file_path}" ]; then
			echo "[func _build_and_update_mod] cannot found target binary at ${binary_file_path}" >&2
			return 1
		fi
		local compressed_file_name="${binary_file_name}.tar.gz"
		rm -f "${compressed_file_name}"
		tar -zcvf "${compressed_file_name}" "${binary_rel_path}"
		if [ ! -f "${compressed_file_name}" ]; then
			echo "[func _build_and_update_mod] compress binary file failed" >&2
			return 1
		fi
		local result_url=`upload_file "${compressed_file_name}"`
		local file_md5=`file_md5 "${binary_file_path}"`
		_update_mod_urls "${mod_name}" "${file_md5}" "${binary_file_name}" "${result_url}" "${branch}" "${commit_hash}"
	)
}
export -f _build_and_update_mod
