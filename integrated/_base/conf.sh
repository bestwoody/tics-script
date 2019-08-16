#!/bin/bash

# bin_paths_file: name \t md5sum \t bin_name \t url
function cp_bin_to_dir_from_paths()
{
	if [ -z ${1+x} ]; then
		echo "[func cp_bin_to_dir_from_paths] usage: <func> name_of_bin_module dest_dir bin_paths_file" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_paths_file="${3}"

	local entry_str=`grep "^${name}" "${bin_paths_file}"`

	local bin_name=`echo "${entry_str}" | awk '{print $2}'`
	local paths_str=`echo "${entry_str}" | awk '{print $3}'`
	local found="false"
	if [ ! -z ${paths_str} ]; then
		local paths=(${paths_str//:/ })
		for path in ${paths[@]}; do
			local path=`replace_substr "${path}" '{integrated}' "${integrated}"`
			if [ -f ${path} ]; then
				cp_when_diff "${path}" "${dest_dir}/${bin_name}"
				found="true"
				break
			fi
		done
	fi

	echo "${found}"
}
export -f cp_bin_to_dir_from_paths

# bin_urls_file: name \t bin_name \t path1:path2:...
function cp_bin_to_dir_from_urls()
{
	if [ -z ${1+x} ]; then
		echo "[func cp_bin_to_dir_from_urls] usage: <func> name_of_bin_module dest_dir bin_urls_file cache_dir" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_urls_file="${3}"
	local cache_dir="${4}"

	local entry_str=`grep "^${name}" "${bin_urls_file}"`
	if [ -z "$entry_str" ]; then
		echo "[func cp_bin_to_dir] ${name} not found in ${bin_paths_file} or in ${bin_urls_file}" >&2
		return 1
	fi

	local md5=`echo "${entry_str}" | awk '{print $2}'`
	local bin_name=`echo "${entry_str}" | awk '{print $3}'`
	local url=`echo "${entry_str}" | awk '{print $4}'`

	if [ -f "${dest_dir}/${bin_name}" ]; then
		local old_md5=`file_md5 "${dest_dir}/${bin_name}"`
		if [ "${old_md5}" == "${md5}" ]; then
			return 0
		else
			mv -f "${dest_dir}/${bin_name}" "${dest_dir}/${bin_name}.prev"
		fi
	fi

	mkdir -p "${cache_dir}"
	if [ -f "${cache_dir}/${bin_name}" ]; then
		local old_md5=`file_md5 "${cache_dir}/${bin_name}"`
		if [ "${old_md5}" == "${md5}" ]; then
			cp_when_diff "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
			return 0
		fi
	fi

	local download_name="`basename ${url}`"
	rm -f "${cache_dir}/${download_name}"
	wget --quiet -nd -P "${cache_dir}" "${url}"

	local download_ext="`print_file_ext "${download_name}"`"
	local download_is_tar=`echo "${download_name}" | grep '.tar.gz'`

	if [ ! -z "${download_is_tar}" ]; then
		tar --overwrite -O -zxf "${cache_dir}/${download_name}" > "${cache_dir}/${bin_name}"
		rm -f "${cache_dir}/${download_name}"
		chmod +x "${cache_dir}/${bin_name}"
		mkdir -p "${dest_dir}"
		cp -f "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
		return 0
	fi

	if [ -z "${download_ext}" ]; then
		if [ ! -f "${cache_dir}/${bin_name}" ]; then
			mv -f "${cache_dir}/${download_name}" "${cache_dir}/${bin_name}"
		fi
		local new_md5=`file_md5 "${cache_dir}/${bin_name}"`
		if [ "${new_md5}" == "${md5}" ]; then
			cp -f "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
			return 0
		else
			echo "[func cp_bin_to_dir] ${md5}(${bin_urls_file}) != ${new_md5}(${cache_dir}/${bin_name}) md5 not matched" >&2
			return 1
		fi
	fi

	echo "[func cp_bin_to_dir] TODO: support .${download_ext} file from url" >&2
	return 1
}
export -f cp_bin_to_dir_from_urls

function cp_bin_to_dir()
{
	if [ -z ${1+x} ]; then
		echo "[func cp_bin_to_dir] usage: <func> name_of_bin_module dest_dir bin_paths_file bin_urls_file [cache_dir]" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_paths_file="${3}"
	local bin_urls_file="${4}"

	if [ -z "${5+x}" ]; then
		local cache_dir="/tmp/ti_integrated/bin_cache"
	else
		local cache_dir="${5}"
	fi

	local found=`cp_bin_to_dir_from_paths "${name}" "${dest_dir}" "${bin_paths_file}"`
	if [ "${found}" != "true" ]; then
		cp_bin_to_dir_from_urls "${name}" "${dest_dir}" "${bin_urls_file}" "${cache_dir}"
	fi
}
export -f cp_bin_to_dir

function render_templ()
{
	if [ -z ${1+x} ]; then
		echo "[func render_templ] usage: <func> templ_file dest_file render_str(k=v#k=v#..)" >&2
		return 1
	fi

	local src="${1}"
	local dest="${2}"
	local kvs="${3}"

	local dest_dir=`dirname "${dest}"`
	mkdir -p "${dest_dir}"

	python "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/render_templ.py" "${kvs}" < "${src}" > "${dest}"
}
export -f render_templ
