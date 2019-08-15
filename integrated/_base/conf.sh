#!/bin/bash

# bin_paths_file: name \t md5sum \t bin_name \t url
# bin_urls_file: name \t bin_name \t path1:path2:...
function cp_bin_to_dir()
{
	if [ -z ${1+x} ]; then
		echo "[func cp_bin_to_dir] usage: <func> name_of_bin_module dest_dir bin_paths_file bin_urls_file" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_paths_file="${3}"
	local bin_urls_file="${4}"

	local entry_str=`grep "^${name}" "${bin_paths_file}"`

	local bin_name=`echo "${entry_str}" | awk '{print $2}'`
	local paths_str=`echo "${entry_str}" | awk '{print $3}'`
	local paths=(${paths_str//:/ })
	local found=false
	for path in ${paths[@]}; do
		local path=`replace_substr "${path}" '{integrated}' "${integrated}"`
		if [ -f ${path} ]; then
			cp_when_diff "${path}" "${dest_dir}/${bin_name}"
			found=true
			break
		fi
	done

	if [ ${found} == true ]; then
		return
	fi

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

	wget --quiet -nd -P "${dest_dir}" "${url}"

	local download_name="`basename ${url}`"
	local download_ext="`print_file_ext "${download_name}"`"
	local download_is_tar=`echo "${download_name}" | grep '.tar.gz'`

	if [ ! -z "${download_is_tar}" ]; then
		tar -O -zxf "${dest_dir}/${download_name}" > "${dest_dir}/${bin_name}"
		rm -f "${dest_dir}/${download_name}"
		chmod +x "${dest_dir}/${bin_name}"
		return 0
	fi

	if [ -z "${download_ext}" ]; then
		if [ ! -f "${dest_dir}/${bin_name}" ]; then
			mv -f "${dest_dir}/${download_name}" "${dest_dir}/${bin_name}"
		fi
		local new_md5=`file_md5 "${dest_dir}/${bin_name}"`
		if [ "${new_md5}" == "${md5}" ]; then
			return 0
		else
			echo "[func cp_bin_to_dir] ${md5}(${bin_urls_file}) != ${new_md5}(${dest_dir}/${bin_name}) md5 not matched" >&2
			return 1
		fi
	fi

	echo "[func cp_bin_to_dir] TODO: support .${download_ext} file from url" >&2
	return 1
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
