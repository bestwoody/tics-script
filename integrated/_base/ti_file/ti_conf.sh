#!/bin/bash

function get_bin_md5_from_conf()
{
	if [ -z "${2+x}" ]; then
		echo "[func get_bin_md5_from_conf] usage: <func> mod_name bin_urls_file" >&2
		return 1
	fi
	local name="${1}"
	local bin_urls_file="${2}"

	local entry_str=`cat "${bin_urls_file}" | { grep "^${name}[[:blank:]]" || test $? = 1; }`
	if [ ! -z "$entry_str" ]; then
		echo "${entry_str}" | awk '{print $2}'
	fi
}
export -f get_bin_md5_from_conf

function get_bin_name_from_conf()
{
	if [ -z "${3+x}" ]; then
		echo "[func get_bin_name_from_conf] usage: <func> mod_name bin_paths_file bin_urls_file" >&2
		return 1
	fi

	local name="${1}"
	local bin_paths_file="${2}"
	local bin_urls_file="${3}"

	local entry_str=`cat "${bin_paths_file}" | { grep "^${name}[[:blank:]]" || test $? = 1; }`
	local bin_name=`echo "${entry_str}" | awk '{print $2}'`
	if [ -z "${bin_name}" ]; then
		local entry_str=`cat "${bin_urls_file}" | { grep "^${name}[[:blank:]]" || test $? = 1; }`
		local bin_name=`echo "${entry_str}" | awk '{print $3}'`
		if [ -z "${bin_name}" ]; then
			echo "[func get_bin_name_from_conf] ${name} not found in ${bin_paths_file} or in ${bin_urls_file}" >&2
			return 1
		fi
	fi
	echo "${bin_name}"
}
export -f get_bin_name_from_conf

# bin_paths_file: name \t bin_name \t path1:path2:...
function cp_bin_to_dir_from_paths()
{
	if [ -z "${4+x}" ]; then
		echo "[func cp_bin_to_dir_from_paths] usage: <func> name_of_bin_module dest_dir bin_paths_file cache_dir" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_paths_file="${3}"
	local cache_dir="${4}"

	local entry_str=`cat "${bin_paths_file}" | { grep "^${name}[[:blank:]]" || test $? = 1; }`

	local bin_name=`echo "${entry_str}" | awk '{print $2}'`
	if [ ! -z "${DEFAULT_BIN_PATH+x}" ] && [ -f "${DEFAULT_BIN_PATH}/${bin_name}" ]; then
		cp_when_diff "${DEFAULT_BIN_PATH}/${bin_name}" "${cache_dir}/${bin_name}"
		cp_when_diff "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
		echo 'true'
		return
	fi

	local paths_str=`echo "${entry_str}" | awk '{print $3}'`
	local found="false"
	if [ ! -z "${paths_str}" ]; then
		local paths=(${paths_str//:/ })
		for path in ${paths[@]}; do
			# TODO: Pass integrated dir from args
			local path=`replace_substr "${path}" '{integrated}' "${integrated}"`
			if [ -f "${path}" ]; then
				cp_when_diff "${path}" "${cache_dir}/${bin_name}"
				cp_when_diff "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
				local found="true"
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
	if [ -z "${4+x}" ]; then
		echo "[func cp_bin_to_dir_from_urls] usage: <func> name_of_bin_module dest_dir bin_urls_file cache_dir" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_urls_file="${3}"
	local cache_dir="${4}"

	local entry_str=`cat "${bin_urls_file}" | { grep "^${name}[[:blank:]]" || test $? = 1; }`
	if [ -z "$entry_str" ]; then
		echo "[func cp_bin_to_dir] ${name} not found in ${bin_paths_file} or in ${bin_urls_file}" >&2
		return 1
	fi

	local md5=`echo "${entry_str}" | awk '{print $2}'`
	local bin_name=`echo "${entry_str}" | awk '{print $3}'`
	local url=`echo "${entry_str}" | awk '{print $4}'`

	# TODO: support md5='-', means not checking md5
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
	local download_dir="/tmp/ti/cache/download"
	mkdir -p "${download_dir}"
	local download_path="${download_dir}/${download_name}.`date +%s`.${RANDOM}"

	local error_handle="$-"
	set +e
	#wget -nv -nd --show-progress --progress=bar:force:noscroll -P "${cache_dir}" "${url}" -O "${download_path}" 2>&1
	wget --quiet -nd -P "${cache_dir}" "${url}" -O "${download_path}"
	local code="$?"
	restore_error_handle_flags "${error_handle}"

	if [ "${code}" != "0" ]; then
		echo "[func cp_bin_to_dir_from_urls] wget --quiet -nd -P '${cache_dir}' '${url}' -O '${download_path}' failed" >&2
		rm -f "${download_path}"
		return 1
	fi
	if [ ! -f "${download_path}" ]; then
		echo "[func cp_bin_to_dir_from_urls] '${url}': wget to '${download_path}' file not found" >&2
		return 1
	fi

	local download_ext="`print_file_ext "${download_name}"`"
	local download_is_tar=`echo "${download_name}" | { grep '.tar.gz' || test $? = 1; }`
	local download_is_tgz=`echo "${download_name}" | { grep '.tgz$' || test $? = 1; }`

	if [ ! -z "${download_is_tar}" ]; then
		local target_tmp_path="${cache_dir}/${bin_name}.`date +%s`.${RANDOM}"
		tar -O -zxf "${download_path}" > "${target_tmp_path}"
		rm -f "${download_path}"
		mv -f "${target_tmp_path}" "${cache_dir}/${bin_name}"
		chmod +x "${cache_dir}/${bin_name}"
		mkdir -p "${dest_dir}"
		cp_when_diff "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
		return 0
	fi

	# TODO: extract tgz is cache dir
	if [ -z "${download_ext}" ] || [ ! -z "${download_is_tgz}" ]; then
		mv -f "${download_path}" "${cache_dir}/${bin_name}"
		local new_md5=`file_md5 "${cache_dir}/${bin_name}"`
		if [ "${new_md5}" == "${md5}" ]; then
			cp_when_diff "${cache_dir}/${bin_name}" "${dest_dir}/${bin_name}"
			return 0
		else
			echo "[func cp_bin_to_dir] ${md5}(${bin_urls_file}) != ${new_md5}(${cache_dir}/${bin_name}) md5 not matched" >&2
			return 1
		fi
	fi

	rm -f "${download_path}"
	echo "[func cp_bin_to_dir] TODO: support .${download_ext} file from url" >&2
	return 1
}
export -f cp_bin_to_dir_from_urls

function cp_bin_to_dir()
{
	if [ -z "${5+x}" ]; then
		echo "[func cp_bin_to_dir] usage: <func> name_of_bin_module dest_dir bin_paths_file bin_urls_file cache_dir [check_os_type]" >&2
		return 1
	fi

	local name="${1}"
	local dest_dir="${2}"
	local bin_paths_file="${3}"
	local bin_urls_file="${4}"
	local cache_dir="${5}"

	if [ -z "${6+x}" ]; then
		local check_os_type='true'
	else
		local check_os_type="${6}"
	fi

	if [ "${check_os_type}" == 'true' ] && [ `uname` == 'Darwin' ]; then
		local bin_urls_file="${bin_urls_file}.mac"
	fi

	local found=`cp_bin_to_dir_from_paths "${name}" "${dest_dir}" "${bin_paths_file}" "${cache_dir}"`
	if [ "${found}" != 'true' ]; then
		cp_bin_to_dir_from_urls "${name}" "${dest_dir}" "${bin_urls_file}" "${cache_dir}"
	fi
}
export -f cp_bin_to_dir

function render_templ()
{
	if [ -z "${3+x}" ]; then
		echo "[func render_templ] usage: <func> templ_file dest_file render_str(k=v#k=v#..)" >&2
		return 1
	fi

	local src="${1}"
	local dest="${2}"
	local kvs="${3}"

	local dest_dir=`dirname "${dest}"`
	mkdir -p "${dest_dir}"

	local here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
	python "${here}/render_templ.py" "${kvs}" < "${src}" > "${dest}"
}
export -f render_templ
