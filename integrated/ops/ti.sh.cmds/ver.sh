#!/bin/bash

function cmd_ti_ver()
{
	local index="${1}"
	local mod_name="${2}"
	local dir="${3}"
	local conf_rel_path="${4}"
	local host="${5}"

	if [ ! -z "${6+x}" ]; then
		local mode="${6}"
	else
		local mode="normal"
	fi

	if [ "${mode}" == 'full' ]; then
		local grep_str=''
	elif [ "${mode}" == 'ver' ]; then
		local grep_str='Release Version'
	elif [ "${mode}" == 'githash' ]; then
		local grep_str='Git Commit Hash'
	else
		local grep_str='Git Commit Hash\|Git Commit Branch\|Release Version\|UTC Build Time'
	fi

	if [ "${mod_name}" == 'pd' ]; then
		if [ ! -f "${dir}/pd-server" ]; then
			local res='MISSED'
		else
			local res=`"${dir}"/pd-server --version | { grep "${grep_str}" || test $? = 1; }`
		fi
	elif [ "${mod_name}" == 'tikv' ]; then
		if [ ! -f "${dir}/tikv-server" ]; then
			local res='MISSED'
		else
			local res=`"${dir}"/tikv-server --version | { grep -v "^TiKV" || test $? = 1; } | \
				{ grep "${grep_str}" || test $? = 1; }`
		fi
	elif [ "${mod_name}" == 'tidb' ]; then
		if [ ! -f "${dir}/tidb-server" ]; then
			local res='MISSED'
		else
			local res=`"${dir}"/tidb-server -V 2>/dev/null | { grep "${grep_str}" || test $? = 1; }`
		fi
	elif [ "${mod_name}" == 'tiflash' ]; then
		if [ ! -f "${dir}/tiflash" ]; then
			local res='MISSED'
		else
			if [ `uname` == "Darwin" ]; then
				install_proxy_lib_on_mac "${dir}"
				local res=`${dir}/tiflash version`
			else
				local tiflash_lib_path="`get_tiflash_lib_path_for_linux "${dir}"`"
				local res=`LD_LIBRARY_PATH="${tiflash_lib_path}" "${dir}"/tiflash version`
			fi
			local tiflash_and_proxy_ver=`echo "${res}" | { grep "Release Version" || test $? = 1; }`
			local tiflash_ver=`echo "${tiflash_and_proxy_ver}" | head -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local proxy_ver=`echo "${tiflash_and_proxy_ver}" | tail -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local ver="${tiflash_ver}(proxy:${proxy_ver})"

			local tiflash_and_proxy_git_hash=`echo "${res}" | { grep "Git Commit Hash" || test $? = 1; }`
			local tiflash_git_hash=`echo "${tiflash_and_proxy_git_hash}"  | head -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local proxy_git_hash=`echo "${tiflash_and_proxy_git_hash}" | tail -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local git_hash="${tiflash_git_hash}(proxy:${proxy_git_hash})"

			local tiflash_and_proxy_branch=`echo "${res}" | { grep "Git Commit Branch" || test $? = 1; }`
			local tiflash_branch=`echo "${tiflash_and_proxy_branch}" | head -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local proxy_branch=`echo "${tiflash_and_proxy_branch}" | tail -n 1 | \
				awk -F ':' '{print $2}' | trim_space`
			local branch="${tiflash_branch}(proxy:${proxy_branch})"

			local tiflash_and_proxy_build_time=`echo "${res}" | { grep "UTC Build Time" || test $? = 1; }`
			local tiflash_build_time=`echo "${tiflash_and_proxy_build_time}" | head -n 1 | \
				awk -F ': ' '{print $2}' | trim_space`
			local proxy_build_time=`echo "${tiflash_and_proxy_build_time}" | tail -n 1 | \
				awk -F ': ' '{print $2}' | trim_space`
			local build_time="${tiflash_build_time}(proxy:${proxy_build_time})"

			local res="Release Version:   ${ver}\n"
			local res="${res}Git Commit Hash:   ${git_hash}\n"
			local res="${res}Git Commit Branch: ${branch}\n"
			local res="${res}UTC Build Time:    ${build_time}"
			local res=`echo -e "${res}" | { grep "${grep_str}" || test $? = 1; }`
		fi
	elif [ "${mod_name}" == 'spark_m' ]; then
		if [ ! -d "${dir}/spark" ]; then
			local res='MISSED'
		else
			local port=`get_value "${dir}/proc.info" 'thriftserver_port'`
			prepare_spark_env
			local raw_res=`${dir}/spark/bin/beeline --verbose=false --silent=true -u "jdbc:hive2://${host}:${port}" -e "select ti_version()" 2>/dev/null`
			local ver=`echo "${raw_res}" | { grep "Release Version" || test $? = 1; } | \
				awk -F ':' '{print $2}' | trim_space`
			local git_hash=`echo "${raw_res}" | { grep "Git Commit Hash" || test $? = 1; } | \
				awk -F ':' '{print $2}' | trim_space`
			local branch=`echo "${raw_res}" | { grep "Git Branch" || test $? = 1; } | \
				awk -F ':' '{print $2}' | trim_space`
			local build_time=`echo "${raw_res}" | { grep "UTC Build Time" || test $? = 1; } | \
				awk -F ': ' '{print $2}' | trim_space`
			local spark_version=`echo "${raw_res}" | { grep "Current Spark Version" || test $? = 1; } | \
				awk -F ': ' '{print $2}' | trim_space`
			if [ -z "${git_hash}" ]; then
				local res='MISSED'
			else
				local res="Release Version:   ${ver}\n"
				local res="${res}Git Commit Hash:   ${git_hash}\n"
				local res="${res}Git Commit Branch: ${branch}\n"
				local res="${res}UTC Build Time:    ${build_time}\n"
				local res="${res}Spark Version:     ${spark_version}\n"
				local res=`echo -e "${res}" | { grep "${grep_str}" || test $? = 1; }`
			fi
		fi
	elif [ "${mod_name}" == 'spark_w' ]; then
		# spark_w version is the same with spark_m, ignore it
		return 0
	else
		local res="TODO: get ${mod_name} version"
	fi

	if [ "${mode}" == 'ver' ]; then
		local res=`echo "${res}" | awk '{print $3}'`
		if [ -z "${res}" ]; then
			local res='unknown'
		fi
		echo "${mod_name}	${res}"
	elif [ "${mode}" == 'githash' ]; then
		local res=`echo "${res}" | awk '{print $4}'`
		if [ -z "${res}" ]; then
			local res='unknown'
		fi
		echo "${mod_name}	${res}"
	else
		echo "=> ${mod_name}"
		echo "${res}" | awk '{print "   "$0}'
	fi
}

set -euo pipefail
cmd_ti_ver "${@}"
