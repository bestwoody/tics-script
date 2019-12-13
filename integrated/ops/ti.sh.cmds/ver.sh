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
		if [ ! -f "${dir}/log/server.log" ]; then
			local res='MISSED'
		else
			local res=`cat "${dir}"/log/server.log | { grep -a 'TiFlash' || test $? = 1; } | \
				tail -n 1 | awk -F 'TiFlash version: TiFlash ' '{print $2}'`
			local ver=`echo "${res}" | awk '{print $1}'`
			local git_hash=`echo "${res}" | awk '{print $2}'`
			local git_hash="${git_hash#HEAD-}"
			local res=`echo -e "Release Version:   ${ver}\nGit Commit Hash:   ${git_hash}" | \
				{ grep "${grep_str}" || test $? = 1; }`
		fi
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
