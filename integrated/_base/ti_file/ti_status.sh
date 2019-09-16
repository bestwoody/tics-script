#!/bin/bash

function _print_mod_info()
{
	local dir="${1}"
	if [ -d "${dir}" ] && [ -f "${dir}/proc.info" ]; then
		local cluster_id=`cat "${dir}/proc.info" | { grep 'cluster_id' || test $? = 1; } | awk -F '\t' '{print $2}'`
	else
		local cluster_id=''
	fi
	if [ ! -z "${cluster_id}" ]; then
		echo "   [deployed from ${cluster_id}] ${dir}"
	else
		echo "   [unmanaged by ops-ti] ${dir}"
	fi
}
export -f _print_mod_info

function ls_tiflash_proc()
{
	local processes=`ps -ef | { grep 'tiflash' || test $? = 1; } | { grep "\-\-config\-file" || test $? = 1; } | \
		{ grep -v grep || test $? = 1; } | awk -F '--config-file' '{print $2}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			local path=`_print_file_dir_when_abs "${conf}"`
			local path=`_print_file_dir_when_abs "${path}"`
			_print_mod_info "${path}"
		done
	fi
}
export -f ls_tiflash_proc

function ls_pd_proc()
{
	local processes=`ps -ef | { grep 'pd-server' || test $? = 1; } | { grep "\-\-config" || test $? = 1; } | \
		{ grep -v grep || test $? = 1; } | awk -F '--config=' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_mod_info `_print_file_dir_when_abs "${conf}"`
		done
	fi
}
export -f ls_pd_proc

function ls_tikv_proc()
{
	local processes=`ps -ef | { grep 'tikv-server' || test $? = 1; } | { grep -v 'tikv-server-rngine' || test $? = 1; } | { grep "\-\-config" || test $? = 1; } | \
		{ grep -v grep || test $? = 1; } | awk -F '--config' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_mod_info `_print_file_dir_when_abs "${conf}"`
		done
	fi
}
export -f ls_tikv_proc

function ls_tidb_proc()
{
	local processes=`ps -ef | { grep 'tidb-server' || test $? = 1; } | { grep "\-\-config" || test $? = 1; } | \
		{ grep -v grep || test $? = 1; } | awk -F '--config=' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_mod_info `_print_file_dir_when_abs "${conf}"`
		done
	fi
}
export -f ls_tidb_proc

function ls_rngine_proc()
{
	local processes=`ps -ef | { grep 'tikv-server-rngine' || test $? = 1; } | { grep "\-\-config" || test $? = 1; } | \
		{ grep -v grep || test $? = 1; } | awk -F '--config' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_mod_info `_print_file_dir_when_abs "${conf}"`
		done
	fi
}
export -f ls_rngine_proc
