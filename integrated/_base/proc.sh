#!/bin/bash

function print_proc_cnt()
{
	if [ -z ${1+x} ]; then
		echo "[func print_proc_cnt] usage: <func> str_for_finding_the_processes [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		str2="${2}"
	fi

	local processes=`ps -ef | grep "${find_str}" | grep "${str2}" | grep -v grep`
	if [ -z "${processes}" ]; then
		echo "0"
	else
		echo "${processes}" | wc -l | awk '{print $1}'
	fi
}
export -f print_proc_cnt

function print_pid()
{
	if [ -z ${1+x} ]; then
		echo "[func print_pid] usage: <func> str_for_finding_the_process [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		str2="${2}"
	fi

	local processes=`ps -ef | grep "${find_str}" | grep "${str2}" | grep -v grep`
	if [ -z "${processes}" ]; then
		echo "[func print_pid] ${find_str} pid not found" >&2
		return 1
	fi
	local pid_count=`echo "${processes}" | wc -l | awk '{print $1}'`
	if [ "${pid_count}" != "1" ]; then
		echo "[func print_pid] ${find_str} pid count: ${pid_count} != 1" >&2
		return 1
	fi
	echo "${processes}" | awk '{print $2}'
}
export -f print_pid

function stop_proc()
{
	if [ -z ${1+x} ]; then
		echo "[func stop_proc] usage: <func> str_for_finding_the_process [str2] [fast_mode=false]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		str2="${2}"
	fi

	if [ -z ${3+x} ]; then
		local fast=""
	else
		local fast="${3}"
	fi

	local pid=`print_pid "${find_str}" "${str2}"`
	if [ -z "${pid}" ]; then
		return 1
	fi
	
	local heavy_kill="false"
	local heaviest_kill="false"

	if [ "${fast}" == "true" ]; then
		heaviest_kill="true"
	fi

	local error_handle="$-"
	set +e

	for ((i=0; i<600; i++)); do
		if [ "${heaviest_kill}" == "true" ]; then
			echo "#${i} pid ${pid} closing, using 'kill -9'..."
			kill -9 ${pid}
		else
			if [ "${heavy_kill}" == "true" ]; then
				if [ $((${i} % 3)) = 0 ] && [ ${i} -ge 10 ]; then
					echo "#${i} pid ${pid} closing..."
				fi
			else
				if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
					echo "#${i} pid ${pid} closing..."
				fi
			fi
			kill ${pid} 2>/dev/null
			if [ "${heavy_kill}" == "true" ]; then
				kill ${pid} 2>/dev/null
			fi
		fi

		if [ "${heaviest_kill}" != "true" ]; then
			sleep 0.05
		fi

		local pid_cnt=`print_proc_cnt "${find_str}" "${str2}"`
		if [ "${pid_cnt}" == "0" ]; then
			# echo "#${i} pid ${pid} closed"
			break
		fi

		sleep 0.5

		if [ ${i} -ge 29 ]; then
			heavy_kill="true"
		fi
		if [ ${i} -ge 49 ]; then
			heaviest_kill="true"
		fi
		if [ ${i} -ge 119 ]; then
			echo "pid ${pid} close failed" >&2
			exit 1
		fi
	done

	restore_error_handle_flags "${error_handle}"
}
export -f stop_proc

function _print_file_dir()
{
	local path="${1}"
	local dir=$(dirname "${path}")
	if [ -z "${dir}" ] || [ "${dir}" == '.' ]; then
		echo "${path}"
	else
		echo "${dir}"
	fi
}
export -f _print_file_dir

function _print_file_parent_dir()
{
	local path="${1}"
	local dir=$(dirname $(dirname "${path}"))
	if [ -z "${dir}" ] || [ "${dir}" == '.' ]; then
		echo "${path}"
	else
		echo "${dir}"
	fi
}
export -f _print_file_parent_dir

function ls_tiflash_proc()
{
	local processes=`ps -ef | grep tiflash | grep "\-\-config\-file" | \
		grep -v grep | awk -F '--config-file' '{print $2}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_parent_dir "${conf}"
		done
	fi
}
export -f ls_tiflash_proc

function ls_pd_proc()
{
	local processes=`ps -ef | grep pd-server | grep "\-\-config" | \
		grep -v grep | awk -F '--config=' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_dir "${conf}"
		done
	fi
}
export -f ls_pd_proc

function ls_tikv_proc()
{
	local processes=`ps -ef | grep tikv-server | grep -v tikv-server-rngine | grep "\-\-config" | \
		grep -v grep | awk -F '--config' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_dir "${conf}"
		done
	fi
}
export -f ls_tikv_proc

function ls_tidb_proc()
{
	local processes=`ps -ef | grep tidb-server | grep "\-\-config" | \
		grep -v grep | awk -F '--config=' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_dir "${conf}"
		done
	fi
}
export -f ls_tidb_proc

function ls_rngine_proc()
{
	local processes=`ps -ef | grep tikv-server-rngine | grep "\-\-config" | \
		grep -v grep | awk -F '--config' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_dir "${conf}"
		done
	fi
}
export -f ls_rngine_proc
