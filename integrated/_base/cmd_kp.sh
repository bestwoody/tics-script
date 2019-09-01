#!/bin/bash

source "${integrated}/_base/kp_file.sh"

function kp_mon_report
{
	if [ -z "${1+x}" ]; then
		echo "[func kp_mon_report] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	local log="${file}.log"
	if [ ! -f "${file}" ] || [ ! -f "${log}" ]; then
		return
	fi

	local logs=`tail -n 9999 "${log}" | grep -v 'RUNNING'`

	kp_file_iter "${file}" | while read line; do
		local last_msg=`echo "${logs}" | grep "${line}" | tail -n 1 | grep 'ERROR'`
		if [ ! -z "${last_msg}" ]; then
			echo "${last_msg}"
		fi
	done
}
export -f kp_mon_report

function cmd_kp()
{
	local file="${1}"
	local cmd="${2}"

	auto_error_handle

	local help_str="[func cmd_kp] usage: <func> kp_file [cmd=run|stop|status|list]"

	if [ -z "${file}" ]; then
		echo "${help_str}" >&2
		return 1
	fi

	if [ -z "${cmd}" ]; then
		local cmd='status'
	fi
	if [ "${cmd}" == 'up' ]; then
		local cmd='run'
	fi
	if [ "${cmd}" == 'down' ]; then
		local cmd='stop'
	fi
	if [ "${cmd}" == 'ls' ]; then
		local cmd='list'
	fi

	local file_abs=`abs_path "${file}"`
	local mon_pid=`_kp_file_pid ${file_abs}.mon`

	if [ "${cmd}" == 'run' ]; then
		if [ ! -z "${mon_pid}" ]; then
			echo "=> [monitor] ${file}"
			echo '   running, skipped'
		else
			echo "# This file is generated" >"${file_abs}.mon"
			echo "kp_file_run \"${file_abs}\"" >>"${file_abs}.mon"
			nohup bash "${integrated}"/_base/call_func.sh \
				keep_script_running "${file_abs}.mon" 'false' '' 10 10 >/dev/null 2>&1 &
			echo "=> [monitor] ${file}"
			echo '   starting'
		fi
		echo "=> [tasks]"
		kp_file_iter "${file}" | awk '{print "   "$0}'
	elif [ "${cmd}" == 'stop' ]; then
		echo "=> [monitor] ${file}"
		if [ ! -z "${mon_pid}" ]; then
			kill "${mon_pid}"
			echo '   stopping'
		else
			echo '   nor running, skipped'
		fi
		kp_file_stop "${file}"
	elif [ "${cmd}" == 'status' ]; then
		local atime=`_kp_sh_last_active "${file}"`
		local atime=", actived ${atime}s ago"
		echo "=> [monitor] ${file}"
		if [ ! -z "${mon_pid}" ]; then
			echo "   running${atime}"
		else
			echo "   not running${atime}"
		fi
		kp_mon_report "${file}" | awk '{print "   "$0}'
		echo
		kp_file_status "${file}"
	elif [ "${cmd}" == 'list' ]; then
		kp_file_iter "${file}"
	else
		echo "${cmd}: unknow command" >&2
		echo "${help_str}" >&2
		return 1
	fi
}
export -f cmd_kp
