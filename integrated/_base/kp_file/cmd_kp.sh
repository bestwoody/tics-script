#!/bin/bash

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

	# TODO: Improve this: generate empty lines by `cat random-file`
	local random=`cat "${BASH_SOURCE[0]}" | awk '{print "-"}'`
	local lines=`cat "${log}" | grep 'START\|RUNNING\|ERROR\|STOP' | tail -n 800`
	echo -e "${random}\n${lines}" | \
		awk '{if ($3 == "START") print "\033[32m+\033[0m"; else if ($3 == "RUNNING") print "\033[32m-\033[0m"; else if ($3 == "ERROR") print "\033[31mE\033[0m"; else if ($3 == "STOP") print "\033[35m!\033[0m"; else print "-"}' | tail -n 80 | \
		tr "\n" ' ' | sed 's/ //g' | awk '{print "\033[32m<<\033[0m"$0}'
}
export -f kp_mon_report

function cmd_kp()
{
	local file="${1}"
	local cmd="${2}"

	auto_error_handle

	local help_str="[func cmd_kp] usage: <func> kp_file [cmd=run|stop|status|list|clean]"

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
			echo "=> [^__^] ${file_abs}"
			echo '   running, skipped'
		else
			echo "# This file is generated" >"${file_abs}.mon"
			echo "kp_file_run \"${file_abs}\"" >>"${file_abs}.mon"
			nohup bash "${integrated}"/_base/call_func.sh \
				keep_script_running "${file_abs}.mon" 'false' '' 10 10 >/dev/null 2>&1 &
			echo "=> [^__^] ${file_abs}"
			echo '   starting'
		fi
		echo "=> [task] (s)"
		kp_file_iter "${file}" | awk '{print "   "$0}'
	elif [ "${cmd}" == 'stop' ]; then
		echo "=> [^__^] ${file_abs}"
		if [ ! -z "${mon_pid}" ]; then
			local error_handle="$-"
			set +e
			kill "${mon_pid}" 2>/dev/null
			restore_error_handle_flags "${error_handle}"
			echo '   stopping'
		else
			echo '   nor running, skipped'
		fi
		kp_file_stop "${file}"
	elif [ "${cmd}" == 'status' ]; then
		local atime=`_kp_sh_last_active "${file}"`
		if [ ! -z "${mon_pid}" ]; then
			local run_status="\033[32m[+]\033[0m"
		else
			local run_status="\033[31m[!]\033[0m"
		fi
		echo -e "${run_status}\033[36m [^__^] ${file_abs}\033[0m \033[35m${atime}s\033[0m"
		kp_mon_report "${file}" | awk '{print "    "$0}'
		kp_file_status "${file}"
	elif [ "${cmd}" == 'list' ]; then
		kp_file_iter "${file}"
	elif [ "${cmd}" == 'clean' ]; then
		rm -f "${file}.mon"
		rm -f "${file}.log"
		kp_file_iter "${file}" | while read line; do
			rm -f "${line}.log"
			rm -f "${line}.err.log"
			rm -f "${line}.data"
			rm -f "${line}.report"
		done
	else
		echo "${cmd}: unknow command" >&2
		echo "${help_str}" >&2
		return 1
	fi
}
export -f cmd_kp
