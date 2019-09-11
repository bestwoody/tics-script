#!/bin/bash

function kp_file_watch
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo "[func kp_file_watch] usage: <func> kp_file [watch_interval=5] [display_width=120]" >&2
		exit 1
	fi

	local file="${1}"
	if [ -z "${2+x}" ]; then
		local interval='5'
	else
		local interval="${2}"
	fi
	if [ -z "${3+x}" ]; then
		local width='120'
	else
		local width="${3}"
	fi

	watch -c -n "${interval}" -t "COLUMNS= ${integrated}/ops/kp.sh \"${file}\" status \"${width}\""
}

function kp_mon_report
{
	if [ -z "${2+x}" ]; then
		echo "[func kp_mon_report] usage: <func> kp_file width" >&2
		return 1
	fi

	local file="${1}"
	local log="${file}.log"
	if [ ! -f "${file}" ] || [ ! -f "${log}" ]; then
		return
	fi

	local width="${2}"

	local logs=`tail -n 9999 "${log}" | grep -v 'RUNNING'`

	kp_file_iter "${file}" | while read line; do
		local last_msg=`echo "${logs}" | grep "${line}" | tail -n 1 | grep 'ERROR'`
		if [ ! -z "${last_msg}" ]; then
			echo "${last_msg}"
		fi
	done

	local random=`yes '-' | head -n 999`
	local lines=`cat "${log}" | grep 'START\|RUNNING\|ERROR\|STOP' | tail -n 999`
	echo -e "${random}\n${lines}" | \
		awk '{if ($3 == "START") print "\033[32m+\033[0m"; else if ($3 == "RUNNING") print "\033[32m-\033[0m"; else if ($3 == "ERROR") print "\033[31mE\033[0m"; else if ($3 == "STOP") print "\033[35m!\033[0m"; else print "-"}' | tail -n "${width}" | \
		tr "\n" ' ' | sed 's/ //g' | awk '{print "\033[32m<<\033[0m"$0}'
}
export -f kp_mon_report

function cmd_kp()
{
	local file="${1}"
	local cmd="${2}"

	auto_error_handle

	local help_str="[func cmd_kp] usage: <func> kp_file [cmd=run|stop|status|list|clean|watch] [width=120]"

	if [ -z "${file}" ]; then
		echo "${help_str}" >&2
		return 1
	fi

	if [ -z "${2+x}" ]; then
		shift 1
	else
		shift 2
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
			echo "source \"${integrated}/_env.sh\"" >>"${file_abs}.mon"
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
			echo "${mon_pid}" | while read pid; do
				local error_handle="$-"
				set +e
				kill "${pid}" 2>/dev/null
				restore_error_handle_flags "${error_handle}"
			done
			local mon_pid=`_kp_file_pid ${file_abs}.mon`
			if [ -z "${mon_pid}" ]; then
				echo '   stopped'
			else
				echo '   stop failed'
			fi
		else
			echo '   nor running, skipped'
		fi
		kp_file_stop "${file}"
	elif [ "${cmd}" == 'status' ]; then
		if [ ! -z "${1+x}" ] && [ ! -z "${1}" ]; then
			local width="${1}"
		else
			local width='120'
		fi
		local atime=`_kp_sh_last_active "${file}"`
		if [ ! -z "${mon_pid}" ]; then
			local mon_proc_cnt=`echo "${mon_pid}" | wc -l | awk '{print $1}'`
			if [ "${mon_proc_cnt}" == '1' ]; then
				local run_status="\033[32m[+]\033[0m"
			else
				local run_status="\033[33m[?]\033[0m"
			fi
		else
			local run_status="\033[31m[!]\033[0m"
		fi
		echo -e "${run_status}\033[36m [^__^] ${file_abs}\033[0m \033[35m${atime}s\033[0m"
		kp_mon_report "${file}" "${width}" | awk '{print "    "$0}'
		kp_file_status "${file}" "${width}"
	elif [ "${cmd}" == 'list' ]; then
		kp_file_iter "${file}"
	elif [ "${cmd}" == 'watch' ]; then
		kp_file_watch "${file}" "${@}"
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
