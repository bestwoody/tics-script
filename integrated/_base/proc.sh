#!/bin/bash

function print_procs()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_procs] usage: <func> str_for_finding_the_procs [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=''
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	ps -ef | { grep "${find_str}" || test $? = 1; } | { grep "${str2}" || test $? = 1; } | { grep -v 'grep' || test $? = 1; }
}
export -f print_procs

function print_pids()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_pids] usage: <func> str_for_finding_the_procs [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=''
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	print_procs "${find_str}" "${str2}" | awk '{print $2}'
}
export -f print_pids

function print_root_pids()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_root_pids] usage: <func> str_for_finding_the_procs [str2] [dump_if_not_uniq]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi
	local dump='true'
	if [ ! -z "${3+x}" ]; then
		local dump="${3}"
	fi

	local procs=`print_procs "${find_str}" "${str2}"`
	local here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
	local result=`echo "${procs}" | awk '{print $2, $3}' | python "${here}/print_root_pid.py"`
	if [ "${dump}" == 'true' ] && [ ! -z "${result}" ]; then
		local cnt=`echo "${result}" | wc -l | awk '{print $1}'`
		if [ "${cnt}" != '1' ]; then
			echo "DUMP START --(${find_str}, ${str2})" >&2
			echo "${procs}" >&2
			echo "DUMP END   --(${find_str}, ${str2})" >&2
		fi
	fi

	if [ ! -z "${result}" ]; then
		echo "${result}"
	fi
}
export -f print_root_pids

function print_pids_by_ppid()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_pids_by_ppid] usage: <func> ppid" >&2
		return 1
	fi

	local ppid="${1}"
	local pids=`ps -f --ppid "${ppid}" | { grep "${ppid}" || test $? = 1; } | awk '{print $2}'`
	echo "${pids}" | while read pid; do
		if [ -z "${pid}" ]; then
			continue
		fi
		echo "${pid}"
		print_pids_by_ppid "${pid}"
	done
}
export -f print_pids_by_ppid

function _print_sub_pids()
{
	local pids="${1}"
	echo "${pids}" | while read pid; do
		if [ ! -z "${pid}" ]; then
			print_pids_by_ppid "${pid}"
		fi
	done
}
export -f _print_sub_pids

function print_tree_pids
{
	if [ -z "${1+x}" ]; then
		echo "[func print_root_pids] usage: <func> str_for_finding_the_procs [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	local pids=`print_pids "${find_str}" "${str2}"`
	local sub_pids=`_print_sub_pids "${pids}"`

	echo "${pids}" | while read pid; do
		if [ -z "${pid}" ]; then
			continue
		fi
		local is_in_sub=`echo "${sub_pids}" | { grep "^${pid}$" || test $? = 1; }`
		if [ -z "${is_in_sub}" ]; then
			echo "${pid}"
		fi
	done

	if [ ! -z "${sub_pids}" ]; then
		echo "${sub_pids}"
	fi
}
export -f print_tree_pids

function print_proc_cnt()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_proc_cnt] usage: <func> str_for_finding_the_procs [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	local procs=`print_procs "${find_str}" "${str2}"`
	if [ -z "${procs}" ]; then
		echo "0"
	else
		echo "${procs}" | wc -l | awk '{print $1}'
	fi
}
export -f print_proc_cnt

# TODO: remove this
function print_pid()
{
	if [ -z "${1+x}" ]; then
		echo "[func print_pid] usage: <func> str_for_finding_the_process [str2]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	local procs=`print_procs "${find_str}" "${str2}"`
	if [ -z "${procs}" ]; then
		return 1
	fi
	local pid_count=`echo "${procs}" | wc -l | awk '{print $1}'`
	if [ "${pid_count}" != "1" ]; then
		echo "[func print_pid] ${find_str} pid count: ${pid_count} != 1" >&2
		return 1
	fi
	if [ ! -z "${procs}" ]; then
		echo "${procs}" | awk '{print $2}'
	fi
}
export -f print_pid

# TODO: Args: timeout sec
function stop_proc()
{
	if [ -z "${1+x}" ]; then
		echo "[func stop_proc] usage: <func> str_for_finding_the_process [str2] [fast_mode=false]" >&2
		return 1
	fi

	local find_str="${1}"
	local str2=""
	if [ ! -z "${2+x}" ]; then
		local str2="${2}"
	fi

	if [ -z "${3+x}" ]; then
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
		local heaviest_kill="true"
	fi

	local error_handle="$-"
	set +e

	for ((i=0; i<600; i++)); do
		if [ "${heaviest_kill}" == "true" ]; then
			echo "#${i} pid ${pid} closing, using 'kill -9'..."
			kill -9 "${pid}" 2>/dev/null
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
			kill "${pid}" 2>/dev/null
			if [ "${heavy_kill}" == "true" ]; then
				kill "${pid}" 2>/dev/null
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
			local heavy_kill="true"
		fi
		if [ ${i} -ge 49 ]; then
			local heaviest_kill="true"
		fi
		if [ ${i} -ge 119 ]; then
			echo "pid ${pid} close failed" >&2
			exit 1
		fi
	done

	restore_error_handle_flags "${error_handle}"
}
export -f stop_proc
