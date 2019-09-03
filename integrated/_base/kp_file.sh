#!/bin/bash

# TODO: unused
function watch_file()
{
	if [ -z "${2+x}" ]; then
		echo "[func watch_file] usage: <func> file_path timeout" >&2
		return 1
	fi

	local file="${1}"
	local timeout="${2}"

	local latest_mtime=`file_mtime "${file}"`

	local unchanged_times='0'
	for (( i = 0; i < "${timeout}"; i++ )); do
		local mtime=`file_mtime "${file}"`
		if [ "${latest_mtime}" != "${mtime}" ]; then
			local unchanged_times='0'
			local latest_mtime="${mtime}"
			local i=0
			echo "changed!! #${i} < ${timeout} ${file}"
			continue
		fi
		local unchanged_times=$((unchanged_times + 1))
		echo "unchanged #${i} < ${timeout} ${file}"
		sleep 1
	done

	# TODO
}
export -f watch_file

# TODO: unused
function watch_files()
{
	if [ -z "${2+x}" ]; then
		echo "[func watch_files] usage: <func> dir_path timeout" >&2
		return 1
	fi

	local dir="${1}"
	local timeout="${2}"
	for file in "${dir}"; do
		watch_file "${file}" "${timeout}"
	done
}
export -f watch_files

function keep_script_running()
{
	if [ -z "${5+x}" ]; then
		echo "[func keep_script_running] usage: <func> script write_log args_string check_interval backoffs" >&2
		return 1
	fi

	local script="${1}"
	local write_log="${2}"
	local args="${3}"
	local interval="${4}"
	shift 4

	if [ "${write_log}" == 'true' ]; then
		local log="${script}.log"
		local err_log="${script}.err.log"
	else
		local log="/dev/null"
		local err_log="/dev/null"
	fi

	local backoffs=("${@}")
	local backoff_i=0

	if [ -z "${args}" ]; then
		local args_str=''
	else
		local args_str=" ${args}"
	fi

	while true; do
		local proc_cnt=`print_proc_cnt "bash ${script}" "${args}"`
		if [ "${proc_cnt}" == '1' ]; then
			echo "[`date +'%D %T'`] RUNNING ${script}${args_str}"
			local backoff_i=0
			sleep "${interval}"
			continue
		fi
		if [ "${proc_cnt}" != '0' ]; then
			echo "[`date +'%D %T'`] ERROR ${script}${args_str}: more than 1 instance: ${proc_cnt}"
			# TODO: for debug
			echo "DUMP: bash ${script} ${args}"
			ps -ef | grep "bash ${script}" | grep "${args}"
			local backoff_i=0
			sleep "${interval}"
			continue
		fi

		local backoff="${backoffs[${backoff_i}]}"
		local backoff_i=$((backoff_i + 1))
		if [ ${backoff_i} -ge ${#backoffs[@]} ]; then
			local backoff_i=$((${#backoffs[@]} - 1))
		fi

		local ts=`date +%s`
		local time=`date +'%D %T'`
		echo "!RUN ${ts} [${time}]" >> "${log}"
		echo "!RUN ${ts} [${time}]" >> "${err_log}"

		local error_handle="$-"
		set +e
		nohup bash "${script}" ${args} >> "${log}" 2>> "${err_log}" && \
			echo "!END ${ts} [`date +'%D %T'`]" >> "${log}" &
		sleep 0.05
		restore_error_handle_flags "${error_handle}"

		local proc_cnt=`print_proc_cnt "bash ${script}"`
		if [ "${proc_cnt}" == '1' ]; then
			echo "[`date +'%D %T'`] START ${script}${args_str}"
			continue
		fi

		echo "[`date +'%D %T'`] ERROR ${script}${args_str}: exited too quick, retry in ${backoff} secs"
		sleep "${backoff}"
	done
}
export -f keep_script_running

function _kp_iter()
{
	if [ -z "${1+x}" ]; then
		echo "[func _kp_iter] usage: <func> path [ignored_file_list]" >&2
		return 1
	fi

	local path="${1}"
	if [ -z "${2+x}" ]; then
		local ignoreds=''
	else
		local ignoreds="${2}"
	fi

	if [ -f "${path}" ]; then
		local ext=`print_file_ext "${path}"`
		if [ "${ext}" != 'sh' ]; then
			return
		fi
		abs_path "${path}"
	elif [ -d "${path}" ]; then
		for file in "${path}"/*; do
			local ignored=`echo "${ignoreds}" | grep "${file}"`
			if [ ! -z "${ignored}" ] && [ "${ignored}" == "!${file}" ]; then
				continue
			fi
			_kp_iter "${file}" "${ignoreds}"
		done
	else
		echo "[func _kp_iter] ${path} is not file or dir" >&2
		return 1
	fi
}
export -f _kp_iter

function kp_file_iter()
{
	if [ -z "${1+x}" ]; then
		echo "[func _kp_file_iter] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	if [ ! -f "${file}" ]; then
		echo "[func kp_file_iter] ${file} is not file" >&2
		return 1
	fi
	local file_abs=`abs_path "${file}"`
	local file_dir=`dirname "${file_abs}"`

	local rendered="/tmp/kp_file_iter.rendered.`date +%s`.${RANDOM}"
	rm -f "${rendered}"

	local lines=`cat "${file_abs}" | grep -v '^#' | grep -v '^$'`
	local uniq_lines=`echo "${lines}" | sort | uniq`
	local lines_cnt=`echo "${lines}" | wc -l | awk '{print $1}'`
	local uniq_cnt=`echo "${uniq_lines}" | wc -l | awk '{print $1}'`
	if [ "${uniq_cnt}" != "${lines_cnt}" ]; then
		echo "[func kp_file_iter] ${file} has duplicated lines ${uniq_cnt} != ${lines_cnt}" >&2
		return 1
	fi

	echo "${lines}" | while read line; do
		if [ "${line:0:1}" == '!' ]; then
			local ignored='true'
			local line="${line:1}"
		else
			local ignored='false'
		fi
		if [ "${line:0:1}" != '/' ]; then
			local line="${file_dir}/${line}"
		fi
		if [ "${ignored}" == 'true' ]; then
			local line="!${line}"
		fi
		echo "${line}" >> "${rendered}"
	done

	local ignoreds=`grep '^!' "${rendered}" | sort | uniq`
	cat "${rendered}" | grep -v '^!' | while read line; do
		_kp_iter "${line}" "${ignoreds}"
	done

	rm -f "${rendered}"
}
export -f kp_file_iter

function _kp_file_pid()
{
	if [ -z "${1+x}" ]; then
		echo "[func _kp_file_pid] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	local pid=`ps -ef | grep "keep_script_running ${file}" | grep -v grep | awk '{if($3==1) print $2}'`
	if [ ! -z "${pid}" ]; then
		echo "${pid}"
		return
	fi

	local processes=`ps -ef | grep "keep_script_running ${file}" | grep -v grep`
	local processed=`ps -ef | grep "keep_script_running ${file}" | grep -v grep | awk '{print $2, $3}'`
	local proc_cnt=`echo "${processed}" | wc -l | awk '{print $1}'`
	if [ "${proc_cnt}" == '1' ]; then
		echo "${processed}" | head -n 1 | awk '{print $1}'
	fi
	if [ "${proc_cnt}" != '2' ]; then
		return
	fi

	local l1c1=`echo "${processed}" | head -n 1 | awk '{print $1}'`
	local l1c2=`echo "${processed}" | head -n 1 | awk '{print $2}'`
	local l2c1=`echo "${processed}" | tail -n 1 | awk '{print $1}'`
	local l2c2=`echo "${processed}" | tail -n 1 | awk '{print $2}'`
	if [ "${l1c1}" == "${l2c2}" ]; then
		echo "${l1c1}"
	elif [ "${l2c1}" == "${l1c2}" ]; then
		echo "${l2c1}"
	fi
}
export -f _kp_file_pid

function kp_file_run()
{
	if [ -z "${1+x}" ]; then
		echo "[func kp_file_run] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	local file_dir=$(dirname `abs_path "${file}"`)

	grep '^!' "${file}" | sort | uniq | while read line; do
		if [ "${line:0:1}" != '/' ]; then
			local line="${file_dir}/${line:1}"
		else
			local line="${line:1}"
		fi
		local result=`kp_sh_stop "${line}"`
		local skipped=`echo "${result}" | grep 'skipped'`
		if [ -z "${skipped}" ]; then
			echo "[`date +'%D %T'`] STOP ${line}" >> "${file}.log"
		fi
	done

	kp_file_iter "${file}" | while read line; do
		local pid=`_kp_file_pid "${line}"`
		echo "=> [task] ${line}"
		if [ ! -z "${pid}" ]; then
			echo "   running, skipped"
		else
			nohup bash "${integrated}"/_base/call_func.sh \
				keep_script_running "${line}" 'true' '' 9 1 2 3 4 8 16 32 >> "${file}.log" 2>&1 &
			echo "   starting"
		fi
	done
}
export -f kp_file_run

function kp_sh_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func kp_sh_stop] usage: <func> sh_file [quiet]" >&2
		return 1
	fi

	local file="${1}"
	if [ -z "${2+x}" ]; then
		local quiet='false'
	else
		local quiet="${2}"
	fi

	local pid=`_kp_file_pid "${file}"`
	if [ -z "${pid}" ]; then
		if [ "${quiet}" != 'true' ]; then
			echo "=> [task] ${file}"
			echo "   not running, skipped"
		fi
		return
	fi

	if [ "${quiet}" != 'true' ]; then
		echo "=> [task] ${file}"
		echo "   stopping"
	fi

	local error_handle="$-"
	set +e
	kill "${pid}" 2>/dev/null
	local pid=`_kp_file_pid "${file}"`
	if [ ! -z "${pid}" ]; then
		kill "${pid}" 2>/dev/null
	fi
	restore_error_handle_flags "${error_handle}"

	if [ "${quiet}" != 'true' ]; then
		stop_proc "bash ${file}"
	else
		stop_proc "bash ${file}" 1>/dev/null
	fi

	local clear_script="${file}.term"
	if [ -f "${clear_script}" ]; then
		local result=`bash "${clear_script}" 2>&1`
		if [ "$?" != 0 ] || [ ! -z "${result}" ]; then
			echo "${result}" > "${clear_script}.log"
		fi
	fi
}
export -f kp_sh_stop

function kp_file_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func kp_file_stop] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	local error_handle="$-"
	set +e
	kp_file_iter "${file}" | while read line; do
		local result=`kp_sh_stop "${line}"`
		local skipped=`echo "${result}" | grep 'skipped'`
		if [ -z "${skipped}" ]; then
			echo "[`date +'%D %T'`] STOP ${line}" >> "${file}.log"
		fi
	done
	restore_error_handle_flags "${error_handle}"
}
export -f kp_file_stop

function _kp_sh_last_active()
{
	if [ -z "${1+x}" ]; then
		echo "[func _kp_sh_last_active] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	local atime='0'
	if [ -f "${file}.log" ]; then
		local atime=`file_mtime "${file}.log"`
	fi
	if [ -f "${file}.err.log" ]; then
		local err_atime=`file_mtime "${file}.err.log"`
		if [ ${err_atime} -gt ${atime} ]; then
			local atime="${err_atime}"
		fi
	fi

	local now=`date +%s`
	if [ "${atime}" != '0' ]; then
		echo $((now - atime))
	else
		echo '(unknown)'
	fi
}
export -f _kp_sh_last_active

function kp_file_status()
{
	if [ -z "${1+x}" ]; then
		echo "[func kp_file_status] usage: <func> kp_file" >&2
		return 1
	fi

	local file="${1}"
	if [ ! -f "${file}" ]; then
		echo "[func kp_file_status] ${file} is not a file" >&2
		return 1
	fi

	local log="${file}.log"
	if [ -f "${log}" ]; then
		local logs=`tail -n 9999 "${log}" | grep -v 'RUNNING'`
	else
		local logs=''
	fi

	kp_file_iter "${file}" | while read line; do
		local status=''
		local atime=`_kp_sh_last_active "${line}"`
		if [ -f "${line}.log" ]; then
			local start_time=`tail -n 99999 "${line}.log" | grep '!RUN' | tail -n 1 | awk '{print $2}'`
		else
			local start_time=''
		fi
		if [ ! -z "${start_time}" ]; then
			local now=`date +%s`
			local stime=$((now - start_time))
			local status="started/actived (${stime}s/${atime}s) ago"
		elif [ ! -z "${atime}" ]; then
			local status="actived ${atime}s ago"
		fi

		local pid=`_kp_file_pid "${line}"`
		echo -e "\033[36m=>\033[0m \033[36m[task] ${line}\033[0m"
		if [ -z "${pid}" ]; then
			echo -e "   \033[31mnot running\033[0m, ${status}"
		else
			echo -e "   running, ${status}"
		fi

		if [ -f "${line}.report" ]; then
			cat "${line}.report" | awk '{print "   \033[34m"$0"\033[0m"}'
		fi

		if [ ! -z "${start_time}" ] && [ -f "${line}.err.log" ]; then
			local stderr=`tail -n 9999 "${line}.err.log" | grep "RUN ${start_time}" -A 9999 | grep -v "${start_time}"`
			if [ ! -z "${stderr}" ]; then
				local err_cnt=`echo "${stderr}" | wc -l | awk '{print $1}'`
				if [ "${err_cnt}" -gt '6' ]; then
					local stderr=`echo "$stderr" | tail -n 6`
					echo '   ..'
				fi
				echo "${stderr}" | awk '{print "   \033[33m"$0"\033[0m"}'
			fi
		fi

		python "${integrated}/_base/kp_log_report.py" "${line}.log" \
			"${line}.err.log" color | awk '{print "   \033[32m<<\033[0m"$0}'
	done
}
export -f kp_file_status
