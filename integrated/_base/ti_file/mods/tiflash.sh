#!/bin/bash

function tiflash_run()
{
	if [ -z "${2+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func tiflash_run] usage: <func> tiflash_dir conf_templ_dir [daemon_mode] [pd_addr] [ports_delta] [listen_host] [cluster_id]" >&2
		return 1
	fi

	local tiflash_dir="${1}"
	local conf_templ_dir="${2}"

	if [ -z "${3+x}" ]; then
		local daemon_mode="false"
	else
		local daemon_mode="${3}"
	fi

	if [ -z "${4+x}" ]; then
		local pd_addr=""
	else
		local pd_addr="${4}"
	fi

	if [ -z "${5+x}" ]; then
		local ports_delta="0"
	else
		local ports_delta="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local listen_host=""
	else
		local listen_host="${6}"
	fi

	if [ -z "${7+x}" ]; then
		local cluster_id="<none>"
	else
		local cluster_id="${7}"
	fi

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func tiflash_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_http_port=`get_value "${default_ports}" 'tiflash_http_port'`
	if [ -z "${default_tiflash_http_port}" ]; then
		echo "[func tiflash_run] get default tiflash_http_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_tcp_port=`get_value "${default_ports}" 'tiflash_tcp_port'`
	if [ -z "${default_tiflash_tcp_port}" ]; then
		echo "[func tiflash_run] get default tiflash_tcp_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_interserver_http_port=`get_value "${default_ports}" 'tiflash_interserver_http_port'`
	if [ -z "${default_tiflash_interserver_http_port}" ]; then
		echo "[func tiflash_run] get default tiflash_interserver_http_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_raft_and_cop_port=`get_value "${default_ports}" 'tiflash_raft_and_cop_port'`
	if [ -z "${default_tiflash_raft_and_cop_port}" ]; then
		echo "[func tiflash_run] get default tiflash_raft_and_cop_port from ${default_ports} failed" >&2
		return 1
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")

	if [ -z "${listen_host}" ]; then
		local listen_host="`must_print_ip`"
	fi

	mkdir -p "${tiflash_dir}"

	if [ ! -d "${tiflash_dir}" ]; then
		echo "[func tiflash_run] ${tiflash_dir} is not a dir" >&2
		return 1
	fi

	local tiflash_dir=`abs_path "${tiflash_dir}"`

	local conf_file="${tiflash_dir}/conf/config.xml"

	local proc_cnt=`print_proc_cnt "${conf_file}" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	local http_port=$((${ports_delta} + ${default_tiflash_http_port}))
	local tcp_port=$((${ports_delta} + ${default_tiflash_tcp_port}))
	local interserver_http_port=$((${ports_delta} + ${default_tiflash_interserver_http_port}))
	local tiflash_raft_and_cop_port=$((${ports_delta} + ${default_tiflash_raft_and_cop_port}))

	local render_str="tiflash_dir=${tiflash_dir}"
	local render_str="${render_str}#tiflash_pd_addr=${pd_addr}"
	local render_str="${render_str}#tiflash_listen_host=${listen_host}"
	local render_str="${render_str}#tiflash_http_port=${http_port}"
	local render_str="${render_str}#tiflash_tcp_port=${tcp_port}"
	local render_str="${render_str}#tiflash_interserver_http_port=${interserver_http_port}"
	local render_str="${render_str}#tiflash_raft_and_cop_port=${tiflash_raft_and_cop_port}"

	render_templ "${conf_templ_dir}/tiflash/config.xml" "${conf_file}" "${render_str}"
	cp_when_diff "${conf_templ_dir}/tiflash/users.xml" "${tiflash_dir}/conf/users.xml"

	# TODO: remove hard code file name from this func
	local target_lib_dir="tiflash_lib"
	if [ ! -d "${tiflash_dir}/${target_lib_dir}" ]; then
		local lib_file_name="tiflash_lib.tgz"
		if [ ! -f "${tiflash_dir}/${lib_file_name}" ]; then
			echo "[func tiflash_run] cannot find lib file"
			return 1
		fi
		tar -zxf "${tiflash_dir}/${lib_file_name}" -C ${tiflash_dir} 1>/dev/null
		rm -f "${tiflash_dir}/${lib_file_name}"
	fi

	if [ "${daemon_mode}" == "false" ]; then
		if [ `uname` != "Darwin" ]; then
			# TODO: check whether the following path exists before use it
			local lib_path="/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:${tiflash_dir}/${target_lib_dir}"
			if [ -z "${LD_LIBRARY_PATH+x}" ]; then
				export LD_LIBRARY_PATH="$lib_path"
			else
				export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$lib_path"
			fi
		fi
		"${tiflash_dir}/tiflash" server --config-file "${conf_file}"
		return $?
	fi

	local info="${tiflash_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "interserver_http_port	${interserver_http_port}" >> "${info}"
	echo "raft_and_cop_port	${tiflash_raft_and_cop_port}" >> "${info}"
	echo "http_port	${http_port}" >> "${info}"
	echo "tcp_port	${tcp_port}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	if [ `uname` != "Darwin" ]; then
		# TODO: check whether the following path exists before use it
		local lib_path="/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:${tiflash_dir}/${target_lib_dir}"
		if [ -z "${LD_LIBRARY_PATH+x}" ]; then
			echo "export LD_LIBRARY_PATH=\"$lib_path\"" > "${tiflash_dir}/run.sh"
		else
			echo "export LD_LIBRARY_PATH=\"$LD_LIBRARY_PATH:$lib_path\"" > "${tiflash_dir}/run.sh"
		fi
		echo "nohup \"${tiflash_dir}/tiflash\" server --config-file \"${conf_file}\" 1>/dev/null 2>&1 &" >> "${tiflash_dir}/run.sh"
	else
		echo "nohup \"${tiflash_dir}/tiflash\" server --config-file \"${conf_file}\" 1>/dev/null 2>&1 &" > "${tiflash_dir}/run.sh"
	fi

	chmod +x "${tiflash_dir}/run.sh"
	bash "${tiflash_dir}/run.sh"

	sleep 0.1
	local pid=`must_print_pid "${conf_file}" "\-\-config"`
	if [ -z "${pid}" ]; then
		echo "[func tiflash_run] pid not found, failed" >&2
		return 1
	fi
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f tiflash_run
