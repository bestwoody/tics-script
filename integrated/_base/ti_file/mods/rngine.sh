#!/bin/bash

function rngine_run()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func rngine_run] usage: <func> rngine_dir conf_templ_dir pd_addr tiflash_addr [advertise_host] [ports_delta] [cluster_id]" >&2
		return 1
	fi

	local rngine_dir="${1}"
	local rngine_dir=`abs_path "${rngine_dir}"`

	local conf_templ_dir="${2}"
	local pd_addr="${3}"
	local tiflash_addr="${4}"

	if [ -z "${5+x}" ]; then
		local advertise_host=""
	else
		local advertise_host="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local ports_delta="0"
	else
		local ports_delta="${6}"
	fi

	if [ -z "${7+x}" ]; then
		local cluster_id="<none>"
	else
		local cluster_id="${7}"
	fi

	echo "=> run rngine: ${rngine_dir}"

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "   get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_rngine_port=`get_value "${default_ports}" 'rngine_port'`
	if [ -z "${default_rngine_port}" ]; then
		echo "   get default rngine_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_raft_and_cop_port=`get_value "${default_ports}" 'tiflash_raft_and_cop_port'`
	if [ -z "${default_tiflash_raft_and_cop_port}" ]; then
		echo "   get default tiflash_raft_and_cop_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_http_port=`get_value "${default_ports}" 'tiflash_http_port'`
	if [ -z "${default_tiflash_http_port}" ]; then
		echo "   get default tiflash_http_port from ${default_ports} failed" >&2
		return 1
	fi

	if [ -z "${tiflash_addr}" ]; then
		tiflash_addr="`must_print_ip`:${default_tiflash_raft_and_cop_port}"
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")

	if [ -z "${advertise_host}" ]; then
		local advertise_host="`must_print_ip`"
	fi

	local rngine_port=$((${ports_delta} + ${default_rngine_port}))
	local tiflash_http_port=$((${ports_delta} + ${default_tiflash_http_port}))

	if [ "${advertise_host}" != "127.0.0.1" ] || [ "${advertise_host}" != "localhost" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local proc_cnt=`print_proc_cnt "${rngine_dir}/rngine.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "   running(${proc_cnt}), skipped"
		return 0
	fi

	local disk_avail=`df -k "${rngine_dir}" | tail -n 1 | awk '{print $4}'`
	local max_capacity=$(( 500 * 1024 * 1024 ))
	if [ ${disk_avail} -gt ${max_capacity} ]; then
		local disk_avail=${max_capacity}
	fi
	local disk_avail=$(( ${disk_avail} * 1024 ))

	local render_str="tiflash_raft_addr=${tiflash_addr}"
	local render_str="${render_str}#tiflash_http_port=${tiflash_http_port}"
	local render_str="${render_str}#disk_avail=${disk_avail}"
	render_templ "${conf_templ_dir}/rngine.toml" "${rngine_dir}/rngine.toml" "${render_str}"

	local info="${rngine_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "rngine_port	${rngine_port}" >> "${info}"
	echo "advertise_host	${advertise_host}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "tiflash_raft_addr	${tiflash_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${rngine_dir}/tikv-server-rngine\" \\" > "${rngine_dir}/run.sh"
	echo "	--addr \"${listen_host}:${rngine_port}\" \\" >> "${rngine_dir}/run.sh"
	echo "	--advertise-addr \"${advertise_host}:${rngine_port}\" \\" >> "${rngine_dir}/run.sh"
	echo "	--pd \"${pd_addr}\" \\" >> "${rngine_dir}/run.sh"
	echo "	--data-dir \"${rngine_dir}/data\" \\" >> "${rngine_dir}/run.sh"
	echo "	--config \"${rngine_dir}/rngine.toml\" \\" >> "${rngine_dir}/run.sh"
	echo "	--log-file \"${rngine_dir}/rngine.log\" \\" >> "${rngine_dir}/run.sh"
	echo "	2>> \"${rngine_dir}/rngine_stderr.log\" 1>&2 &" >> "${rngine_dir}/run.sh"

	chmod +x "${rngine_dir}/run.sh"
	bash "${rngine_dir}/run.sh"

	sleep 0.3
	local pid=`must_print_pid "${rngine_dir}/rngine.toml" "\-\-config" 2>/dev/null`
	if [ -z "${pid}" ]; then
		echo "   pid not found, failed" >&2
		return 1
	fi
	echo "pid	${pid}" >> "${info}"
	echo "   ${pid}"
}
export -f rngine_run
