#!/bin/bash

function tikv_run()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func tikv_run] usage: <func> tikv_dir conf_templ_dir pd_addr [advertise_host] [ports_delta] [cluster_id]" >&2
		return 1
	fi

	local tikv_dir="${1}"
	local tikv_dir=`abs_path "${tikv_dir}"`

	local conf_templ_dir="${2}"
	local pd_addr="${3}"

	if [ -z "${4+x}" ]; then
		local advertise_host=""
	else
		local advertise_host="${4}"
	fi

	if [ -z "${5+x}" ]; then
		local ports_delta="0"
	else
		local ports_delta="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local cluster_id="<none>"
	else
		local cluster_id="${6}"
	fi

	echo "=> run tikv: ${tikv_dir}"

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "   get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tikv_port=`get_value "${default_ports}" 'tikv_port'`
	if [ -z "${default_tikv_port}" ]; then
		echo "   get default tikv_port from ${default_ports} failed" >&2
		return 1
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")

	if [ -z "${advertise_host}" ]; then
		local ip_cnt="`print_ip_cnt`"
		if [ "${ip_cnt}" != "1" ]; then
			local advertise_host="127.0.0.1"
		else
			local advertise_host="`print_ip`"
		fi
	fi

	local listen_host=""
	if [ "${advertise_host}" != "127.0.0.1" ] || [ "${advertise_host}" != "localhost" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local tikv_port=$((${ports_delta} + ${default_tikv_port}))

	local proc_cnt=`print_proc_cnt "${tikv_dir}/tikv.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "   running(${proc_cnt}), skipped"
		return 0
	fi

	local disk_avail=`df -k "${tikv_dir}" | tail -n 1 | awk '{print $4}'`
	local max_capacity=$(( 500 * 1024 * 1024 ))
	if [ ${disk_avail} -gt ${max_capacity} ]; then
		local disk_avail=${max_capacity}
	fi
	local disk_avail=$(( ${disk_avail} * 1024 ))

	local render_str="disk_avail=${disk_avail}"
	render_templ "${conf_templ_dir}/tikv.toml" "${tikv_dir}/tikv.toml" "${render_str}"

	local info="${tikv_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "tikv_port	${tikv_port}" >> "${info}"
	echo "advertise_host	${advertise_host}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${tikv_dir}/tikv-server\" \\" > "${tikv_dir}/run.sh"
	echo "	--addr \"${listen_host}:${tikv_port}\" \\" >> "${tikv_dir}/run.sh"
	echo "	--advertise-addr \"${advertise_host}:${tikv_port}\" \\" >> "${tikv_dir}/run.sh"
	echo "	--pd \"${pd_addr}\" \\" >> "${tikv_dir}/run.sh"
	echo "	--data-dir \"${tikv_dir}/data\" \\" >> "${tikv_dir}/run.sh"
	echo "	--log-level info \\" >> "${tikv_dir}/run.sh"
	echo "	--config \"${tikv_dir}/tikv.toml\" \\" >> "${tikv_dir}/run.sh"
	echo "	--log-file \"${tikv_dir}/tikv.log\" \\" >> "${tikv_dir}/run.sh"
	echo "	2>> \"${tikv_dir}/tikv_stderr.log\" 1>&2 &" >> "${tikv_dir}/run.sh"

	chmod +x "${tikv_dir}/run.sh"
	bash "${tikv_dir}/run.sh"

	sleep 0.3
	local pid=`must_print_pid "${tikv_dir}/tikv.toml" "\-\-config" 2>/dev/null`
	if [ -z "${pid}" ]; then
		echo "   pid not found, failed" >&2
		return 1
	fi
	echo "pid	${pid}" >> "${info}"
	echo "   ${pid}"
}
export -f tikv_run
