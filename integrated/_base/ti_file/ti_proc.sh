#!/bin/bash

# TODO: Check if ip changed when run a module

function pd_run()
{
	if [ -z "${2+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func pd_run] usage: <func> pd_dir conf_templ_dir [name_ports_delta] [advertise_host] [pd_name] [initial_cluster] [cluster_id]" >&2
		return 1
	fi

	local pd_dir="${1}"
	local conf_templ_dir="${2}"

	if [ -z "${3+x}" ]; then
		local name_ports_delta="0"
	else
		local name_ports_delta="${3}"
	fi

	if [ -z "${4+x}" ]; then
		local advertise_host=""
	else
		local advertise_host="${4}"
	fi

	if [ -z "${5+x}" ]; then
		local pd_name=""
	else
		local pd_name="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local initial_cluster=""
	else
		local initial_cluster="${6}"
	fi

	if [ -z "${7+x}" ]; then
		local cluster_id="<none>"
	else
		local cluster_id="${7}"
	fi

	if [ -z "${advertise_host}" ]; then
		local ip_cnt="`print_ip_cnt`"
		if [ "${ip_cnt}" != "1" ]; then
			local advertise_host="127.0.0.1"
		else
			local advertise_host="`print_ip`"
		fi
	fi

	if [ -z "${pd_name}" ]; then
		local pd_name="pd${name_ports_delta}"
	fi

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func pd_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_pd_peer_port=`get_value "${default_ports}" 'pd_peer_port'`
	if [ -z "${default_pd_peer_port}" ]; then
		echo "[func pd_run] get default pd_peer_port from ${default_ports} failed" >&2
		return 1
	fi

	local pd_port=$((${name_ports_delta} + ${default_pd_port}))
	local peer_port=$((${name_ports_delta} + ${default_pd_peer_port}))

	if [ -z "${initial_cluster}" ]; then
		local initial_cluster="${pd_name}=http://${advertise_host}:${peer_port}"
	else
		local initial_cluster=$(cal_addr "${initial_cluster}" "${advertise_host}" "${default_pd_peer_port}" "${pd_name}")
	fi

	local pd_dir=`abs_path "${pd_dir}"`

	local proc_cnt=`print_proc_cnt "${pd_dir}/pd.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	cp_when_diff "${conf_templ_dir}/pd.toml" "${pd_dir}/pd.toml"

	local info="${pd_dir}/proc.info"
	echo "pd_name	${pd_name}" > "${info}"
	echo "advertise_host	${advertise_host}" >> "${info}"
	echo "pd_port	${pd_port}" >> "${info}"
	echo "peer_port	${peer_port}" >> "${info}"
	echo "initial_cluster	${initial_cluster}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${pd_dir}/pd-server\" \
		--name=\"${pd_name}\" \
		--client-urls=\"http://${advertise_host}:${pd_port}\" \
		--advertise-client-urls=\"http://${advertise_host}:${pd_port}\" \
		--peer-urls=\"http://${advertise_host}:${peer_port}\" \
		--advertise-peer-urls=\"http://${advertise_host}:${peer_port}\" \
		--data-dir=\"${pd_dir}/data\" \
		--initial-cluster=\"${initial_cluster}\" \
		--config=\"${pd_dir}/pd.toml\" \
		--log-file=\"${pd_dir}/pd.log\" 2>> \"${pd_dir}/pd_stderr.log\" 1>&2 &" > "${pd_dir}/run.sh"

	chmod +x "${pd_dir}/run.sh"
	bash "${pd_dir}/run.sh"

	local pid=`print_pid "${pd_dir}/pd.toml" "\-\-config"`
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f pd_run

function tikv_run()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func tikv_run] usage: <func> tikv_dir conf_templ_dir pd_addr [advertise_host] [ports_delta] [cluster_id]" >&2
		return 1
	fi

	local tikv_dir="${1}"
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

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func tikv_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tikv_port=`get_value "${default_ports}" 'tikv_port'`
	if [ -z "${default_tikv_port}" ]; then
		echo "[func tikv_run] get default tikv_port from ${default_ports} failed" >&2
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

	local tikv_dir=`abs_path "${tikv_dir}"`

	local proc_cnt=`print_proc_cnt "${tikv_dir}/tikv.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	cp_when_diff "${conf_templ_dir}/tikv.toml" "${tikv_dir}/tikv.toml"

	local info="${tikv_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "tikv_port	${tikv_port}" >> "${info}"
	echo "advertise_host	${advertise_host}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${tikv_dir}/tikv-server\" \
		--addr \"${listen_host}:${tikv_port}\" \
		--advertise-addr \"${advertise_host}:${tikv_port}\" \
		--pd \"${pd_addr}\" \
		--data-dir \"${tikv_dir}/data\" \
		--log-level info \
		--config \"${tikv_dir}/tikv.toml\" \
		--log-file \"${tikv_dir}/tikv.log\" 2>> \"${tikv_dir}/tikv_stderr.log\" 1>&2 &" > "${tikv_dir}/run.sh"

	chmod +x "${tikv_dir}/run.sh"
	bash "${tikv_dir}/run.sh"

	local pid=`print_pid "${tikv_dir}/tikv.toml" "\-\-config"`
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f tikv_run

function tidb_run()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func tidb_run] usage: <func> tidb_dir conf_templ_dir pd_addr [advertise_host] [ports_delta] [cluster_id]" >&2
		return 1
	fi

	local tidb_dir="${1}"
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

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func tidb_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tidb_port=`get_value "${default_ports}" 'tidb_port'`
	if [ -z "${default_tidb_port}" ]; then
		echo "[func tidb_run] get default tidb_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tidb_status_port=`get_value "${default_ports}" 'tidb_status_port'`
	if [ -z "${default_tidb_status_port}" ]; then
		echo "[func tidb_run] get default tidb_status_port from ${default_ports} failed" >&2
		return 1
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")

	if [ -z "${advertise_host}" ]; then
		local advertise_host="`must_print_ip`"
	fi

	local tidb_port=$((${ports_delta} + ${default_tidb_port}))
	local status_port=$((${ports_delta} + ${default_tidb_status_port}))

	local tidb_dir=`abs_path "${tidb_dir}"`

	local listen_host=""
	if [ "${advertise_host}" != "127.0.0.1" ] || [ "${advertise_host}" != "localhost" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local proc_cnt=`print_proc_cnt "${tidb_dir}/tidb.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	local render_str="tidb_listen_host=${listen_host}"
	render_templ "${conf_templ_dir}/tidb.toml" "${tidb_dir}/tidb.toml" "${render_str}"

	local info="${tidb_dir}/proc.info"
	echo "advertise_host	${advertise_host}" > "${info}"
	echo "tidb_port	${tidb_port}" >> "${info}"
	echo "status_port	${status_port}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${tidb_dir}/tidb-server\" \
		-P \"${tidb_port}\" \
		--status=\"${status_port}\" \
		--advertise-address=\"${advertise_host}\" \
		--path=\"${pd_addr}\" \
		--config=\"${tidb_dir}/tidb.toml\" \
		--log-slow-query=\"${tidb_dir}/tidb_slow.log\" \
		--log-file=\"${tidb_dir}/tidb.log\" 2>> \"${tidb_dir}/tidb_stderr.log\" 1>&2 &" > "${tidb_dir}/run.sh"

	chmod +x "${tidb_dir}/run.sh"
	bash "${tidb_dir}/run.sh"

	local pid=`print_pid "${tidb_dir}/tidb.toml" "\-\-config"`
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f tidb_run

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
	local default_tiflash_raft_port=`get_value "${default_ports}" 'tiflash_raft_port'`
	if [ -z "${default_tiflash_raft_port}" ]; then
		echo "[func tiflash_run] get default tiflash_raft_port from ${default_ports} failed" >&2
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
	local raft_port=$((${ports_delta} + ${default_tiflash_raft_port}))

	local render_str="tiflash_dir=${tiflash_dir}"
	local render_str="${render_str}#tiflash_pd_addr=${pd_addr}"
	local render_str="${render_str}#tiflash_listen_host=${listen_host}"
	local render_str="${render_str}#tiflash_http_port=${http_port}"
	local render_str="${render_str}#tiflash_tcp_port=${tcp_port}"
	local render_str="${render_str}#tiflash_interserver_http_port=${interserver_http_port}"
	local render_str="${render_str}#tiflash_raft_port=${raft_port}"

	render_templ "${conf_templ_dir}/tiflash/config.xml" "${conf_file}" "${render_str}"
	cp_when_diff "${conf_templ_dir}/tiflash/users.xml" "${tiflash_dir}/conf/users.xml"

	if [ "${daemon_mode}" == "false" ]; then
		"${tiflash_dir}/tiflash" server --config-file "${conf_file}"
		return $?
	fi

	local info="${tiflash_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "interserver_http_port	${interserver_http_port}" >> "${info}"
	echo "raft_port	${raft_port}" >> "${info}"
	echo "http_port	${http_port}" >> "${info}"
	echo "tcp_port	${tcp_port}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${tiflash_dir}/tiflash\" server --config-file \"${conf_file}\" 1>/dev/null 2>&1 &" > "${tiflash_dir}/run.sh"
	chmod +x "${tiflash_dir}/run.sh"
	bash "${tiflash_dir}/run.sh"

	local pid=`print_pid "${conf_file}" "\-\-config"`
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f tiflash_run

function rngine_run()
{
	if [ -z "${3+x}" ] || [ -z "${1}" ] || [ -z "${2}" ]; then
		echo "[func rngine_run] usage: <func> rngine_dir conf_templ_dir pd_addr tiflash_addr [advertise_host] [ports_delta] [cluster_id]" >&2
		return 1
	fi

	local rngine_dir="${1}"
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

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func rngine_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_rngine_port=`get_value "${default_ports}" 'rngine_port'`
	if [ -z "${default_rngine_port}" ]; then
		echo "[func rngine_run] get default rngine_port from ${default_ports} failed" >&2
		return 1
	fi
	local default_tiflash_raft_port=`get_value "${default_ports}" 'tiflash_raft_port'`
	if [ -z "${default_tiflash_raft_port}" ]; then
		echo "[func rngine_run] get default tiflash_raft_port from ${default_ports} failed" >&2
		return 1
	fi

	if [ -z "${tiflash_addr}" ]; then
		tiflash_addr="`must_print_ip`:${default_tiflash_raft_port}"
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")

	if [ -z "${advertise_host}" ]; then
		local advertise_host="`must_print_ip`"
	fi

	local rngine_port=$((${ports_delta} + ${default_rngine_port}))

	rngine_dir=`abs_path "${rngine_dir}"`

	if [ "${advertise_host}" != "127.0.0.1" ] || [ "${advertise_host}" != "localhost" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local proc_cnt=`print_proc_cnt "${rngine_dir}/rngine.toml" "\-\-config"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	local render_str="tiflash_raft_addr=${tiflash_addr}"
	render_templ "${conf_templ_dir}/rngine.toml" "${rngine_dir}/rngine.toml" "${render_str}"

	local info="${rngine_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "rngine_port	${rngine_port}" >> "${info}"
	echo "advertise_host	${advertise_host}" >> "${info}"
	echo "pd_addr	${pd_addr}" >> "${info}"
	echo "tiflash_raft_addr	${tiflash_addr}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	echo "nohup \"${rngine_dir}/tikv-server-rngine\" \
		--addr \"${listen_host}:${rngine_port}\" \
		--advertise-addr \"${advertise_host}:${rngine_port}\" \
		--pd \"${pd_addr}\" \
		--data-dir \"${rngine_dir}/data\" \
		--config \"${rngine_dir}/rngine.toml\" \
		--log-level info \
		--log-file \"${rngine_dir}/rngine.log\" 2>> \"${rngine_dir}/rngine_stderr.log\" 1>&2 &" > "${rngine_dir}/run.sh"

	chmod +x "${rngine_dir}/run.sh"
	bash "${rngine_dir}/run.sh"

	local pid=`print_pid "${rngine_dir}/rngine.toml" "\-\-config"`
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f rngine_run

function spark_file_prepare()
{
	if [ -z "${4+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ]; then
		echo "[func spark_file_prepare] usage: <func> spark_mod_dir conf_templ_dir pd_addr tiflash_addr" >&2
		return 1
	fi

	local spark_mod_dir="${1}"
	local conf_templ_dir="${2}"
	local pd_addr="${3}"
	local tiflash_addr="${4}"
	local jmxremote_port="${5}"

	local default_ports="${conf_templ_dir}/default.ports"

	local default_pd_port=`get_value "${default_ports}" 'pd_port'`
	if [ -z "${default_pd_port}" ]; then
		echo "[func spark_master_run] get default pd_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_tiflash_tcp_port=`get_value "${default_ports}" 'tiflash_tcp_port'`
	if [ -z "${default_tiflash_tcp_port}" ]; then
		echo "[func spark_master_run] get default tiflash_tcp_port from ${default_ports} failed" >&2
		return 1
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")
	local tiflash_addr=$(cal_addr "${tiflash_addr}" `must_print_ip` "${default_tiflash_tcp_port}")

	local render_str="pd_addresses=${pd_addr}"
	local render_str="${render_str}#flash_addresses=${tiflash_addr}"
	local render_str="${render_str}#spark_local_dir=${spark_mod_dir}/spark_local_dir"
	local render_str="${render_str}#jmxremote_port=${jmxremote_port}"
	render_templ "${conf_templ_dir}/spark-defaults.conf" "${spark_mod_dir}/spark-defaults.conf" "${render_str}"

	if [ ! -d "${spark_mod_dir}/spark" ]; then
		local spark_file="spark-2.3.3-bin-hadoop2.7"
		local spark_file_name="${spark_file}.tgz"
		if [ ! -f "${spark_mod_dir}/${spark_file_name}" ]; then
			echo "[func spark_file_prepare] cannot find spark file"
			return 1
		fi
		tar -zxf "${spark_mod_dir}/${spark_file_name}" -C ${spark_mod_dir} 1>/dev/null
		rm -f "${spark_mod_dir}/${spark_file_name}"
		mv "${spark_mod_dir}/${spark_file}" "${spark_mod_dir}/spark"
	fi
	mkdir -p "${spark_mod_dir}/pids"
	if [ -f "${spark_mod_dir}/spark-defaults.conf" ]; then
		mv "${spark_mod_dir}/spark-defaults.conf" "${spark_mod_dir}/spark/conf"
	fi
	if [ -f "${spark_mod_dir}/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" ]; then
		mv "${spark_mod_dir}/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar" "${spark_mod_dir}/spark/jars/tiflashspark-0.1.0-SNAPSHOT-jar-with-dependencies.jar"
	fi
	if [ ! -f "${spark_mod_dir}/spark/conf/spark-env.sh" ]; then
		echo "SPARK_PID_DIR=\"${spark_mod_dir}/pids\"" > "${spark_mod_dir}/spark/conf/spark-env.sh"
	fi
}
export -f spark_file_prepare

function spark_master_run()
{
	if [ -z "${4+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ]; then
		echo "[func spark_master_run] usage: <func> spark_master_dir conf_templ_dir pd_addr tiflash_addr [ports_delta] [advertise_host]" >&2
		return 1
	fi

	local spark_master_dir="${1}"
	local conf_templ_dir="${2}"
	local pd_addr="${3}"
	local tiflash_addr="${4}"

	if [ -z "${5+x}" ]; then
		local ports_delta="0"
	else
		local ports_delta="${5}"
	fi

	if [ -z "${6+x}" ]; then
		local advertise_host="0"
	else
		local advertise_host="${6}"
	fi

	local default_ports="${conf_templ_dir}/default.ports"

	local default_spark_master_port=`get_value "${default_ports}" 'spark_master_port'`
	if [ -z "${default_spark_master_port}" ]; then
		echo "[func spark_master_run] get default spark_master_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_thriftserver_port=`get_value "${default_ports}" 'thriftserver_port'`
	if [ -z "${default_thriftserver_port}" ]; then
		echo "[func spark_master_run] get default thriftserver_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_spark_master_webui_port=`get_value "${default_ports}" 'spark_master_webui_port'`
	if [ -z "${default_spark_master_webui_port}" ]; then
		echo "[func spark_master_run] get default spark_master_webui_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_jmxremote_port=`get_value "${default_ports}" 'jmxremote_port'`
	if [ -z "${default_jmxremote_port}" ]; then
		echo "[func spark_master_run] get default jmxremote_port from ${default_ports} failed" >&2
		return 1
	fi

	local spark_master_webui_port=$((${ports_delta} + ${default_spark_master_webui_port}))
	local jmxremote_port=$((${ports_delta} + ${default_jmxremote_port}))

	local listen_host=""
	if [ "${advertise_host}" != "127.0.0.1" ] && [ "${advertise_host}" != "localhost" ] && [ "${listen_host}" != "" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local spark_master_dir=`abs_path "${spark_master_dir}"`

	local proc_cnt=`print_proc_cnt "${spark_master_dir}" "org.apache.spark.deploy.master.Master"`

	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	local spark_master_port=$((${ports_delta} + ${default_spark_master_port}))
	local thriftserver_port=$((${ports_delta} + ${default_thriftserver_port}))

	spark_file_prepare "${spark_master_dir}" "${conf_templ_dir}" "${pd_addr}" "${tiflash_addr}" "${jmxremote_port}"

	if [ ! -f "${spark_master_dir}/run_master.sh" ]; then
		echo "export SPARK_MASTER_HOST=${listen_host}" > "${spark_master_dir}/run_master_temp.sh"
		echo "export SPARK_MASTER_PORT=${spark_master_port}" >> "${spark_master_dir}/run_master_temp.sh"
		echo "${spark_master_dir}/spark/sbin/start-master.sh --webui-port ${spark_master_webui_port}" >> "${spark_master_dir}/run_master_temp.sh"
		echo "${spark_master_dir}/spark/sbin/start-thriftserver.sh --hiveconf hive.server2.thrift.port=${thriftserver_port}" >> "${spark_master_dir}/run_master_temp.sh"
		chmod +x "${spark_master_dir}/run_master_temp.sh"
		mv "${spark_master_dir}/run_master_temp.sh" "${spark_master_dir}/run_master.sh"
	fi
	mkdir -p "${spark_master_dir}/logs"
	bash "${spark_master_dir}/run_master.sh" 2>&1 1> "${spark_master_dir}/logs/spark_master.log"

	local info="${spark_master_dir}/proc.info"
	echo "thriftserver_port	${thriftserver_port}" > "${info}"

	if [ ! -f "${spark_master_dir}/extra_str_to_find_proc" ]; then
		echo "org.apache.spark.deploy.master.Master" > "${spark_master_dir}/extra_str_to_find_proc"
	fi

	local pid=`print_pid "${spark_master_dir}" "org.apache.spark.deploy.master.Master"`
	echo "${pid}"
}
export -f spark_master_run

function spark_worker_run()
{
	if [ -z "${5+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ]; then
		echo "[func spark_worker_run] usage: <func> spark_worker_dir conf_templ_dir pd_addr tiflash_addr spark_master_addr [ports_delta] [cores] [memory]" >&2
		return 1
	fi

	local spark_worker_dir="${1}"
	local conf_templ_dir="${2}"
	local pd_addr="${3}"
	local tiflash_addr="${4}"
	local spark_master_addr="${5}"

	if [ -z "${6+x}" ]; then
		local ports_delta="0"
	else
		local ports_delta="${6}"
	fi

	if [ -z "${7+x}" ]; then
		local worker_cores=""
	else
		local worker_cores="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local worker_memory=""
	else
		local worker_memory="${8}"
	fi

	local default_ports="${conf_templ_dir}/default.ports"

	local default_spark_master_port=`get_value "${default_ports}" 'spark_master_port'`
	if [ -z "${default_spark_master_port}" ]; then
		echo "[func spark_worker_run] get default spark_master_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_thriftserver_port=`get_value "${default_ports}" 'thriftserver_port'`
	if [ -z "${default_thriftserver_port}" ]; then
		echo "[func spark_worker_run] get default thriftserver_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_spark_worker_webui_port=`get_value "${default_ports}" 'spark_worker_webui_port'`
	if [ -z "${default_spark_worker_webui_port}" ]; then
		echo "[func spark_worker_run] get default spark_worker_webui_port from ${default_ports} failed" >&2
		return 1
	fi

	local default_jmxremote_port=`get_value "${default_ports}" 'jmxremote_port'`
	if [ -z "${default_jmxremote_port}" ]; then
		echo "[func spark_master_run] get default jmxremote_port from ${default_ports} failed" >&2
		return 1
	fi

	local spark_worker_webui_port=$((${ports_delta} + ${default_spark_worker_webui_port}))
    local jmxremote_port=$((${ports_delta} + ${default_jmxremote_port}))

	local spark_worker_dir=`abs_path "${spark_worker_dir}"`

	local proc_cnt=`print_proc_cnt "${spark_worker_dir}" "org.apache.spark.deploy.worker.Worker"`

	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	local spark_master_addr=$(cal_addr "${spark_master_addr}" `must_print_ip` "${default_spark_master_port}")

	spark_file_prepare "${spark_worker_dir}" "${conf_templ_dir}" "${pd_addr}" "${tiflash_addr}" "${jmxremote_port}"

	if [ ! -f "${spark_worker_dir}/run_worker.sh" ]; then
		local run_worker_cmd="${spark_worker_dir}/spark/sbin/start-slave.sh ${spark_master_addr} --webui-port ${spark_worker_webui_port}"
		if [ "${worker_cores}" != "" ]; then
			local run_worker_cmd="${run_worker_cmd} --cores ${worker_cores}"
		fi
		if [ "${worker_memory}" != "" ]; then
			local run_worker_cmd="${run_worker_cmd} --memory ${worker_memory}"
		fi
		echo "${run_worker_cmd}" > "${spark_worker_dir}/run_worker_temp.sh"
		chmod +x "${spark_worker_dir}/run_worker_temp.sh"
		mv "${spark_worker_dir}/run_worker_temp.sh" "${spark_worker_dir}/run_worker.sh"
	fi
	mkdir -p "${spark_worker_dir}/logs"
	bash "${spark_worker_dir}/run_worker.sh" 2>&1 1> "${spark_worker_dir}/logs/spark_worker.log"

	if [ ! -f "${spark_worker_dir}/extra_str_to_find_proc" ]; then
		echo "org.apache.spark.deploy.worker.Worker" > "${spark_worker_dir}/extra_str_to_find_proc"
	fi

	local pid=`print_pid "${spark_worker_dir}" "org.apache.spark.deploy.worker.Worker"`
	echo "${pid}"
}
export -f spark_worker_run
