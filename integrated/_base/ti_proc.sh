#!/bin/bash

# TODO: Check if ip changed when run a module

function tiflash_run()
{
	if [ -z "${2+x}" ]; then
		echo "[func tiflash_run] usage: <func> tiflash_dir conf_templ_dir [daemon_mode] [pd_addr] [ports_delta] [listen_host]" >&2
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
		echo "running(${proc_cnt}), skipped" >&2
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
	echo "listen_host ${listen_host}" > "${info}"
	echo "interserver_http_port ${interserver_http_port}" >> "${info}"
	echo "raft_port ${raft_port}" >> "${info}"
	echo "http_port ${http_port}" >> "${info}"
	echo "tcp_port ${tcp_port}" >> "${info}"
	echo "pd_addr ${pd_addr}" >> "${info}"

	echo "nohup \"${tiflash_dir}/tiflash\" server --config-file \"${conf_file}\" 1>/dev/null 2>&1 &" > "${tiflash_dir}/run.sh"
	chmod +x "${tiflash_dir}/run.sh"
	bash "${tiflash_dir}/run.sh"

	local pid=`print_pid "${conf_file}" "\-\-config"`
	echo "pid ${pid}" >> "${info}"
	echo "${pid}"
}
export -f tiflash_run

function _ti_stop()
{
	if [ -z "${2+x}" ]; then
		echo "[func _ti_stop] usage: <func> module_dir conf_rel_path [fast=false]" >&2
		return 1
	fi

	local ti_dir="${1}"
	local conf_rel_path="${2}"

	if [ -z "${3+x}" ]; then
		local fast=""
	else
		local fast="${3}"
	fi

	local ti_dir=`abs_path "${ti_dir}"`
	local conf_file="${ti_dir}/${conf_rel_path}"

	local proc_cnt=`print_proc_cnt "${conf_file}" "\-\-config"`
	if [ "${proc_cnt}" == "0" ]; then
		echo "[func ti_stop] ${ti_dir} is not running, skipping" >&2
		return 0
	fi

	if [ "${proc_cnt}" != "1" ]; then
		echo "[func ti_stop] ${ti_dir} has ${proc_cnt} instances, skipping" >&2
		return 1
	fi

	stop_proc "${conf_file}" "\-\-config" "${fast}"
}
export -f _ti_stop

function tiflash_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func tiflash_stop] usage: <func> tiflash_dir [fast_mode=false]" >&2
		return 1
	fi
	local fast="false"
	if [ ! -z "${2+x}" ]; then
		local fast="${2}"
	fi
	_ti_stop "${1}" "conf/config.xml" "${fast}"
}
export -f tiflash_stop

function pd_run()
{
	if [ -z "${2+x}" ]; then
		echo "[func pd_run] usage: <func> pd_dir conf_templ_dir [name_ports_delta] [advertise_host] [pd_name] [initial_cluster]" >&2
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
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	cp_when_diff "${conf_templ_dir}/pd.toml" "${pd_dir}/pd.toml"

	local info="${pd_dir}/proc.info"
	echo "pd_name ${pd_name}" > "${info}"
	echo "advertise_host ${advertise_host}" >> "${info}"
	echo "pd_port ${pd_port}" >> "${info}"
	echo "peer_port ${peer_port}" >> "${info}"
	echo "initial_cluster ${initial_cluster}" >> "${info}"

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
	echo "pid ${pid}" >> "${info}"
	echo "${pid}"
}
export -f pd_run

function pd_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func pd_stop] usage: <func> pd_dir [fast_mode=false]" >&2
		return 1
	fi
	local fast="false"
	if [ ! -z "${2+x}" ]; then
		local fast="${2}"
	fi
	_ti_stop "${1}" "pd.toml" "${fast}"
}
export -f pd_stop

function tikv_run()
{
	if [ -z "${3+x}" ]; then
		echo "[func tikv_run] usage: <func> tikv_dir conf_templ_dir pd_addr [advertise_host] [ports_delta]" >&2
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
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	cp_when_diff "${conf_templ_dir}/tikv.toml" "${tikv_dir}/tikv.toml"

	local info="${tikv_dir}/proc.info"
	echo "listen_host ${listen_host}" > "${info}"
	echo "tikv_port ${tikv_port}" >> "${info}"
	echo "advertise_host ${advertise_host}" >> "${info}"
	echo "pd_addr ${pd_addr}" >> "${info}"

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
	echo "pid ${pid}" >> "${info}"
	echo "${pid}"
}
export -f tikv_run

function tikv_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func tikv_stop] usage: <func> tikv_dir [fast_mode=false]" >&2
		return 1
	fi
	local fast="false"
	if [ ! -z "${2+x}" ]; then
		local fast="${2}"
	fi
	_ti_stop "${1}" "tikv.toml" "${fast}"
}
export -f tikv_stop

function tidb_run()
{
	if [ -z "${3+x}" ]; then
		echo "[func tidb_run] usage: <func> tidb_dir conf_templ_dir pd_addr [advertise_host] [ports_delta]" >&2
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
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	local render_str="tidb_listen_host=${listen_host}"
	render_templ "${conf_templ_dir}/tidb.toml" "${tidb_dir}/tidb.toml" "${render_str}"

	local info="${tidb_dir}/proc.info"
	echo "advertise_host ${advertise_host}" > "${info}"
	echo "tidb_port ${tidb_port}" >> "${info}"
	echo "status_port ${status_port}" >> "${info}"
	echo "pd_addr ${pd_addr}" >> "${info}"

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
	echo "pid ${pid}" >> "${info}"
	echo "${pid}"
}
export -f tidb_run

function tidb_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func tidb_stop] usage: <func> tidb_dir [fast_mode=false]" >&2
		return 1
	fi
	local fast="false"
	if [ ! -z "${2+x}" ]; then
		local fast="${2}"
	fi
	_ti_stop "${1}" "tidb.toml" "true" "${fast}"
}
export -f tidb_stop

function rngine_run()
{
	if [ -z "${3+x}" ]; then
		echo "[func rngine_run] usage: <func> rngine_dir conf_templ_dir pd_addr tiflash_addr [advertise_host] [ports_delta]" >&2
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
		echo "running(${proc_cnt}), skipped" >&2
		return 0
	fi

	local render_str="tiflash_raft_addr=${tiflash_addr}"
	render_templ "${conf_templ_dir}/rngine.toml" "${rngine_dir}/rngine.toml" "${render_str}"

	local info="${rngine_dir}/proc.info"
	echo "listen_host ${listen_host}" > "${info}"
	echo "rngine_port ${rngine_port}" >> "${info}"
	echo "advertise_host ${advertise_host}" >> "${info}"
	echo "pd_addr ${pd_addr}" >> "${info}"
	echo "tiflash_raft_addr ${tiflash_addr}" >> "${info}"

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
	echo "pid ${pid}" >> "${info}"
	echo "${pid}"
}
export -f rngine_run

function rngine_stop()
{
	if [ -z "${1+x}" ]; then
		echo "[func rngine_stop] usage: <func> rngine_dir [fast_mode=false]" >&2
		return 1
	fi
	local fast="false"
	if [ ! -z "${2+x}" ]; then
		local fast="${2}"
	fi
	_ti_stop "${1}" "rngine.toml" "${fast}"
}
export -f rngine_stop

function wait_for_tidb()
{
	if [ -z "${1+x}" ]; then
		echo "[func wait_for_tidb] usage: <func> tidb_dir [timeout=180s]" >&2
		return 1
	fi

	local tidb_dir="${1}"
	local timeout=600
	if [ ! -z "${2+x}" ]; then
		timeout="${2}"
	fi

	local tidb_info="${tidb_dir}/proc.info"
	local host=`cat "${tidb_info}" | grep 'advertise_host' | awk '{print $2}'`
	local port=`cat "${tidb_info}" | grep 'tidb_port' | awk '{print $2}'`

	local error_handle="$-"
	set +e

	mysql --help 1>/dev/null 2>&1
	if [ "$?" != "0" ]; then
		echo "[func wait_for_tidb] run mysql client failed" >&2
		return 1
	fi

	for ((i=0; i<${timeout}; i++)); do
		mysql -h "${host}" -P "${port}" -u root -e 'show databases' 1>/dev/null 2>&1
		if [ "$?" == "0" ]; then
			break
		else
			if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
				echo "#${i} waiting for tidb at '${tidb_dir}'"
			fi
			# TODO: If PD/TiKV/TiDB down, do fast-failure
			sleep 1
		fi
	done

	restore_error_handle_flags "${error_handle}"
}
export -f wait_for_tidb

function get_tiflash_addr_from_dir()
{
	if [ -z "${1+x}" ]; then
		echo "[func get_tiflash_addr_from_dir] usage: <func> tiflash_dir" >&2
		return 1
	fi
	local dir="${1}"
	local host=`grep 'listen_host' "${dir}/proc.info" | awk '{print $2}'`
	local port=`grep 'raft_port' "${dir}/proc.info" | awk '{print $2}'`
	echo "${host}:${port}"
}
export -f get_tiflash_addr_from_dir

function ls_tiflash_proc()
{
	local processes=`ps -ef | grep 'tiflash' | grep "\-\-config\-file" | \
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
	local processes=`ps -ef | grep 'tikv-server' | grep -v tikv-server-rngine | grep "\-\-config" | \
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
	local processes=`ps -ef | grep 'tidb-server' | grep "\-\-config" | \
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
	local processes=`ps -ef | grep 'tikv-server-rngine' | grep "\-\-config" | \
		grep -v grep | awk -F '--config' '{print $2}' | awk '{print $1}'`
	if [ ! -z "${processes}" ]; then
		echo "${processes}" | while read conf; do
			_print_file_dir "${conf}"
		done
	fi
}
export -f ls_rngine_proc
