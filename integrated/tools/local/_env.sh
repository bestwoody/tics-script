#!/bin/bash

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/../_env.sh"

function tiflash_run()
{
	if [ -z ${1+x} ]; then
		echo "[func tiflash_run] usage: <func> tiflash_dir conf_templ_dir [daemon_mode] [pd_addr] [port_delta] [listen_host]" >&2
		return 1
	fi

	local tiflash_dir="${1}"
	local conf_templ_dir="${2}"

	if [ -z ${3+x} ]; then
		local daemon_mode="false"
	else
		local daemon_mode="${3}"
	fi

	if [ -z ${4+x} ]; then
		local pd_addr=""
	else
		local pd_addr="${4}"
	fi

	if [ -z ${5+x} ]; then
		local ports_delta="0"
	else
		local ports_delta="${5}"
	fi

	if [ -z ${6+x} ]; then
		local listen_host="0.0.0.0"
	else
		local listen_host="${6}"
	fi

	mkdir -p "${tiflash_dir}"

	if [ ! -d "${tiflash_dir}" ]; then
		echo "[func tiflash_run] ${tiflash_dir} is not a dir" >&2
		return 1
	fi

	tiflash_dir=`abs_path "${tiflash_dir}"`
	local conf_file="${tiflash_dir}/conf/config.xml"

	local proc_cnt=`print_proc_cnt "${conf_file}"`
	if [ "${proc_cnt}" != "0" ]; then
		echo "[func tiflash_run] ${tiflash_dir} maybe running(${proc_cnt} processes), skipping" >&2
		return 1
	fi

	local http_port=$((${ports_delta} + 8123))
	local tcp_port=$((${ports_delta} + 9000))
	local interserver_http_port=$((${ports_delta} + 9009))
	local raft_port=$((${ports_delta} + 3930))

	local render_str="tiflash_dir=${tiflash_dir}"
	render_str="${render_str};tiflash_pd_addr=${pd_addr}"
	render_str="${render_str};tiflash_listen_host=${listen_host}"
	render_str="${render_str};tiflash_http_port=${http_port}"
	render_str="${render_str};tiflash_tcp_port=${tcp_port}"
	render_str="${render_str};tiflash_interserver_http_port=${interserver_http_port}"
	render_str="${render_str};tiflash_raft_port=${raft_port}"

	render_templ "${conf_templ_dir}/config.xml" "${conf_file}" "${render_str}"
	cp_when_diff "${conf_templ_dir}/users.xml" "${tiflash_dir}/conf/users.xml"

	if [ "${daemon_mode}" != "false" ]; then
		"${tiflash_dir}/tiflash" server --config-file "${conf_file}" 2>/dev/null &
		print_pid "${conf_file}"
	else
		"${tiflash_dir}/tiflash" server --config-file "${conf_file}"
	fi
}
export -f tiflash_run

function tiflash_stop()
{
	if [ -z ${1+x} ]; then
		echo "[func tiflash_stop] usage: <func> tiflash_dir" >&2
		return 1
	fi

	local tiflash_dir="${1}"
	tiflash_dir=`abs_path "${tiflash_dir}"`
	local conf_file="${tiflash_dir}/conf/config.xml"

	local proc_cnt=`print_proc_cnt "${conf_file}"`
	if [ "${proc_cnt}" == "0" ]; then
		echo "[func tiflash_stop] ${tiflash_dir} is not running, skipping" >&2
		return 0
	fi

	if [ "${proc_cnt}" != "1" ]; then
		echo "[func tiflash_stop] ${tiflash_dir} maybe running(${proc_cnt} processes), skipping" >&2
		return 1
	fi

	stop_proc "${conf_file}"
}
export -f tiflash_stop
