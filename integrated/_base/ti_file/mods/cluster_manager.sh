#!/bin/bash

function cluster_manager_run()
{
	if [ -z "${1+x}" ] || [ -z "${1}" ]; then
		echo "[func cluster_manager_run] usage: <func> tiflash_dir [timeout=300]" >&2
		return 1
	fi

	local tiflash_dir="${1}"

	if [ -z "${2+x}" ]; then
		local timeout='300'
	else
		local timeout="${2}"
	fi

	local conf_file="${tiflash_dir}/conf/config.xml"
	if [ ! -f "${conf_file}" ]; then
		echo "   ${conf_file} not found" >&2
		return 1
	fi
	if [ ! -f "${tiflash_dir}/proc.info" ]; then
		echo "   ${tiflash_dir}/proc.info not found" >&2
		return 1
	fi
	local cluster_manager_binary="${tiflash_dir}/flash_cluster_manager"
	if [ ! -f "${cluster_manager_binary}" ]; then
		echo "   ${cluster_manager_binary} not found" >&2
		return 1
	fi

	if [ ! -f "${tiflash_dir}/proc.info" ]; then
		echo "    cannot find ${tiflash_dir}/proc.info" >&2
		return 1
	fi
	local listen_host=`get_value "${tiflash_dir}/proc.info" "listen_host"`

	local http_port=`get_value "${tiflash_dir}/proc.info" "http_port"`
	local tiflash_pid=`get_value "${tiflash_dir}/proc.info" "pid"`
	if [ -z "${listen_host}" ]; then
		echo "   cannot find tiflash listen_host from ${tiflash_dir}/proc.info" >&2
		return 1
	fi
	if [ -z "${http_port}" ]; then
		echo "   cannot find tiflash http_port from ${tiflash_dir}/proc.info" >&2
		return 1
	fi
	if [ -z "${tiflash_pid}" ]; then
		echo "   cannot find tiflash pid from ${tiflash_dir}/proc.info" >&2
		return 1
	fi

	local error_handle="$-"
	set +e
	local nc_installed=`print_cmd_installed "nc"`
	if [ "${nc_installed}" == "false" ]; then
		echo "    nc not installed" >&2
		return 1
	fi
	local http_port_ready='false'
	for ((i=0; i<${timeout}; i++)); do
		nc -z "${listen_host}" "${http_port}" 1>/dev/null 2>&1
		if [ "${?}" == '0' ]; then
			local http_port_ready='true'
			break
		else
			if [ $((${i} % 10)) = 0 ] && [ ${i} -ge 10 ]; then
				echo "   #${i} waiting for tiflash http port ready at ${listen_host}:${http_port}"
			fi
			sleep 1
		fi
	done
	restore_error_handle_flags "${error_handle}"
	if [ "${http_port_ready}" == 'false' ]; then
		echo "   tiflash http port ${listen_host}:${http_port} not ready" >&2
		return 1
	fi

	echo "nohup \"${cluster_manager_binary}\" \"${conf_file}\" \"${tiflash_pid}\" 1>/dev/null 2>&1 &" > "${tiflash_dir}/run_cluster_manager.sh"

	# TODO: make cluster_manager status visible in ti.sh interface
	chmod +x "${tiflash_dir}/run_cluster_manager.sh"
	bash "${tiflash_dir}/run_cluster_manager.sh"
	echo "   related cluster_manager is running"
}
export -f cluster_manager_run
