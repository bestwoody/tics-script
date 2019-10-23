#!/bin/bash

function spark_worker_run()
{
	if [ -z "${5+x}" ] || [ -z "${1}" ] || [ -z "${2}" ] || [ -z "${3}" ] || [ -z "${4}" ] || [ -z "${5}" ]; then
		echo "[func spark_worker_run] usage: <func> spark_worker_dir conf_templ_dir pd_addr tiflash_addr spark_master_addr [ports_delta] [advertise_host] [cores] [memory] [cluster_id]" >&2
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
		local advertise_host=""
	else
		local advertise_host="${7}"
	fi

	if [ -z "${8+x}" ]; then
		local worker_cores=""
	else
		local worker_cores="${8}"
	fi

	if [ -z "${9+x}" ]; then
		local worker_memory=""
	else
		local worker_memory="${9}"
	fi

	if [ -z "${10+x}" ]; then
		local cluster_id="<none>"
	else
		local cluster_id="${10}"
	fi

	local java_installed=`print_java_installed`
	if [ "${java_installed}" == "false" ]; then
		echo "java not installed" >&2
		return 1
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

	local default_jdwp_port=`get_value "${default_ports}" 'jdwp_port'`
	if [ -z "${default_jdwp_port}" ]; then
		echo "[func spark_master_run] get default jdwp_port from ${default_ports} failed" >&2
		return 1
	fi

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

	local listen_host=""
	if [ "${advertise_host}" != "127.0.0.1" ] && [ "${advertise_host}" != "localhost" ] && [ "${advertise_host}" != "" ]; then
		local listen_host="${advertise_host}"
	else
		local listen_host="`must_print_ip`"
	fi

	local spark_worker_webui_port=$((${ports_delta} + ${default_spark_worker_webui_port}))
	local jmxremote_port=$((${ports_delta} + ${default_jmxremote_port}))
	local jdwp_port=$((${ports_delta} + ${default_jdwp_port}))

	local spark_worker_dir=`abs_path "${spark_worker_dir}"`
	local str_for_finding_spark_worker="${spark_worker_dir}/spark/jars/"

	local proc_cnt=`print_proc_cnt "${str_for_finding_spark_worker}" "org.apache.spark.deploy.worker.Worker"`

	if [ "${proc_cnt}" != "0" ]; then
		echo "running(${proc_cnt}), skipped"
		return 0
	fi

	local pd_addr=$(cal_addr "${pd_addr}" `must_print_ip` "${default_pd_port}")
	local tiflash_addr=$(cal_addr "${tiflash_addr}" `must_print_ip` "${default_tiflash_tcp_port}")
	local spark_master_addr=$(cal_addr "${spark_master_addr}" `must_print_ip` "${default_spark_master_port}")

	spark_file_prepare "${spark_worker_dir}" "${conf_templ_dir}" "${pd_addr}" "${tiflash_addr}" "${jmxremote_port}" "${jdwp_port}"

	local info="${spark_worker_dir}/proc.info"
	echo "listen_host	${listen_host}" > "${info}"
	echo "spark_worker_webui_port	${spark_worker_webui_port}" >> "${info}"
	echo "cluster_id	${cluster_id}" >> "${info}"

	if [ ! -f "${spark_worker_dir}/run_worker.sh" ]; then
		local run_worker_cmd="${spark_worker_dir}/spark/sbin/start-slave.sh ${spark_master_addr} --host ${listen_host}"
		if [ "${worker_cores}" != "" ]; then
			local run_worker_cmd="${run_worker_cmd} --cores ${worker_cores}"
		fi
		if [ "${worker_memory}" != "" ]; then
			local run_worker_cmd="${run_worker_cmd} --memory ${worker_memory}"
		fi
		echo "export WEBUI_PORT=${spark_worker_webui_port}" >> "${spark_worker_dir}/run_worker_temp.sh"
		echo "${run_worker_cmd}" > "${spark_worker_dir}/run_worker_temp.sh"
		chmod +x "${spark_worker_dir}/run_worker_temp.sh"
		mv "${spark_worker_dir}/run_worker_temp.sh" "${spark_worker_dir}/run_worker.sh"
	fi
	mkdir -p "${spark_worker_dir}/logs"
	bash "${spark_worker_dir}/run_worker.sh" 2>&1 1>/dev/null

	if [ ! -f "${spark_worker_dir}/extra_str_to_find_proc" ]; then
		echo "org.apache.spark.deploy.worker.Worker" > "${spark_worker_dir}/extra_str_to_find_proc"
	fi

	local spark_worker_log_name=`ls -tr "${spark_worker_dir}/spark/logs" | { grep "org.apache.spark.deploy.worker.Worker" || test $? = 1; } | tail -n 1`
	if [ -z "${spark_worker_log_name}" ]; then
		echo "[func spark_worker_run] spark worker logs not found, failed" >&2
		return 1
	fi

	mkdir -p "${spark_worker_dir}/logs"
	if [ ! -f "${spark_worker_dir}/logs/spark_worker.log" ]; then
		ln "${spark_worker_dir}/spark/logs/${spark_worker_log_name}" "${spark_worker_dir}/logs/spark_worker.log"
	fi

	sleep 0.1
	local pid=`must_print_pid "${str_for_finding_spark_worker}" "org.apache.spark.deploy.worker.Worker"`
	if [ -z "${pid}" ]; then
		echo "[func spark_worker_run] pid not found, failed" >&2
		return 1
	fi
	echo "pid	${pid}" >> "${info}"
	echo "${pid}"
}
export -f spark_worker_run