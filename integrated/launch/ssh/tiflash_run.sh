#!/bin/bash

# The host to launch tiflash
host="${1}"
# Where to launch tiflash
tiflash_dir="${2}"
# Specify tiflash listening ports
ports_delta="${3}"
# Where to find pd
pd_addr="${4}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${host}" ] || [ -z "${tiflash_dir}" ]; then
	echo "[script tiflash_run] usage: <script> host tiflash_dir [ports_delta=0] [pd_addr='']" >&2
	exit 1
fi

conf_templ_dir="${integrated}/conf"
cache_dir="/tmp/ti"
remote_env="${cache_dir}/worker/integrated"

ssh_ping "${host}"
cp_env_to_host "${integrated}" "${cache_dir}/master/integrated" "${host}" "${cache_dir}/worker"

bin_name=`ssh_prepare_run "${host}" 'tiflash' "${tiflash_dir}" "${conf_templ_dir}" "${cache_dir}" "${remote_env}"`
call_remote_func "${host}" "${remote_env}" tiflash_run "${tiflash_dir}" "${remote_env}/conf" "true" "${pd_addr}" "${ports_delta}" "${host}"
