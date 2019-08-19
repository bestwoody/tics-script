#!/bin/bash

# Where to launch tiflash
tiflash_dir="${1}"
# Specify tiflash listening ports
ports_delta="${2}"
# Where to find pd
pd_addr="${3}"
# Run as daemon or not
daemon_mode="${4}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${tiflash_dir}" ]; then
	echo "[script tiflash_run] usage: <script> tiflash_dir [ports_delta=0] [pd_addr=''] [daemon=true]" >&2
	exit 1
fi

# Where is tiflash config template files
conf_templ_dir="${integrated}/conf"
cache_dir="/tmp/ti/integrated/master/bins"

cp_bin_to_dir "tiflash" "${tiflash_dir}" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls" "${cache_dir}"
tiflash_run "${tiflash_dir}" "${conf_templ_dir}" "${daemon_mode}" "${pd_addr}" "${ports_delta}"
