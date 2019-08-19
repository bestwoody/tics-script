#!/bin/bash

# Where to launch tikv
tikv_dir="${1}"
# Specify tikv listening ports
ports_delta="${2}"
# Where to find pd
pd_addr="${3}"
# Tell others where to find me
advertise_host="${4}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${tikv_dir}" ]; then
	echo "[script tikv_run] usage: <script> tikv_dir [ports_delta=0] [pd_addr=''] [advertise_host=auto]" >&2
	exit 1
fi

# Where is tikv config template files
conf_templ_dir="${integrated}/conf"
cache_dir="/tmp/ti/integrated/master/bins"

cp_bin_to_dir "tikv" "${tikv_dir}" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls" "${cache_dir}"
tikv_run "${tikv_dir}" "${conf_templ_dir}" "${pd_addr}" "${advertise_host}" "${ports_delta}"
