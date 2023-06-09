#!/bin/bash

# Where to launch pd
pd_dir="${1}"

# Specify pd name and listening ports
name_ports_delta="${2}"

# Tell others where to find me
advertise_host="${3}"

# Cluster pd urls
initial_cluster="${4}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${pd_dir}" ]; then
	echo "[script pd_run] usage: <script> pd_dir [name_ports_delta=0] [advertise_host=auto] [initial_cluster=me]" >&2
	exit 1
fi

# Where is pd config template files
conf_templ_dir="${integrated}/conf"
bin_cache_dir="/tmp/ti/master/bins"

cp_bin_to_dir "pd" "${pd_dir}" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls" "${bin_cache_dir}"

pd_run "${pd_dir}" "${conf_templ_dir}" "${name_ports_delta}" "${advertise_host}" "" "${initial_cluster}"
