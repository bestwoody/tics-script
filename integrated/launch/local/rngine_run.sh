#!/bin/bash

# Where to launch rngine
rngine_dir="${1}"

# Specify rngine listening ports
ports_delta="${2}"

# Where to find pd
pd_addr="${3}"

# Where to find tiflash
tiflash_addr="${4}"

# Tell others where to find me
advertise_host="${5}"

source "`cd $(dirname ${BASH_SOURCE[0]}) && pwd`/_env.sh"
auto_error_handle

if [ -z "${rngine_dir}" ]; then
	echo "[script rngine_run] usage: <script> rngine_dir [ports_delta=0] [pd_addr=''] [tiflash_addr=''] [advertise_host=auto]" >&2
	exit 1
fi

if [ -z "${ports_delta}" ]; then
	ports_delta="0"
fi

if [ -z "${pd_addr}" ]; then
	pd_addr=""
fi

if [ -z "${advertise_host}" ]; then
	advertise_host=""
fi

# Where is rngine config template files
conf_templ_dir="${integrated}/conf"
cache_dir="/tmp/ti/integrated/master/bins"

cp_bin_to_dir "rngine" "${rngine_dir}" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls" "${cache_dir}"

rngine_run "${rngine_dir}" "${conf_templ_dir}" "${pd_addr}" "${tiflash_addr}" "${advertise_host}" "${ports_delta}"
