#!/bin/bash

# Where to launch tikv
tikv_dir="${1}"

# Specify tikv listening ports
ports_delta="${2}"

# Where to find pd
pd_addr="${3}"

# Tell others where to find me
advertise_host="${4}"

here="`cd $(dirname ${BASH_SOURCE[0]}) && pwd`"
source "${here}/_env.sh"
auto_error_handle

if [ -z "${tikv_dir}" ]; then
	echo "[script tikv_run] usage: <script> tikv_dir [ports_delta=0] [pd_addr=''] [advertise_host=auto]" >&2
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

# Where is tikv config template files
conf_templ_dir="${here}/conf_templ"

cp_bin_to_dir "tikv" "${tikv_dir}" "${conf_templ_dir}/bin.paths" "${conf_templ_dir}/bin.urls"

tikv_run "${tikv_dir}" "${conf_templ_dir}" "${pd_addr}" "${advertise_host}" "${ports_delta}"
